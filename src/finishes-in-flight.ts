import { ErrorCode } from "bullmq";

import type { EpochMilliseconds } from "./durations.js";
import type { RunDurationFields } from "./failure-recording.js";
import type { Logger } from "./logger.js";

export const LOCK_ERROR_CODES = new Set<unknown>([ErrorCode.JobLockNotExist, ErrorCode.JobLockMismatch]);

export type JobOnServer = {
  state: string;
  processedOn: EpochMilliseconds | undefined;
  attemptsMade: number;
};

export type FinishReportingWorkerEvent = "completed" | "failed";

export type FinishInFlight = {
  token: string | undefined;
  reportedBy: FinishReportingWorkerEvent;
  isConfirmedBy: (jobOnServer: JobOnServer) => boolean;
  onConfirmed: (jobOnServer?: JobOnServer) => void;
  onUnconfirmed: (error: unknown) => void;
};

export type FinishesInFlight = {
  trackFinish: (jobId: string, finish: FinishInFlight) => void;
  recordFinishReported: (jobId: string, workerEvent: FinishReportingWorkerEvent) => void;
  routeWorkerError: (error: unknown) => void;
};

type FinishOutcome = "confirmed" | "lock lost" | "unconfirmed";

type TrackedFinish = FinishInFlight & {
  outcome?: Promise<FinishOutcome>;
  lockErrorCount: number;
};

const MISSING_LOCK_MESSAGE = /^Missing lock for job (.+)\. \w+$/;

const LOCK_MISMATCH_MESSAGE = /^Lock mismatch for job (.+)\. Cmd \w+ from \w+$/;

const LOCK_ERRORS_PER_LOST_REPLY_BY_REPORTING_WORKER_EVENT: Record<FinishReportingWorkerEvent, number> = {
  completed: 2,
  failed: 1,
};

function lockedJobIdOf(error: unknown): string | undefined {
  if (!(error instanceof Error) || !LOCK_ERROR_CODES.has((error as { code?: unknown }).code)) {
    return undefined;
  }
  const match = MISSING_LOCK_MESSAGE.exec(error.message) ?? LOCK_MISMATCH_MESSAGE.exec(error.message);
  return match?.[1];
}

function isCompletedByActivation(processedOn: EpochMilliseconds | undefined) {
  return (jobOnServer: JobOnServer) =>
    jobOnServer.state === "completed" && processedOn !== undefined && jobOnServer.processedOn === processedOn;
}

export function runCompletionInFlight({
  processedOn,
  token,
  runLogger,
  durationFields,
  removeSavedSteps,
}: {
  processedOn: EpochMilliseconds | undefined;
  token: string | undefined;
  runLogger: Logger;
  durationFields: RunDurationFields;
  removeSavedSteps: () => Promise<void>;
}): FinishInFlight {
  return {
    token,
    reportedBy: "completed",
    isConfirmedBy: isCompletedByActivation(processedOn),
    onConfirmed: () => {
      removeSavedSteps().catch(() => undefined);
      runLogger.info(durationFields, "run completed");
    },
    onUnconfirmed: (error) => {
      runLogger.warn({ err: error }, "run completion unconfirmed");
    },
  };
}

function silentCompletionInFlight(
  processedOn: EpochMilliseconds | undefined,
  token: string | undefined,
  warnCompletionUnconfirmed: (error: unknown) => void,
): FinishInFlight {
  return {
    token,
    reportedBy: "completed",
    isConfirmedBy: isCompletedByActivation(processedOn),
    onConfirmed: () => undefined,
    onUnconfirmed: warnCompletionUnconfirmed,
  };
}

export function fanOutCompletionInFlight({
  processedOn,
  token,
  logger,
  eventName,
  eventId,
}: {
  processedOn: EpochMilliseconds | undefined;
  token: string | undefined;
  logger: Logger;
  eventName: string;
  eventId: string;
}): FinishInFlight {
  return silentCompletionInFlight(processedOn, token, (error) => {
    logger.warn({ eventName, eventId, err: error }, "fan-out completion unconfirmed");
  });
}

export function firingCompletionInFlight({
  processedOn,
  token,
  logger,
  taskName,
  scheduledAt,
}: {
  processedOn: EpochMilliseconds | undefined;
  token: string | undefined;
  logger: Logger;
  taskName: string;
  scheduledAt: Date;
}): FinishInFlight {
  return silentCompletionInFlight(processedOn, token, (error) => {
    logger.warn(
      { task: taskName, scheduledAt: scheduledAt.toISOString(), err: error },
      "firing completion unconfirmed",
    );
  });
}

const STATES_AFTER_A_STORED_FAILURE = new Set(["failed", "delayed", "waiting"]);

export function runFailureInFlight({
  processedOn,
  attemptsMade,
  token,
  runLogger,
  sendFailureNotice,
}: {
  processedOn: EpochMilliseconds | undefined;
  attemptsMade: number;
  token: string | undefined;
  runLogger: Logger;
  sendFailureNotice: () => Promise<void>;
}): FinishInFlight {
  return {
    token,
    reportedBy: "failed",
    isConfirmedBy: (jobOnServer) =>
      STATES_AFTER_A_STORED_FAILURE.has(jobOnServer.state) &&
      processedOn !== undefined &&
      jobOnServer.processedOn === processedOn &&
      jobOnServer.attemptsMade === attemptsMade + 1,
    onConfirmed: (jobOnServer) => {
      if (jobOnServer?.state === "failed") {
        sendFailureNotice().catch(() => undefined);
      }
    },
    onUnconfirmed: (error) => {
      runLogger.warn({ err: error }, "run failure unconfirmed");
    },
  };
}

export function createFinishesInFlight({
  readJobOnServer,
  findActiveJobIdLockedBy,
  moveActiveJobToWait,
  logger,
  logWorkerError,
}: {
  readJobOnServer: (jobId: string) => Promise<JobOnServer | undefined>;
  findActiveJobIdLockedBy: (token: string) => Promise<string | undefined>;
  moveActiveJobToWait: (jobId: string, token: string) => Promise<void>;
  logger: Logger;
  logWorkerError: (error: unknown) => void;
}): FinishesInFlight {
  const finishesByJobId = new Map<string, TrackedFinish>();

  const handBackNextJobLockedBy = async (token: string) => {
    let nextJobId: string | undefined;
    try {
      nextJobId = await findActiveJobIdLockedBy(token);
      if (nextJobId !== undefined) {
        await moveActiveJobToWait(nextJobId, token);
      }
    } catch (handBackError) {
      logger.warn(
        nextJobId === undefined ? { err: handBackError } : { jobId: nextJobId, err: handBackError },
        "next job hand-back failed",
      );
    }
  };

  const confirmedOutcomeOf = async (
    jobId: string,
    finish: TrackedFinish,
    firstLockError: unknown,
  ): Promise<FinishOutcome> => {
    let jobOnServer: JobOnServer | undefined;
    try {
      jobOnServer = await readJobOnServer(jobId);
    } catch (readError) {
      finish.onUnconfirmed(readError);
      return "unconfirmed";
    }
    if (jobOnServer === undefined) {
      finish.onUnconfirmed(firstLockError);
      return "unconfirmed";
    }
    if (finish.isConfirmedBy(jobOnServer)) {
      finish.onConfirmed(jobOnServer);
      if (finish.token !== undefined) {
        void handBackNextJobLockedBy(finish.token);
      }
      return "confirmed";
    }
    return "lock lost";
  };

  return {
    trackFinish: (jobId, finish) => {
      finishesByJobId.set(jobId, { ...finish, lockErrorCount: 0 });
    },
    recordFinishReported: (jobId, workerEvent) => {
      const finish = finishesByJobId.get(jobId);
      if (finish === undefined) {
        return;
      }
      finishesByJobId.delete(jobId);
      if (finish.reportedBy === workerEvent) {
        finish.onConfirmed();
      }
    },
    routeWorkerError: (error) => {
      const lockedJobId = lockedJobIdOf(error);
      const finish = lockedJobId === undefined ? undefined : finishesByJobId.get(lockedJobId);
      if (lockedJobId === undefined || finish === undefined) {
        logWorkerError(error);
        return;
      }
      finish.outcome ??= confirmedOutcomeOf(lockedJobId, finish, error);
      finish.lockErrorCount += 1;
      if (finish.lockErrorCount === LOCK_ERRORS_PER_LOST_REPLY_BY_REPORTING_WORKER_EVENT[finish.reportedBy]) {
        finishesByJobId.delete(lockedJobId);
      }
      void finish.outcome.then((outcome) => {
        if (outcome === "lock lost") {
          logWorkerError(error);
        }
      });
    },
  };
}
