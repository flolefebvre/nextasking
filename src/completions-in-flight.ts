import { ErrorCode } from "bullmq";

import type { EpochMilliseconds } from "./durations.js";
import type { RunDurationFields } from "./failure-recording.js";
import type { Logger } from "./logger.js";

export const LOCK_ERROR_CODES = new Set<unknown>([ErrorCode.JobLockNotExist, ErrorCode.JobLockMismatch]);

export type JobOnServer = {
  state: string;
  processedOn: EpochMilliseconds | undefined;
};

export type CompletionInFlight = {
  processedOn: EpochMilliseconds | undefined;
  runLogger: Logger;
  durationFields: RunDurationFields;
  removeSavedSteps: () => Promise<void>;
};

export type CompletionsInFlight = {
  trackCompletion: (jobId: string, completion: CompletionInFlight) => void;
  recordRunCompleted: (jobId: string) => void;
  routeWorkerError: (error: unknown) => void;
};

type CompletionOutcome = "completed" | "lock lost" | "unconfirmed";

type TrackedCompletion = CompletionInFlight & {
  outcome?: Promise<CompletionOutcome>;
  lockErrorCount: number;
};

const MISSING_LOCK_MESSAGE = /^Missing lock for job (.+)\. \w+$/;

const LOCK_MISMATCH_MESSAGE = /^Lock mismatch for job (.+)\. Cmd \w+ from \w+$/;

const LOCK_ERRORS_PER_LOST_COMPLETION_REPLY = 2;

function lockedJobIdOf(error: unknown): string | undefined {
  if (!(error instanceof Error) || !LOCK_ERROR_CODES.has((error as { code?: unknown }).code)) {
    return undefined;
  }
  const match = MISSING_LOCK_MESSAGE.exec(error.message) ?? LOCK_MISMATCH_MESSAGE.exec(error.message);
  return match?.[1];
}

function logRunCompleted(completion: CompletionInFlight): void {
  completion.removeSavedSteps().catch(() => undefined);
  completion.runLogger.info(completion.durationFields, "run completed");
}

function warnCompletionUnconfirmed(completion: CompletionInFlight, error: unknown): void {
  completion.runLogger.warn({ err: error }, "run completion unconfirmed");
}

export function createCompletionsInFlight({
  readJobOnServer,
  logWorkerError,
}: {
  readJobOnServer: (jobId: string) => Promise<JobOnServer | undefined>;
  logWorkerError: (error: unknown) => void;
}): CompletionsInFlight {
  const completionsByJobId = new Map<string, TrackedCompletion>();

  const confirmedOutcomeOf = async (
    jobId: string,
    completion: TrackedCompletion,
    firstLockError: unknown,
  ): Promise<CompletionOutcome> => {
    let jobOnServer: JobOnServer | undefined;
    try {
      jobOnServer = await readJobOnServer(jobId);
    } catch (readError) {
      warnCompletionUnconfirmed(completion, readError);
      return "unconfirmed";
    }
    if (jobOnServer === undefined) {
      warnCompletionUnconfirmed(completion, firstLockError);
      return "unconfirmed";
    }
    if (
      jobOnServer.state === "completed" &&
      completion.processedOn !== undefined &&
      jobOnServer.processedOn === completion.processedOn
    ) {
      logRunCompleted(completion);
      return "completed";
    }
    return "lock lost";
  };

  return {
    trackCompletion: (jobId, completion) => {
      completionsByJobId.set(jobId, { ...completion, lockErrorCount: 0 });
    },
    recordRunCompleted: (jobId) => {
      const completion = completionsByJobId.get(jobId);
      if (completion === undefined) {
        return;
      }
      completionsByJobId.delete(jobId);
      logRunCompleted(completion);
    },
    routeWorkerError: (error) => {
      const lockedJobId = lockedJobIdOf(error);
      const completion = lockedJobId === undefined ? undefined : completionsByJobId.get(lockedJobId);
      if (lockedJobId === undefined || completion === undefined) {
        logWorkerError(error);
        return;
      }
      completion.outcome ??= confirmedOutcomeOf(lockedJobId, completion, error);
      completion.lockErrorCount += 1;
      if (completion.lockErrorCount === LOCK_ERRORS_PER_LOST_COMPLETION_REPLY) {
        completionsByJobId.delete(lockedJobId);
      }
      void completion.outcome.then((outcome) => {
        if (outcome === "lock lost") {
          logWorkerError(error);
        }
      });
    },
  };
}
