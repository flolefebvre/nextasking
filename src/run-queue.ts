import { Queue } from "bullmq";
import type { Job, JobsOptions } from "bullmq";
import type { Redis } from "ioredis";

import { closeRedisClient, listenerLoggingAnsweredErrors } from "./connection.js";
import type { RedisClientRole, RedisConnections } from "./connection.js";
import type { ConcurrencyKey } from "./definitions.js";
import type { Duration, DurationInMilliseconds, EpochMilliseconds } from "./durations.js";
import type { ReplayRefusal } from "./errors.js";
import { saveFailedJobDataIfUnchanged } from "./failed-job-data.js";
import { createRandomId, deriveJobId, deriveListenerRunId } from "./ids.js";
import type { EventId, JobId, RunId } from "./ids.js";
import { jobKeyOf } from "./job-lock.js";
import { COMPLETED_JOB_RETENTION, FAILED_JOB_RETENTION } from "./job-retention.js";
import type { Logger } from "./logger.js";
import { keepSavedStepsForFailedRunRetention } from "./redis-step-store.js";
import { scheduleChangesBetween } from "./schedules.js";
import type { DeclaredSchedule, ScheduleChange } from "./schedules.js";

export const RUN_QUEUE_NAME = "runs";

export const FIRING_JOB_NAME = "nextasking/firing";

const FAN_OUT_RETRIES = 5;

const BULLMQ_STALL_COUNT_FIELD = "stc";

const FAN_OUT_FIRST_BACKOFF_IN_MILLISECONDS = 30_000;

const RUN_BACKOFF = { type: "custom" };

type WriteToken = string;

export type SourceEvent = {
  eventName: string;
  eventId: EventId;
};

export type ErrorRecord = {
  attempt: number;
  name: string;
  message: string;
  stack: string;
  failedAt: string;
};

export type FailureReason = "retries exhausted" | "non-retriable" | "stalled" | "unknown task";

export type StoredDelayedRunState =
  | { state: "waitingForConcurrencySlot" }
  | { state: "suspended"; resumesAt: IsoTimestamp }
  | { state: "backingOff"; nextAttemptAt: IsoTimestamp }
  | { state: "pending" };

export type StoredRun = {
  runId: RunId;
  payload: unknown;
  sourceEvent?: SourceEvent;
  scheduledAt?: IsoTimestamp;
  concurrencyKey?: ConcurrencyKey;
  firstFoundUnknownAt?: IsoTimestamp;
  errors: ErrorRecord[];
  reason?: FailureReason;
  failureCount?: number;
  notifiedFailureCount?: number;
  delayedRunState?: StoredDelayedRunState;
  writeToken: WriteToken;
};

export function storedRunAfterFinalFailure(storedRun: StoredRun, errorRecord: ErrorRecord, reason: FailureReason) {
  return {
    ...storedRun,
    errors: [...storedRun.errors, errorRecord],
    reason,
    failureCount: (storedRun.failureCount ?? 0) + 1,
  } satisfies StoredRun;
}

function storedRunAfterReplay(storedRun: StoredRun): StoredRun {
  return {
    ...storedRun,
    errors: [],
    reason: undefined,
    delayedRunState: undefined,
    firstFoundUnknownAt: undefined,
  };
}

export type StoredFanOut = {
  eventId: EventId;
  payload: unknown;
  listenerNames: readonly string[];
  firstFoundUnknownAt?: IsoTimestamp;
  finalError?: ErrorRecord;
  failureCount?: number;
  notifiedFailureCount?: number;
  writeToken: WriteToken;
};

export function storedFanOutAfterFinalFailure(storedFanOut: StoredFanOut, finalError: ErrorRecord) {
  return {
    ...storedFanOut,
    finalError,
    failureCount: (storedFanOut.failureCount ?? 0) + 1,
  } satisfies StoredFanOut;
}

function storedFanOutAfterReplay(storedFanOut: StoredFanOut): StoredFanOut {
  return { ...storedFanOut, firstFoundUnknownAt: undefined, finalError: undefined };
}

export type StoredFiring = {
  taskName: string;
  plannedInstant?: EpochMilliseconds;
};

export type IsoTimestamp = string;

export type RunToEnqueue = {
  runId: RunId;
  payload: unknown;
  retries: number;
  delay?: Duration;
  delayInMilliseconds?: DurationInMilliseconds;
  scheduledAt?: Date;
  concurrencyKey?: ConcurrencyKey;
};

export function scheduledAtOf(storedRun: StoredRun): Date | undefined {
  return storedRun.scheduledAt === undefined ? undefined : new Date(storedRun.scheduledAt);
}

export function plannedInstantOfFiring(firingJobId: JobId | undefined, taskName: string): EpochMilliseconds {
  const firingJobIdPrefix = `repeat:${taskName}:`;
  const plannedInstant = Number(firingJobId?.slice(firingJobIdPrefix.length));
  if (firingJobId?.startsWith(firingJobIdPrefix) !== true || !Number.isSafeInteger(plannedInstant)) {
    throw new Error(`Firing job "${String(firingJobId)}" of task "${taskName}" has no planned instant in its id`);
  }
  return plannedInstant;
}

export type FanOutToEnqueue = {
  eventId: EventId;
  payload: unknown;
  listenerNames: readonly string[];
};

export type ListenerRunToEnqueue = {
  listenerName: string;
  retries: number;
  concurrencyKey: ConcurrencyKey | undefined;
};

export type EnqueueOutcome = {
  isDuplicate: boolean;
};

export type ReplayOutcome = "replayed" | ReplayRefusal;

export type RunQueue = {
  enqueueRun: (taskName: string, run: RunToEnqueue) => Promise<EnqueueOutcome>;
  replayRun: (taskName: string, runId: RunId) => Promise<ReplayOutcome>;
  enqueueFanOut: (eventName: string, fanOut: FanOutToEnqueue) => Promise<EnqueueOutcome>;
  replayFanOut: (eventName: string, eventId: EventId) => Promise<ReplayOutcome>;
  enqueueListenerRuns: (
    eventName: string,
    fanOut: StoredFanOut,
    listenerRuns: readonly ListenerRunToEnqueue[],
  ) => Promise<void>;
  syncSchedules: (declaredSchedules: readonly DeclaredSchedule[]) => Promise<ScheduleChange[]>;
  enqueueFiring: (taskName: string, plannedInstant: EpochMilliseconds) => Promise<void>;
  readonly bullmqQueue: Queue;
  readonly client: Redis;
  readonly hasFailedToOpen: boolean;
  close: () => Promise<void>;
};

const FAN_OUT_AND_FIRING_JOB_OPTIONS = {
  attempts: FAN_OUT_RETRIES + 1,
  backoff: { type: "exponential", delay: FAN_OUT_FIRST_BACKOFF_IN_MILLISECONDS },
  removeOnComplete: COMPLETED_JOB_RETENTION,
  removeOnFail: FAILED_JOB_RETENTION,
};

function jobForRun(taskName: string, storedRun: StoredRun, retries: number) {
  return {
    name: taskName,
    data: storedRun,
    opts: {
      jobId: deriveJobId(taskName, storedRun.runId),
      attempts: retries + 1,
      backoff: RUN_BACKOFF,
      removeOnComplete: COMPLETED_JOB_RETENTION,
      removeOnFail: FAILED_JOB_RETENTION,
    },
  };
}

export function createRunQueue(
  connections: RedisConnections,
  role: RedisClientRole,
  prefix: string,
  logger: Logger,
): RunQueue {
  const client = connections.createClient(role);
  const queue = new Queue(RUN_QUEUE_NAME, { connection: client, prefix });
  queue.on("error", listenerLoggingAnsweredErrors(logger, "queue error"));
  let hasFailedToOpen = false;
  queue.waitUntilReady().catch(() => {
    hasFailedToOpen = true;
  });

  const addUnlessDuplicate = async (
    name: string,
    data: { writeToken: WriteToken },
    opts: JobsOptions & { jobId: string },
  ): Promise<EnqueueOutcome> => {
    await queue.add(name, data, opts);
    const storedJob = await queue.getJob(opts.jobId);
    const storedWriteToken = (storedJob?.data as { writeToken?: WriteToken } | undefined)?.writeToken;
    return { isDuplicate: storedJob !== undefined && storedWriteToken !== data.writeToken };
  };

  const replayFailedJob = async <Data>(jobId: JobId, dataAfterReplay: (data: Data) => Data): Promise<ReplayOutcome> => {
    const job = (await queue.getJob(jobId)) as Job<Data> | undefined;
    if (job === undefined) {
      return "not found";
    }
    if ((await saveFailedJobDataIfUnchanged(client, job, dataAfterReplay(job.data))) !== "saved") {
      return "not failed";
    }
    await client.hset(jobKeyOf(job), BULLMQ_STALL_COUNT_FIELD, 0);
    await keepSavedStepsForFailedRunRetention(client, job);
    try {
      await job.retry("failed", { resetAttemptsMade: true, resetAttemptsStarted: true });
    } catch (error) {
      if (await job.isFailed()) {
        throw error;
      }
      return "not failed";
    }
    return "replayed";
  };

  return {
    bullmqQueue: queue,
    client,
    get hasFailedToOpen() {
      return hasFailedToOpen;
    },
    replayRun: (taskName, runId) => replayFailedJob(deriveJobId(taskName, runId), storedRunAfterReplay),
    replayFanOut: (eventName, eventId) => replayFailedJob(deriveJobId(eventName, eventId), storedFanOutAfterReplay),
    enqueueRun: async (taskName, run) => {
      const job = jobForRun(
        taskName,
        {
          runId: run.runId,
          payload: run.payload,
          ...(run.scheduledAt === undefined ? {} : { scheduledAt: run.scheduledAt.toISOString() }),
          ...(run.concurrencyKey === undefined ? {} : { concurrencyKey: run.concurrencyKey }),
          errors: [],
          writeToken: createRandomId(),
        },
        run.retries,
      );
      return addUnlessDuplicate(job.name, job.data, {
        ...job.opts,
        ...(run.delayInMilliseconds === undefined ? {} : { delay: run.delayInMilliseconds }),
      });
    },
    enqueueFanOut: async (eventName, fanOut) => {
      const storedFanOut: StoredFanOut = { ...fanOut, writeToken: createRandomId() };
      return addUnlessDuplicate(eventName, storedFanOut, {
        ...FAN_OUT_AND_FIRING_JOB_OPTIONS,
        jobId: deriveJobId(eventName, fanOut.eventId),
      });
    },
    enqueueListenerRuns: async (eventName, fanOut, listenerRuns) => {
      const sourceEvent = { eventName, eventId: fanOut.eventId };
      await queue.addBulk(
        listenerRuns.map(({ listenerName, retries, concurrencyKey }) =>
          jobForRun(
            listenerName,
            {
              runId: deriveListenerRunId(fanOut.eventId, listenerName),
              payload: fanOut.payload,
              sourceEvent,
              ...(concurrencyKey === undefined ? {} : { concurrencyKey }),
              errors: [],
              writeToken: createRandomId(),
            },
            retries,
          ),
        ),
      );
    },
    syncSchedules: async (declaredSchedules) => {
      const storedSchedulers = await queue.getJobSchedulers(0, -1);
      const scheduleChanges = scheduleChangesBetween(
        storedSchedulers.map((storedScheduler) => ({
          taskName: storedScheduler.key,
          cron: storedScheduler.pattern,
          timezone: storedScheduler.tz,
        })),
        declaredSchedules,
      );
      for (const scheduleChange of scheduleChanges) {
        if (scheduleChange.kind === "removed") {
          await queue.removeJobScheduler(scheduleChange.taskName);
          continue;
        }
        const storedFiring: StoredFiring = { taskName: scheduleChange.taskName };
        await queue.upsertJobScheduler(
          scheduleChange.taskName,
          { pattern: scheduleChange.cron, tz: scheduleChange.timezone },
          { name: FIRING_JOB_NAME, data: storedFiring, opts: FAN_OUT_AND_FIRING_JOB_OPTIONS },
        );
      }
      return scheduleChanges;
    },
    enqueueFiring: async (taskName, plannedInstant) => {
      const storedFiring: StoredFiring = { taskName, plannedInstant };
      await queue.add(FIRING_JOB_NAME, storedFiring, {
        ...FAN_OUT_AND_FIRING_JOB_OPTIONS,
        jobId: deriveJobId(FIRING_JOB_NAME, `${taskName}:${plannedInstant}`),
      });
    },
    close: async () => {
      await queue.close();
      await closeRedisClient(client);
    },
  };
}
