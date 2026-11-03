import { Queue } from "bullmq";
import type { JobsOptions } from "bullmq";

import { closeRedisClient, createRedisClient } from "./connection.js";
import type { ConnectionConfig } from "./connection.js";
import type { ConcurrencyKey } from "./definitions.js";
import type { DurationInMilliseconds, EpochMilliseconds } from "./durations.js";
import { createRandomId, deriveJobId, deriveListenerRunId } from "./ids.js";
import type { EventId, JobId, RunId } from "./ids.js";
import { scheduleChangesBetween } from "./schedules.js";
import type { DeclaredSchedule, ScheduleChange } from "./schedules.js";

export const RUN_QUEUE_NAME = "runs";

export const FIRING_JOB_NAME = "nextasking/firing";

const SECONDS_IN_A_DAY = 24 * 60 * 60;

const COMPLETED_JOB_RETENTION = { age: SECONDS_IN_A_DAY, count: 1000 };

export const FAILED_JOB_RETENTION = { age: 14 * SECONDS_IN_A_DAY, count: 10_000 };

const FAN_OUT_RETRIES = 5;

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

export type StoredRun = {
  runId: RunId;
  payload: unknown;
  sourceEvent?: SourceEvent;
  scheduledAt?: IsoTimestamp;
  concurrencyKey?: ConcurrencyKey;
  firstFoundUnknownAt?: IsoTimestamp;
  errors: ErrorRecord[];
  reason?: FailureReason;
  writeToken: WriteToken;
};

export type StoredFanOut = {
  eventId: EventId;
  payload: unknown;
  listenerNames: readonly string[];
  firstFoundUnknownAt?: IsoTimestamp;
  writeToken: WriteToken;
};

export type StoredFiring = {
  taskName: string;
};

export type IsoTimestamp = string;

export type RunToEnqueue = {
  runId: RunId;
  payload: unknown;
  retries: number;
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

export type RunQueue = {
  enqueueRun: (taskName: string, run: RunToEnqueue) => Promise<EnqueueOutcome>;
  enqueueFanOut: (eventName: string, fanOut: FanOutToEnqueue) => Promise<EnqueueOutcome>;
  enqueueListenerRuns: (
    eventName: string,
    fanOut: StoredFanOut,
    listenerRuns: readonly ListenerRunToEnqueue[],
  ) => Promise<void>;
  syncSchedules: (declaredSchedules: readonly DeclaredSchedule[]) => Promise<ScheduleChange[]>;
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

export function createRunQueue(connection: ConnectionConfig, prefix: string): RunQueue {
  const client = createRedisClient(connection, "instance");
  const queue = new Queue(RUN_QUEUE_NAME, { connection: client, prefix });
  queue.on("error", () => undefined);

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

  return {
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
    close: async () => {
      await queue.close();
      await closeRedisClient(client);
    },
  };
}
