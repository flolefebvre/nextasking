import { Queue } from "bullmq";
import type { JobsOptions } from "bullmq";

import { closeRedisClient, createRedisClient } from "./connection.js";
import type { ConnectionConfig } from "./connection.js";
import type { DurationInMilliseconds } from "./durations.js";
import { createRandomId, deriveJobId, deriveListenerRunId } from "./ids.js";
import type { EventId, RunId } from "./ids.js";

export const RUN_QUEUE_NAME = "runs";

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

export type FailureReason = "retries exhausted" | "non-retriable" | "stalled";

export type StoredRun = {
  runId: RunId;
  payload: unknown;
  sourceEvent?: SourceEvent;
  errors: ErrorRecord[];
  reason?: FailureReason;
  writeToken: WriteToken;
};

export type StoredFanOut = {
  eventId: EventId;
  payload: unknown;
  listenerNames: readonly string[];
  writeToken: WriteToken;
};

export type RunToEnqueue = {
  runId: RunId;
  payload: unknown;
  retries: number;
  delayInMilliseconds?: DurationInMilliseconds;
};

export type FanOutToEnqueue = {
  eventId: EventId;
  payload: unknown;
  listenerNames: readonly string[];
};

export type ListenerRunToEnqueue = {
  listenerName: string;
  retries: number;
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
  close: () => Promise<void>;
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
        { runId: run.runId, payload: run.payload, errors: [], writeToken: createRandomId() },
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
        jobId: deriveJobId(eventName, fanOut.eventId),
        attempts: FAN_OUT_RETRIES + 1,
        backoff: { type: "exponential", delay: FAN_OUT_FIRST_BACKOFF_IN_MILLISECONDS },
        removeOnComplete: COMPLETED_JOB_RETENTION,
        removeOnFail: FAILED_JOB_RETENTION,
      });
    },
    enqueueListenerRuns: async (eventName, fanOut, listenerRuns) => {
      const sourceEvent = { eventName, eventId: fanOut.eventId };
      await queue.addBulk(
        listenerRuns.map(({ listenerName, retries }) =>
          jobForRun(
            listenerName,
            {
              runId: deriveListenerRunId(fanOut.eventId, listenerName),
              payload: fanOut.payload,
              sourceEvent,
              errors: [],
              writeToken: createRandomId(),
            },
            retries,
          ),
        ),
      );
    },
    close: async () => {
      await queue.close();
      await closeRedisClient(client);
    },
  };
}
