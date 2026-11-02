import { Queue } from "bullmq";

import { closeRedisClient, createRedisClient } from "./connection.js";
import type { ConnectionConfig } from "./connection.js";
import { createRandomId, deriveJobId, deriveListenerRunId } from "./ids.js";
import type { EventId, RunId } from "./ids.js";

export const RUN_QUEUE_NAME = "runs";

const SECONDS_IN_A_DAY = 24 * 60 * 60;

const COMPLETED_JOB_RETENTION = { age: SECONDS_IN_A_DAY, count: 1000 };

const FAILED_FAN_OUT_RETENTION = { age: 14 * SECONDS_IN_A_DAY };

const FAN_OUT_RETRIES = 5;

const FAN_OUT_FIRST_BACKOFF_IN_MILLISECONDS = 30_000;

export type SourceEvent = {
  eventName: string;
  eventId: EventId;
};

export type StoredRun = {
  runId: RunId;
  payload: unknown;
  sourceEvent?: SourceEvent;
};

export type StoredFanOut = {
  eventId: EventId;
  payload: unknown;
  listenerNames: readonly string[];
};

export type RunQueue = {
  enqueueRun: (taskName: string, payload: unknown) => Promise<RunId>;
  enqueueFanOut: (eventName: string, fanOut: StoredFanOut) => Promise<void>;
  enqueueListenerRuns: (eventName: string, fanOut: StoredFanOut) => Promise<void>;
  close: () => Promise<void>;
};

function jobForRun(taskName: string, storedRun: StoredRun) {
  return {
    name: taskName,
    data: storedRun,
    opts: { jobId: deriveJobId(taskName, storedRun.runId), removeOnComplete: COMPLETED_JOB_RETENTION },
  };
}

export function createRunQueue(connection: ConnectionConfig, prefix: string): RunQueue {
  const client = createRedisClient(connection, "instance");
  const queue = new Queue(RUN_QUEUE_NAME, { connection: client, prefix });
  queue.on("error", () => undefined);

  return {
    enqueueRun: async (taskName, payload) => {
      const job = jobForRun(taskName, { runId: createRandomId(), payload });
      await queue.add(job.name, job.data, job.opts);
      return job.data.runId;
    },
    enqueueFanOut: async (eventName, fanOut) => {
      await queue.add(eventName, fanOut, {
        jobId: deriveJobId(eventName, fanOut.eventId),
        attempts: FAN_OUT_RETRIES + 1,
        backoff: { type: "exponential", delay: FAN_OUT_FIRST_BACKOFF_IN_MILLISECONDS },
        removeOnComplete: COMPLETED_JOB_RETENTION,
        removeOnFail: FAILED_FAN_OUT_RETENTION,
      });
    },
    enqueueListenerRuns: async (eventName, fanOut) => {
      const sourceEvent = { eventName, eventId: fanOut.eventId };
      await queue.addBulk(
        fanOut.listenerNames.map((listenerName) =>
          jobForRun(listenerName, {
            runId: deriveListenerRunId(fanOut.eventId, listenerName),
            payload: fanOut.payload,
            sourceEvent,
          }),
        ),
      );
    },
    close: async () => {
      await queue.close();
      await closeRedisClient(client);
    },
  };
}
