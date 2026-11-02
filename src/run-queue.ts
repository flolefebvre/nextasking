import { Queue } from "bullmq";

import { closeRedisClient, createRedisClient } from "./connection.js";
import type { ConnectionConfig } from "./connection.js";
import { createRandomId, deriveJobId } from "./ids.js";
import type { EventId, RunId } from "./ids.js";

export const RUN_QUEUE_NAME = "runs";

const SECONDS_IN_A_DAY = 24 * 60 * 60;

const COMPLETED_JOB_RETENTION = { age: SECONDS_IN_A_DAY, count: 1000 };

const FAILED_FAN_OUT_RETENTION = { age: 14 * SECONDS_IN_A_DAY };

const FAN_OUT_RETRIES = 5;

const FAN_OUT_FIRST_BACKOFF_IN_MILLISECONDS = 30_000;

export type StoredFanOut = {
  eventId: EventId;
  payload: unknown;
  listenerNames: readonly string[];
};

export type RunQueue = {
  enqueueRun: (taskName: string, payload: unknown) => Promise<RunId>;
  enqueueFanOut: (eventName: string, fanOut: StoredFanOut) => Promise<void>;
  close: () => Promise<void>;
};

export function createRunQueue(connection: ConnectionConfig, prefix: string): RunQueue {
  const client = createRedisClient(connection, "instance");
  const queue = new Queue(RUN_QUEUE_NAME, { connection: client, prefix });
  queue.on("error", () => undefined);

  return {
    enqueueRun: async (taskName, payload) => {
      const runId = createRandomId();
      await queue.add(taskName, payload, { jobId: runId, removeOnComplete: COMPLETED_JOB_RETENTION });
      return runId;
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
    close: async () => {
      await queue.close();
      await closeRedisClient(client);
    },
  };
}
