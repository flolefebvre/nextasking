import { Queue } from "bullmq";

import { closeRedisClient, createRedisClient } from "./connection.js";
import type { ConnectionConfig } from "./connection.js";
import { createRunId } from "./ids.js";
import type { RunId } from "./ids.js";

export const RUN_QUEUE_NAME = "runs";

const SECONDS_IN_A_DAY = 24 * 60 * 60;

const COMPLETED_RUN_RETENTION = { age: SECONDS_IN_A_DAY, count: 1000 };

export type RunQueue = {
  enqueueRun: (taskName: string, payload: unknown) => Promise<RunId>;
  close: () => Promise<void>;
};

export function createRunQueue(connection: ConnectionConfig, prefix: string): RunQueue {
  const client = createRedisClient(connection, "instance");
  const queue = new Queue(RUN_QUEUE_NAME, { connection: client, prefix });
  queue.on("error", () => undefined);

  return {
    enqueueRun: async (taskName, payload) => {
      const runId = createRunId();
      await queue.add(taskName, payload, { jobId: runId, removeOnComplete: COMPLETED_RUN_RETENTION });
      return runId;
    },
    close: async () => {
      await queue.close();
      await closeRedisClient(client);
    },
  };
}
