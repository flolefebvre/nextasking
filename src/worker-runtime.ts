import { Worker } from "bullmq";

import { closeRedisClient, createRedisClient } from "./connection.js";
import type { ConnectionConfig } from "./connection.js";
import type { AnyTaskDefinition, TaskHandler } from "./definitions.js";
import { RUN_QUEUE_NAME } from "./run-queue.js";

export type RunningWorker = {
  close: () => Promise<void>;
};

export type WorkerConfig = {
  connection: ConnectionConfig;
  prefix: string;
  tasksByName: ReadonlyMap<string, AnyTaskDefinition>;
};

let isWorkerRunningInThisProcess = false;

export function startRunWorker(config: WorkerConfig): RunningWorker {
  if (isWorkerRunningInThisProcess) {
    throw new Error("The nextasking worker is already started in this process");
  }
  isWorkerRunningInThisProcess = true;

  const client = createRedisClient(config.connection, "worker");
  const worker = new Worker(
    RUN_QUEUE_NAME,
    async (job) => {
      const task = config.tasksByName.get(job.name);
      if (task === undefined) {
        throw new Error(`Unknown task "${job.name}"`);
      }
      const handler = task.handler as TaskHandler<unknown>;
      await handler({ payload: job.data, runId: String(job.id), attempt: job.attemptsMade + 1 });
    },
    { connection: client, prefix: config.prefix },
  );
  worker.on("error", () => undefined);

  return {
    close: async () => {
      await worker.close();
      await closeRedisClient(client);
      isWorkerRunningInThisProcess = false;
    },
  };
}
