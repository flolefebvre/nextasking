import { Worker } from "bullmq";

import { closeRedisClient, createRedisClient } from "./connection.js";
import type { ConnectionConfig } from "./connection.js";
import type { AnyTaskDefinition, TaskHandler } from "./definitions.js";
import { loggerWithFields } from "./logger.js";
import type { Logger } from "./logger.js";
import { RUN_QUEUE_NAME } from "./run-queue.js";

export type RunningWorker = {
  close: () => Promise<void>;
};

export type WorkerConfig = {
  connection: ConnectionConfig;
  prefix: string;
  logger: Logger;
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
      const attemptStartedAt = Date.now();
      const runId = String(job.id);
      const attempt = job.attemptsMade + 1;
      const runLogger = loggerWithFields(config.logger, { task: task.name, runId, attempt });
      runLogger.debug({}, "attempt started");
      const handler = task.handler as TaskHandler<unknown>;
      await handler({ payload: job.data, runId, attempt, logger: runLogger });
      const runCompletedAt = Date.now();
      runLogger.info(
        {
          runDurationInMilliseconds: runCompletedAt - job.timestamp,
          executionDurationInMilliseconds: runCompletedAt - attemptStartedAt,
        },
        "run completed",
      );
    },
    { connection: client, prefix: config.prefix },
  );
  worker.on("error", () => undefined);
  config.logger.info({}, "worker started");

  return {
    close: async () => {
      await worker.close();
      await closeRedisClient(client);
      isWorkerRunningInThisProcess = false;
      config.logger.info({}, "worker stopped");
    },
  };
}
