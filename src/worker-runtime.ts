import { Worker } from "bullmq";
import type { Job } from "bullmq";

import { closeRedisClient, createRedisClient } from "./connection.js";
import type { ConnectionConfig } from "./connection.js";
import type { AnyTaskDefinition, EventDefinition, TaskHandler } from "./definitions.js";
import { loggerWithFields } from "./logger.js";
import type { Logger } from "./logger.js";
import { RUN_QUEUE_NAME } from "./run-queue.js";
import type { RunQueue, StoredFanOut, StoredRun } from "./run-queue.js";

export type RunningWorker = {
  close: () => Promise<void>;
};

export type WorkerConfig = {
  connection: ConnectionConfig;
  prefix: string;
  logger: Logger;
  tasksByName: ReadonlyMap<string, AnyTaskDefinition>;
  eventsByName: ReadonlyMap<string, EventDefinition>;
  openRunQueue: () => RunQueue;
};

let isWorkerRunningInThisProcess = false;

export function startRunWorker(config: WorkerConfig): RunningWorker {
  if (isWorkerRunningInThisProcess) {
    throw new Error("The nextasking worker is already started in this process");
  }
  isWorkerRunningInThisProcess = true;

  const executeRunAttempt = async (task: AnyTaskDefinition, job: Job<StoredRun>) => {
    const attemptStartedAt = Date.now();
    const { runId, payload, sourceEvent } = job.data;
    const attempt = job.attemptsMade + 1;
    const runLogger = loggerWithFields(config.logger, { task: task.name, runId, attempt, ...sourceEvent });
    runLogger.debug({}, "attempt started");
    const handler = task.handler as TaskHandler<unknown>;
    await handler({ payload, runId, attempt, logger: runLogger });
    const runCompletedAt = Date.now();
    runLogger.info(
      {
        runDurationInMilliseconds: runCompletedAt - job.timestamp,
        executionDurationInMilliseconds: runCompletedAt - attemptStartedAt,
      },
      "run completed",
    );
  };

  const client = createRedisClient(config.connection, "worker");
  const worker = new Worker(
    RUN_QUEUE_NAME,
    async (job) => {
      if (config.eventsByName.has(job.name)) {
        await config.openRunQueue().enqueueListenerRuns(job.name, job.data as StoredFanOut);
        return;
      }
      const task = config.tasksByName.get(job.name);
      if (task === undefined) {
        throw new Error(`Unknown task "${job.name}"`);
      }
      await executeRunAttempt(task, job as Job<StoredRun>);
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
