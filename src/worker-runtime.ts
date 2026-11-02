import { Worker } from "bullmq";
import type { Job, MinimalJob } from "bullmq";

import { backoffInMillisecondsFor } from "./backoff.js";
import { closeRedisClient, createRedisClient } from "./connection.js";
import type { ConnectionConfig } from "./connection.js";
import { backoffOf, retriesOf } from "./definitions.js";
import type { AnyTaskDefinition, EventDefinition, TaskHandler } from "./definitions.js";
import { parseDuration } from "./durations.js";
import type { DurationInMilliseconds } from "./durations.js";
import { errorFromThrownValue, errorRecordFor } from "./failed-attempts.js";
import { loggerWithFields } from "./logger.js";
import type { Logger } from "./logger.js";
import { RUN_QUEUE_NAME } from "./run-queue.js";
import type { FailureReason, RunQueue, StoredFanOut, StoredRun } from "./run-queue.js";

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

const NO_RETRY = -1;

let isWorkerRunningInThisProcess = false;

export function startRunWorker(config: WorkerConfig): RunningWorker {
  if (isWorkerRunningInThisProcess) {
    throw new Error("The nextasking worker is already started in this process");
  }
  isWorkerRunningInThisProcess = true;

  const backoffInMillisecondsByFailedJob = new WeakMap<MinimalJob, DurationInMilliseconds>();

  const executeRunAttempt = async (task: AnyTaskDefinition, job: Job<StoredRun>) => {
    const attemptStartedAt = Date.now();
    const { runId, payload, sourceEvent } = job.data;
    const attempt = job.attemptsMade + 1;
    const runLogger = loggerWithFields(config.logger, { task: task.name, runId, attempt, ...sourceEvent });
    const durationFieldsAt = (runEndedAt: number) => ({
      runDurationInMilliseconds: runEndedAt - job.timestamp,
      executionDurationInMilliseconds: runEndedAt - attemptStartedAt,
    });
    runLogger.debug({}, "attempt started");
    const handler = task.handler as TaskHandler<unknown>;
    try {
      await handler({ payload, runId, attempt, logger: runLogger });
    } catch (thrownValue) {
      const error = errorFromThrownValue(thrownValue);
      const failedAt = new Date();
      const retriesLeft = (job.opts.attempts ?? 1) - attempt;
      const reason: FailureReason | undefined = retriesLeft > 0 ? undefined : "retries exhausted";
      await job.updateData({
        ...job.data,
        errors: [...job.data.errors, errorRecordFor(error, attempt, failedAt)],
        ...(reason === undefined ? {} : { reason }),
      });
      if (reason === undefined) {
        const backoffInMilliseconds = backoffInMillisecondsFor(parseDuration(backoffOf(task)), attempt, Math.random());
        backoffInMillisecondsByFailedJob.set(job, backoffInMilliseconds);
        runLogger.warn({ err: error, retriesLeft, backoffInMilliseconds }, "attempt failed");
      } else {
        backoffInMillisecondsByFailedJob.set(job, NO_RETRY);
        runLogger.error({ err: error, reason, ...durationFieldsAt(failedAt.getTime()) }, "run failed");
      }
      throw error;
    }
    runLogger.info(durationFieldsAt(Date.now()), "run completed");
  };

  const client = createRedisClient(config.connection, "worker");
  const worker = new Worker(
    RUN_QUEUE_NAME,
    async (job) => {
      if (config.eventsByName.has(job.name)) {
        const fanOut = job.data as StoredFanOut;
        await config.openRunQueue().enqueueListenerRuns(
          job.name,
          fanOut,
          fanOut.listenerNames.map((listenerName) => ({
            listenerName,
            retries: retriesOf(config.tasksByName.get(listenerName)),
          })),
        );
        return;
      }
      const task = config.tasksByName.get(job.name);
      if (task === undefined) {
        throw new Error(`Unknown task "${job.name}"`);
      }
      await executeRunAttempt(task, job as Job<StoredRun>);
    },
    {
      connection: client,
      prefix: config.prefix,
      settings: {
        backoffStrategy: (_attemptsMade, _backoffType, _error, job) =>
          (job === undefined ? undefined : backoffInMillisecondsByFailedJob.get(job)) ?? NO_RETRY,
      },
    },
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
