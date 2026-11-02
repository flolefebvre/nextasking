import { DelayedError, Worker } from "bullmq";
import type { Job, MinimalJob } from "bullmq";

import { settleWithinTimeout } from "./attempt-timeout.js";
import { backoffInMillisecondsFor } from "./backoff.js";
import { closeRedisClient, createRedisClient } from "./connection.js";
import type { ConnectionConfig } from "./connection.js";
import { backoffOf, retriesOf } from "./definitions.js";
import type { AnyTaskDefinition, EventDefinition, TaskHandler } from "./definitions.js";
import { parseDuration } from "./durations.js";
import type { DurationInMilliseconds } from "./durations.js";
import { errorFromThrownValue, errorRecordFor, failureReasonFor } from "./failed-attempts.js";
import { loggerWithFields } from "./logger.js";
import type { Logger } from "./logger.js";
import { createRedisStepStore, keepSavedStepsForFailedRunRetention, removeSavedSteps } from "./redis-step-store.js";
import type { DispatchByName, EmitByName } from "./registry.js";
import { RUN_QUEUE_NAME } from "./run-queue.js";
import type { RunQueue, StoredFanOut, StoredRun } from "./run-queue.js";
import { openStepEngine } from "./step-engine.js";
import type { WakeTimeInEpochMilliseconds } from "./step-engine.js";

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
  dispatch: DispatchByName;
  emit: EmitByName;
};

const NO_RETRY = -1;

let isWorkerRunningInThisProcess = false;

export function startRunWorker(config: WorkerConfig): RunningWorker {
  if (isWorkerRunningInThisProcess) {
    throw new Error("The nextasking worker is already started in this process");
  }
  isWorkerRunningInThisProcess = true;

  const backoffInMillisecondsByFailedJob = new WeakMap<MinimalJob, DurationInMilliseconds>();

  const executeRunAttempt = async (task: AnyTaskDefinition, job: Job<StoredRun>, token: string | undefined) => {
    const stretchStartedAt = Date.now();
    const { runId, payload, sourceEvent } = job.data;
    const attempt = job.attemptsMade + 1;
    const runLogger = loggerWithFields(config.logger, { task: task.name, runId, attempt, ...sourceEvent });
    const durationFieldsAt = (runEndedAt: number) => ({
      runDurationInMilliseconds: runEndedAt - job.timestamp,
      executionDurationInMilliseconds: runEndedAt - stretchStartedAt,
    });
    const handler = task.handler as TaskHandler<unknown>;
    let wakeAt: WakeTimeInEpochMilliseconds | undefined;
    try {
      const stepEngine = await openStepEngine({
        taskName: task.name,
        runId,
        attempt,
        store: createRedisStepStore(client, job),
        dispatch: config.dispatch,
        emit: config.emit,
      });
      runLogger.debug({}, stepEngine.isResumingAfterSleep ? "run resumed" : "attempt started");
      const { step, dispatch, emit } = stepEngine;
      try {
        wakeAt = await settleWithinTimeout(
          task.name,
          task.timeout,
          stepEngine.executeHandler(() =>
            handler({ payload, step, emit, dispatch, runId, attempt, logger: runLogger }),
          ),
        );
      } finally {
        stepEngine.throwRecordedMisuse();
      }
      if (wakeAt !== undefined) {
        await job.moveToDelayed(wakeAt, token);
      }
    } catch (thrownValue) {
      const error = errorFromThrownValue(thrownValue);
      const failedAt = new Date();
      const retriesLeft = (job.opts.attempts ?? 1) - attempt;
      const reason = failureReasonFor(error, retriesLeft);
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
        await keepSavedStepsForFailedRunRetention(client, job).catch(() => undefined);
        backoffInMillisecondsByFailedJob.set(job, NO_RETRY);
        runLogger.error({ err: error, reason, ...durationFieldsAt(failedAt.getTime()) }, "run failed");
      }
      throw error;
    }
    if (wakeAt !== undefined) {
      runLogger.debug({ wakeAt: new Date(wakeAt).toISOString() }, "run suspended");
      throw new DelayedError();
    }
    runLogger.info(durationFieldsAt(Date.now()), "run completed");
  };

  const client = createRedisClient(config.connection, "worker");
  const worker = new Worker(
    RUN_QUEUE_NAME,
    async (job, token) => {
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
      await executeRunAttempt(task, job as Job<StoredRun>, token);
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
  worker.on("completed", (job) => {
    removeSavedSteps(client, job).catch(() => undefined);
  });
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
