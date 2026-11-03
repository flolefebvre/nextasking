import { DelayedError, Worker } from "bullmq";
import type { Job, MinimalJob } from "bullmq";

import { AttemptAbortedError, abortCauseOf } from "./attempt-abort.js";
import type { AttemptAbortCause } from "./attempt-abort.js";
import { settleWithinTimeout } from "./attempt-timeout.js";
import { backoffInMillisecondsFor } from "./backoff.js";
import { closeRedisClient, createRedisClient } from "./connection.js";
import type { ConnectionConfig } from "./connection.js";
import { backoffOf, retriesOf } from "./definitions.js";
import type { AnyTaskDefinition, EventDefinition, TaskHandler } from "./definitions.js";
import { parseDuration } from "./durations.js";
import type { Duration, DurationInMilliseconds } from "./durations.js";
import { errorFromThrownValue, errorRecordFor, failureReasonFor } from "./failed-attempts.js";
import type { RunId } from "./ids.js";
import { loggerWithFields } from "./logger.js";
import type { Logger } from "./logger.js";
import { createRedisStepStore, keepSavedStepsForFailedRunRetention, removeSavedSteps } from "./redis-step-store.js";
import type { DispatchByName, EmitByName } from "./registry.js";
import { RUN_QUEUE_NAME } from "./run-queue.js";
import type { RunQueue, StoredFanOut, StoredRun } from "./run-queue.js";
import { openStepEngine } from "./step-engine.js";
import type { WakeTimeInEpochMilliseconds } from "./step-engine.js";
import { bullmqMaxStalledCountOf, workerTiming } from "./worker-timing.js";

export type RunningWorker = {
  close: () => Promise<void>;
};

export type StartWorkerOptions = {
  concurrency?: number;
  shutdownTimeout?: Duration;
};

export type WorkerOptions = {
  concurrency: number;
  shutdownTimeoutInMilliseconds: DurationInMilliseconds;
};

const UNLIMITED_CONCURRENCY = Number.MAX_SAFE_INTEGER;

const DEFAULT_SHUTDOWN_TIMEOUT: Duration = "25s";

export function workerOptionsFrom(options: StartWorkerOptions): WorkerOptions {
  const concurrency: unknown = options.concurrency ?? UNLIMITED_CONCURRENCY;
  if (typeof concurrency !== "number" || !Number.isInteger(concurrency) || concurrency <= 0) {
    const shownConcurrency = typeof concurrency === "string" ? JSON.stringify(concurrency) : String(concurrency);
    throw new Error(`Invalid startWorker concurrency: ${shownConcurrency} is not a positive integer`);
  }
  let shutdownTimeoutInMilliseconds: DurationInMilliseconds;
  try {
    shutdownTimeoutInMilliseconds = parseDuration(options.shutdownTimeout ?? DEFAULT_SHUTDOWN_TIMEOUT);
  } catch (error) {
    throw new Error(`Invalid startWorker shutdownTimeout: ${(error as Error).message}`, { cause: error });
  }
  return { concurrency, shutdownTimeoutInMilliseconds };
}

export type WorkerConfig = {
  options: WorkerOptions;
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
  const abortAttemptByJobId = new Map<string, (abortCause: AttemptAbortCause) => void>();

  const trackAttemptInFlight = (job: Job<StoredRun>, runId: RunId) => {
    const abortController = new AbortController();
    const abortAttempt = (abortCause: AttemptAbortCause) => {
      abortController.abort(new AttemptAbortedError(abortCause, runId));
    };
    const jobId = job.id ?? "";
    abortAttemptByJobId.set(jobId, abortAttempt);
    return {
      abortSignal: abortController.signal,
      abortAttempt,
      untrack: () => {
        if (abortAttemptByJobId.get(jobId) === abortAttempt) {
          abortAttemptByJobId.delete(jobId);
        }
      },
    };
  };

  type TrackedAttempt = ReturnType<typeof trackAttemptInFlight>;

  const executeRunAttempt = async (task: AnyTaskDefinition, job: Job<StoredRun>, token: string | undefined) => {
    const trackedAttempt = trackAttemptInFlight(job, job.data.runId);
    try {
      await executeTrackedRunAttempt(task, job, token, trackedAttempt);
    } finally {
      trackedAttempt.untrack();
    }
  };

  const executeTrackedRunAttempt = async (
    task: AnyTaskDefinition,
    job: Job<StoredRun>,
    token: string | undefined,
    { abortSignal, abortAttempt }: TrackedAttempt,
  ) => {
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
        store: createRedisStepStore(client, job, () => {
          abortAttempt("lock lost");
        }),
        dispatch: config.dispatch,
        emit: config.emit,
        abortSignal,
      });
      runLogger.debug({}, stepEngine.isResumingAfterSleep ? "run resumed" : "attempt started");
      const { step, dispatch, emit } = stepEngine;
      try {
        wakeAt = await settleWithinTimeout(
          task.name,
          task.timeout,
          stepEngine.executeHandler(() =>
            handler({ payload, step, emit, dispatch, runId, attempt, logger: runLogger, abortSignal }),
          ),
        );
      } finally {
        stepEngine.throwRecordedMisuse();
      }
      if (abortCauseOf(abortSignal) === "lock lost") {
        throw abortSignal.reason;
      }
      if (wakeAt !== undefined) {
        await job.moveToDelayed(wakeAt, token);
      }
    } catch (thrownValue) {
      if (abortCauseOf(abortSignal) === "lock lost") {
        runLogger.debug({}, "attempt aborted: lock lost");
        throw thrownValue;
      }
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

  const timing = workerTiming();
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
      concurrency: config.options.concurrency,
      lockDuration: timing.lockDurationInMilliseconds,
      stalledInterval: timing.stallIntervalInMilliseconds,
      maxStalledCount: bullmqMaxStalledCountOf(timing),
      settings: {
        backoffStrategy: (_attemptsMade, _backoffType, _error, job) =>
          (job === undefined ? undefined : backoffInMillisecondsByFailedJob.get(job)) ?? NO_RETRY,
      },
    },
  );
  worker.on("error", () => undefined);
  worker.on("lockRenewalFailed", (jobIds) => {
    for (const jobId of jobIds) {
      abortAttemptByJobId.get(jobId)?.("lock lost");
    }
  });
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
