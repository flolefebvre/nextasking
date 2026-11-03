import { DelayedError, ErrorCode, Job, WaitingError, Worker } from "bullmq";
import type { MinimalJob } from "bullmq";

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
import { StalledRunError, errorFromThrownValue, errorRecordFor, failureReasonFor } from "./failed-attempts.js";
import type { RunId } from "./ids.js";
import { loggerWithFields } from "./logger.js";
import type { Logger } from "./logger.js";
import { createRedisStepStore, keepSavedStepsForFailedRunRetention, removeSavedSteps } from "./redis-step-store.js";
import type { DispatchByName, EmitByName } from "./registry.js";
import { RUN_QUEUE_NAME } from "./run-queue.js";
import type { FailureReason, RunQueue, StoredFanOut, StoredRun } from "./run-queue.js";
import { openStepEngine } from "./step-engine.js";
import type { HandlerOutcome } from "./step-engine.js";
import { bullmqMaxStalledCountOf, workerTiming } from "./worker-timing.js";

export type RunningWorker = {
  close: () => Promise<void>;
  shutDown: () => Promise<void>;
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

type RunDurationFields = {
  runDurationInMilliseconds: DurationInMilliseconds;
  executionDurationInMilliseconds: DurationInMilliseconds;
};

type EpochMilliseconds = number;

function runDurationFieldsOf(
  job: MinimalJob,
  executionStartedAt: EpochMilliseconds,
  runEndedAt: EpochMilliseconds,
): RunDurationFields {
  return {
    runDurationInMilliseconds: runEndedAt - job.timestamp,
    executionDurationInMilliseconds: runEndedAt - executionStartedAt,
  };
}

const STALL_LIMIT_FAILED_REASON = "job stalled more than allowable limit";

const LOCK_ERROR_CODES = new Set<unknown>([ErrorCode.JobLockNotExist, ErrorCode.JobLockMismatch]);

let isWorkerRunningInThisProcess = false;

async function handBackAtShutdown(job: Job, token: string | undefined, runLogger: Logger): Promise<never> {
  const isHandedBack = await job.moveToWait(token).then(
    () => true,
    () => false,
  );
  if (isHandedBack) {
    runLogger.debug({}, "run yielded at shutdown");
  }
  throw new WaitingError();
}

async function isSettledWithin(settlement: Promise<void>, timeoutInMilliseconds: DurationInMilliseconds) {
  let timeoutTimer: NodeJS.Timeout | undefined;
  const timeoutExpiry = new Promise<false>((resolve) => {
    timeoutTimer = setTimeout(() => {
      resolve(false);
    }, timeoutInMilliseconds);
  });
  try {
    return await Promise.race([settlement.then(() => true), timeoutExpiry]);
  } finally {
    clearTimeout(timeoutTimer);
  }
}

export function startRunWorker(config: WorkerConfig): RunningWorker {
  if (isWorkerRunningInThisProcess) {
    throw new Error("The nextasking worker is already started in this process");
  }
  isWorkerRunningInThisProcess = true;

  const backoffInMillisecondsByFailedJob = new WeakMap<MinimalJob, DurationInMilliseconds>();
  const durationFieldsByCompletedJob = new WeakMap<MinimalJob, RunDurationFields>();

  const runLoggerOf = (job: Job<StoredRun>, attempt: number) =>
    loggerWithFields(config.logger, { task: job.name, runId: job.data.runId, attempt, ...job.data.sourceEvent });
  const abortAttemptByJobId = new Map<string, (abortCause: AttemptAbortCause) => void>();
  let isDraining = false;

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
    if (isDraining) {
      return handBackAtShutdown(job, token, runLoggerOf(job, job.attemptsMade + 1));
    }
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
    const { runId, payload } = job.data;
    const attempt = job.attemptsMade + 1;
    const runLogger = runLoggerOf(job, attempt);
    const durationFieldsAt = (runEndedAt: number) => runDurationFieldsOf(job, stretchStartedAt, runEndedAt);
    const handler = task.handler as TaskHandler<unknown>;
    const handOff = async (moveJob: () => Promise<unknown>) => {
      try {
        await moveJob();
      } catch (error) {
        if (LOCK_ERROR_CODES.has((error as { code?: unknown }).code)) {
          abortAttempt("lock lost");
        }
        throw error;
      }
    };
    let outcome: HandlerOutcome;
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
        isWorkerDraining: () => isDraining,
      });
      runLogger.debug({}, stepEngine.isResumingAfterSleep ? "run resumed" : "attempt started");
      const { step, dispatch, emit } = stepEngine;
      try {
        outcome = await settleWithinTimeout(
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
      if (outcome.kind === "suspended") {
        const { wakeAt } = outcome;
        await handOff(() => job.moveToDelayed(wakeAt, token));
      }
      if (outcome.kind === "yielded") {
        await handOff(() => job.moveToWait(token));
      }
    } catch (thrownValue) {
      const abortCause = abortCauseOf(abortSignal);
      if (abortCause === "lock lost") {
        runLogger.debug({}, "attempt aborted: lock lost");
        throw thrownValue;
      }
      if (abortCause === "shutdown deadline") {
        return handBackAtShutdown(job, token, runLogger);
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
    if (outcome.kind === "suspended") {
      runLogger.debug({ wakeAt: new Date(outcome.wakeAt).toISOString() }, "run suspended");
      throw new DelayedError();
    }
    if (outcome.kind === "yielded") {
      runLogger.debug({}, "run yielded at shutdown");
      throw new WaitingError();
    }
    durationFieldsByCompletedJob.set(job, durationFieldsAt(Date.now()));
  };

  const timing = workerTiming();
  const client = createRedisClient(config.connection, "worker");
  const attemptSettlements = new Set<Promise<void>>();
  const trackSettlement = (execution: Promise<void>): Promise<void> => {
    const settlement = execution.then(
      () => undefined,
      () => undefined,
    );
    attemptSettlements.add(settlement);
    void settlement.then(() => attemptSettlements.delete(settlement));
    return execution;
  };
  const waitForAttemptsInFlight = async () => {
    while (attemptSettlements.size > 0) {
      await Promise.all(attemptSettlements);
    }
  };
  const worker = new Worker(RUN_QUEUE_NAME, (job, token) => trackSettlement(processJob(job, token)), {
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
  });
  async function processJob(job: Job, token: string | undefined): Promise<void> {
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
  }
  const logAttemptStalled = async (jobId: string) => {
    const job = (await Job.fromId(worker, jobId)) as Job<StoredRun> | undefined;
    if (job !== undefined && config.tasksByName.has(job.name)) {
      runLoggerOf(job, job.attemptsMade + 1).warn({}, "attempt stalled");
    }
  };
  const failRunForStalling = async (job: Job<StoredRun>) => {
    const failedAt = new Date();
    const error = new StalledRunError(job.data.runId, timing.stallCountThatFailsARun);
    const reason: FailureReason = "stalled";
    await job.updateData({
      ...job.data,
      errors: [...job.data.errors, errorRecordFor(error, job.attemptsMade, failedAt)],
      reason,
    });
    await keepSavedStepsForFailedRunRetention(client, job).catch(() => undefined);
    runLoggerOf(job, job.attemptsMade).error(
      {
        err: error,
        reason,
        ...runDurationFieldsOf(job, job.processedOn ?? failedAt.getTime(), failedAt.getTime()),
      },
      "run failed",
    );
  };
  worker.on("error", () => undefined);
  worker.on("lockRenewalFailed", (jobIds) => {
    for (const jobId of jobIds) {
      abortAttemptByJobId.get(jobId)?.("lock lost");
    }
  });
  worker.on("completed", (job) => {
    removeSavedSteps(client, job).catch(() => undefined);
    const durationFields = durationFieldsByCompletedJob.get(job);
    if (durationFields !== undefined) {
      runLoggerOf(job as Job<StoredRun>, job.attemptsMade).info(durationFields, "run completed");
    }
  });
  worker.on("stalled", (jobId) => {
    logAttemptStalled(jobId).catch(() => undefined);
  });
  worker.on("failed", (job, error) => {
    if (job !== undefined && error.message === STALL_LIMIT_FAILED_REASON && config.tasksByName.has(job.name)) {
      failRunForStalling(job as Job<StoredRun>).catch(() => undefined);
    }
  });
  config.logger.info({}, "worker started");

  const closeWorker = async (shouldForce: boolean) => {
    await worker.close(shouldForce);
    await closeRedisClient(client);
    isWorkerRunningInThisProcess = false;
    config.logger.info({}, "worker stopped");
  };

  return {
    close: () => closeWorker(false),
    shutDown: async () => {
      isDraining = true;
      await worker.pause(true);
      let isDrained = await isSettledWithin(waitForAttemptsInFlight(), config.options.shutdownTimeoutInMilliseconds);
      if (!isDrained) {
        for (const abortAttempt of abortAttemptByJobId.values()) {
          abortAttempt("shutdown deadline");
        }
        config.logger.warn({}, "attempts aborted at shutdown deadline");
        isDrained = await isSettledWithin(waitForAttemptsInFlight(), timing.abortedAttemptsGraceInMilliseconds);
      }
      await closeWorker(!isDrained);
    },
  };
}
