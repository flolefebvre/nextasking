import { DelayedError, ErrorCode, Job, UnrecoverableError, WaitingError, Worker } from "bullmq";
import type { MinimalJob } from "bullmq";

import { AttemptAbortedError, abortCauseOf } from "./attempt-abort.js";
import type { AttemptAbortCause } from "./attempt-abort.js";
import { settleWithinTimeout } from "./attempt-timeout.js";
import { backoffInMillisecondsFor } from "./backoff.js";
import { setTimeout as sleep } from "node:timers/promises";

import { createConcurrencySlots } from "./concurrency-slots.js";
import type { ConcurrencySlot } from "./concurrency-slots.js";
import { closeRedisClient, createRedisClient } from "./connection.js";
import type { ConnectionConfig } from "./connection.js";
import { backoffOf, concurrencyKeyOf, retriesOf } from "./definitions.js";
import type { AnyTaskDefinition, EventDefinition, ScheduledTaskContext, TaskContext } from "./definitions.js";
import { parseDuration, shownDurationOf } from "./durations.js";
import type { Duration, DurationInMilliseconds, EpochMilliseconds } from "./durations.js";
import {
  StalledRunError,
  UnknownTaskRunError,
  errorFromThrownValue,
  errorRecordFor,
  failureReasonFor,
} from "./failed-attempts.js";
import { deriveFiringRunId } from "./ids.js";
import type { RunId } from "./ids.js";
import { loggerWithFields } from "./logger.js";
import type { Logger } from "./logger.js";
import { createRedisStepStore, keepSavedStepsForFailedRunRetention, removeSavedSteps } from "./redis-step-store.js";
import type { DispatchByName, EmitByName } from "./registry.js";
import { FIRING_JOB_NAME, RUN_QUEUE_NAME, plannedInstantOfFiring, scheduledAtOf } from "./run-queue.js";
import type { FailureReason, RunQueue, StoredFanOut, StoredFiring, StoredRun } from "./run-queue.js";
import { attemptOf } from "./run-states.js";
import { openStepEngine } from "./step-engine.js";
import type { HandlerOutcome } from "./step-engine.js";
import { handBackUnlessGivenUp } from "./unknown-name-hand-back.js";
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

type AnyTaskHandler = (context: TaskContext<unknown> | ScheduledTaskContext) => Promise<void>;

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

async function yieldAtShutdown(job: Job, token: string | undefined, runLogger: Logger): Promise<never> {
  const isYielded = await job.moveToWait(token).then(
    () => true,
    () => false,
  );
  if (isYielded) {
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
      return yieldAtShutdown(job, token, runLoggerOf(job, attemptOf(job, "executing")));
    }
    const trackedAttempt = trackAttemptInFlight(job, job.data.runId);
    const concurrencySlot = concurrencySlots.slotFor(job, token ?? "", () => {
      trackedAttempt.abortAttempt("lock lost");
    });
    try {
      await executeTrackedRunAttempt(task, job, token, trackedAttempt, concurrencySlot);
    } finally {
      trackedAttempt.untrack();
      await concurrencySlot.release();
    }
  };

  const takeConcurrencySlotOrPark = async (
    task: AnyTaskDefinition,
    job: Job<StoredRun>,
    concurrencySlot: ConcurrencySlot,
    park: () => Promise<void>,
    runLogger: Logger,
  ): Promise<"taken" | "parked"> => {
    if (task.concurrency === undefined) {
      return "taken";
    }
    const concurrencyKey = job.data.concurrencyKey ?? concurrencyKeyOf(task, job.data.payload);
    const slotTaking = await concurrencySlot.take(task.concurrency.limit, concurrencyKey);
    if (slotTaking === "taken after waiting") {
      runLogger.debug({}, "run got a concurrency slot");
    }
    if (slotTaking !== "waiting") {
      return "taken";
    }
    if (timing.testOnlyPauseBeforeParkingInMilliseconds > 0) {
      await sleep(timing.testOnlyPauseBeforeParkingInMilliseconds);
    }
    await park();
    runLogger.debug({}, "run waiting for a concurrency slot");
    await concurrencySlot.wakeOldestWaitingRunIfASlotIsFree();
    return "parked";
  };

  const executeTrackedRunAttempt = async (
    task: AnyTaskDefinition,
    job: Job<StoredRun>,
    token: string | undefined,
    { abortSignal, abortAttempt }: TrackedAttempt,
    concurrencySlot: ConcurrencySlot,
  ) => {
    let stretchStartedAt = Date.now();
    const { runId, payload } = job.data;
    const attempt = attemptOf(job, "executing");
    const runLogger = runLoggerOf(job, attempt);
    const durationFieldsAt = (runEndedAt: number) => runDurationFieldsOf(job, stretchStartedAt, runEndedAt);
    const handler = task.handler as AnyTaskHandler;
    const handOff = async (moveJob: () => Promise<unknown>) => {
      concurrencySlot.stopRenewing();
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
      const slotTaking = await takeConcurrencySlotOrPark(
        task,
        job,
        concurrencySlot,
        () => handOff(() => job.moveToDelayed(Date.now() + timing.concurrencySlotSafetyDelayInMilliseconds, token)),
        runLogger,
      );
      if (slotTaking === "parked") {
        throw new DelayedError();
      }
      stretchStartedAt = Date.now();
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
          stepEngine.executeHandler(() => {
            const runContext = { step, emit, dispatch, runId, attempt, logger: runLogger, abortSignal };
            return handler(
              task.schedule === undefined
                ? { ...runContext, payload }
                : { ...runContext, scheduledAt: scheduledAtOf(job.data) },
            );
          }),
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
      if (thrownValue instanceof DelayedError) {
        throw thrownValue;
      }
      const abortCause = abortCauseOf(abortSignal);
      if (abortCause === "lock lost") {
        runLogger.debug({}, "attempt aborted: lock lost");
        throw thrownValue;
      }
      if (abortCause === "shutdown deadline") {
        return yieldAtShutdown(job, token, runLogger);
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
  const concurrencySlots = createConcurrencySlots(client, worker, timing.lockDurationInMilliseconds);
  async function enqueueFiringRun(firingJob: Job<StoredFiring>): Promise<void> {
    const { taskName } = firingJob.data;
    const plannedInstant: EpochMilliseconds = plannedInstantOfFiring(firingJob.id, taskName);
    const scheduledAt = new Date(plannedInstant);
    const task = config.tasksByName.get(taskName);
    if (task?.schedule === undefined) {
      config.logger.debug(
        { task: taskName, scheduledAt: scheduledAt.toISOString() },
        "firing dropped: task not scheduled",
      );
      return;
    }
    await config.openRunQueue().enqueueRun(taskName, {
      runId: deriveFiringRunId(taskName, plannedInstant),
      payload: undefined,
      retries: retriesOf(task),
      scheduledAt,
    });
  }
  async function processJob(job: Job, token: string | undefined): Promise<void> {
    if (job.name === FIRING_JOB_NAME) {
      await enqueueFiringRun(job as Job<StoredFiring>);
      return;
    }
    if (config.eventsByName.has(job.name)) {
      const fanOut = job.data as StoredFanOut;
      await config.openRunQueue().enqueueListenerRuns(
        job.name,
        fanOut,
        fanOut.listenerNames.map((listenerName) => {
          const listener = config.tasksByName.get(listenerName);
          return {
            listenerName,
            retries: retriesOf(listener),
            concurrencyKey: listener === undefined ? undefined : concurrencyKeyOf(listener, fanOut.payload),
          };
        }),
      );
      return;
    }
    const task = config.tasksByName.get(job.name);
    if (task !== undefined) {
      await executeRunAttempt(task, job as Job<StoredRun>, token);
      return;
    }
    if ("listenerNames" in (job.data as object)) {
      await handBackFanOutOfUnknownEvent(job as Job<StoredFanOut>, token);
      return;
    }
    await handBackRunOfUnknownTask(job as Job<StoredRun>, token);
  }
  async function handBackRunOfUnknownTask(job: Job<StoredRun>, token: string | undefined): Promise<void> {
    const runLogger = runLoggerOf(job, attemptOf(job, "executing"));
    if ((await handBackUnlessGivenUp(job, token, timing)) === "handed back") {
      runLogger.debug({}, "run handed back: unknown task");
      throw new DelayedError();
    }
    const error = new UnknownTaskRunError(
      job.name,
      job.data.runId,
      shownDurationOf(timing.unknownNameGiveUpDelayInMilliseconds),
    );
    await recordFinalFailure(job, error, "unknown task", attemptOf(job, "executing"), Date.now());
    throw error;
  }
  async function handBackFanOutOfUnknownEvent(job: Job<StoredFanOut>, token: string | undefined): Promise<void> {
    if ((await handBackUnlessGivenUp(job, token, timing)) === "handed back") {
      config.logger.debug({ eventName: job.name, eventId: job.data.eventId }, "fan-out handed back: unknown event");
      throw new DelayedError();
    }
    throw new UnrecoverableError(
      `No worker defined event "${job.name}" within ${shownDurationOf(timing.unknownNameGiveUpDelayInMilliseconds)} after a worker first found event "${job.data.eventId}" unknown.`,
    );
  }
  const logAttemptStalled = async (jobId: string) => {
    const job = (await Job.fromId(worker, jobId)) as Job<StoredRun> | undefined;
    if (job !== undefined && config.tasksByName.has(job.name)) {
      runLoggerOf(job, attemptOf(job, "executing")).warn({}, "attempt stalled");
    }
  };
  async function recordFinalFailure(
    job: Job<StoredRun>,
    error: Error,
    reason: FailureReason,
    attempt: number,
    executionStartedAt: EpochMilliseconds,
  ): Promise<void> {
    const failedAt = new Date();
    await job.updateData({
      ...job.data,
      errors: [...job.data.errors, errorRecordFor(error, attempt, failedAt)],
      reason,
    });
    await keepSavedStepsForFailedRunRetention(client, job).catch(() => undefined);
    runLoggerOf(job, attempt).error(
      { err: error, reason, ...runDurationFieldsOf(job, executionStartedAt, failedAt.getTime()) },
      "run failed",
    );
  }
  const failRunForStalling = (job: Job<StoredRun>) =>
    recordFinalFailure(
      job,
      new StalledRunError(job.data.runId, timing.stallCountThatFailsARun),
      "stalled",
      job.attemptsMade,
      job.processedOn ?? Date.now(),
    );
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
      runLoggerOf(job as Job<StoredRun>, attemptOf(job as Job<StoredRun>, "completed")).info(
        durationFields,
        "run completed",
      );
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
