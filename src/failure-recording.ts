import type { Job } from "bullmq";
import type { Redis } from "ioredis";

import { StalledRunError, errorFromThrownValue, errorRecordFor, failureReasonFor } from "./attempt-errors.js";
import { backoffInMillisecondsFor } from "./backoff.js";
import { backoffOf } from "./definitions.js";
import type { AnyTaskDefinition } from "./definitions.js";
import { parseDuration } from "./durations.js";
import type { DurationInMilliseconds, EpochMilliseconds } from "./durations.js";
import { saveRunDataIfLockIsHeld } from "./job-lock.js";
import type { Logger } from "./logger.js";
import { keepSavedStepsForFailedRunRetention } from "./redis-step-store.js";
import { storedFanOutAfterFinalFailure, storedRunAfterFinalFailure } from "./run-queue.js";
import type { FailureReason, StoredFanOut, StoredRun } from "./run-queue.js";

export const NO_RETRY: DurationInMilliseconds = -1;

export type RunDurationFields = {
  runDurationInMilliseconds: DurationInMilliseconds;
  executionDurationInMilliseconds: DurationInMilliseconds;
};

export function runDurationFieldsOf(
  job: { timestamp: EpochMilliseconds },
  executionStartedAt: EpochMilliseconds,
  runEndedAt: EpochMilliseconds,
): RunDurationFields {
  return {
    runDurationInMilliseconds: runEndedAt - job.timestamp,
    executionDurationInMilliseconds: runEndedAt - executionStartedAt,
  };
}

export type FailedRunAttempt = {
  client: Redis;
  job: Job<StoredRun>;
  token: string | undefined;
  task: AnyTaskDefinition;
  attempt: number;
  executionStartedAt: EpochMilliseconds;
  runLogger: Logger;
};

export type RecordedAttemptFailure = {
  error: Error;
  backoffInMilliseconds: DurationInMilliseconds;
};

export async function recordFailedAttempt(
  { client, job, token, task, attempt, executionStartedAt, runLogger }: FailedRunAttempt,
  thrownValue: unknown,
): Promise<RecordedAttemptFailure> {
  const error = errorFromThrownValue(thrownValue);
  const failedAt = new Date();
  const retriesLeft = (job.opts.attempts ?? 1) - attempt;
  const reason = failureReasonFor(error, retriesLeft);
  if (reason !== undefined) {
    await recordFinalFailure(client, job, { error, reason, attempt, executionStartedAt, failedAt, runLogger }, (data) =>
      saveRunDataIfLockIsHeld(client, job, token, data),
    );
    return { error, backoffInMilliseconds: NO_RETRY };
  }
  const backoffInMilliseconds = backoffInMillisecondsFor(parseDuration(backoffOf(task)), attempt, Math.random());
  await saveRunDataIfLockIsHeld(client, job, token, {
    ...job.data,
    errors: [...job.data.errors, errorRecordFor(error, attempt, failedAt)],
    delayedRunState: {
      state: "backingOff",
      nextAttemptAt: new Date(failedAt.getTime() + backoffInMilliseconds).toISOString(),
    },
  });
  runLogger.warn({ err: error, retriesLeft, backoffInMilliseconds }, "attempt failed");
  return { error, backoffInMilliseconds };
}

export type FinalFailure = {
  error: Error;
  reason: FailureReason;
  attempt: number;
  executionStartedAt: EpochMilliseconds;
  failedAt: Date;
  runLogger: Logger;
};

export async function recordFinalFailure(
  client: Redis,
  job: Job<StoredRun>,
  { error, reason, attempt, executionStartedAt, failedAt, runLogger }: FinalFailure,
  saveRunData: (data: StoredRun) => Promise<void>,
): Promise<void> {
  await saveRunData(storedRunAfterFinalFailure(job.data, errorRecordFor(error, attempt, failedAt), reason));
  await keepSavedStepsForFailedRunRetention(client, job).catch(() => undefined);
  runLogger.error(
    { err: error, reason, ...runDurationFieldsOf(job, executionStartedAt, failedAt.getTime()) },
    "run failed",
  );
}

export async function recordFanOutFailure(job: Job<StoredFanOut>, error: Error, logger: Logger): Promise<void> {
  const failedAt = new Date(job.finishedOn ?? Date.now());
  await job.updateData(storedFanOutAfterFinalFailure(job.data, errorRecordFor(error, job.attemptsMade, failedAt)));
  logger.error({ eventName: job.name, eventId: job.data.eventId, err: error }, "fan-out failed");
}

export async function recordRunFailedForStalling(
  client: Redis,
  job: Job<StoredRun>,
  stallCountThatFailsARun: number,
  runLogger: Logger,
): Promise<void> {
  await recordFinalFailure(
    client,
    job,
    {
      error: new StalledRunError(job.data.runId, stallCountThatFailsARun),
      reason: "stalled",
      attempt: job.attemptsMade,
      executionStartedAt: job.processedOn ?? Date.now(),
      failedAt: new Date(),
      runLogger,
    },
    (data) => job.updateData(data),
  );
}
