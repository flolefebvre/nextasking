import type { DurationInMilliseconds } from "./durations.js";
import { RECORD_VERSION } from "./engine/engine.js";
import type { RetentionLimit } from "./engine/engine.js";
import { MOST_LOCK_TOKENS_PER_FETCH } from "./engine/fetch-script.js";

export type WorkerTiming = {
  lockDurationInMilliseconds: DurationInMilliseconds;
  stallCountThatFailsARun: number;
  abortedAttemptsGraceInMilliseconds: DurationInMilliseconds;
  commandConnectionCloseBoundInMilliseconds: DurationInMilliseconds;
  unknownNameHandBackDelayInMilliseconds: DurationInMilliseconds;
  tickIntervalInMilliseconds: DurationInMilliseconds;
  lockTimeLeftThatTriggersRenewalInMilliseconds: DurationInMilliseconds;
  mostLockTokensPerFetch: number;
  passOverWaitInMilliseconds: DurationInMilliseconds;
  completedRunRetention: RetentionLimit;
  failedRunRetention: RetentionLimit;
  supportedRecordVersion: number;
};

const DAY_IN_MILLISECONDS: DurationInMilliseconds = 24 * 60 * 60 * 1000;

export const DEFAULT_WORKER_TIMING: WorkerTiming = {
  lockDurationInMilliseconds: 2 * 60 * 1000,
  stallCountThatFailsARun: 3,
  abortedAttemptsGraceInMilliseconds: 1000,
  commandConnectionCloseBoundInMilliseconds: 1000,
  unknownNameHandBackDelayInMilliseconds: 10 * 1000,
  tickIntervalInMilliseconds: 1000,
  lockTimeLeftThatTriggersRenewalInMilliseconds: 60 * 1000,
  mostLockTokensPerFetch: MOST_LOCK_TOKENS_PER_FETCH,
  passOverWaitInMilliseconds: 10 * 1000,
  completedRunRetention: { maxAgeInMilliseconds: DAY_IN_MILLISECONDS, maxRunCount: 1000 },
  failedRunRetention: { maxAgeInMilliseconds: 14 * DAY_IN_MILLISECONDS, maxRunCount: 10_000 },
  supportedRecordVersion: RECORD_VERSION,
};

let currentWorkerTiming: WorkerTiming = DEFAULT_WORKER_TIMING;

export function workerTiming(): WorkerTiming {
  return currentWorkerTiming;
}

export function overrideWorkerTiming(timingOverride: Partial<WorkerTiming>): void {
  currentWorkerTiming = { ...DEFAULT_WORKER_TIMING, ...timingOverride };
}
