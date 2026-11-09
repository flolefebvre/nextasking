import type { DurationInMilliseconds } from "./durations.js";
import type { RetentionLimit } from "./engine/engine.js";
import { MOST_LOCK_TOKENS_PER_FETCH } from "./engine/fetch-script.js";

export type WorkerTiming = {
  lockDurationInMilliseconds: DurationInMilliseconds;
  stallIntervalInMilliseconds: DurationInMilliseconds;
  stallCountThatFailsARun: number;
  abortedAttemptsGraceInMilliseconds: DurationInMilliseconds;
  commandClientQuitBoundInMilliseconds: DurationInMilliseconds;
  concurrencySlotSafetyDelayInMilliseconds: DurationInMilliseconds;
  unknownNameHandBackDelayInMilliseconds: DurationInMilliseconds;
  unknownNameGiveUpDelayInMilliseconds: DurationInMilliseconds;
  testOnlyPauseBeforeParkingInMilliseconds: DurationInMilliseconds;
  tickIntervalInMilliseconds: DurationInMilliseconds;
  lockTimeLeftThatTriggersRenewalInMilliseconds: DurationInMilliseconds;
  mostLockTokensPerFetch: number;
  passOverWaitInMilliseconds: DurationInMilliseconds;
  completedRunRetention: RetentionLimit;
  failedRunRetention: RetentionLimit;
};

const DAY_IN_MILLISECONDS: DurationInMilliseconds = 24 * 60 * 60 * 1000;

export const DEFAULT_WORKER_TIMING: WorkerTiming = {
  lockDurationInMilliseconds: 2 * 60 * 1000,
  stallIntervalInMilliseconds: 30 * 1000,
  stallCountThatFailsARun: 3,
  abortedAttemptsGraceInMilliseconds: 1000,
  commandClientQuitBoundInMilliseconds: 1000,
  concurrencySlotSafetyDelayInMilliseconds: 2 * 60 * 1000,
  unknownNameHandBackDelayInMilliseconds: 10 * 1000,
  unknownNameGiveUpDelayInMilliseconds: 30 * 60 * 1000,
  testOnlyPauseBeforeParkingInMilliseconds: 0,
  tickIntervalInMilliseconds: 1000,
  lockTimeLeftThatTriggersRenewalInMilliseconds: 60 * 1000,
  mostLockTokensPerFetch: MOST_LOCK_TOKENS_PER_FETCH,
  passOverWaitInMilliseconds: 10 * 1000,
  completedRunRetention: { maxAgeInMilliseconds: DAY_IN_MILLISECONDS, maxRunCount: 1000 },
  failedRunRetention: { maxAgeInMilliseconds: 14 * DAY_IN_MILLISECONDS, maxRunCount: 10_000 },
};

let currentWorkerTiming: WorkerTiming = DEFAULT_WORKER_TIMING;

export function workerTiming(): WorkerTiming {
  return currentWorkerTiming;
}

export function overrideWorkerTiming(timingOverride: Partial<WorkerTiming>): void {
  currentWorkerTiming = { ...DEFAULT_WORKER_TIMING, ...timingOverride };
}

export function bullmqMaxStalledCountOf(timing: WorkerTiming): number {
  return timing.stallCountThatFailsARun - 1;
}
