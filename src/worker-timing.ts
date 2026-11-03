import type { DurationInMilliseconds } from "./durations.js";

export type WorkerTiming = {
  lockDurationInMilliseconds: DurationInMilliseconds;
  stallIntervalInMilliseconds: DurationInMilliseconds;
  stallCountThatFailsARun: number;
  abortedAttemptsGraceInMilliseconds: DurationInMilliseconds;
  concurrencySlotSafetyDelayInMilliseconds: DurationInMilliseconds;
  unknownNameHandBackDelayInMilliseconds: DurationInMilliseconds;
  unknownNameGiveUpDelayInMilliseconds: DurationInMilliseconds;
  testOnlyPauseBeforeParkingInMilliseconds: DurationInMilliseconds;
};

export const DEFAULT_WORKER_TIMING: WorkerTiming = {
  lockDurationInMilliseconds: 2 * 60 * 1000,
  stallIntervalInMilliseconds: 30 * 1000,
  stallCountThatFailsARun: 3,
  abortedAttemptsGraceInMilliseconds: 1000,
  concurrencySlotSafetyDelayInMilliseconds: 2 * 60 * 1000,
  unknownNameHandBackDelayInMilliseconds: 10 * 1000,
  unknownNameGiveUpDelayInMilliseconds: 30 * 60 * 1000,
  testOnlyPauseBeforeParkingInMilliseconds: 0,
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
