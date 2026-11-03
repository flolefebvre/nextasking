import type { JobState } from "bullmq";

import type { DurationInMilliseconds, EpochMilliseconds } from "./durations.js";
import type { FailureReason, StoredRun } from "./run-queue.js";

export type RunStateFields =
  | { state: "delayed"; startsAt: Date }
  | { state: "pending" }
  | { state: "waitingForConcurrencySlot" }
  | { state: "executing" }
  | { state: "suspended"; resumesAt: Date }
  | { state: "backingOff"; nextAttemptAt: Date }
  | { state: "completed" }
  | { state: "failed"; reason: FailureReason };

export type RunState = RunStateFields["state"];

export const RUN_STATES: readonly RunState[] = [
  "delayed",
  "pending",
  "waitingForConcurrencySlot",
  "executing",
  "suspended",
  "backingOff",
  "completed",
  "failed",
];

export type BullmqJobState = JobState | "unknown";

export type RunJobSnapshot = {
  bullmqState: BullmqJobState;
  storedRun: StoredRun;
  dispatchedAt: EpochMilliseconds;
  delayInMilliseconds: DurationInMilliseconds;
};

const FALLBACK_FAILURE_REASON: FailureReason = "retries exhausted";

export function runStateFieldsOf(snapshot: RunJobSnapshot): RunStateFields | undefined {
  const { storedRun } = snapshot;
  switch (snapshot.bullmqState) {
    case "waiting":
    case "prioritized":
    case "waiting-children":
      return { state: "pending" };
    case "active":
      return { state: "executing" };
    case "completed":
      return { state: "completed" };
    case "failed":
      return { state: "failed", reason: storedRun.reason ?? FALLBACK_FAILURE_REASON };
    case "delayed":
      return delayedRunStateFieldsOf(snapshot);
    default:
      return undefined;
  }
}

function delayedRunStateFieldsOf({ storedRun, dispatchedAt, delayInMilliseconds }: RunJobSnapshot): RunStateFields {
  const marker = storedRun.delayedRunState;
  if (marker === undefined) {
    return { state: "delayed", startsAt: new Date(dispatchedAt + delayInMilliseconds) };
  }
  switch (marker.state) {
    case "suspended":
      return { state: "suspended", resumesAt: new Date(marker.resumesAt) };
    case "backingOff":
      return { state: "backingOff", nextAttemptAt: new Date(marker.nextAttemptAt) };
    default:
      return { state: marker.state };
  }
}

export function attemptOf(job: { attemptsMade: number; data: StoredRun }, state: RunState): number {
  if (state === "failed") {
    return job.data.errors.at(-1)?.attempt ?? job.attemptsMade;
  }
  return state === "completed" ? job.attemptsMade : job.attemptsMade + 1;
}
