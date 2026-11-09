import type { JobState } from "bullmq";

import type { DurationInMilliseconds, EpochMilliseconds } from "./durations.js";
import type { FailureReason, StoredDelayedRunState, StoredRun } from "./run-queue.js";

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

export function isRunState(value: unknown): value is RunState {
  return RUN_STATES.some((state) => state === value);
}

export type BullmqJobState = JobState | "unknown";

export type BullmqJobSet = "wait" | "prioritized" | "waiting-children" | "delayed" | "active" | "completed" | "failed";

export const BULLMQ_JOB_STATE_BY_SET: Readonly<Record<BullmqJobSet, JobState>> = {
  wait: "waiting",
  prioritized: "prioritized",
  "waiting-children": "waiting-children",
  delayed: "delayed",
  active: "active",
  completed: "completed",
  failed: "failed",
};

export const BULLMQ_SETS_BY_RUN_STATE: Readonly<Record<RunState, readonly BullmqJobSet[]>> = {
  delayed: ["delayed"],
  pending: ["wait", "prioritized", "waiting-children", "delayed"],
  waitingForConcurrencySlot: ["delayed"],
  executing: ["active"],
  suspended: ["delayed"],
  backingOff: ["delayed"],
  completed: ["completed"],
  failed: ["failed"],
};

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

export function runStateOf(
  bullmqState: BullmqJobState,
  delayedRunState: StoredDelayedRunState | undefined,
): RunState | undefined {
  switch (bullmqState) {
    case "waiting":
    case "prioritized":
    case "waiting-children":
      return "pending";
    case "active":
      return "executing";
    case "completed":
    case "failed":
      return bullmqState;
    case "delayed":
      return delayedRunState?.state ?? "delayed";
    default:
      return undefined;
  }
}

export function attemptOf(job: { attemptsMade: number; data: StoredRun }, state: RunState): number {
  if (state === "failed") {
    return job.data.errors.at(-1)?.attempt ?? job.attemptsMade;
  }
  return state === "completed" ? job.attemptsMade : job.attemptsMade + 1;
}
