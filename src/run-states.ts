export type FailureReason = "retries exhausted" | "non-retriable" | "stalled" | "unknown task";

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
