import type { DurationInMilliseconds, EpochMilliseconds } from "./durations.js";
import type { FailureNotice } from "./engine/engine.js";
import type { FailureReason } from "./run-queue.js";

export type AttemptOutcome =
  | { kind: "complete" }
  | { kind: "fail"; reason: FailureReason; error: Error; failureNotice: FailureNotice | undefined }
  | { kind: "retry"; error: Error; retriesLeft: number; backoffInMilliseconds: DurationInMilliseconds }
  | { kind: "suspend"; resumesAt: EpochMilliseconds }
  | { kind: "yield" }
  | { kind: "handBack"; error: Error; failureNotice: FailureNotice | undefined };
