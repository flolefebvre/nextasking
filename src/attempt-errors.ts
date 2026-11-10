import { NonRetriableError, StepMisuseError } from "./errors.js";
import type { RunId } from "./ids.js";
import type { AttemptErrorReport } from "./engine/engine.js";
import type { FailureReason } from "./run-states.js";

export class StalledRunError extends Error {
  override readonly name = "StalledRunError";

  constructor(runId: RunId, stallCount: number) {
    super(
      `Run "${runId}" stalled ${String(stallCount)} times with no step saved in between: each attempt lost its lock, from a crash, a kill or an event loop blocked past the lock duration.`,
    );
  }
}

export class UnknownTaskRunError extends Error {
  override readonly name = "UnknownTaskRunError";

  constructor(taskName: string, runId: RunId, giveUpDelay: string) {
    super(
      `No worker defined task "${taskName}" within ${giveUpDelay} after a worker first found run "${runId}" unknown.`,
    );
  }
}

const NON_RETRIABLE_ERROR_NAMES = new Set(["NonRetriableError", "StepMisuseError"]);

export function failureReasonFor(error: Error, retriesLeft: number): FailureReason | undefined {
  if (
    error instanceof NonRetriableError ||
    error instanceof StepMisuseError ||
    NON_RETRIABLE_ERROR_NAMES.has(error.name)
  ) {
    return "non-retriable";
  }
  return retriesLeft > 0 ? undefined : "retries exhausted";
}

export function errorFromThrownValue(thrownValue: unknown): Error {
  return thrownValue instanceof Error ? thrownValue : new Error(stringFormOf(thrownValue));
}

function stringFormOf(value: unknown): string {
  try {
    return String(value);
  } catch {
    return Object.prototype.toString.call(value);
  }
}

export function attemptErrorReportOf(error: Error, attempt: number): AttemptErrorReport {
  return {
    attempt,
    name: error.name,
    message: error.message,
    stack: error.stack ?? "",
  };
}
