import { NonRetriableError } from "./errors.js";
import type { ErrorRecord, FailureReason } from "./run-queue.js";

export function failureReasonFor(error: Error, retriesLeft: number): FailureReason | undefined {
  if (error instanceof NonRetriableError || error.name === "NonRetriableError") {
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

export function errorRecordFor(error: Error, attempt: number, failedAt: Date): ErrorRecord {
  return {
    attempt,
    name: error.name,
    message: error.message,
    stack: error.stack ?? "",
    failedAt: failedAt.toISOString(),
  };
}
