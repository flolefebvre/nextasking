import { parseDuration } from "./durations.js";
import type { Duration, DurationInMilliseconds } from "./durations.js";

export function timeoutInMillisecondsOf(callName: string, timeout: Duration): DurationInMilliseconds {
  try {
    return parseDuration(timeout);
  } catch (error) {
    throw new Error(`Invalid ${callName} timeout: ${(error as Error).message}`, { cause: error });
  }
}
