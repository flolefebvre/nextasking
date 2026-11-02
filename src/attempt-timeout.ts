import { parseDuration } from "./durations.js";
import type { Duration } from "./durations.js";

class AttemptTimeoutError extends Error {
  override readonly name = "AttemptTimeoutError";

  constructor(taskName: string, timeout: Duration) {
    super(`Task "${taskName}" timed out after ${timeout}`);
  }
}

export async function settleWithinTimeout(
  taskName: string,
  timeout: Duration | undefined,
  attemptExecution: Promise<void>,
): Promise<void> {
  if (timeout === undefined) {
    return attemptExecution;
  }
  let timeoutTimer: NodeJS.Timeout | undefined;
  const timeoutExpiry = new Promise<never>((_resolve, reject) => {
    timeoutTimer = setTimeout(() => {
      reject(new AttemptTimeoutError(taskName, timeout));
    }, parseDuration(timeout));
  });
  try {
    await Promise.race([attemptExecution, timeoutExpiry]);
  } finally {
    clearTimeout(timeoutTimer);
  }
}
