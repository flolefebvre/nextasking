import { parseDuration } from "./durations.js";
import type { Duration } from "./durations.js";

class AttemptTimeoutError extends Error {
  override readonly name = "AttemptTimeoutError";

  constructor(taskName: string, timeout: Duration) {
    super(`Task "${taskName}" timed out after ${timeout}`);
  }
}

export async function settleWithinTimeout<Outcome>(
  taskName: string,
  timeout: Duration | undefined,
  stretchExecution: Promise<Outcome>,
): Promise<Outcome> {
  if (timeout === undefined) {
    return stretchExecution;
  }
  let timeoutTimer: NodeJS.Timeout | undefined;
  const timeoutExpiry = new Promise<never>((_resolve, reject) => {
    timeoutTimer = setTimeout(() => {
      reject(new AttemptTimeoutError(taskName, timeout));
    }, parseDuration(timeout));
  });
  try {
    return await Promise.race([stretchExecution, timeoutExpiry]);
  } finally {
    clearTimeout(timeoutTimer);
  }
}
