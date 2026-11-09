import type { DurationInMilliseconds } from "./durations.js";

export async function isSettledWithin(
  settlement: Promise<unknown>,
  timeoutInMilliseconds: DurationInMilliseconds,
): Promise<boolean> {
  let timeoutTimer: NodeJS.Timeout | undefined;
  const timeoutExpiry = new Promise<false>((resolve) => {
    timeoutTimer = setTimeout(() => {
      resolve(false);
    }, timeoutInMilliseconds);
  });
  try {
    return await Promise.race([settlement.then(() => true), timeoutExpiry]);
  } finally {
    clearTimeout(timeoutTimer);
  }
}
