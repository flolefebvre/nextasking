import type { DurationInMilliseconds } from "./durations.js";

const LOWEST_SHARE_OF_THE_DOUBLED_BACKOFF = 0.8;

export function backoffInMillisecondsFor(
  firstBackoffInMilliseconds: DurationInMilliseconds,
  failedAttempt: number,
  randomBetweenZeroAndOne: number,
): DurationInMilliseconds {
  const doubledBackoffInMilliseconds = firstBackoffInMilliseconds * 2 ** (failedAttempt - 1);
  const jitteredShare =
    LOWEST_SHARE_OF_THE_DOUBLED_BACKOFF + (1 - LOWEST_SHARE_OF_THE_DOUBLED_BACKOFF) * randomBetweenZeroAndOne;
  return Math.floor(doubledBackoffInMilliseconds * jitteredShare);
}
