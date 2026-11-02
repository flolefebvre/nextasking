import { describe, expect, test } from "vitest";

import { backoffInMillisecondsFor } from "./backoff.js";

const THIRTY_SECONDS_IN_MILLISECONDS = 30_000;

const RANDOM_JUST_BELOW_ONE = 0.999_999;

describe("backoffInMillisecondsFor", () => {
  test.each([
    [1, 30_000],
    [2, 60_000],
    [3, 120_000],
    [4, 240_000],
    [5, 480_000],
  ])("waits at most the doubled value before retry %i: %i ms", (attempt, expectedMaximum) => {
    const backoffInMilliseconds = backoffInMillisecondsFor(
      THIRTY_SECONDS_IN_MILLISECONDS,
      attempt,
      RANDOM_JUST_BELOW_ONE,
    );

    expect(backoffInMilliseconds).toBeLessThanOrEqual(expectedMaximum);
    expect(backoffInMilliseconds).toBeGreaterThanOrEqual(expectedMaximum - 1);
  });

  test.each([
    [1, 24_000],
    [2, 48_000],
    [3, 96_000],
    [4, 192_000],
    [5, 384_000],
  ])("waits 80%% of the doubled value before retry %i at the lowest jitter: %i ms", (attempt, expectedMinimum) => {
    expect(backoffInMillisecondsFor(THIRTY_SECONDS_IN_MILLISECONDS, attempt, 0)).toBe(expectedMinimum);
  });

  test("spreads the wait across the jitter range", () => {
    expect(backoffInMillisecondsFor(1000, 1, 0.5)).toBe(900);
  });

  test("returns a whole number of milliseconds", () => {
    expect(Number.isInteger(backoffInMillisecondsFor(1000, 2, 0.123_456))).toBe(true);
  });

  test("never waits for a first backoff of zero", () => {
    expect(backoffInMillisecondsFor(0, 3, 0.7)).toBe(0);
  });
});
