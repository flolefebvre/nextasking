import { expect, test } from "vitest";

import { DEFAULT_WORKER_TIMING, bullmqMaxStalledCountOf } from "./worker-timing.js";

test("holds a 2-minute lock, a 30-second stall check, 3 stalls to fail a run and a 1-second abort grace", () => {
  expect(DEFAULT_WORKER_TIMING).toEqual({
    lockDurationInMilliseconds: 120_000,
    stallIntervalInMilliseconds: 30_000,
    stallCountThatFailsARun: 3,
    abortedAttemptsGraceInMilliseconds: 1000,
  });
});

test("gives BullMQ one stall less than the count that fails a run, since BullMQ fails once the count exceeds it", () => {
  expect(bullmqMaxStalledCountOf(DEFAULT_WORKER_TIMING)).toBe(2);
});
