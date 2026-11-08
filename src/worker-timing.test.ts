import { expect, test } from "vitest";

import { DEFAULT_WORKER_TIMING, bullmqMaxStalledCountOf } from "./worker-timing.js";

test("holds a 2-minute lock, a 30-second stall check, 3 stalls to fail a run, a 1-second abort grace, a 2-minute safety delay for waiting runs, and a 10-second hand-back given up after 30 minutes for an unknown name, a 1-second tick, a renewal under 1 minute left, 50 lock tokens per fetch, and a retention of 24 hours or 1,000 completed and 14 days or 10,000 failed runs", () => {
  expect(DEFAULT_WORKER_TIMING).toEqual({
    lockDurationInMilliseconds: 120_000,
    stallIntervalInMilliseconds: 30_000,
    stallCountThatFailsARun: 3,
    abortedAttemptsGraceInMilliseconds: 1000,
    concurrencySlotSafetyDelayInMilliseconds: 120_000,
    unknownNameHandBackDelayInMilliseconds: 10_000,
    unknownNameGiveUpDelayInMilliseconds: 1_800_000,
    testOnlyPauseBeforeParkingInMilliseconds: 0,
    tickIntervalInMilliseconds: 1000,
    lockTimeLeftThatTriggersRenewalInMilliseconds: 60_000,
    mostLockTokensPerFetch: 50,
    completedRunRetention: { maxAgeInMilliseconds: 86_400_000, maxRunCount: 1000 },
    failedRunRetention: { maxAgeInMilliseconds: 1_209_600_000, maxRunCount: 10_000 },
  });
});

test("gives BullMQ one stall less than the count that fails a run, since BullMQ fails once the count exceeds it", () => {
  expect(bullmqMaxStalledCountOf(DEFAULT_WORKER_TIMING)).toBe(2);
});
