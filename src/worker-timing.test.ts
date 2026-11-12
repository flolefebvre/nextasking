import { expect, test } from "vitest";

import { RECORD_VERSION } from "./engine/engine.js";
import { DEFAULT_WORKER_TIMING } from "./worker-timing.js";

test("holds a 2-minute lock, 3 stalls to fail a run, a 1-second abort grace, a 1-second bound on the command client quit, a 2-second bound on the wait for onWorkerStopped, a 10-second hand-back for an unknown name, a 1-second tick, a renewal under 1 minute left, 50 lock tokens per fetch, a 10-second wait after a pass over, and a retention of 24 hours or 1,000 completed and 14 days or 10,000 failed runs, and supports the record version the engine writes", () => {
  expect(DEFAULT_WORKER_TIMING).toEqual({
    lockDurationInMilliseconds: 120_000,
    stallCountThatFailsARun: 3,
    abortedAttemptsGraceInMilliseconds: 1000,
    commandConnectionCloseBoundInMilliseconds: 1000,
    onWorkerStoppedBoundInMilliseconds: 2000,
    unknownNameHandBackDelayInMilliseconds: 10_000,
    tickIntervalInMilliseconds: 1000,
    lockTimeLeftThatTriggersRenewalInMilliseconds: 60_000,
    mostLockTokensPerFetch: 50,
    passOverWaitInMilliseconds: 10_000,
    completedRunRetention: { maxAgeInMilliseconds: 86_400_000, maxRunCount: 1000 },
    failedRunRetention: { maxAgeInMilliseconds: 1_209_600_000, maxRunCount: 10_000 },
    supportedRecordVersion: RECORD_VERSION,
  });
});
