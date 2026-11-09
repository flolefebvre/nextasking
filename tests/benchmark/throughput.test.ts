import { expect, test } from "vitest";

import { runsPerSecondOf, throughputLineOf } from "./throughput.js";

test("the runs per second of a measured window are its runs divided by its seconds", () => {
  expect(runsPerSecondOf({ runCount: 50_000, elapsedInMilliseconds: 4_000 })).toBe(12_500);
});

test("the throughput line names the scenario, the runs, the seconds and the rounded runs per second", () => {
  expect(throughputLineOf("one worker at concurrency 50", { runCount: 50_000, elapsedInMilliseconds: 4_731.2 })).toBe(
    "benchmark: one worker at concurrency 50: 50000 runs in 4.73 s, 10568 runs/s",
  );
});
