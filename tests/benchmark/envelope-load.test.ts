import { expect, test } from "vitest";

import { ARRIVAL_TICK_COUNT, arrivalCountOfTick } from "./envelope-load.js";

function arrivalCountsPerSecond(): number[] {
  const arrivalCounts: number[] = [];
  for (let tick = 0; tick < ARRIVAL_TICK_COUNT; tick += 1) {
    const second = Math.floor(tick / 10);
    arrivalCounts[second] = (arrivalCounts[second] ?? 0) + arrivalCountOfTick(tick);
  }
  return arrivalCounts;
}

test("the envelope's load is 10 runs a second for 60 seconds, but 100 a second in three bursts of 5 seconds", () => {
  const arrivalCounts = arrivalCountsPerSecond();

  expect(arrivalCounts).toHaveLength(60);
  expect(arrivalCounts.flatMap((arrivalCount, second) => (arrivalCount === 100 ? [second] : []))).toEqual([
    10, 11, 12, 13, 14, 30, 31, 32, 33, 34, 50, 51, 52, 53, 54,
  ]);
  expect(arrivalCounts.filter((arrivalCount) => arrivalCount === 10)).toHaveLength(45);
});

test("a burst spreads its 100 runs a second evenly over its ticks", () => {
  expect([arrivalCountOfTick(99), arrivalCountOfTick(100), arrivalCountOfTick(149), arrivalCountOfTick(150)]).toEqual([
    1, 10, 10, 1,
  ]);
});
