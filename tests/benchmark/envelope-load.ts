import type { DurationInMilliseconds } from "../../src/durations.js";

export const ARRIVAL_TICK_IN_MILLISECONDS: DurationInMilliseconds = 100;

export const LOAD_DURATION_IN_MILLISECONDS: DurationInMilliseconds = 60_000;

export const ARRIVAL_TICK_COUNT = LOAD_DURATION_IN_MILLISECONDS / ARRIVAL_TICK_IN_MILLISECONDS;

const SUSTAINED_ARRIVALS_PER_TICK = 1;

const BURST_ARRIVALS_PER_TICK = 10;

const BURSTS: readonly { firstTick: number; tickCount: number }[] = [
  { firstTick: 100, tickCount: 50 },
  { firstTick: 300, tickCount: 50 },
  { firstTick: 500, tickCount: 50 },
];

export function arrivalCountOfTick(tick: number): number {
  const isInABurst = BURSTS.some(({ firstTick, tickCount }) => tick >= firstTick && tick < firstTick + tickCount);
  return isInABurst ? BURST_ARRIVALS_PER_TICK : SUSTAINED_ARRIVALS_PER_TICK;
}
