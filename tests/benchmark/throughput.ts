import type { DurationInMilliseconds } from "../../src/durations.js";

export type RunsPerSecond = number;

export type MeasuredThroughput = { runCount: number; elapsedInMilliseconds: DurationInMilliseconds };

const MILLISECONDS_PER_SECOND = 1000;

export function runsPerSecondOf({ runCount, elapsedInMilliseconds }: MeasuredThroughput): RunsPerSecond {
  return (runCount * MILLISECONDS_PER_SECOND) / elapsedInMilliseconds;
}

export function throughputLineOf(scenarioName: string, measuredThroughput: MeasuredThroughput): string {
  const elapsedInSeconds = measuredThroughput.elapsedInMilliseconds / MILLISECONDS_PER_SECOND;
  return `benchmark: ${scenarioName}: ${String(measuredThroughput.runCount)} runs in ${elapsedInSeconds.toFixed(2)} s, ${String(Math.round(runsPerSecondOf(measuredThroughput)))} runs/s`;
}
