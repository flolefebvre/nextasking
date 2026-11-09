import { monitorEventLoopDelay } from "node:perf_hooks";
import { setTimeout as sleep } from "node:timers/promises";

const STALL_SAMPLING_IN_MILLISECONDS = 10;

const NANOSECONDS_PER_MILLISECOND = 1_000_000;

export type MeasuredDuration<Result> = {
  result: Result;
  elapsedInMilliseconds: number;
  longestEventLoopStallInMilliseconds: number;
  elapsedNetOfStallInMilliseconds: number;
};

export async function measuredDurationOf<Result>(call: () => Promise<Result>): Promise<MeasuredDuration<Result>> {
  const eventLoopDelays = monitorEventLoopDelay({ resolution: STALL_SAMPLING_IN_MILLISECONDS });
  eventLoopDelays.enable();
  try {
    const startedAt = performance.now();
    const result = await call();
    const elapsedInMilliseconds = performance.now() - startedAt;
    await sleep(2 * STALL_SAMPLING_IN_MILLISECONDS);
    const longestEventLoopStallInMilliseconds = Math.min(
      Math.max(eventLoopDelays.max / NANOSECONDS_PER_MILLISECOND - STALL_SAMPLING_IN_MILLISECONDS, 0),
      elapsedInMilliseconds,
    );
    return {
      result,
      elapsedInMilliseconds,
      longestEventLoopStallInMilliseconds,
      elapsedNetOfStallInMilliseconds: elapsedInMilliseconds - longestEventLoopStallInMilliseconds,
    };
  } finally {
    eventLoopDelays.disable();
  }
}

export function measuredRejectionOf(call: () => Promise<unknown>): Promise<MeasuredDuration<unknown>> {
  return measuredDurationOf(() =>
    call().then(
      () => new Error("the call resolved"),
      (error: unknown) => error,
    ),
  );
}
