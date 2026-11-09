import type { DurationInMilliseconds } from "./durations.js";

export type RedisOutage = {
  isRedisAway: (error: unknown) => boolean;
  waitForRedis: (awayError: unknown, stopSignal?: AbortSignal) => Promise<void>;
};

export type OutageWaitOptions = {
  callAbortSignal?: AbortSignal;
  resendStopSignal?: AbortSignal;
};

export function untilAborted<Value>(promise: Promise<Value>, abortSignal: AbortSignal | undefined): Promise<Value> {
  if (abortSignal === undefined) {
    return promise;
  }
  return new Promise<Value>((resolve, reject) => {
    const rejectWithAbortReason = () => {
      reject(abortSignal.reason as Error);
    };
    abortSignal.addEventListener("abort", rejectWithAbortReason, { once: true });
    promise.then(
      (value) => {
        abortSignal.removeEventListener("abort", rejectWithAbortReason);
        resolve(value);
      },
      (error: unknown) => {
        abortSignal.removeEventListener("abort", rejectWithAbortReason);
        reject(error as Error);
      },
    );
    if (abortSignal.aborted) {
      rejectWithAbortReason();
    }
  });
}

export function delayUnlessAborted(
  delayInMilliseconds: DurationInMilliseconds,
  abortSignal: AbortSignal | undefined,
): Promise<void> {
  return new Promise<void>((resolve) => {
    const endDelay = () => {
      clearTimeout(delayTimer);
      abortSignal?.removeEventListener("abort", endDelay);
      resolve();
    };
    const delayTimer = setTimeout(endDelay, delayInMilliseconds);
    abortSignal?.addEventListener("abort", endDelay);
    if (abortSignal?.aborted === true) {
      endDelay();
    }
  });
}

export async function answeredDespiteOutage<Answer>(
  redisOutage: RedisOutage,
  call: () => Promise<Answer>,
  { callAbortSignal, resendStopSignal }: OutageWaitOptions = {},
): Promise<Answer> {
  const waitEndSignals = [callAbortSignal, resendStopSignal].filter((signal) => signal !== undefined);
  const waitEndSignal = waitEndSignals.length === 0 ? undefined : AbortSignal.any(waitEndSignals);
  for (;;) {
    callAbortSignal?.throwIfAborted();
    try {
      return await untilAborted(call(), callAbortSignal);
    } catch (error) {
      if (waitEndSignal?.aborted === true || !redisOutage.isRedisAway(error)) {
        throw error;
      }
      await untilAborted(redisOutage.waitForRedis(error, waitEndSignal), waitEndSignal);
    }
  }
}
