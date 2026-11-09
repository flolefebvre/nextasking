import { afterEach, describe, expect, test, vi } from "vitest";

import { answeredDespiteOutage, delayUnlessAborted, untilAborted } from "./redis-outage.js";
import type { RedisOutage } from "./redis-outage.js";

const REDIS_AWAY = new Error("Redis is away");

type RecordingRedisOutage = RedisOutage & { awayErrors: unknown[]; answeredCount: () => number; endOutage: () => void };

function recordingRedisOutage(): RecordingRedisOutage {
  const awayErrors: unknown[] = [];
  let answeredCount = 0;
  let endOutage = () => undefined as void;
  return {
    isRedisAway: (error) => error === REDIS_AWAY,
    waitForRedis: (awayError) => {
      awayErrors.push(awayError);
      return new Promise<void>((resolve) => {
        endOutage = resolve;
      });
    },
    noteRedisAnswered: () => {
      answeredCount += 1;
    },
    awayErrors,
    answeredCount: () => answeredCount,
    endOutage: () => {
      endOutage();
    },
  };
}

function unhandledRejectionsDuring(): { unhandledRejections: unknown[]; stopRecording: () => void } {
  const unhandledRejections: unknown[] = [];
  const recordUnhandledRejection = (reason: unknown) => {
    unhandledRejections.push(reason);
  };
  process.on("unhandledRejection", recordUnhandledRejection);
  return {
    unhandledRejections,
    stopRecording: () => {
      process.off("unhandledRejection", recordUnhandledRejection);
    },
  };
}

afterEach(() => {
  vi.useRealTimers();
});

describe("untilAborted", () => {
  test("rejects with the abort reason on abort, and a rejection of the call arriving later is handled", async () => {
    const recording = unhandledRejectionsDuring();
    try {
      const abortController = new AbortController();
      let rejectCall: (error: Error) => void = () => undefined;
      const call = new Promise<string>((_resolve, reject) => {
        rejectCall = reject;
      });
      const abortReason = new Error("lock lost");

      const waited = untilAborted(call, abortController.signal);
      abortController.abort(abortReason);
      const outcome = await waited.catch((error: unknown) => error);
      rejectCall(new Error("Connection is closed."));
      await new Promise((resolve) => setImmediate(resolve));

      expect(outcome).toBe(abortReason);
      expect(recording.unhandledRejections).toEqual([]);
    } finally {
      recording.stopRecording();
    }
  });

  test("resolves with the call's value and leaves no abort listener behind", async () => {
    const abortController = new AbortController();
    const addSpy = vi.spyOn(abortController.signal, "addEventListener");
    const removeSpy = vi.spyOn(abortController.signal, "removeEventListener");

    const value = await untilAborted(Promise.resolve("answered"), abortController.signal);

    expect(value).toBe("answered");
    expect(removeSpy).toHaveBeenCalledWith("abort", addSpy.mock.calls[0]?.[1]);
  });

  test("rejects at once when the signal is already aborted", async () => {
    const abortController = new AbortController();
    abortController.abort(new Error("stopped"));

    await expect(untilAborted(new Promise<void>(() => undefined), abortController.signal)).rejects.toThrow("stopped");
  });
});

describe("delayUnlessAborted", () => {
  test("resolves after its delay, or at once on abort", async () => {
    vi.useFakeTimers();
    const abortController = new AbortController();
    let isDelayOver = false;
    let isAbortedDelayOver = false;
    void delayUnlessAborted(1000, undefined).then(() => {
      isDelayOver = true;
    });
    void delayUnlessAborted(1000, abortController.signal).then(() => {
      isAbortedDelayOver = true;
    });

    abortController.abort();
    await vi.advanceTimersByTimeAsync(999);
    const isDelayOverBefore = isDelayOver;
    await vi.advanceTimersByTimeAsync(1);

    expect(isAbortedDelayOver).toBe(true);
    expect(isDelayOverBefore).toBe(false);
    expect(isDelayOver).toBe(true);
  });
});

describe("answeredDespiteOutage", () => {
  test("resends the same call after waiting for Redis on an away error, and notes Redis answered once it answers", async () => {
    const redisOutage = recordingRedisOutage();
    const call = vi.fn<() => Promise<string>>().mockRejectedValueOnce(REDIS_AWAY).mockResolvedValueOnce("answered");

    const answer = answeredDespiteOutage(redisOutage, call);
    await vi.waitFor(() => {
      expect(redisOutage.awayErrors).toEqual([REDIS_AWAY]);
    });
    const callCountDuringTheOutage = call.mock.calls.length;
    redisOutage.endOutage();

    expect(await answer).toBe("answered");
    expect(callCountDuringTheOutage).toBe(1);
    expect(call).toHaveBeenCalledTimes(2);
    expect(redisOutage.answeredCount()).toBe(1);
  });

  test("rethrows an error that is not Redis being away, without waiting or noting an answer", async () => {
    const redisOutage = recordingRedisOutage();
    const unreadableReply = new Error("The fetch script sent a reply the engine cannot read");

    await expect(answeredDespiteOutage(redisOutage, () => Promise.reject(unreadableReply))).rejects.toBe(
      unreadableReply,
    );
    expect(redisOutage.awayErrors).toEqual([]);
    expect(redisOutage.answeredCount()).toBe(0);
  });

  test("does not note Redis answered for an answer that is not complete", async () => {
    const redisOutage = recordingRedisOutage();

    await answeredDespiteOutage(redisOutage, () => Promise.resolve("refused write"), { isAnswerComplete: () => false });

    expect(redisOutage.answeredCount()).toBe(0);
  });

  test("ends a wait for Redis when its stop signal is aborted", async () => {
    const redisOutage = recordingRedisOutage();
    const stopController = new AbortController();
    const stopReason = new Error("stopped");

    const answer = answeredDespiteOutage(redisOutage, () => Promise.reject(REDIS_AWAY), {
      stopSignal: stopController.signal,
    });
    await vi.waitFor(() => {
      expect(redisOutage.awayErrors).toHaveLength(1);
    });
    stopController.abort(stopReason);

    await expect(answer).rejects.toBe(stopReason);
  });
});
