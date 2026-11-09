import { setImmediate } from "node:timers/promises";
import { describe, expect, test, vi } from "vitest";

import { createRecordingLogger } from "../tests/recording-logger.js";
import type { RunQueue } from "./run-queue.js";
import { bootWorker, finishBootInBackground } from "./worker-boot.js";
import type { Logger } from "./logger.js";
import { CONFIG_DISABLED, INFO_DENIED, maxmemoryPolicySourceAnswering } from "../tests/info-replies.js";

describe("bootWorker", () => {
  test("writes maxmemory-policy unreadable at warn with err, and boots on, when neither command yields the policy", async () => {
    const { logger, lines } = createRecordingLogger();
    const runQueue = {
      client: {
        ...maxmemoryPolicySourceAnswering({
          info: async () => {
            throw INFO_DENIED();
          },
          config: async () => {
            throw CONFIG_DISABLED();
          },
        }),
        zrangebyscore: async () => [],
      },
      bullmqQueue: { toKey: (type: string) => `bull:runs:${type}` },
      syncSchedules: async () => [],
    } as unknown as RunQueue;

    const { isFirstRoundTripAnswered, completion } = await bootWorker({
      runQueue,
      declaredSchedules: [],
      logger,
      sendBuiltInEvent: async () => undefined,
      giveUpAfterInMilliseconds: 1_000,
    });

    await expect(completion).resolves.toBeUndefined();
    expect(isFirstRoundTripAnswered).toBe(true);
    expect(lines).toEqual([
      { level: "warn", fields: { err: expect.any(AggregateError) }, message: "maxmemory-policy unreadable" },
    ]);
  });
});

describe("finishBootInBackground", () => {
  const bootFailure = new Error('maxmemory-policy is "allkeys-lru"');

  function backgroundBootRecordingItsSteps({
    completion,
    onWorkerNotStarted,
    isWorkerRunQueueOpen = () => true,
  }: {
    completion: Promise<void>;
    onWorkerNotStarted?: (error: unknown) => void | Promise<void>;
    isWorkerRunQueueOpen?: () => boolean;
  }) {
    const steps: string[] = [];
    const { logger, lines } = createRecordingLogger();
    const loggerRecordingSteps: Logger = {
      ...logger,
      error: (fields, message) => {
        steps.push(`line: ${message}`);
        logger.error(fields, message);
      },
    };
    const finishing = finishBootInBackground({
      completion,
      isWorkerRunQueueOpen,
      startTheWorker: () => steps.push("worker started"),
      closeWorkerRunQueue: async () => {
        steps.push("run queue closed");
      },
      onWorkerNotStarted:
        onWorkerNotStarted &&
        (async (error) => {
          steps.push("onWorkerNotStarted called");
          await onWorkerNotStarted(error);
          steps.push("onWorkerNotStarted settled");
        }),
      logger: loggerRecordingSteps,
    });
    return { finishing, steps, lines };
  }

  test("starts the worker when the boot completes", async () => {
    const { finishing, steps } = backgroundBootRecordingItsSteps({ completion: Promise.resolve() });

    await finishing;

    expect(steps).toEqual(["worker started"]);
  });

  test("writes worker not started, awaits onWorkerNotStarted called with the error, then closes the run queue", async () => {
    const onWorkerNotStarted = vi.fn(async () => {
      await setImmediate();
    });
    const { finishing, steps, lines } = backgroundBootRecordingItsSteps({
      completion: Promise.reject(bootFailure),
      onWorkerNotStarted,
    });

    await finishing;

    expect(steps).toEqual([
      "line: worker not started",
      "onWorkerNotStarted called",
      "onWorkerNotStarted settled",
      "run queue closed",
    ]);
    expect(onWorkerNotStarted.mock.calls).toEqual([[bootFailure]]);
    expect(lines).toEqual([{ level: "error", fields: { err: bootFailure }, message: "worker not started" }]);
  });

  test.each([
    [
      "throws",
      () => {
        throw new Error("logger flush failed");
      },
    ],
    ["rejects", async () => Promise.reject(new Error("logger flush failed"))],
  ])(
    "writes onWorkerNotStarted failed at error with err when the callback %s, then still closes the run queue",
    async (_, onWorkerNotStarted) => {
      const { finishing, steps, lines } = backgroundBootRecordingItsSteps({
        completion: Promise.reject(bootFailure),
        onWorkerNotStarted,
      });

      await finishing;

      expect(lines).toEqual([
        { level: "error", fields: { err: bootFailure }, message: "worker not started" },
        {
          level: "error",
          fields: { err: expect.objectContaining({ message: "logger flush failed" }) },
          message: "onWorkerNotStarted failed",
        },
      ]);
      expect(steps.at(-1)).toBe("run queue closed");
    },
  );

  test("writes worker not started, then closes the run queue, without a callback", async () => {
    const { finishing, steps } = backgroundBootRecordingItsSteps({ completion: Promise.reject(bootFailure) });

    await finishing;

    expect(steps).toEqual(["line: worker not started", "run queue closed"]);
  });

  test("does nothing when the boot fails after the instance closed the worker's run queue", async () => {
    const onWorkerNotStarted = vi.fn();
    const { finishing, steps, lines } = backgroundBootRecordingItsSteps({
      completion: Promise.reject(bootFailure),
      onWorkerNotStarted,
      isWorkerRunQueueOpen: () => false,
    });

    await finishing;

    expect(steps).toEqual([]);
    expect(lines).toEqual([]);
    expect(onWorkerNotStarted).not.toHaveBeenCalled();
  });

  test("starts no worker when the boot completes after the instance closed the worker's run queue", async () => {
    const { finishing, steps } = backgroundBootRecordingItsSteps({
      completion: Promise.resolve(),
      isWorkerRunQueueOpen: () => false,
    });

    await finishing;

    expect(steps).toEqual([]);
  });
});
