import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "../close-nextasking-and-check-its-duration.js";
import { createRecordingLogger } from "../recording-logger.js";
import { benchmarkTest } from "./benchmark-test.js";
import { runsPerSecondOf, throughputLineOf } from "./throughput.js";
import type { MeasuredThroughput } from "./throughput.js";
import { unfinishedRunCountIn } from "./unfinished-run-count.js";

const RUN_COUNT = 50_000;

const FLOOR_IN_RUNS_PER_SECOND = 500;

const DRAIN_TIMEOUT_IN_MILLISECONDS = (RUN_COUNT / FLOOR_IN_RUNS_PER_SECOND) * 1000;

const DISPATCHES_IN_FLIGHT = 1000;

const DISPATCH_BUDGET_IN_MILLISECONDS = 30_000;

const COMPLETION_POLL_INTERVAL_IN_MILLISECONDS = 100;

const CONCURRENCY = 50;

benchmarkTest(
  "one worker at concurrency 50 completes 50,000 runs at 500 runs per second or more",
  { timeout: DISPATCH_BUDGET_IN_MILLISECONDS + DRAIN_TIMEOUT_IN_MILLISECONDS + 25_000 },
  async ({ redisUrl, keyPrefix }) => {
    let handledRunCount = 0;
    const noOp = defineTask("no-op", {
      schema: z.object({}),
      handler: async () => {
        handledRunCount += 1;
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger: createRecordingLogger().logger,
      tasks: [noOp],
    });
    try {
      const dispatchStartedAt = performance.now();
      for (let dispatchedRunCount = 0; dispatchedRunCount < RUN_COUNT; dispatchedRunCount += DISPATCHES_IN_FLIGHT) {
        await Promise.all(
          Array.from({ length: Math.min(DISPATCHES_IN_FLIGHT, RUN_COUNT - dispatchedRunCount) }, () =>
            nextasking.dispatch("no-op", {}),
          ),
        );
      }
      const dispatchThroughput: MeasuredThroughput = {
        runCount: RUN_COUNT,
        elapsedInMilliseconds: performance.now() - dispatchStartedAt,
      };

      const workerStartedAt = performance.now();
      await nextasking.startWorker({ concurrency: CONCURRENCY });
      const isEveryRunCompleted = await vi
        .waitUntil(
          async () => handledRunCount === RUN_COUNT && unfinishedRunCountIn(await nextasking.countRuns()) === 0,
          { timeout: DRAIN_TIMEOUT_IN_MILLISECONDS, interval: COMPLETION_POLL_INTERVAL_IN_MILLISECONDS },
        )
        .then(
          () => true,
          () => false,
        );
      const workerThroughput: MeasuredThroughput = {
        runCount: handledRunCount,
        elapsedInMilliseconds: performance.now() - workerStartedAt,
      };
      console.log(
        [
          throughputLineOf(`dispatch before the worker starts`, dispatchThroughput),
          throughputLineOf(`one worker at concurrency ${String(CONCURRENCY)}`, workerThroughput),
        ].join("\n"),
      );

      expect(isEveryRunCompleted, `${String(handledRunCount)} of ${String(RUN_COUNT)} runs completed`).toBe(true);
      expect(runsPerSecondOf(workerThroughput)).toBeGreaterThanOrEqual(FLOOR_IN_RUNS_PER_SECOND);
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);
