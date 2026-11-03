import { expect, vi } from "vitest";

import { integrationTest } from "./integration-test.js";
import { limitedExecutionsOf } from "./limited-executions.js";
import { startWorkerProcess } from "./worker-process.js";
import { createWorkerProcessNextasking } from "./worker-process-nextasking.js";

const RUN_COUNT = 8;

integrationTest(
  "a slot released while a run is between joining the waiting list and parking still wakes a waiting run, long before the safety delay",
  { timeout: 30_000 },
  async ({ redisUrl, keyPrefix }) => {
    const nextasking = createWorkerProcessNextasking(redisUrl, keyPrefix);
    const workerProcess = await startWorkerProcess({
      redisUrl,
      keyPrefix,
      timing: {
        concurrencySlotSafetyDelayInMilliseconds: 60_000,
        testOnlyPauseBeforeParkingInMilliseconds: 200,
      },
    });

    try {
      const runIds = await Promise.all(
        Array.from({ length: RUN_COUNT }, async () => {
          const { runId } = await nextasking.dispatch("one-per-key", {
            concurrencyKey: "account-7",
            workInMilliseconds: 20,
          });
          return runId;
        }),
      );

      await vi.waitFor(
        async () => {
          const executions = await limitedExecutionsOf(redisUrl, keyPrefix, ["one-per-key"], new Map());
          expect(executions.filter((execution) => execution.endedAt !== undefined)).toHaveLength(runIds.length);
        },
        { timeout: 20_000, interval: 200 },
      );
    } finally {
      await workerProcess.stop();
      await nextasking.close();
    }
  },
);
