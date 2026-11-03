import { expect } from "vitest";

import { fixtureRecordsOf } from "./fixture-records.js";
import { integrationTest } from "./integration-test.js";
import { waitForLogMessage } from "./recording-logger.js";
import { withRedis } from "./with-redis.js";
import { startWorkerProcess } from "./worker-process.js";
import { createWorkerProcessNextasking } from "./worker-process-nextasking.js";

integrationTest(
  "an attempt whose concurrency slot is gone at renewal is aborted as having lost its lock",
  { timeout: 30_000 },
  async ({ redisUrl, keyPrefix }) => {
    const nextasking = createWorkerProcessNextasking(redisUrl, keyPrefix);
    const workerProcess = await startWorkerProcess({
      redisUrl,
      keyPrefix,
      timing: { lockDurationInMilliseconds: 1000, stallIntervalInMilliseconds: 60_000 },
    });

    try {
      const { runId } = await nextasking.dispatch("hold-concurrency-slot", {});
      await expect
        .poll(() => fixtureRecordsOf(redisUrl, keyPrefix, "hold-concurrency-slot"), { timeout: 10_000 })
        .toHaveLength(1);
      await withRedis(redisUrl, (redis) => redis.del(`${keyPrefix}:runs:concurrency-slots:hold-concurrency-slot`));
      await waitForLogMessage(workerProcess.logLines, "attempt aborted: lock lost");

      await expect
        .poll(() => fixtureRecordsOf(redisUrl, keyPrefix, "hold-concurrency-slot"), { timeout: 10_000 })
        .toEqual([
          { event: "started" },
          { event: "aborted", abortReason: expect.stringContaining("it lost its lock") },
        ]);
      expect(workerProcess.logLines.find((line) => line.message === "attempt aborted: lock lost")?.fields).toEqual({
        task: "hold-concurrency-slot",
        runId,
        attempt: 1,
      });
    } finally {
      await workerProcess.stop();
      await nextasking.close();
    }
  },
);
