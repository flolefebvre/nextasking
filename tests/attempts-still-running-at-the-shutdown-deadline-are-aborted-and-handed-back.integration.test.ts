import { expect } from "vitest";

import { fixtureRecordsOf } from "./fixture-records.js";
import { integrationTest } from "./integration-test.js";
import { runQueuePosition } from "./run-queue-position.js";
import { storedRun } from "./stored-run.js";
import { startWorkerProcess } from "./worker-process.js";
import { createWorkerProcessNextasking } from "./worker-process-nextasking.js";

integrationTest(
  "at the shutdown deadline a worker aborts the attempts still running, hands back those that stop, and exits 0",
  { timeout: 30_000 },
  async ({ redisUrl, keyPrefix }) => {
    const nextasking = createWorkerProcessNextasking(redisUrl, keyPrefix);
    const workerProcess = await startWorkerProcess({
      redisUrl,
      keyPrefix,
      startWorkerOptions: { shutdownTimeout: "1s" },
    });

    try {
      const { runId } = await nextasking.dispatch("wait-for-abort", {});
      await expect
        .poll(() => fixtureRecordsOf(redisUrl, keyPrefix, "wait-for-abort"), { timeout: 10_000 })
        .toEqual([{ event: "started" }]);
      const signalSentAt = Date.now();
      workerProcess.kill("SIGTERM");
      const exit = await workerProcess.exited;
      const shutdownDurationInMilliseconds = Date.now() - signalSentAt;
      const records = await fixtureRecordsOf(redisUrl, keyPrefix, "wait-for-abort");
      const run = await storedRun(redisUrl, keyPrefix, "wait-for-abort", runId);
      const position = await runQueuePosition(redisUrl, keyPrefix, "wait-for-abort", runId);

      expect(exit).toEqual({ code: 0, signal: null });
      expect(shutdownDurationInMilliseconds).toBeGreaterThanOrEqual(1000);
      expect(shutdownDurationInMilliseconds).toBeLessThan(5000);
      expect(records).toEqual([
        { event: "started" },
        {
          event: "aborted",
          abortReason: `The attempt of run "${runId}" was aborted: the worker reached its shutdown deadline while the attempt was running.`,
        },
      ]);
      expect(workerProcess.logLines.filter((line) => line.message !== "run yielded at shutdown")).toEqual([
        {
          level: "info",
          fields: { task: "block-event-loop-on-schedule", cron: "0 0 1 1 *", timezone: "UTC" },
          message: "schedule added",
        },
        { level: "info", fields: {}, message: "worker started" },
        { level: "debug", fields: { task: "wait-for-abort", runId, attempt: 1 }, message: "attempt started" },
        { level: "warn", fields: {}, message: "attempts aborted at shutdown deadline" },
        { level: "info", fields: {}, message: "worker stopped" },
      ]);
      expect(workerProcess.logLines).toContainEqual({
        level: "debug",
        fields: { task: "wait-for-abort", runId, attempt: 1 },
        message: "run yielded at shutdown",
      });
      expect(run).toMatchObject({ errors: [] });
      expect(position).toEqual({ isWaiting: true, attemptsMade: 0, stallCount: 0 });
    } finally {
      await workerProcess.stop();
      await nextasking.close();
    }
  },
);
