import { expect } from "vitest";

import { fixtureRecordsOf } from "./fixture-records.js";
import { integrationTest } from "./integration-test.js";
import { waitForLogMessage } from "./recording-logger.js";
import { storedRun } from "./stored-run.js";
import { savedSteps } from "./saved-steps.js";
import { startWorkerProcess } from "./worker-process.js";
import { createWorkerProcessNextasking } from "./worker-process-nextasking.js";

integrationTest(
  "on SIGTERM, repeats ignored, a worker lets the step in flight save, sends the run back to pending at the next step, and exits 0",
  { timeout: 60_000 },
  async ({ redisUrl, keyPrefix }) => {
    const nextasking = createWorkerProcessNextasking(redisUrl, keyPrefix);
    const shuttingDownProcess = await startWorkerProcess({ redisUrl, keyPrefix });
    let resumingProcess: Awaited<ReturnType<typeof startWorkerProcess>> | undefined;

    try {
      const { runId } = await nextasking.dispatch("wait-then-record", { waitInMilliseconds: 1500 });
      await expect
        .poll(() => fixtureRecordsOf(redisUrl, keyPrefix, "wait-then-record"), { timeout: 10_000 })
        .toEqual([{ step: "wait", attempt: 1 }]);
      shuttingDownProcess.kill("SIGTERM");
      shuttingDownProcess.kill("SIGTERM");
      shuttingDownProcess.kill("SIGINT");
      const exit = await shuttingDownProcess.exited;
      const stepsAfterTheYield = await savedSteps(redisUrl, keyPrefix, "wait-then-record", runId);
      const runAfterTheYield = await storedRun(redisUrl, keyPrefix, "wait-then-record", runId);

      const runFields = { task: "wait-then-record", runId, attempt: 1 };
      expect(exit).toEqual({ code: 0, signal: null });
      expect(stepsAfterTheYield.serializedResultsByStepId).toEqual({ wait: '["waited"]' });
      expect(runAfterTheYield).toMatchObject({ state: "pending", attempt: "1", stalls: "0" });
      expect(new Set(shuttingDownProcess.logLines.map((line) => line.message))).toEqual(
        new Set(["schedule added", "worker started", "attempt started", "run yielded at shutdown", "worker stopped"]),
      );
      expect(shuttingDownProcess.logLines.filter((line) => line.message === "attempt started")).toHaveLength(1);
      expect(shuttingDownProcess.logLines.at(-1)?.message).toBe("worker stopped");
      expect(shuttingDownProcess.logLines).toContainEqual({
        level: "debug",
        fields: runFields,
        message: "run yielded at shutdown",
      });

      resumingProcess = await startWorkerProcess({ redisUrl, keyPrefix });
      await waitForLogMessage(resumingProcess.logLines, "run completed");

      expect(await fixtureRecordsOf(redisUrl, keyPrefix, "wait-then-record")).toEqual([
        { step: "wait", attempt: 1 },
        { step: "record", attempt: 1 },
      ]);
    } finally {
      await Promise.all([shuttingDownProcess.stop(), resumingProcess?.stop()]);
      await nextasking.close();
    }
  },
);
