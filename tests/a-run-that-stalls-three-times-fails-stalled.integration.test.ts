import { expect } from "vitest";

import { fixtureRecordsOf } from "./fixture-records.js";
import { integrationTest } from "./integration-test.js";
import { waitForLogMessage } from "./recording-logger.js";
import { startStallCheckedWorkerProcesses } from "./stall-checked-worker-processes.js";
import { storedRun } from "./stored-run.js";

integrationTest(
  "a run whose attempts stall three times with no step saved fails stalled, in a worker other than the one that counted the stalls",
  { timeout: 60_000 },
  async ({ redisUrl, keyPrefix }) => {
    const { nextasking, stallCheckingProcess, blockingProcess, stop } = await startStallCheckedWorkerProcesses(
      redisUrl,
      keyPrefix,
    );

    try {
      const { runId } = await nextasking.dispatch("block-event-loop", { blockInMilliseconds: 4000 });
      await waitForLogMessage(blockingProcess.logLines, "run failed", { timeout: 40_000 });
      const run = await storedRun(redisUrl, keyPrefix, "block-event-loop", runId);
      const attemptsStarted = await fixtureRecordsOf(redisUrl, keyPrefix, "block-event-loop");

      const runFields = { task: "block-event-loop", runId, attempt: 1 };
      expect(stallCheckingProcess.logLines.filter((line) => line.message === "attempt stalled")).toEqual([
        { level: "warn", fields: runFields, message: "attempt stalled" },
        { level: "warn", fields: runFields, message: "attempt stalled" },
        { level: "warn", fields: runFields, message: "attempt stalled" },
      ]);
      expect(attemptsStarted).toEqual([
        { runId, event: "started" },
        { runId, event: "started" },
        { runId, event: "started" },
      ]);
      expect(blockingProcess.logLines.find((line) => line.message === "run failed")).toEqual({
        level: "error",
        fields: {
          ...runFields,
          err: {
            name: "StalledRunError",
            message: `Run "${runId}" stalled 3 times with no step saved in between: each attempt lost its lock, from a crash, a kill or an event loop blocked past the lock duration.`,
          },
          reason: "stalled",
          runDurationInMilliseconds: expect.any(Number),
          executionDurationInMilliseconds: expect.any(Number),
        },
        message: "run failed",
      });
      expect(blockingProcess.logLines.map((line) => line.message)).not.toContain("run completed");
      expect(run).toMatchObject({
        reason: "stalled",
        errors: [{ attempt: 1, name: "StalledRunError", message: expect.stringContaining("stalled 3 times") }],
      });
    } finally {
      await stop();
    }
  },
);
