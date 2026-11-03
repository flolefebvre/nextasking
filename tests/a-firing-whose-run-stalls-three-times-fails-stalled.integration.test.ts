import { expect } from "vitest";

import { deriveFiringRunId } from "../src/ids.js";
import { fixtureRecordsOf } from "./fixture-records.js";
import { integrationTest } from "./integration-test.js";
import { waitForLogMessage } from "./recording-logger.js";
import { startStallCheckedWorkerProcesses } from "./stall-checked-worker-processes.js";
import { storedRun } from "./stored-run.js";
import { promoteNextFiring } from "./with-run-queue.js";

integrationTest(
  "the run a firing starts fails stalled after three stalls with no step saved, under the id derived from its planned instant",
  { timeout: 60_000 },
  async ({ redisUrl, keyPrefix }) => {
    const workerProcesses = await startStallCheckedWorkerProcesses(redisUrl, keyPrefix);

    try {
      const plannedInstant = await promoteNextFiring(redisUrl, keyPrefix, "block-event-loop-on-schedule");
      await waitForLogMessage(workerProcesses.blockingProcess.logLines, "run failed", { timeout: 40_000 });
      const runId = deriveFiringRunId("block-event-loop-on-schedule", plannedInstant);
      const scheduledAt = new Date(plannedInstant).toISOString();
      const stallLines = workerProcesses.stallCheckingProcess.logLines.filter(
        (line) => line.message === "attempt stalled",
      );
      const failureLine = workerProcesses.blockingProcess.logLines.find((line) => line.message === "run failed");

      const runFields = { task: "block-event-loop-on-schedule", runId, attempt: 1 };
      expect(stallLines).toHaveLength(3);
      for (const stallLine of stallLines) {
        expect(stallLine.fields).toEqual(runFields);
      }
      expect(await fixtureRecordsOf(redisUrl, keyPrefix, "block-event-loop-on-schedule")).toEqual(
        Array.from({ length: 3 }, () => ({ runId, scheduledAt, event: "started" })),
      );
      expect(failureLine).toMatchObject({
        level: "error",
        fields: { ...runFields, err: { name: "StalledRunError" }, reason: "stalled" },
      });
      expect(await storedRun(redisUrl, keyPrefix, "block-event-loop-on-schedule", runId)).toMatchObject({
        scheduledAt,
        reason: "stalled",
        errors: [{ attempt: 1, name: "StalledRunError" }],
      });
    } finally {
      await workerProcesses.stop();
    }
  },
);
