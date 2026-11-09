import { expect, vi } from "vitest";

import { fixtureRecordsOf } from "./fixture-records.js";
import { integrationTest } from "./integration-test.js";
import { waitForLogMessage } from "./recording-logger.js";
import { storedRun } from "./stored-run.js";
import { startWorkerProcess } from "./worker-process.js";
import { createWorkerProcessNextasking } from "./worker-process-nextasking.js";

integrationTest(
  "an attempt whose event loop blocks past its lock is aborted: its later steps throw the reason, and it writes nothing to the run",
  { timeout: 30_000 },
  async ({ redisUrl, keyPrefix }) => {
    const workerProcess = await startWorkerProcess({
      redisUrl,
      keyPrefix,
      timing: { lockDurationInMilliseconds: 1000, lockTimeLeftThatTriggersRenewalInMilliseconds: 500 },
    });
    const nextasking = createWorkerProcessNextasking(redisUrl, keyPrefix);

    try {
      const { runId } = await nextasking.dispatch("record-abort-after-blocking", { blockInMilliseconds: 2500 });
      await vi.waitFor(
        async () => {
          expect(await fixtureRecordsOf(redisUrl, keyPrefix, "record-abort-after-blocking")).toHaveLength(1);
        },
        { timeout: 10_000 },
      );
      await waitForLogMessage(workerProcess.logLines, "attempt aborted: lock lost");
      const records = await fixtureRecordsOf(redisUrl, keyPrefix, "record-abort-after-blocking");
      const run = await storedRun(redisUrl, keyPrefix, "record-abort-after-blocking", runId);

      const abortReason = `The attempt of run "${runId}" was aborted: it lost its lock, so another attempt may own the run now.`;
      expect(records).toEqual([{ abortReason, stepError: abortReason }]);
      expect(workerProcess.logLines).toContainEqual({
        level: "debug",
        fields: { task: "record-abort-after-blocking", runId, attempt: 1 },
        message: "attempt aborted: lock lost",
      });
      expect(run).toMatchObject({ errors: [] });
      expect(run).not.toHaveProperty("reason");
    } finally {
      await workerProcess.stop();
      await nextasking.close();
    }
  },
);
