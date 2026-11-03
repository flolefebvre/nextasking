import { expect } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { deriveJobId } from "../src/ids.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";
import { waitForLogMessage } from "./recording-logger.js";
import { storedRun } from "./stored-run.js";
import { withReaderAndWorker } from "./with-reader-and-worker.js";
import { withRunQueue } from "./with-run-queue.js";

const ONE_HOUR_IN_MILLISECONDS = 60 * 60 * 1000;

const ISO_TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

const storedMarkersByRunId = {
  "run-parked": { state: "waitingForConcurrencySlot" },
  "run-asleep": { state: "suspended", resumesAt: "2030-01-01T08:00:00.000Z" },
  "run-backing-off": { state: "backingOff", nextAttemptAt: "2030-01-01T09:00:00.000Z" },
  "run-handed-back": { state: "pending" },
  "run-dispatched-with-a-delay": undefined,
};

integrationTest(
  "a delayed run's state marker, in the format earlier releases store, reads back as its state",
  async ({ redisUrl, keyPrefix }) => {
    const exportReport = defineTask("export-report", {
      schema: z.object({ reportId: z.string() }),
      handler: async () => undefined,
    });
    const nextasking = createNextasking({ connection: { url: redisUrl }, prefix: keyPrefix, tasks: [exportReport] });

    try {
      await withRunQueue(redisUrl, keyPrefix, async (runQueue) => {
        for (const [runId, delayedRunState] of Object.entries(storedMarkersByRunId)) {
          await runQueue.add(
            "export-report",
            {
              runId,
              payload: { reportId: "report-1" },
              errors: [],
              writeToken: "earlier-release",
              ...(delayedRunState === undefined ? {} : { delayedRunState }),
            },
            { jobId: deriveJobId("export-report", runId), delay: ONE_HOUR_IN_MILLISECONDS },
          );
        }
      });

      const parkedRun = await nextasking.getRun("export-report", "run-parked");
      const asleepRun = await nextasking.getRun("export-report", "run-asleep");
      const backingOffRun = await nextasking.getRun("export-report", "run-backing-off");
      const handedBackRun = await nextasking.getRun("export-report", "run-handed-back");
      const delayedRun = await nextasking.getRun("export-report", "run-dispatched-with-a-delay");
      expect(parkedRun).toMatchObject({ state: "waitingForConcurrencySlot" });
      expect(asleepRun).toMatchObject({ state: "suspended", resumesAt: new Date("2030-01-01T08:00:00.000Z") });
      expect(backingOffRun).toMatchObject({
        state: "backingOff",
        nextAttemptAt: new Date("2030-01-01T09:00:00.000Z"),
      });
      expect(handedBackRun).toMatchObject({ state: "pending" });
      expect(delayedRun).toMatchObject({
        state: "delayed",
        startsAt: new Date((delayedRun?.dispatchedAt.getTime() ?? 0) + ONE_HOUR_IN_MILLISECONDS),
      });
    } finally {
      await closeNextasking(nextasking);
    }
  },
);

integrationTest(
  "a worker stores a delayed run's state marker in the format later releases read",
  { timeout: 20_000 },
  async ({ redisUrl, keyPrefix }) => {
    await withReaderAndWorker(redisUrl, keyPrefix, async ({ reader, startWorker, workerLines }) => {
      await startWorker();
      await reader.dispatch("import-contacts", { fileName: "a.csv" }, { runId: "run-importing" });
      await reader.dispatch("import-contacts", { fileName: "b.csv" }, { runId: "run-parked" });
      await reader.dispatch("sync-calendar", { calendarId: "work" }, { runId: "run-asleep" });
      await reader.dispatch("charge-card", { cardId: "card-1" }, { runId: "run-backing-off" });
      await reader.dispatch("archive-invoices", { year: 2025 }, { runId: "run-handed-back" });
      for (const message of [
        "run waiting for a concurrency slot",
        "run suspended",
        "attempt failed",
        "run handed back: unknown task",
      ]) {
        await waitForLogMessage(workerLines, message);
      }

      expect(await storedRun(redisUrl, keyPrefix, "import-contacts", "run-parked")).toMatchObject({
        delayedRunState: { state: "waitingForConcurrencySlot" },
      });
      expect(await storedRun(redisUrl, keyPrefix, "sync-calendar", "run-asleep")).toMatchObject({
        delayedRunState: { state: "suspended", resumesAt: expect.stringMatching(ISO_TIMESTAMP) },
      });
      expect(await storedRun(redisUrl, keyPrefix, "charge-card", "run-backing-off")).toMatchObject({
        delayedRunState: { state: "backingOff", nextAttemptAt: expect.stringMatching(ISO_TIMESTAMP) },
      });
      expect(await storedRun(redisUrl, keyPrefix, "archive-invoices", "run-handed-back")).toMatchObject({
        delayedRunState: { state: "pending" },
      });
    });
  },
);
