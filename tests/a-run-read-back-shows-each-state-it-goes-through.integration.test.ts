import { expect, vi } from "vitest";

import { integrationTest } from "./integration-test.js";
import { waitForLogMessage } from "./recording-logger.js";
import { withReaderAndWorker } from "./with-reader-and-worker.js";

const ONE_MINUTE_IN_MILLISECONDS = 60_000;

const ONE_HOUR_IN_MILLISECONDS = 60 * ONE_MINUTE_IN_MILLISECONDS;

integrationTest(
  "a run read back from a process with no worker shows each state it goes through",
  { timeout: 30_000 },
  async ({ redisUrl, keyPrefix }) => {
    await withReaderAndWorker(redisUrl, keyPrefix, async ({ reader, startWorker, workerLines }) => {
      const stateOf = async (taskName: Parameters<typeof reader.getRun>[0], runId: string) =>
        (await reader.getRun(taskName, runId))?.state;

      const pendingReminder = await reader.dispatch("send-reminder", { email: "ada@example.com" });
      const delayedReminder = await reader.dispatch("send-reminder", { email: "grace@example.com" }, { delay: "1m" });

      const pendingRun = await reader.getRun("send-reminder", pendingReminder.runId);
      const delayedRun = await reader.getRun("send-reminder", delayedReminder.runId);
      expect(pendingRun).toEqual({
        task: "send-reminder",
        runId: pendingReminder.runId,
        state: "pending",
        payload: { email: "ada@example.com" },
        attempt: 1,
        dispatchedAt: expect.any(Date),
        errors: [],
        steps: [],
      });
      expect(delayedRun).toMatchObject({ state: "delayed", attempt: 1 });
      expect(delayedRun?.state === "delayed" && delayedRun.startsAt.getTime()).toBe(
        (delayedRun?.dispatchedAt.getTime() ?? 0) + ONE_MINUTE_IN_MILLISECONDS,
      );

      await startWorker();
      const firstImport = await reader.dispatch("import-contacts", { fileName: "a.csv" });
      const secondImport = await reader.dispatch("import-contacts", { fileName: "b.csv" });
      const calendarSync = await reader.dispatch("sync-calendar", { calendarId: "work" });
      const cardCharge = await reader.dispatch("charge-card", { cardId: "card-1" });
      const accountClosing = await reader.dispatch("close-account", { accountId: "account-7" });
      const invoiceArchiving = await reader.dispatch("archive-invoices", { year: 2025 });
      await waitForLogMessage(workerLines, "run handed back: unknown task");
      await vi.waitFor(
        async () => {
          expect(await stateOf("send-reminder", pendingReminder.runId)).toBe("completed");
          expect(await stateOf("import-contacts", firstImport.runId)).toBe("executing");
          expect(await stateOf("import-contacts", secondImport.runId)).toBe("waitingForConcurrencySlot");
          expect(await stateOf("sync-calendar", calendarSync.runId)).toBe("suspended");
          expect(await stateOf("charge-card", cardCharge.runId)).toBe("backingOff");
          expect(await stateOf("close-account", accountClosing.runId)).toBe("failed");
        },
        { timeout: 15_000 },
      );

      expect(await reader.getRun("send-reminder", pendingReminder.runId)).toMatchObject({
        state: "completed",
        attempt: 1,
        finishedAt: expect.any(Date),
      });
      expect(await reader.getRun("import-contacts", firstImport.runId)).toMatchObject({ attempt: 1 });
      const suspendedRun = await reader.getRun("sync-calendar", calendarSync.runId);
      expect(suspendedRun?.steps).toEqual([{ id: "fetch-events", result: { count: 3 } }]);
      expect(suspendedRun?.state === "suspended" && suspendedRun.resumesAt.getTime()).toBeGreaterThan(
        Date.now() + ONE_HOUR_IN_MILLISECONDS - ONE_MINUTE_IN_MILLISECONDS,
      );
      const backingOffRun = await reader.getRun("charge-card", cardCharge.runId);
      expect(backingOffRun).toMatchObject({
        attempt: 2,
        errors: [{ attempt: 1, name: "Error", message: "card declined", failedAt: expect.any(Date) }],
      });
      expect(backingOffRun?.state === "backingOff" && backingOffRun.nextAttemptAt.getTime()).toBeGreaterThan(
        (backingOffRun?.errors[0]?.failedAt.getTime() ?? 0) + 0.8 * ONE_MINUTE_IN_MILLISECONDS - 1,
      );
      expect(await reader.getRun("close-account", accountClosing.runId)).toMatchObject({
        state: "failed",
        reason: "non-retriable",
        attempt: 1,
        finishedAt: expect.any(Date),
        errors: [{ name: "NonRetriableError", message: "account has open invoices" }],
      });
      expect(await stateOf("archive-invoices", invoiceArchiving.runId)).toBe("pending");
      expect(await reader.getRun("send-reminder", "no-such-run")).toBeUndefined();
    });
  },
);
