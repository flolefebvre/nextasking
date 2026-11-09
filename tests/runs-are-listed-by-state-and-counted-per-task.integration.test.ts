import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineEvent, defineListener, defineTask } from "../src/index.js";
import type { RunState } from "../src/index.js";
import { deriveListenerRunId } from "../src/ids.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";
import { withReaderAndWorker } from "./with-reader-and-worker.js";

const userSignedUp = defineEvent("user.signed-up", { schema: z.object({ userId: z.string() }) });

function createDispatchOnlyNextasking(redisUrl: string, keyPrefix: string) {
  return createNextasking({
    connection: { url: redisUrl },
    prefix: keyPrefix,
    logger: createRecordingLogger().logger,
    events: [userSignedUp],
    tasks: [
      defineTask("send-reminder", { schema: z.object({ email: z.string() }), handler: async () => undefined }),
      defineTask("archive-invoices", { schema: z.object({ year: z.number() }), handler: async () => undefined }),
      defineListener("create-default-workspace", { on: userSignedUp, handler: async () => undefined }),
    ],
  });
}

async function everyPageOf<ListedRunOfAnyTask>(
  listPage: (cursor: string | undefined) => Promise<{ runs: ListedRunOfAnyTask[]; nextCursor?: string }>,
): Promise<ListedRunOfAnyTask[][]> {
  const pages: ListedRunOfAnyTask[][] = [];
  let cursor: string | undefined;
  do {
    const page = await listPage(cursor);
    pages.push(page.runs);
    cursor = page.nextCursor;
  } while (cursor !== undefined);
  return pages;
}

integrationTest(
  "listRuns pages through a state's runs with an opaque cursor, by task too, the runs an emit started among them",
  async ({ redisUrl, keyPrefix }) => {
    const nextasking = createDispatchOnlyNextasking(redisUrl, keyPrefix);

    try {
      const reminderRunIds: string[] = [];
      for (const email of ["ada@example.com", "grace@example.com", "edsger@example.com"]) {
        reminderRunIds.push((await nextasking.dispatch("send-reminder", { email })).runId);
      }
      const archivingRunIds = [
        (await nextasking.dispatch("archive-invoices", { year: 2024 })).runId,
        (await nextasking.dispatch("archive-invoices", { year: 2025 })).runId,
      ];
      const { eventId } = await nextasking.emit("user.signed-up", { userId: "user-1" });
      const delayedReminder = await nextasking.dispatch(
        "send-reminder",
        { email: "alan@example.com" },
        { delay: "1h" },
      );

      const pendingPages = await everyPageOf((cursor) =>
        nextasking.listRuns({ state: "pending", limit: 2, ...(cursor === undefined ? {} : { cursor }) }),
      );
      const archivingPages = await everyPageOf((cursor) =>
        nextasking.listRuns({
          state: "pending",
          task: "archive-invoices",
          limit: 1,
          ...(cursor === undefined ? {} : { cursor }),
        }),
      );
      const delayedPage = await nextasking.listRuns({ state: "delayed" });
      expect(pendingPages.every((page) => page.length <= 2)).toBe(true);
      expect(
        pendingPages
          .flat()
          .map((run) => run.runId)
          .sort(),
      ).toEqual(
        [...reminderRunIds, ...archivingRunIds, deriveListenerRunId(eventId, "create-default-workspace")].sort(),
      );
      expect(
        archivingPages
          .flat()
          .map((run) => run.runId)
          .sort(),
      ).toEqual([...archivingRunIds].sort());
      expect(archivingPages[0]).toHaveLength(1);
      expect(delayedPage).toEqual({
        runs: [
          {
            task: "send-reminder",
            runId: delayedReminder.runId,
            state: "delayed",
            startsAt: expect.any(Date),
            attempt: 1,
            dispatchedAt: expect.any(Date),
          },
        ],
      });
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);

integrationTest(
  "listRuns refuses an unknown state, an unknown task, a bad limit and a cursor it never returned",
  async ({ redisUrl, keyPrefix }) => {
    const nextasking = createDispatchOnlyNextasking(redisUrl, keyPrefix);

    try {
      await expect(nextasking.listRuns({ state: "sleeping" as RunState })).rejects.toThrow(
        'Invalid listRuns state: "sleeping" is not one of',
      );
      await expect(nextasking.listRuns({ state: "pending", task: "send-goodbye" as "send-reminder" })).rejects.toThrow(
        'Unknown task "send-goodbye"',
      );
      await expect(nextasking.listRuns({ state: "pending", limit: 0 })).rejects.toThrow(
        "Invalid listRuns limit: 0 is not a positive integer",
      );
      await expect(nextasking.listRuns({ state: "pending", cursor: "not-a-cursor" })).rejects.toThrow(
        "Invalid listRuns cursor",
      );
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);

integrationTest(
  "a failed run is listed with its last error message, and countRuns counts every run per task and state",
  { timeout: 20_000 },
  async ({ redisUrl, keyPrefix }) => {
    await withReaderAndWorker(redisUrl, keyPrefix, async ({ reader, startWorker }) => {
      await startWorker();
      const accountClosing = await reader.dispatch("close-account", { accountId: "account-7" });
      await reader.dispatch("charge-card", { cardId: "card-1" });
      await reader.dispatch("import-contacts", { fileName: "a.csv" });
      await reader.dispatch("import-contacts", { fileName: "b.csv" });
      await reader.dispatch("send-reminder", { email: "ada@example.com" }, { delay: "1h" });
      await vi.waitFor(
        async () => {
          const runCounts = await reader.countRuns();
          expect(runCounts["close-account"].failed).toBe(1);
          expect(runCounts["charge-card"].backingOff).toBe(1);
          expect(runCounts["import-contacts"].waitingForConcurrencySlot).toBe(1);
        },
        { timeout: 10_000 },
      );

      const failedPage = await reader.listRuns({ state: "failed" });
      const runCounts = await reader.countRuns();
      expect(failedPage.runs).toEqual([
        {
          task: "close-account",
          runId: accountClosing.runId,
          state: "failed",
          reason: "non-retriable",
          attempt: 1,
          dispatchedAt: expect.any(Date),
          finishedAt: expect.any(Date),
          lastErrorMessage: "account has open invoices",
        },
      ]);
      const noRuns = {
        delayed: 0,
        pending: 0,
        waitingForConcurrencySlot: 0,
        executing: 0,
        suspended: 0,
        backingOff: 0,
        completed: 0,
        failed: 0,
      };
      expect(runCounts).toEqual({
        "send-reminder": { ...noRuns, delayed: 1 },
        "import-contacts": { ...noRuns, executing: 1, waitingForConcurrencySlot: 1 },
        "sync-calendar": noRuns,
        "charge-card": { ...noRuns, backingOff: 1 },
        "close-account": { ...noRuns, failed: 1 },
        "archive-invoices": noRuns,
      });
    });
  },
);
