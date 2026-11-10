import { expect, vi } from "vitest";
import { z } from "zod";

import {
  NonRetriableError,
  createNextasking,
  defineEvent,
  defineListener,
  defineTask,
  runFailed,
} from "../src/index.js";
import type { Logger, RunFailedPayload } from "../src/index.js";
import { deriveFailureNoticeEventId } from "../src/ids.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { TEST_TIMEOUT_PAST_ITS_WAITS_IN_MILLISECONDS, integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";
import { storedRun } from "./stored-run.js";

const ISO_TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

const NOTICES_TIMEOUT_IN_MILLISECONDS = 10_000;

type Alerting = {
  runFailedNotices: RunFailedPayload[];
  isRunFailedAlertBroken: boolean;
};

function noAlerting(): Alerting {
  return { runFailedNotices: [], isRunFailedAlertBroken: false };
}

const invoicePaid = defineEvent("invoice.paid", { schema: z.object({ invoiceId: z.string() }) });

const syncAccount = defineTask("sync-account", {
  schema: z.object({ accountId: z.string(), email: z.string() }),
  retries: 0,
  handler: async () => {
    throw new NonRetriableError("account deleted upstream");
  },
});

const recordPayment = defineListener("record-payment", {
  on: invoicePaid,
  retries: 0,
  handler: async () => {
    throw new Error("ledger unavailable");
  },
});

function createAlertingNextasking(redisUrl: string, keyPrefix: string, logger: Logger, alerting?: Alerting) {
  const alertListeners =
    alerting === undefined
      ? []
      : [
          defineListener("alert-on-run-failed", {
            on: runFailed,
            retries: 0,
            handler: async ({ payload }) => {
              alerting.runFailedNotices.push(payload);
              if (alerting.isRunFailedAlertBroken && payload.task !== "alert-on-run-failed") {
                throw new Error("pager unavailable");
              }
            },
          }),
        ];
  return createNextasking({
    connection: { url: redisUrl },
    prefix: keyPrefix,
    logger,
    events: [invoicePaid],
    tasks: [syncAccount, recordPayment, ...alertListeners],
  });
}

async function waitForNotices(notices: readonly unknown[], count: number): Promise<void> {
  await vi.waitFor(
    () => {
      expect(notices).toHaveLength(count);
    },
    { timeout: NOTICES_TIMEOUT_IN_MILLISECONDS },
  );
}

integrationTest(
  "a failed run sends one run.failed notice, without its payload, under an id derived from its failure",
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const alerting = noAlerting();
    const nextasking = createAlertingNextasking(redisUrl, keyPrefix, logger, alerting);

    try {
      await nextasking.startWorker();
      const { runId } = await nextasking.dispatch("sync-account", { accountId: "account-7", email: "a@b.c" });
      await waitForNotices(alerting.runFailedNotices, 1);
      const failedRun = await storedRun(redisUrl, keyPrefix, "sync-account", runId);
      const noticeEvent = await nextasking.getEvent(
        "nextasking/run.failed",
        deriveFailureNoticeEventId("sync-account", runId, 0),
      );

      expect(alerting.runFailedNotices).toEqual([
        {
          task: "sync-account",
          runId,
          reason: "non-retriable",
          error: {
            name: "NonRetriableError",
            message: "account deleted upstream",
            stack: expect.stringContaining("account deleted upstream"),
          },
          attempt: 1,
          failedAt: expect.stringMatching(ISO_TIMESTAMP),
        },
      ]);
      expect(alerting.runFailedNotices[0]?.failedAt).toBe(
        (failedRun?.errors as { failedAt: string }[] | undefined)?.[0]?.failedAt,
      );
      expect(noticeEvent.runsByListener["alert-on-run-failed"]).toMatchObject({ state: "completed" });
      expect(failedRun).toMatchObject({ state: "failed", reason: "non-retriable", replays: "0" });
      expect(lines.map((line) => line.message)).not.toContain("no listeners for event");
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);

integrationTest(
  "a failed listener run's notice names its event, and a replayed run that fails again sends a second notice",
  { timeout: 2 * NOTICES_TIMEOUT_IN_MILLISECONDS + TEST_TIMEOUT_PAST_ITS_WAITS_IN_MILLISECONDS },
  async ({ redisUrl, keyPrefix }) => {
    const { logger } = createRecordingLogger();
    const alerting = noAlerting();
    const nextasking = createAlertingNextasking(redisUrl, keyPrefix, logger, alerting);

    try {
      await nextasking.startWorker();
      const { eventId } = await nextasking.emit("invoice.paid", { invoiceId: "inv-7" });
      await waitForNotices(alerting.runFailedNotices, 1);
      const { runId } = alerting.runFailedNotices[0] as RunFailedPayload;

      await nextasking.replay("record-payment", runId);
      await waitForNotices(alerting.runFailedNotices, 2);

      expect(alerting.runFailedNotices).toEqual([
        expect.objectContaining({ task: "record-payment", runId, eventName: "invoice.paid", eventId, attempt: 1 }),
        expect.objectContaining({ task: "record-payment", runId, eventName: "invoice.paid", eventId, attempt: 1 }),
      ]);
      expect(await storedRun(redisUrl, keyPrefix, "record-payment", runId)).toMatchObject({
        state: "failed",
        replays: "1",
      });
      for (const replays of [0, 1]) {
        const noticeEvent = await nextasking.getEvent(
          "nextasking/run.failed",
          deriveFailureNoticeEventId("record-payment", runId, replays),
        );
        expect(noticeEvent.runsByListener["alert-on-run-failed"]).toBeDefined();
      }
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);

integrationTest(
  "a failed listener of a built-in event sends no notice, so alerting can't loop",
  { timeout: 15_000 },
  async ({ redisUrl, keyPrefix }) => {
    const { logger } = createRecordingLogger();
    const alerting: Alerting = { ...noAlerting(), isRunFailedAlertBroken: true };
    const nextasking = createAlertingNextasking(redisUrl, keyPrefix, logger, alerting);

    try {
      await nextasking.startWorker();
      await nextasking.dispatch("sync-account", { accountId: "account-7", email: "a@b.c" });
      await waitForNotices(alerting.runFailedNotices, 1);
      const alertRunId = await vi.waitUntil(
        async () => (await nextasking.listRuns({ state: "failed", task: "alert-on-run-failed" })).runs[0]?.runId,
        { timeout: 10_000 },
      );
      await nextasking.dispatch("sync-account", { accountId: "account-8", email: "a@b.c" }, { runId: "second-run" });
      await waitForNotices(alerting.runFailedNotices, 2);

      const noticeOfAlertRun = await nextasking.getEvent(
        "nextasking/run.failed",
        deriveFailureNoticeEventId("alert-on-run-failed", alertRunId, 0),
      );

      expect(noticeOfAlertRun.runsByListener["alert-on-run-failed"]).toBeUndefined();
      expect(alerting.runFailedNotices.map((notice) => notice.task)).toEqual(["sync-account", "sync-account"]);
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);

integrationTest(
  "a failure with no listener on its built-in event sends nothing, silently",
  { timeout: 15_000 },
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const nextasking = createAlertingNextasking(redisUrl, keyPrefix, logger);

    try {
      await nextasking.startWorker();
      const { runId } = await nextasking.dispatch("sync-account", { accountId: "account-7", email: "a@b.c" });
      await waitForLogMessage(lines, "run failed");

      const noticeEvent = await nextasking.getEvent(
        "nextasking/run.failed",
        deriveFailureNoticeEventId("sync-account", runId, 0),
      );

      expect(noticeEvent.runsByListener).toEqual({});
      expect(await storedRun(redisUrl, keyPrefix, "sync-account", runId)).toMatchObject({ state: "failed" });
      expect(lines.map((line) => line.message)).not.toContain("no listeners for event");
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);
