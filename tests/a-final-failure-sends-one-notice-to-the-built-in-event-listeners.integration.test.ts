import { expect, vi } from "vitest";
import { z } from "zod";

import {
  NonRetriableError,
  createNextasking,
  defineEvent,
  defineListener,
  defineTask,
  fanOutFailed,
  runFailed,
} from "../src/index.js";
import type { FanOutFailedPayload, Logger, RunFailedPayload } from "../src/index.js";
import { deriveFailureNoticeEventId } from "../src/ids.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";
import { runJobKey } from "./run-job-key.js";
import { storedRun } from "./stored-run.js";
import { withRedis } from "./with-redis.js";

const ISO_TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

type Alerting = {
  runFailedNotices: RunFailedPayload[];
  fanOutFailedNotices: FanOutFailedPayload[];
  isRunFailedAlertBroken: boolean;
};

function noAlerting(): Alerting {
  return { runFailedNotices: [], fanOutFailedNotices: [], isRunFailedAlertBroken: false };
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
          defineListener("alert-on-fan-out-failed", {
            on: fanOutFailed,
            handler: async ({ payload }) => {
              alerting.fanOutFailedNotices.push(payload);
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
    { timeout: 10_000 },
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
        deriveFailureNoticeEventId("sync-account", runId, 1),
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
      expect(failedRun).toMatchObject({ failureCount: 1, notifiedFailureCount: 1 });
      expect(lines.map((line) => line.message)).not.toContain("no listeners for event");
    } finally {
      await closeNextasking(nextasking);
    }
  },
);

integrationTest(
  "a failed listener run's notice names its event, and a replayed run that fails again sends a second notice",
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
        failureCount: 2,
        notifiedFailureCount: 2,
      });
    } finally {
      await closeNextasking(nextasking);
    }
  },
);

integrationTest(
  "a failed listener of a built-in event sends no notice, so alerting can't loop",
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
        deriveFailureNoticeEventId("alert-on-run-failed", alertRunId, 1),
      );

      expect(alerting.runFailedNotices.map((notice) => notice.task)).toEqual(["sync-account", "sync-account"]);
      expect(noticeOfAlertRun.fanOut).toBeUndefined();
    } finally {
      await closeNextasking(nextasking);
    }
  },
);

integrationTest(
  "a failure with no listener on its built-in event is marked notified, silently",
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const nextasking = createAlertingNextasking(redisUrl, keyPrefix, logger);

    try {
      await nextasking.startWorker();
      const { runId } = await nextasking.dispatch("sync-account", { accountId: "account-7", email: "a@b.c" });
      await vi.waitFor(
        async () => {
          expect(await storedRun(redisUrl, keyPrefix, "sync-account", runId)).toMatchObject({
            notifiedFailureCount: 1,
          });
        },
        { timeout: 10_000 },
      );

      const noticeEvent = await nextasking.getEvent(
        "nextasking/run.failed",
        deriveFailureNoticeEventId("sync-account", runId, 1),
      );

      expect(noticeEvent.fanOut).toBeUndefined();
      expect(lines.map((line) => line.message)).not.toContain("no listeners for event");
    } finally {
      await closeNextasking(nextasking);
    }
  },
);

integrationTest(
  "a fan-out failed by stalls logs fan-out failed and sends one fan-out.failed notice",
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const alerting = noAlerting();
    const nextasking = createAlertingNextasking(redisUrl, keyPrefix, logger, alerting);
    const stallLimitReason = "job stalled more than allowable limit";

    try {
      const { eventId } = await nextasking.emit("invoice.paid", { invoiceId: "inv-7" });
      await withRedis(redisUrl, (redis) =>
        redis.hset(runJobKey(keyPrefix, "invoice.paid", eventId), "defa", stallLimitReason),
      );

      await nextasking.startWorker();
      await waitForNotices(alerting.fanOutFailedNotices, 1);

      expect(alerting.fanOutFailedNotices).toEqual([
        {
          eventName: "invoice.paid",
          eventId,
          error: { name: expect.any(String), message: stallLimitReason, stack: expect.any(String) },
          failedAt: expect.stringMatching(ISO_TIMESTAMP),
        },
      ]);
      expect(lines.filter((line) => line.message === "fan-out failed")).toEqual([
        {
          level: "error",
          fields: { eventName: "invoice.paid", eventId, err: expect.objectContaining({ message: stallLimitReason }) },
          message: "fan-out failed",
        },
      ]);
      expect(await storedRun(redisUrl, keyPrefix, "invoice.paid", eventId)).toMatchObject({
        failureCount: 1,
        notifiedFailureCount: 1,
        finalError: { message: stallLimitReason },
      });
      expect(alerting.runFailedNotices).toEqual([]);
    } finally {
      await closeNextasking(nextasking);
    }
  },
);

integrationTest(
  "a replayed fan-out clears its final error and keeps its failure count",
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const nextasking = createAlertingNextasking(redisUrl, keyPrefix, logger);

    try {
      const { eventId } = await nextasking.emit("invoice.paid", { invoiceId: "inv-7" });
      await withRedis(redisUrl, (redis) =>
        redis.hset(runJobKey(keyPrefix, "invoice.paid", eventId), "defa", "job stalled more than allowable limit"),
      );
      await nextasking.startWorker();
      await waitForLogMessage(lines, "fan-out failed");
      await vi.waitFor(
        async () => {
          expect(await storedRun(redisUrl, keyPrefix, "invoice.paid", eventId)).toMatchObject({
            notifiedFailureCount: 1,
          });
        },
        { timeout: 10_000 },
      );

      await nextasking.replayEvent("invoice.paid", eventId);
      await vi.waitFor(
        async () => {
          expect(await nextasking.getEvent("invoice.paid", eventId)).toMatchObject({ fanOut: "completed" });
        },
        { timeout: 10_000 },
      );

      const replayedFanOut = await storedRun(redisUrl, keyPrefix, "invoice.paid", eventId);
      expect(replayedFanOut).toMatchObject({ failureCount: 1, notifiedFailureCount: 1 });
      expect(replayedFanOut).not.toHaveProperty("finalError");
    } finally {
      await closeNextasking(nextasking);
    }
  },
);
