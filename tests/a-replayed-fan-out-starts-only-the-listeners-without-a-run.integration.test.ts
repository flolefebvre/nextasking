import { UnrecoverableError, Worker } from "bullmq";
import { Redis } from "ioredis";
import { expect, vi } from "vitest";
import { z } from "zod";

import { FanOutNotReplayableError, createNextasking, defineEvent, defineListener } from "../src/index.js";
import { deriveListenerRunId } from "../src/ids.js";
import { closeNextasking } from "../src/registry.js";
import { RUN_QUEUE_NAME } from "../src/run-queue.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";

function createInvoicePaidNextasking(
  redisUrl: string,
  keyPrefix: string,
  executionsByListener: Record<string, number>,
  isLedgerFixed: () => boolean,
  logger = createRecordingLogger().logger,
) {
  const invoicePaid = defineEvent("invoice.paid", { schema: z.object({ invoiceId: z.string() }) });
  const recordPayment = defineListener("record-payment", {
    on: invoicePaid,
    retries: 0,
    handler: async () => {
      executionsByListener["record-payment"] = (executionsByListener["record-payment"] ?? 0) + 1;
      if (!isLedgerFixed()) {
        throw new Error("ledger unavailable");
      }
    },
  });
  const sendReceipt = defineListener("send-receipt", {
    on: invoicePaid,
    handler: async () => {
      executionsByListener["send-receipt"] = (executionsByListener["send-receipt"] ?? 0) + 1;
    },
  });
  return createNextasking({
    connection: { url: redisUrl },
    prefix: keyPrefix,
    logger,
    events: [invoicePaid],
    tasks: [recordPayment, sendReceipt],
  });
}

async function failNextJobWithoutRunningIt(redisUrl: string, keyPrefix: string): Promise<void> {
  const redis = new Redis(redisUrl, { maxRetriesPerRequest: null });
  redis.on("error", () => undefined);
  let hasFailedAJob = false;
  const failingWorker = new Worker(
    RUN_QUEUE_NAME,
    async () => {
      hasFailedAJob = true;
      throw new UnrecoverableError("worker crashed mid fan-out");
    },
    { connection: redis, prefix: keyPrefix },
  );
  failingWorker.on("error", () => undefined);
  try {
    await vi.waitUntil(() => hasFailedAJob, { timeout: 10_000 });
  } finally {
    await failingWorker.close();
    await redis.quit().catch(() => undefined);
  }
}

integrationTest(
  "a replayed fan-out starts the listeners that have no run yet and skips the one that already has one",
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const executionsByListener: Record<string, number> = {};
    const nextasking = createInvoicePaidNextasking(redisUrl, keyPrefix, executionsByListener, () => true, logger);

    try {
      const { eventId } = await nextasking.emit("invoice.paid", { invoiceId: "inv-7" });
      await failNextJobWithoutRunningIt(redisUrl, keyPrefix);
      await nextasking.dispatch(
        "record-payment",
        { invoiceId: "inv-7" },
        { runId: deriveListenerRunId(eventId, "record-payment") },
      );
      await nextasking.startWorker();
      await waitForLogMessage(lines, "run completed");

      const replayedEvent = await nextasking.replayEvent("invoice.paid", eventId);
      await vi.waitFor(
        async () => {
          expect(await nextasking.getEvent("invoice.paid", eventId)).toMatchObject({
            fanOut: "completed",
            runsByListener: { "record-payment": { state: "completed" }, "send-receipt": { state: "completed" } },
          });
        },
        { timeout: 10_000 },
      );

      expect(replayedEvent).toEqual({ eventId });
      expect(executionsByListener).toEqual({ "record-payment": 1, "send-receipt": 1 });
    } finally {
      await closeNextasking(nextasking);
    }
  },
);

integrationTest(
  "one listener's failed run is replayed on its own, without running the other listeners again",
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const executionsByListener: Record<string, number> = {};
    let isLedgerFixed = false;
    const nextasking = createInvoicePaidNextasking(
      redisUrl,
      keyPrefix,
      executionsByListener,
      () => isLedgerFixed,
      logger,
    );

    try {
      await nextasking.startWorker();
      const { eventId } = await nextasking.emit("invoice.paid", { invoiceId: "inv-7" });
      await waitForLogMessage(lines, "run failed");
      await waitForLogMessage(lines, "run completed");
      isLedgerFixed = true;

      await nextasking.replay("record-payment", deriveListenerRunId(eventId, "record-payment"));
      await vi.waitFor(
        async () => {
          expect(await nextasking.getEvent("invoice.paid", eventId)).toMatchObject({
            runsByListener: { "record-payment": { state: "completed" } },
          });
        },
        { timeout: 10_000 },
      );

      expect(executionsByListener).toEqual({ "record-payment": 2, "send-receipt": 1 });
    } finally {
      await closeNextasking(nextasking);
    }
  },
);

integrationTest("replayEvent throws for a fan-out that is unknown or not failed", async ({ redisUrl, keyPrefix }) => {
  const nextasking = createInvoicePaidNextasking(redisUrl, keyPrefix, {}, () => true);

  try {
    await nextasking.startWorker();
    const { eventId } = await nextasking.emit("invoice.paid", { invoiceId: "inv-7" });
    await vi.waitFor(
      async () => {
        expect(await nextasking.getEvent("invoice.paid", eventId)).toMatchObject({ fanOut: "completed" });
      },
      { timeout: 10_000 },
    );

    const completedFanOutReplayError: unknown = await nextasking
      .replayEvent("invoice.paid", eventId)
      .catch((error) => error);
    const unknownFanOutReplayError: unknown = await nextasking
      .replayEvent("invoice.paid", "event-that-never-was")
      .catch((error) => error);

    expect(completedFanOutReplayError).toBeInstanceOf(FanOutNotReplayableError);
    expect(completedFanOutReplayError).toHaveProperty(
      "message",
      `Fan-out "${eventId}" of event "invoice.paid" can't be replayed: it is not failed.`,
    );
    expect(unknownFanOutReplayError).toHaveProperty(
      "message",
      `Fan-out "event-that-never-was" of event "invoice.paid" can't be replayed: no such fan-out is kept, its event id is unknown or the fan-out was removed.`,
    );
  } finally {
    await closeNextasking(nextasking);
  }
});
