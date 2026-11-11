import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineEvent, defineListener, defineTask } from "../src/index.js";
import { deriveListenerRunId } from "../src/ids.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";
import { storedRunIdentities } from "./stored-run-identities.js";
import { storedRun } from "./stored-run.js";

const invoicePaidEvent = defineEvent("invoice.paid", {
  schema: z.object({ accountId: z.string(), invoiceId: z.string() }),
});

integrationTest(
  "a dispatched run and a listener run store the concurrency key computed from their validated payload",
  { timeout: 15_000 },
  async ({ redisUrl, keyPrefix }) => {
    const rebuildIndexTask = defineTask("rebuild-index", {
      schema: z.object({ accountId: z.string().trim() }),
      concurrency: { key: (payload) => `account-${payload.accountId}`, limit: 1 },
      handler: async () => undefined,
    });
    const recordInvoicePaymentTask = defineListener("record-invoice-payment", {
      on: invoicePaidEvent,
      concurrency: { key: (payload) => payload.accountId, limit: 2 },
      handler: async () => undefined,
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger: createRecordingLogger().logger,
      events: [invoicePaidEvent],
      tasks: [rebuildIndexTask, recordInvoicePaymentTask],
    });

    try {
      const { runId } = await nextasking.dispatch("rebuild-index", { accountId: " 7 " });
      const { eventId } = await nextasking.emit("invoice.paid", { accountId: "account-9", invoiceId: "inv-1" });
      await nextasking.startWorker();
      const listenerRunId = deriveListenerRunId(eventId, "record-invoice-payment");
      await vi.waitFor(
        async () => {
          expect(await storedRun(redisUrl, keyPrefix, "record-invoice-payment", listenerRunId)).toBeDefined();
        },
        { timeout: 10_000 },
      );

      expect(await storedRun(redisUrl, keyPrefix, "rebuild-index", runId)).toMatchObject({
        concurrencyKey: "account-7",
      });
      expect(await storedRun(redisUrl, keyPrefix, "record-invoice-payment", listenerRunId)).toMatchObject({
        concurrencyKey: "account-9",
      });
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);

integrationTest(
  "an emit whose listener's key function throws throws, naming the listener, and stores no run of any listener",
  async ({ redisUrl, keyPrefix }) => {
    const recordInvoicePaymentTask = defineListener("record-invoice-payment", {
      on: invoicePaidEvent,
      handler: async () => undefined,
    });
    const billAccountTask = defineListener("bill-account", {
      on: invoicePaidEvent,
      concurrency: {
        key: () => {
          throw new Error("no account");
        },
        limit: 1,
      },
      handler: async () => undefined,
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger: createRecordingLogger().logger,
      events: [invoicePaidEvent],
      tasks: [recordInvoicePaymentTask, billAccountTask],
    });

    try {
      const emitting = nextasking.emit("invoice.paid", { accountId: "account-9", invoiceId: "inv-1" });

      await expect(emitting).rejects.toThrow(
        'Task "bill-account" has an invalid concurrency.key: it threw "no account"',
      );
      expect(await storedRunIdentities(redisUrl, keyPrefix)).toEqual([]);
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);
