import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineEvent, defineListener, defineTask } from "../src/index.js";
import { deriveJobId, deriveListenerRunId } from "../src/ids.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";
import { storedJobNames } from "./stored-job-names.js";
import { storedRun } from "./stored-run.js";
import { withRunQueue } from "./with-run-queue.js";

const invoicePaid = defineEvent("invoice.paid", {
  schema: z.object({ accountId: z.string(), invoiceId: z.string() }),
});

integrationTest(
  "a dispatched run and a listener run store the concurrency key computed from their validated payload",
  async ({ redisUrl, keyPrefix }) => {
    const rebuildIndex = defineTask("rebuild-index", {
      schema: z.object({ accountId: z.string().trim() }),
      concurrency: { key: (payload) => `account-${payload.accountId}`, limit: 1 },
      handler: async () => undefined,
    });
    const recordInvoicePayment = defineListener("record-invoice-payment", {
      on: invoicePaid,
      concurrency: { key: (payload) => payload.accountId, limit: 2 },
      handler: async () => undefined,
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      events: [invoicePaid],
      tasks: [rebuildIndex, recordInvoicePayment],
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
      await closeNextasking(nextasking);
    }
  },
);

integrationTest(
  "a listener key function that throws fails the fan-out attempt, naming the listener, and starts no listener run",
  async ({ redisUrl, keyPrefix }) => {
    const recordInvoicePayment = defineListener("record-invoice-payment", {
      on: invoicePaid,
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
      events: [invoicePaid],
      tasks: [recordInvoicePayment],
    });

    try {
      const { eventId } = await nextasking.emit("invoice.paid", { accountId: "account-9", invoiceId: "inv-1" });
      await nextasking.startWorker();
      const fanOutFailure = await vi.waitFor(
        () =>
          withRunQueue(redisUrl, keyPrefix, async (runQueue) => {
            const fanOutJob = await runQueue.getJob(deriveJobId("invoice.paid", eventId));
            expect(fanOutJob?.attemptsMade).toBe(1);
            return { failedReason: fanOutJob?.failedReason, state: await fanOutJob?.getState() };
          }),
        { timeout: 10_000 },
      );

      expect(fanOutFailure).toEqual({
        failedReason: 'Task "record-invoice-payment" has an invalid concurrency.key: it threw "no account"',
        state: "delayed",
      });
      expect(await storedJobNames(redisUrl, keyPrefix)).toEqual(["invoice.paid"]);
    } finally {
      await closeNextasking(nextasking);
    }
  },
);
