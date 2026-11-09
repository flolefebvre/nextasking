import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineEvent, defineListener } from "../src/index.js";
import { deriveListenerRunId } from "../src/ids.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";
import { storedRunIdentities } from "./stored-run-identities.js";

integrationTest("a repeated eventId returns the first id and fans out once", async ({ redisUrl, keyPrefix }) => {
  const { logger, lines } = createRecordingLogger();
  const recordedInvoiceIds: string[] = [];
  const invoicePaid = defineEvent("invoice.paid", {
    schema: z.object({ invoiceId: z.string() }),
  });
  const recordInvoicePayment = defineListener("record-invoice-payment", {
    on: invoicePaid,
    handler: async ({ payload }) => {
      recordedInvoiceIds.push(payload.invoiceId);
    },
  });
  const nextasking = createNextasking({
    connection: { url: redisUrl },
    prefix: keyPrefix,
    logger,
    events: [invoicePaid],
    tasks: [recordInvoicePayment],
  });
  const eventId = "stripe-evt-1";

  try {
    const firstEmit = await nextasking.emit("invoice.paid", { invoiceId: "inv-1" }, { eventId });
    const pendingRepeat = await nextasking.emit("invoice.paid", { invoiceId: "inv-2" }, { eventId });
    await nextasking.startWorker();
    await vi.waitFor(
      () => {
        expect(recordedInvoiceIds).toEqual(["inv-1"]);
      },
      { timeout: 10_000 },
    );
    const completedRepeat = await nextasking.emit("invoice.paid", { invoiceId: "inv-3" }, { eventId });

    expect([firstEmit, pendingRepeat, completedRepeat]).toEqual([{ eventId }, { eventId }, { eventId }]);
    expect(await storedRunIdentities(redisUrl, keyPrefix)).toEqual([
      { task: "record-invoice-payment", runId: deriveListenerRunId(eventId, "record-invoice-payment") },
    ]);
    expect(lines.filter((line) => line.message === "duplicate dispatch dropped")).toEqual([
      { level: "debug", fields: { eventName: "invoice.paid", eventId }, message: "duplicate dispatch dropped" },
      { level: "debug", fields: { eventName: "invoice.paid", eventId }, message: "duplicate dispatch dropped" },
    ]);
    expect(recordedInvoiceIds).toEqual(["inv-1"]);
  } finally {
    await closeNextaskingAndCheckItsDuration(nextasking);
  }
});
