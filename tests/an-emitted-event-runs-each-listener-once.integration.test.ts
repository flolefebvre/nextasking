import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineEvent, defineListener } from "../src/index.js";
import type { TaskContext } from "../src/index.js";
import { deriveListenerRunId } from "../src/ids.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";
import { storedRunIdentities } from "./stored-run-identities.js";

type InvoicePaidPayload = { invoiceId: string; amountInCents: number };

integrationTest(
  "an emitted event runs each of its listeners once, with the event's schema output",
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const recordedContextsByListener: Record<string, TaskContext<InvoicePaidPayload>[]> = {
      "record-invoice-payment": [],
      "send-receipt": [],
    };
    const invoicePaid = defineEvent("invoice.paid", {
      schema: z.object({ invoiceId: z.string().trim(), amountInCents: z.int().positive() }),
    });
    const recordInvoicePayment = defineListener("record-invoice-payment", {
      on: invoicePaid,
      handler: async (context) => {
        recordedContextsByListener["record-invoice-payment"]?.push(context);
      },
    });
    const sendReceipt = defineListener("send-receipt", {
      on: invoicePaid,
      handler: async (context) => {
        context.logger.info({}, "receipt sent");
        recordedContextsByListener["send-receipt"]?.push(context);
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      events: [invoicePaid],
      tasks: [recordInvoicePayment, sendReceipt],
    });

    try {
      await nextasking.startWorker();

      const { eventId } = await nextasking.emit("invoice.paid", { invoiceId: " inv-1 ", amountInCents: 4200 });

      await vi.waitFor(
        () => {
          expect(lines.filter((line) => line.message === "run completed")).toHaveLength(2);
        },
        { timeout: 10_000 },
      );
      const listenerContexts = Object.values(recordedContextsByListener).flat();
      expect(listenerContexts).toHaveLength(2);
      for (const context of listenerContexts) {
        expect(context).toMatchObject({ payload: { invoiceId: "inv-1", amountInCents: 4200 }, attempt: 1 });
      }
      const [recordInvoicePaymentContext] = recordedContextsByListener["record-invoice-payment"] ?? [];
      const [sendReceiptContext] = recordedContextsByListener["send-receipt"] ?? [];
      expect(recordInvoicePaymentContext?.runId).toBe(deriveListenerRunId(eventId, "record-invoice-payment"));
      expect(sendReceiptContext?.runId).toBe(deriveListenerRunId(eventId, "send-receipt"));
      expect(await storedRunIdentities(redisUrl, keyPrefix)).toEqual([
        { task: "record-invoice-payment", runId: recordInvoicePaymentContext?.runId },
        { task: "send-receipt", runId: sendReceiptContext?.runId },
      ]);
      expect(lines).toContainEqual({
        level: "info",
        fields: {
          task: "send-receipt",
          runId: sendReceiptContext?.runId,
          attempt: 1,
          eventName: "invoice.paid",
          eventId,
        },
        message: "receipt sent",
      });
      expect(lines).toContainEqual({
        level: "debug",
        fields: {
          task: "record-invoice-payment",
          runId: recordInvoicePaymentContext?.runId,
          attempt: 1,
          eventName: "invoice.paid",
          eventId,
        },
        message: "attempt started",
      });
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);
