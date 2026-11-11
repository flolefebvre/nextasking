import { expect, vi } from "vitest";
import { z } from "zod";

import { NonRetriableError, createNextasking, defineEvent, defineListener } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";

integrationTest(
  "getEvent lists the run of each listener of an event id, pending from the emit on, and none for an id never emitted",
  { timeout: 20_000 },
  async ({ redisUrl, keyPrefix }) => {
    const invoicePaidEvent = defineEvent("invoice.paid", { schema: z.object({ invoiceId: z.string() }) });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger: createRecordingLogger().logger,
      events: [invoicePaidEvent],
      tasks: [
        defineListener("record-invoice-payment", { on: invoicePaidEvent, handler: async () => undefined }),
        defineListener("notify-accountant", {
          on: invoicePaidEvent,
          handler: async () => {
            throw new NonRetriableError("no accountant on file");
          },
        }),
      ],
    });

    try {
      const { eventId } = await nextasking.emit("invoice.paid", { invoiceId: "inv-1" });
      const eventBeforeTheWorker = await nextasking.getEvent("invoice.paid", eventId);
      await nextasking.startWorker();
      await vi.waitFor(
        async () => {
          const event = await nextasking.getEvent("invoice.paid", eventId);
          expect(event.runsByListener["record-invoice-payment"]?.state).toBe("completed");
          expect(event.runsByListener["notify-accountant"]?.state).toBe("failed");
        },
        { timeout: 15_000 },
      );

      expect(eventBeforeTheWorker).toMatchObject({
        event: "invoice.paid",
        eventId,
        runsByListener: {
          "record-invoice-payment": { task: "record-invoice-payment", state: "pending" },
          "notify-accountant": { task: "notify-accountant", state: "pending" },
        },
      });
      expect(eventBeforeTheWorker).not.toHaveProperty("fanOut");
      expect((await nextasking.getEvent("invoice.paid", eventId)).runsByListener["notify-accountant"]).toMatchObject({
        task: "notify-accountant",
        payload: { invoiceId: "inv-1" },
        reason: "non-retriable",
        errors: [{ message: "no accountant on file" }],
      });
      expect(await nextasking.getEvent("invoice.paid", "never-emitted")).toEqual({
        event: "invoice.paid",
        eventId: "never-emitted",
        runsByListener: { "record-invoice-payment": undefined, "notify-accountant": undefined },
      });
      await expect(nextasking.getEvent("invoice.voided" as "invoice.paid", eventId)).rejects.toThrow(
        'Unknown event "invoice.voided"',
      );
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);
