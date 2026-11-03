import { expect, vi } from "vitest";
import { z } from "zod";

import { NonRetriableError, createNextasking, defineEvent, defineListener } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";

integrationTest(
  "getEvent shows an event's fan-out state and the run of each of its listeners",
  { timeout: 20_000 },
  async ({ redisUrl, keyPrefix }) => {
    const invoicePaid = defineEvent("invoice.paid", { schema: z.object({ invoiceId: z.string() }) });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      events: [invoicePaid],
      tasks: [
        defineListener("record-invoice-payment", { on: invoicePaid, handler: async () => undefined }),
        defineListener("notify-accountant", {
          on: invoicePaid,
          handler: async () => {
            throw new NonRetriableError("no accountant on file");
          },
        }),
      ],
    });

    try {
      const { eventId } = await nextasking.emit("invoice.paid", { invoiceId: "inv-1" });
      const eventBeforeFanOut = await nextasking.getEvent("invoice.paid", eventId);
      await nextasking.startWorker();
      await vi.waitFor(
        async () => {
          const event = await nextasking.getEvent("invoice.paid", eventId);
          expect(event.fanOut).toBe("completed");
          expect(event.runsByListener["record-invoice-payment"]?.state).toBe("completed");
          expect(event.runsByListener["notify-accountant"]?.state).toBe("failed");
        },
        { timeout: 15_000 },
      );

      expect(eventBeforeFanOut).toEqual({
        event: "invoice.paid",
        eventId,
        fanOut: "pending",
        runsByListener: { "record-invoice-payment": undefined, "notify-accountant": undefined },
      });
      expect((await nextasking.getEvent("invoice.paid", eventId)).runsByListener["notify-accountant"]).toMatchObject({
        task: "notify-accountant",
        payload: { invoiceId: "inv-1" },
        reason: "non-retriable",
        errors: [{ message: "no accountant on file" }],
      });
      expect(await nextasking.getEvent("invoice.paid", "never-emitted")).toEqual({
        event: "invoice.paid",
        eventId: "never-emitted",
        fanOut: undefined,
        runsByListener: { "record-invoice-payment": undefined, "notify-accountant": undefined },
      });
      await expect(nextasking.getEvent("invoice.voided" as "invoice.paid", eventId)).rejects.toThrow(
        'Unknown event "invoice.voided"',
      );
    } finally {
      await closeNextasking(nextasking);
    }
  },
);
