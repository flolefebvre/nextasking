import { defineListener } from "@flefebvre/nextasking";
import { invoicePaidEvent } from "@/nextasking/events/invoice-paid";
import { writeRecord } from "@/nextasking/records";

export const sendReceiptTask = defineListener("send-receipt", {
  on: invoicePaidEvent,
  handler: async ({ payload, runId }) => {
    await writeRecord("send-receipt", { runId, payload });
  },
});
