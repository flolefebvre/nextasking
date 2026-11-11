import { defineListener } from "@flefebvre/nextasking";
import { invoicePaidEvent } from "@/nextasking/events/invoice-paid";
import { writeRecord } from "@/nextasking/records";

export const recordInvoicePaymentTask = defineListener("record-invoice-payment", {
  on: invoicePaidEvent,
  handler: async ({ payload, runId }) => {
    await writeRecord("record-invoice-payment", { runId, payload });
  },
});
