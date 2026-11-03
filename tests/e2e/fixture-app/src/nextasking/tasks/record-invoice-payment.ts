import { defineListener } from "@flefebvre/nextasking";
import { invoicePaid } from "@/nextasking/events/invoice-paid";
import { writeRecord } from "@/nextasking/records";

export const recordInvoicePayment = defineListener("record-invoice-payment", {
  on: invoicePaid,
  handler: async ({ payload, runId }) => {
    await writeRecord("record-invoice-payment", { runId, payload });
  },
});
