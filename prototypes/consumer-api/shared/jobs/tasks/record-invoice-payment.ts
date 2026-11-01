import { defineTask } from "@flefebvre/nextasking";
import { db } from "@/lib/fakes";
import { invoicePaid } from "@/jobs/events";

export const recordInvoicePayment = defineTask("record-invoice-payment", {
  on: invoicePaid,
  handler: async ({ payload }) => {
    await db.invoice.markPaid(payload.invoiceId);
  },
});
