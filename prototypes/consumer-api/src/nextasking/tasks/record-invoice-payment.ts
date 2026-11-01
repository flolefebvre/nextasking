import { defineListener } from "@flefebvre/nextasking";
import { db } from "@/lib/fakes";
import { invoicePaid } from "@/nextasking/events";

export const recordInvoicePayment = defineListener("record-invoice-payment", {
  on: invoicePaid,
  handler: async ({ payload }) => {
    await db.invoice.markPaid(payload.invoiceId);
  },
});
