import { defineTask } from "@flefebvre/nextasking";
import { z } from "zod";
import { db, pdf } from "@/lib/fakes";
import { invoicePdfReady } from "@/jobs/events";

export const generateInvoicePdf = defineTask("generate-invoice-pdf", {
  schema: z.object({ invoiceId: z.string(), accountId: z.string() }),
  retries: 3,
  timeout: "2m",
  concurrency: { key: (payload) => payload.accountId, limit: 1 },
  handler: async ({ payload, step, emit }) => {
    const pdfUrl = await step.run("render", () => pdf.renderInvoice(payload.invoiceId));
    await step.run("save-url", () => db.invoice.setPdfUrl(payload.invoiceId, pdfUrl));
    await emit(invoicePdfReady, { invoiceId: payload.invoiceId, pdfUrl });
  },
});
