import { defineEvent } from "@flefebvre/nextasking";
import { z } from "zod";

export const userSignedUp = defineEvent("user.signed-up", {
  schema: z.object({ userId: z.string(), email: z.email(), plan: z.enum(["free", "pro"]) }),
});

export const invoicePaid = defineEvent("invoice.paid", {
  schema: z.object({ invoiceId: z.string(), amountInCents: z.int().positive() }),
});

export const invoicePdfReady = defineEvent("invoice.pdf-ready", {
  schema: z.object({ invoiceId: z.string(), pdfUrl: z.url() }),
});
