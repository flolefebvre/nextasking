import { defineEvent } from "@flefebvre/nextasking";
import { z } from "zod";

export const userSignedUp = defineEvent(
  "user.signed-up",
  z.object({ userId: z.string(), email: z.email(), plan: z.enum(["free", "pro"]) }),
);

export const invoicePaid = defineEvent(
  "invoice.paid",
  z.object({ invoiceId: z.string(), amountInCents: z.int().positive() }),
);

export const invoicePdfReady = defineEvent("invoice.pdf-ready", z.object({ invoiceId: z.string(), pdfUrl: z.url() }));
