import { defineEvent } from "@flefebvre/nextasking";
import { z } from "zod";

export const invoicePaid = defineEvent("invoice.paid", {
  schema: z.object({ invoiceId: z.string() }),
});
