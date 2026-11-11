import { defineEvent } from "@flefebvre/nextasking";
import { z } from "zod";

export const invoicePaidEvent = defineEvent("invoice.paid", {
  schema: z.object({ invoiceId: z.string() }),
});
