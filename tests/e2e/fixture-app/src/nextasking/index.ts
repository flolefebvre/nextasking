import { createNextasking } from "@flefebvre/nextasking";
import { invoicePaid } from "@/nextasking/events/invoice-paid";
import { recordInvoicePayment } from "@/nextasking/tasks/record-invoice-payment";
import { sendReceipt } from "@/nextasking/tasks/send-receipt";
import { sendWelcomeEmail } from "@/nextasking/tasks/send-welcome-email";

export const nextasking = createNextasking({
  connection: { url: process.env.REDIS_URL ?? "redis://localhost:6379" },
  prefix: process.env.NEXTASKING_PREFIX ?? "nextasking",
  events: process.env.NEXTASKING_E2E_CONFIG_ERROR === "unlisted-event" ? [] : [invoicePaid],
  tasks: [sendWelcomeEmail, recordInvoicePayment, sendReceipt],
});

declare module "@flefebvre/nextasking" {
  interface Register {
    nextasking: typeof nextasking;
  }
}
