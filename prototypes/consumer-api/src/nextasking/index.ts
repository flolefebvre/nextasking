import { createNextasking } from "@flefebvre/nextasking";
import { invoicePaid, invoicePdfReady, userSignedUp } from "@/nextasking/events";
import { generateInvoicePdf } from "@/nextasking/tasks/generate-invoice-pdf";
import { purgeExpiredSessions } from "@/nextasking/tasks/purge-expired-sessions";
import { recordInvoicePayment } from "@/nextasking/tasks/record-invoice-payment";
import { sendWelcomeEmail } from "@/nextasking/tasks/send-welcome-email";

export const nextasking = createNextasking({
  connection: { url: process.env.REDIS_URL ?? "redis://localhost:6379" },
  events: [userSignedUp, invoicePaid, invoicePdfReady],
  tasks: [sendWelcomeEmail, recordInvoicePayment, generateInvoicePdf, purgeExpiredSessions],
});

declare module "@flefebvre/nextasking" {
  interface Register {
    nextasking: typeof nextasking;
  }
}
