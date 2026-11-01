import { createNextasking } from "@flefebvre/nextasking";
import { invoicePaid, invoicePdfReady, userSignedUp } from "@/jobs/events";
import { generateInvoicePdf } from "@/jobs/tasks/generate-invoice-pdf";
import { purgeExpiredSessions } from "@/jobs/tasks/purge-expired-sessions";
import { recordInvoicePayment } from "@/jobs/tasks/record-invoice-payment";
import { sendWelcomeEmail } from "@/jobs/tasks/send-welcome-email";

export const nextasking = createNextasking({
  connection: { url: process.env.REDIS_URL ?? "redis://localhost:6379" },
  events: [userSignedUp, invoicePaid, invoicePdfReady],
  tasks: [sendWelcomeEmail, recordInvoicePayment, generateInvoicePdf, purgeExpiredSessions],
});

export const { dispatch, emit } = nextasking;
