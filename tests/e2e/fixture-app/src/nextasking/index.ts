import { createNextasking } from "@flefebvre/nextasking";
import { invoicePaidEvent } from "@/nextasking/events/invoice-paid";
import { logger } from "@/nextasking/logger";
import { buildReportTask } from "@/nextasking/tasks/build-report";
import { recordInvoicePaymentTask } from "@/nextasking/tasks/record-invoice-payment";
import { refreshExchangeRatesTask } from "@/nextasking/tasks/refresh-exchange-rates";
import { sendReceiptTask } from "@/nextasking/tasks/send-receipt";
import { sendWelcomeEmailTask } from "@/nextasking/tasks/send-welcome-email";

export const nextasking = createNextasking({
  connection: { url: process.env.REDIS_URL ?? "redis://localhost:6379" },
  prefix: process.env.NEXTASKING_PREFIX ?? "nextasking",
  logger,
  events: process.env.NEXTASKING_E2E_CONFIG_ERROR === "unlisted-event" ? [] : [invoicePaidEvent],
  tasks: [
    sendWelcomeEmailTask,
    buildReportTask,
    recordInvoicePaymentTask,
    sendReceiptTask,
    ...(process.env.NEXTASKING_E2E_SCHEDULE === "on" ? [refreshExchangeRatesTask] : []),
  ],
});

declare module "@flefebvre/nextasking" {
  interface Register {
    nextasking: typeof nextasking;
  }
}
