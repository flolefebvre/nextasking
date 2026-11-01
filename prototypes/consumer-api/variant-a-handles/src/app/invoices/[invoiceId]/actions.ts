"use server";

import { db } from "@/lib/fakes";
import { dispatch } from "@/jobs/nextasking";
import { generateInvoicePdf } from "@/jobs/tasks/generate-invoice-pdf";
import { purgeExpiredSessions } from "@/jobs/tasks/purge-expired-sessions";

export async function regenerateInvoicePdf(invoiceId: string) {
  const invoice = await db.invoice.find(invoiceId);
  await dispatch(
    generateInvoicePdf,
    { invoiceId, accountId: invoice.accountId },
    { id: `invoice-pdf:${invoiceId}`, delay: "5s" },
  );
}

export async function purgeSessionsNow() {
  await dispatch(purgeExpiredSessions);
}
