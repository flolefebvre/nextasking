"use server";

import { db } from "@/lib/fakes";
import { nextasking } from "@/jobs/nextasking";

export async function regenerateInvoicePdf(invoiceId: string) {
  const invoice = await db.invoice.find(invoiceId);
  await nextasking.dispatch(
    "generate-invoice-pdf",
    { invoiceId, accountId: invoice.accountId },
    { id: `invoice-pdf:${invoiceId}`, delay: "5s" },
  );
}

export async function purgeSessionsNow() {
  await nextasking.dispatch("purge-expired-sessions");
}
