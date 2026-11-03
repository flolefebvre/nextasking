"use server";
import { nextasking } from "@/nextasking";

export async function payInvoice(formData: FormData) {
  await nextasking.emit("invoice.paid", { invoiceId: String(formData.get("invoiceId")) });
}
