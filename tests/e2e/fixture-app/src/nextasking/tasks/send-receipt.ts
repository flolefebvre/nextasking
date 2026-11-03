import { defineListener } from "@flefebvre/nextasking";
import { invoicePaid } from "@/nextasking/events/invoice-paid";
import { writeRecord } from "@/nextasking/records";

export const sendReceipt = defineListener("send-receipt", {
  on: invoicePaid,
  handler: async ({ payload, runId }) => {
    await writeRecord("send-receipt", { runId, payload });
  },
});
