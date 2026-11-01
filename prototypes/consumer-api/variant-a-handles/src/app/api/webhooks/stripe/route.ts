import { stripe } from "@/lib/fakes";
import { invoicePaid } from "@/jobs/events";
import { emit } from "@/jobs/nextasking";

export async function POST(request: Request) {
  const webhook = await stripe.parseWebhook(request);
  if (webhook.type === "invoice.paid") {
    await emit(invoicePaid, { invoiceId: webhook.invoiceId, amountInCents: webhook.amountInCents });
  }
  return new Response(null, { status: 204 });
}
