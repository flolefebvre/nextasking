import { stripe } from "@/lib/fakes";
import { nextasking } from "@/nextasking";

export async function POST(request: Request) {
  const webhook = await stripe.parseWebhook(request);
  if (webhook.type === "invoice.paid") {
    await nextasking.emit("invoice.paid", { invoiceId: webhook.invoiceId, amountInCents: webhook.amountInCents });
  }
  return new Response(null, { status: 204 });
}
