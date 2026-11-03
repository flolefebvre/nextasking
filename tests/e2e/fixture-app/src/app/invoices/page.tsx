import { payInvoice } from "./actions";

export const dynamic = "force-dynamic";

export default function InvoicesPage() {
  return (
    <form action={payInvoice}>
      <input name="invoiceId" />
      <button type="submit">Pay</button>
    </form>
  );
}
