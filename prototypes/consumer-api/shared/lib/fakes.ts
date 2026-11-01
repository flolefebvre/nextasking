// PROTOTYPE: stand-ins for the app's own modules (database, email, Stripe, PDF), so the consumer code typechecks.
export declare const db: {
  user: {
    create: (data: {
      email: string;
      name: string;
    }) => Promise<{ id: string; email: string; name: string; plan: "free" | "pro" }>;
    find: (id: string) => Promise<{ id: string; email: string; name: string; onboarded: boolean }>;
  };
  invoice: {
    find: (id: string) => Promise<{ id: string; accountId: string; pdfUrl: string | null }>;
    markPaid: (id: string) => Promise<void>;
    setPdfUrl: (id: string, url: string) => Promise<void>;
  };
  session: { deleteExpired: () => Promise<number> };
};
export declare const email: {
  send: (message: { to: string; template: string; idempotencyKey: string }) => Promise<void>;
};
export declare const stripe: {
  parseWebhook: (request: Request) => Promise<{ type: string; invoiceId: string; amountInCents: number }>;
};
export declare const pdf: { renderInvoice: (invoiceId: string) => Promise<string> };
