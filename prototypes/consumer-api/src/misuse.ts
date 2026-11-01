import { defineEvent, defineListener, defineTask } from "@flefebvre/nextasking";
import { z } from "zod";
import { invoicePaid } from "@/nextasking/events";
import { nextasking } from "@/nextasking";

export async function misuse() {
  // @ts-expect-error wrong payload: plan is not in the enum
  await nextasking.emit("user.signed-up", { userId: "u1", email: "a@b.c", plan: "gold" });

  // @ts-expect-error wrong payload: missing accountId
  await nextasking.dispatch("generate-invoice-pdf", { invoiceId: "i1" });

  // @ts-expect-error a scheduled task takes no payload
  await nextasking.dispatch("purge-expired-sessions", { force: true });

  // @ts-expect-error an event name is not a task name
  await nextasking.dispatch("user.signed-up", { userId: "u1", email: "a@b.c", plan: "pro" });

  // @ts-expect-error a typo in a task name
  await nextasking.dispatch("generate-invoice-pfd", { invoiceId: "i1", accountId: "a1" });

  // @ts-expect-error a typo in an event name
  await nextasking.emit("user.signedup", { userId: "u1", email: "a@b.c", plan: "pro" });

  // @ts-expect-error not a duration
  await nextasking.dispatch("purge-expired-sessions", { delay: "5sfdsfsdfs" });

  defineTask("emits-a-wrong-payload-from-a-handler", {
    schema: z.object({ id: z.string() }),
    handler: async ({ emit }) => {
      // @ts-expect-error inside a handler too: wrong payload
      await emit("invoice.paid", { invoiceId: "i1" });
    },
  });

  defineListener("listener-reading-a-field-its-event-lacks", {
    on: invoicePaid,
    handler: async ({ payload }) => {
      // @ts-expect-error invoice.paid has no userId
      console.log(payload.userId);
    },
  });

  defineListener("misspelt-option", {
    on: invoicePaid,
    // @ts-expect-error a misspelt option is reported on the property
    retrys: 5,
    handler: async () => {},
  });

  // @ts-expect-error a payload that does not survive JSON
  defineEvent("has-a-date", { schema: z.object({ at: z.date() }) });

  const webhookName: string = "invoice.paid";
  // @ts-expect-error a name from the outside world is a plain string
  await nextasking.emit(webhookName, {});
}
