import { defineEvent, defineTask } from "@flefebvre/nextasking";
import { z } from "zod";
import { invoicePaid, userSignedUp } from "@/jobs/events";
import { dispatch, emit } from "@/jobs/nextasking";
import { generateInvoicePdf } from "@/jobs/tasks/generate-invoice-pdf";
import { purgeExpiredSessions } from "@/jobs/tasks/purge-expired-sessions";

export async function misuse() {
  // @ts-expect-error wrong payload: plan is not in the enum
  await emit(userSignedUp, { userId: "u1", email: "a@b.c", plan: "gold" });

  // @ts-expect-error wrong payload: missing accountId
  await dispatch(generateInvoicePdf, { invoiceId: "i1" });

  // @ts-expect-error a scheduled task takes no payload
  await dispatch(purgeExpiredSessions, { force: true });

  // @ts-expect-error an event is not a task
  await dispatch(userSignedUp, { userId: "u1", email: "a@b.c", plan: "pro" });

  const unregisteredTask = defineTask("unregistered", {
    schema: z.object({ id: z.string() }),
    handler: async () => {},
  });
  // @ts-expect-error a task missing from the registry
  await dispatch(unregisteredTask, { id: "x" });

  const unregisteredEvent = defineEvent("unregistered.event", z.object({ id: z.string() }));
  // @ts-expect-error an event missing from the registry
  await emit(unregisteredEvent, { id: "x" });

  defineTask("listener-reading-a-field-its-event-lacks", {
    on: invoicePaid,
    handler: async ({ payload }) => {
      // @ts-expect-error invoice.paid has no userId
      console.log(payload.userId);
    },
  });

  // @ts-expect-error a payload that does not survive JSON
  defineEvent("has-a-date", z.object({ at: z.date() }));

  // @ts-expect-error at most one source: an event or a schedule
  defineTask("both-sources", {
    on: invoicePaid,
    schedule: { cron: "* * * * *" },
    handler: async () => {},
  });
}
