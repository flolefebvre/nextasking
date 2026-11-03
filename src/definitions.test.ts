import * as v from "valibot";
import { describe, expect, expectTypeOf, test } from "vitest";
import { z } from "zod";

import { defineEvent, defineListener, defineScheduledTask, defineTask } from "./definitions.js";
import type { StepTools } from "./step-engine.js";

describe("defineTask", () => {
  test("takes a Zod schema and hands the handler the schema's output", () => {
    const sendWelcomeEmail = defineTask("send-welcome-email", {
      schema: z.object({ email: z.email(), visits: z.string().transform((visits) => Number(visits)) }),
      handler: async ({ payload }) => {
        expectTypeOf(payload).toEqualTypeOf<{ email: string; visits: number }>();
      },
    });

    expect(sendWelcomeEmail.name).toBe("send-welcome-email");
    expectTypeOf(sendWelcomeEmail.name).toEqualTypeOf<"send-welcome-email">();
  });

  test("takes a Valibot schema and hands the handler the schema's output", () => {
    const generateInvoicePdf = defineTask("generate-invoice-pdf", {
      schema: v.object({ invoiceId: v.string(), pages: v.pipe(v.string(), v.transform(Number)) }),
      handler: async ({ payload }) => {
        expectTypeOf(payload).toEqualTypeOf<{ invoiceId: string; pages: number }>();
      },
    });

    expect(generateInvoicePdf.name).toBe("generate-invoice-pdf");
  });

  test("hands the handler the run id, the attempt number and the step tools", () => {
    defineTask("purge-sessions", {
      schema: z.object({}),
      handler: async ({ runId, attempt, step }) => {
        expectTypeOf(runId).toEqualTypeOf<string>();
        expectTypeOf(attempt).toEqualTypeOf<number>();
        expectTypeOf(step).toEqualTypeOf<StepTools>();
      },
    });
  });

  test("rejects a schema that isn't a Standard Schema", () => {
    defineTask("send-welcome-email", {
      // @ts-expect-error a plain object is not a Standard Schema
      schema: { email: "string" },
      handler: async () => undefined,
    });
  });

  test("rejects a schema whose output isn't JSON", () => {
    defineTask("send-reminder", {
      // @ts-expect-error a Date isn't JSON
      schema: z.object({ sendAt: z.date() }),
      handler: async () => undefined,
    });
    defineTask("send-reminder", {
      // @ts-expect-error a transform into a Date isn't JSON
      schema: z.object({ sendAt: z.string().transform((sendAt) => new Date(sendAt)) }),
      handler: async () => undefined,
    });
  });

  test("rejects a schema whose input isn't JSON", () => {
    defineTask("send-reminder", {
      // @ts-expect-error a Date input isn't JSON, even transformed into a string
      schema: z.object({ sendAt: z.date().transform((sendAt) => sendAt.toISOString()) }),
      handler: async () => undefined,
    });
  });

  test("takes optional properties, arrays and nulls", () => {
    defineTask("tag-user", {
      schema: z.object({ userId: z.string(), nickname: z.string().optional(), tags: z.array(z.string()) }),
      handler: async ({ payload }) => {
        expectTypeOf(payload).toEqualTypeOf<{ userId: string; nickname?: string | undefined; tags: string[] }>();
      },
    });
    defineTask("clear-avatar", {
      schema: v.object({ avatarUrl: v.nullable(v.string()), sizes: v.optional(v.array(v.number())) }),
      handler: async () => undefined,
    });
  });

  test("records the task options as passed, with no defaults", () => {
    const rebuildIndex = defineTask("rebuild-index", {
      schema: z.object({ accountId: z.string() }),
      retries: 5,
      backoff: "30s",
      timeout: "10m",
      concurrency: { key: (payload) => payload.accountId, limit: 1 },
      handler: async () => undefined,
    });
    const purgeSessions = defineTask("purge-sessions", { schema: z.object({}), handler: async () => undefined });

    expect(rebuildIndex).toMatchObject({ retries: 5, backoff: "30s", timeout: "10m", concurrency: { limit: 1 } });
    expect(rebuildIndex.concurrency?.key?.({ accountId: "account-7" })).toBe("account-7");
    expect(Object.keys(purgeSessions).sort()).toEqual(["handler", "name", "schema"]);
  });

  test("types the concurrency key from the schema's output", () => {
    defineTask("count-page-views", {
      schema: z.object({ views: z.string().transform((views) => Number(views)) }),
      concurrency: {
        key: (payload) => {
          expectTypeOf(payload).toEqualTypeOf<{ views: number }>();
          return String(payload.views);
        },
        limit: 2,
      },
      handler: async () => undefined,
    });
  });

  test("rejects a misspelt option on its own line", () => {
    defineTask("rebuild-index", {
      schema: z.object({}),
      // @ts-expect-error retires is not an option
      retires: 3,
      handler: async () => undefined,
    });
  });

  test("rejects a duration that isn't a whole number and a unit", () => {
    defineTask("rebuild-index", {
      schema: z.object({}),
      // @ts-expect-error a fraction isn't a duration
      backoff: "0.5h",
      // @ts-expect-error a space isn't a duration
      timeout: "5 s",
      handler: async () => undefined,
    });
  });

  test("rejects a handler typed for another payload", () => {
    defineTask("send-welcome-email", {
      schema: z.object({ email: z.string() }),
      // @ts-expect-error the handler expects a number where the schema gives a string
      handler: async (context: { payload: { email: number } }) => {
        expectTypeOf(context.payload.email).toBeNumber();
      },
    });
  });

  test("rejects a handler reading a field the schema lacks", () => {
    defineTask("send-welcome-email", {
      schema: z.object({ email: z.email() }),
      handler: async ({ payload }) => {
        // @ts-expect-error the schema has no userId
        expectTypeOf(payload.userId).toBeString();
      },
    });
  });
});

const invoicePaid = defineEvent("invoice.paid", {
  schema: z.object({ invoiceId: z.string(), amountInCents: z.string().transform((amount) => Number(amount)) }),
});

describe("defineEvent", () => {
  test("records the name and the schema, and nothing else", () => {
    expect(Object.keys(invoicePaid).sort()).toEqual(["name", "schema"]);
    expect(invoicePaid.name).toBe("invoice.paid");
    expectTypeOf(invoicePaid.name).toEqualTypeOf<"invoice.paid">();
  });

  test("rejects a schema whose output isn't JSON", () => {
    defineEvent("reminder.due", {
      // @ts-expect-error a Date isn't JSON
      schema: z.object({ dueAt: z.date() }),
    });
  });

  test("rejects an option an event doesn't take", () => {
    defineEvent("invoice.voided", {
      schema: z.object({}),
      // @ts-expect-error an event has no handler
      handler: async () => undefined,
    });
  });
});

describe("defineListener", () => {
  test("hands the handler the event's schema output, the run id and the attempt", () => {
    const recordInvoicePayment = defineListener("record-invoice-payment", {
      on: invoicePaid,
      handler: async ({ payload, runId, attempt }) => {
        expectTypeOf(payload).toEqualTypeOf<{ invoiceId: string; amountInCents: number }>();
        expectTypeOf(runId).toEqualTypeOf<string>();
        expectTypeOf(attempt).toEqualTypeOf<number>();
      },
    });

    expect(recordInvoicePayment.name).toBe("record-invoice-payment");
    expectTypeOf(recordInvoicePayment.name).toEqualTypeOf<"record-invoice-payment">();
    expect(recordInvoicePayment.on).toBe(invoicePaid);
    expect(recordInvoicePayment.schema).toBe(invoicePaid.schema);
  });

  test("records the task options as passed, with no defaults", () => {
    const sendReceipt = defineListener("send-receipt", {
      on: invoicePaid,
      retries: 3,
      backoff: "1m",
      timeout: "30s",
      concurrency: { key: (payload) => payload.invoiceId, limit: 2 },
      handler: async () => undefined,
    });
    const archiveInvoice = defineListener("archive-invoice", { on: invoicePaid, handler: async () => undefined });

    expect(sendReceipt).toMatchObject({ retries: 3, backoff: "1m", timeout: "30s", concurrency: { limit: 2 } });
    expect(sendReceipt.concurrency?.key?.({ invoiceId: "inv-7", amountInCents: 100 })).toBe("inv-7");
    expect(Object.keys(archiveInvoice).sort()).toEqual(["handler", "name", "on", "schema"]);
  });

  test("rejects a handler typed for another payload", () => {
    defineListener("record-invoice-payment", {
      on: invoicePaid,
      // @ts-expect-error the event gives amountInCents as a number
      handler: async (context: { payload: { amountInCents: string } }) => {
        expectTypeOf(context.payload.amountInCents).toBeString();
      },
    });
  });

  test("rejects a schema of its own and a misspelt option", () => {
    defineListener("record-invoice-payment", {
      on: invoicePaid,
      // @ts-expect-error a listener takes its event's schema
      schema: z.object({}),
      handler: async () => undefined,
    });
    defineListener("record-invoice-payment", {
      on: invoicePaid,
      // @ts-expect-error retires is not an option
      retires: 3,
      handler: async () => undefined,
    });
  });

  test("rejects an on that isn't an event definition", () => {
    defineListener("record-invoice-payment", {
      // @ts-expect-error on takes the event definition, not its name
      on: "invoice.paid",
      handler: async () => undefined,
    });
  });
});

describe("defineScheduledTask", () => {
  test("hands the handler scheduledAt and no payload, with the other context fields of a task", () => {
    const purgeExpiredSessions = defineScheduledTask("purge-expired-sessions", {
      schedule: { cron: "0 3 * * *", timezone: "Europe/Paris" },
      handler: async (context) => {
        expectTypeOf(context.scheduledAt).toEqualTypeOf<Date | undefined>();
        expectTypeOf(context.runId).toEqualTypeOf<string>();
        expectTypeOf(context.attempt).toEqualTypeOf<number>();
        expectTypeOf(context.step).toEqualTypeOf<StepTools>();
        expectTypeOf(context).not.toHaveProperty("payload");
      },
    });

    expect(purgeExpiredSessions.name).toBe("purge-expired-sessions");
    expectTypeOf(purgeExpiredSessions.name).toEqualTypeOf<"purge-expired-sessions">();
  });

  test("records the schedule and the task options as passed, with no defaults", () => {
    const sendDigest = defineScheduledTask("send-digest", {
      schedule: { cron: "0 8 * * 1", timezone: "America/New_York" },
      retries: 2,
      backoff: "1m",
      timeout: "10m",
      concurrency: { limit: 1 },
      handler: async () => undefined,
    });
    const purgeExpiredSessions = defineScheduledTask("purge-expired-sessions", {
      schedule: { cron: "0 3 * * *" },
      handler: async () => undefined,
    });

    expect(sendDigest).toMatchObject({
      schedule: { cron: "0 8 * * 1", timezone: "America/New_York" },
      retries: 2,
      backoff: "1m",
      timeout: "10m",
      concurrency: { limit: 1 },
    });
    expect(Object.keys(purgeExpiredSessions).sort()).toEqual(["handler", "name", "schedule"]);
    expect(purgeExpiredSessions.schedule).toEqual({ cron: "0 3 * * *" });
  });

  test("rejects a schema, a concurrency key and a schedule without a cron", () => {
    defineScheduledTask("purge-expired-sessions", {
      schedule: { cron: "0 3 * * *" },
      // @ts-expect-error a scheduled task takes no payload, so no schema
      schema: z.object({}),
      handler: async () => undefined,
    });
    defineScheduledTask("purge-expired-sessions", {
      schedule: { cron: "0 3 * * *" },
      // @ts-expect-error a scheduled task has no payload to key on
      concurrency: { key: () => "all", limit: 1 },
      handler: async () => undefined,
    });
    defineScheduledTask("purge-expired-sessions", {
      // @ts-expect-error a schedule needs its cron
      schedule: { timezone: "UTC" },
      handler: async () => undefined,
    });
  });
});
