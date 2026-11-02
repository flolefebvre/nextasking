import * as v from "valibot";
import { describe, expect, expectTypeOf, test } from "vitest";
import { z } from "zod";

import { defineTask } from "./definitions.js";

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

  test("hands the handler the run id and the attempt number", () => {
    defineTask("purge-sessions", {
      schema: z.object({}),
      handler: async ({ runId, attempt }) => {
        expectTypeOf(runId).toEqualTypeOf<string>();
        expectTypeOf(attempt).toEqualTypeOf<number>();
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
