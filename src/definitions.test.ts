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
