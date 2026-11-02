import { setImmediate } from "node:timers/promises";
import * as v from "valibot";
import { describe, expect, expectTypeOf, test } from "vitest";
import { z } from "zod";

import { defineTask } from "./definitions.js";
import { InvalidPayloadError, UnknownTaskError } from "./errors.js";
import type { Duration } from "./durations.js";
import { createNextasking } from "./registry.js";

const UNREACHABLE_URL = "redis://nextasking.invalid:6379";

const sendWelcomeEmail = defineTask("send-welcome-email", {
  schema: z.object({ email: z.email(), address: z.object({ zipCode: z.string().length(5) }) }),
  handler: async () => undefined,
});

const generateInvoicePdf = defineTask("generate-invoice-pdf", {
  schema: v.object({ invoiceId: v.string(), contactEmail: v.pipe(v.string(), v.email()) }),
  handler: async () => undefined,
});

const countPageViews = defineTask("count-page-views", {
  schema: z.object({ views: z.string().transform((views) => Number(views)) }),
  handler: async () => undefined,
});

function createTestNextasking() {
  return createNextasking({
    connection: { url: UNREACHABLE_URL },
    tasks: [sendWelcomeEmail, generateInvoicePdf, countPageViews],
  });
}

function networkResources(): string[] {
  return process.getActiveResourcesInfo().filter((resource) => /TCP|GetAddrInfo|Pipe/.test(resource));
}

describe("createNextasking", () => {
  test("opens no connection", async () => {
    const networkResourcesBefore = networkResources();

    createTestNextasking();
    await setImmediate();

    expect(networkResources()).toEqual(networkResourcesBefore);
  });

  test("rejects two tasks with the same name", () => {
    const otherSendWelcomeEmail = defineTask("send-welcome-email", {
      schema: z.object({}),
      handler: async () => undefined,
    });

    expect(() =>
      createNextasking({
        connection: { url: UNREACHABLE_URL },
        tasks: [sendWelcomeEmail, generateInvoicePdf, otherSendWelcomeEmail],
      }),
    ).toThrow('Duplicate name "send-welcome-email": every task needs its own name');
  });

  test.each([
    [0, "0"],
    [-1, "-1"],
    [1.5, "1.5"],
    [Number.NaN, "NaN"],
    ["2" as unknown as number, '"2"'],
  ])("rejects a concurrency limit of %s", (limit, shownLimit) => {
    const rebuildIndex = defineTask("rebuild-index", {
      schema: z.object({}),
      concurrency: { limit },
      handler: async () => undefined,
    });

    expect(() => createNextasking({ connection: { url: UNREACHABLE_URL }, tasks: [rebuildIndex] })).toThrow(
      `Task "rebuild-index" has an invalid concurrency.limit: ${shownLimit} is not a positive integer`,
    );
  });

  test.each(["backoff", "timeout"] as const)("rejects an invalid %s, naming the task and the option", (option) => {
    const rebuildIndex = defineTask("rebuild-index", {
      schema: z.object({}),
      [option]: "0.5h" as Duration,
      handler: async () => undefined,
    });

    const creating = () => createNextasking({ connection: { url: UNREACHABLE_URL }, tasks: [rebuildIndex] });

    expect(creating).toThrow(
      `Task "rebuild-index" has an invalid ${option}: "0.5h" is not a duration: a whole number followed by s, m, h or d, such as "30s"`,
    );
    expect(creating).toThrow(expect.objectContaining({ cause: expect.any(Error) }));
  });

  test("accepts valid options", () => {
    const rebuildIndex = defineTask("rebuild-index", {
      schema: z.object({ accountId: z.string() }),
      retries: 3,
      backoff: "30s",
      timeout: "5m",
      concurrency: { key: (payload) => payload.accountId, limit: 1 },
      handler: async () => undefined,
    });

    expect(() => createNextasking({ connection: { url: UNREACHABLE_URL }, tasks: [rebuildIndex] })).not.toThrow();
  });

  test("rejects an invalid config before opening any connection", async () => {
    const rebuildIndex = defineTask("rebuild-index", {
      schema: z.object({}),
      timeout: "soon" as Duration,
      handler: async () => undefined,
    });
    const networkResourcesBefore = networkResources();

    expect(() => createNextasking({ connection: { url: UNREACHABLE_URL }, tasks: [rebuildIndex] })).toThrow();
    await setImmediate();

    expect(networkResources()).toEqual(networkResourcesBefore);
  });
});

describe("dispatch", () => {
  test("rejects an unknown task name, listing the valid names", async () => {
    const nextasking = createTestNextasking();

    // @ts-expect-error an unknown task name doesn't compile
    const dispatching = nextasking.dispatch("send-welcom-email", { email: "ada@example.com" });

    await expect(dispatching).rejects.toThrow(UnknownTaskError);
    await expect(dispatching).rejects.toThrow(
      'Unknown task "send-welcom-email". Valid task names: "send-welcome-email", "generate-invoice-pdf", "count-page-views".',
    );
  });

  test("rejects a payload the Zod schema refuses, listing each issue's path and message", async () => {
    const nextasking = createTestNextasking();

    const dispatching = nextasking.dispatch("send-welcome-email", {
      email: "not-an-email",
      address: { zipCode: "123" },
    });

    await expect(dispatching).rejects.toThrow(InvalidPayloadError);
    await expect(dispatching).rejects.toMatchObject({
      message:
        'Invalid payload for task "send-welcome-email": email: Invalid email address; address.zipCode: Too small: expected string to have exactly 5 characters',
      issues: [
        { path: "email", message: "Invalid email address" },
        { path: "address.zipCode", message: "Too small: expected string to have exactly 5 characters" },
      ],
    });
  });

  test("rejects a payload the Valibot schema refuses without the payload's values", async () => {
    const nextasking = createTestNextasking();

    const dispatching = nextasking.dispatch("generate-invoice-pdf", {
      invoiceId: "inv_secret_42",
      contactEmail: "secret-contact",
    });

    await expect(dispatching).rejects.toMatchObject({
      message: 'Invalid payload for task "generate-invoice-pdf": contactEmail: Invalid email: Received [redacted]',
      issues: [{ path: "contactEmail", message: "Invalid email: Received [redacted]" }],
    });
  });

  test("redacts a non-string payload value, and leaves a value matching only part of a word", async () => {
    const nextasking = createTestNextasking();

    const dispatching = nextasking.dispatch("generate-invoice-pdf", {
      invoiceId: "ring",
      contactEmail: 42 as unknown as string,
    });

    await expect(dispatching).rejects.toMatchObject({
      issues: [{ path: "contactEmail", message: "Invalid type: Expected string but received [redacted]" }],
    });
  });

  test("names the root when the payload itself is refused", async () => {
    const nextasking = createTestNextasking();

    const dispatching = nextasking.dispatch("send-welcome-email", [] as never);

    await expect(dispatching).rejects.toMatchObject({
      issues: [{ path: "(root)", message: "Invalid input: expected object, received array" }],
    });
  });

  test("takes the schema's input and rejects a wrong payload at compile time", () => {
    const nextasking = createTestNextasking();

    expectTypeOf(nextasking.dispatch<"count-page-views">)
      .parameter(1)
      .toEqualTypeOf<{ views: string }>();
    // @ts-expect-error a wrong payload doesn't compile
    void nextasking.dispatch("count-page-views", { views: 12 }).catch(() => undefined);
  });
});
