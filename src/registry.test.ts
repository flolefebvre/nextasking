import { setImmediate } from "node:timers/promises";
import * as v from "valibot";
import { describe, expect, expectTypeOf, test, vi } from "vitest";
import { z } from "zod";

import { defineEvent, defineListener, defineScheduledTask, defineTask } from "./definitions.js";
import { InvalidPayloadError, UnknownEventError, UnknownTaskError } from "./errors.js";
import type { Duration } from "./durations.js";
import type { Logger } from "./logger.js";
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

const invoicePaid = defineEvent("invoice.paid", {
  schema: z.object({ invoiceId: z.string(), amountInCents: z.int().positive() }),
});

const userSignedUp = defineEvent("user.signed-up", {
  schema: z.object({ userId: z.string() }),
});

const recordInvoicePayment = defineListener("record-invoice-payment", {
  on: invoicePaid,
  handler: async () => undefined,
});

function createTestNextasking(logger?: Logger) {
  return createNextasking({
    connection: { url: UNREACHABLE_URL },
    ...(logger === undefined ? {} : { logger }),
    events: [invoicePaid, userSignedUp],
    tasks: [sendWelcomeEmail, generateInvoicePdf, countPageViews, recordInvoicePayment],
  });
}

function createSpyLogger() {
  return { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() } satisfies Logger;
}

const VALID_WELCOME_PAYLOAD = { email: "ada@example.com", address: { zipCode: "75011" } };

const RANDOM_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

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
    ).toThrow('Duplicate name "send-welcome-email": every task and event needs its own name');
  });

  test("rejects a task and an event with the same name", () => {
    const invoicePaidTask = defineTask("invoice.paid", { schema: z.object({}), handler: async () => undefined });

    expect(() =>
      createNextasking({ connection: { url: UNREACHABLE_URL }, events: [invoicePaid], tasks: [invoicePaidTask] }),
    ).toThrow('Duplicate name "invoice.paid": every task and event needs its own name');
  });

  test("rejects a listener whose event is not listed in events", () => {
    expect(() =>
      createNextasking({ connection: { url: UNREACHABLE_URL }, events: [userSignedUp], tasks: [recordInvoicePayment] }),
    ).toThrow('Listener "record-invoice-payment" listens to event "invoice.paid", which is not listed in events');
    expect(() => createNextasking({ connection: { url: UNREACHABLE_URL }, tasks: [recordInvoicePayment] })).toThrow(
      'Listener "record-invoice-payment" listens to event "invoice.paid", which is not listed in events',
    );
  });

  test("rejects an event name starting with nextasking/", () => {
    const reservedEvent = defineEvent("nextasking/run.failed", { schema: z.object({}) });

    expect(() =>
      createNextasking({ connection: { url: UNREACHABLE_URL }, events: [reservedEvent], tasks: [] }),
    ).toThrow(
      'Event "nextasking/run.failed" has a reserved name: names starting with "nextasking/" belong to the built-in events',
    );
  });

  test("accepts events and their listeners, and no events at all", () => {
    expect(() => createTestNextasking()).not.toThrow();
    expect(() => createNextasking({ connection: { url: UNREACHABLE_URL }, tasks: [sendWelcomeEmail] })).not.toThrow();
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

  test.each([
    [-1, "-1"],
    [1.5, "1.5"],
    [Number.POSITIVE_INFINITY, "Infinity"],
    ["3" as unknown as number, '"3"'],
  ])("rejects retries of %s", (retries, shownRetries) => {
    const tasks = [defineTask("rebuild-index", { schema: z.object({}), retries, handler: async () => undefined })];

    expect(() => createNextasking({ connection: { url: UNREACHABLE_URL }, tasks })).toThrow(
      `Task "rebuild-index" has an invalid retries: ${shownRetries} is not a non-negative integer`,
    );
  });

  test("accepts retries of 0, for a task that runs once", () => {
    const tasks = [defineTask("rebuild-index", { schema: z.object({}), retries: 0, handler: async () => undefined })];

    expect(() => createNextasking({ connection: { url: UNREACHABLE_URL }, tasks })).not.toThrow();
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
      'Unknown task "send-welcom-email". Valid task names: "send-welcome-email", "generate-invoice-pdf", "count-page-views", "record-invoice-payment".',
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

  test("takes a listener's name with its event's input", () => {
    const nextasking = createTestNextasking();

    expectTypeOf(nextasking.dispatch<"record-invoice-payment">)
      .parameter(1)
      .toEqualTypeOf<{ invoiceId: string; amountInCents: number }>();
  });

  test.each([
    ["", '""'],
    [42 as unknown as string, "42"],
  ])("rejects the runId %j, naming the task, before connecting", async (runId, shownRunId) => {
    const nextasking = createTestNextasking();

    const dispatching = nextasking.dispatch("send-welcome-email", VALID_WELCOME_PAYLOAD, { runId });

    await expect(dispatching).rejects.toThrow(
      `Invalid runId for task "send-welcome-email": ${shownRunId} is not a non-empty string`,
    );
  });

  test("rejects an invalid delay, naming the task, before connecting", async () => {
    const nextasking = createTestNextasking();

    const dispatching = nextasking.dispatch("send-welcome-email", VALID_WELCOME_PAYLOAD, {
      delay: "0.5h" as Duration,
    });

    await expect(dispatching).rejects.toThrow(
      'Invalid delay for task "send-welcome-email": "0.5h" is not a duration: a whole number followed by s, m, h or d, such as "30s"',
    );
    await expect(dispatching).rejects.toMatchObject({ cause: expect.any(Error) });
  });

  test("takes a runId and a delay duration, and rejects a delay that isn't one at compile time", () => {
    const nextasking = createTestNextasking();

    expectTypeOf(nextasking.dispatch<"send-welcome-email">)
      .parameter(2)
      .toEqualTypeOf<{ runId?: string; delay?: Duration } | undefined>();
    // @ts-expect-error a space isn't a duration
    void nextasking.dispatch("send-welcome-email", VALID_WELCOME_PAYLOAD, { delay: "5 m" }).catch(() => undefined);
  });

  test("rejects a run whose concurrency key function throws or returns an empty string, naming the task", async () => {
    const rebuildIndex = defineTask("rebuild-index", {
      schema: z.object({ accountId: z.string() }),
      concurrency: {
        key: (payload) => {
          if (payload.accountId === "unknown") {
            throw new Error("no account");
          }
          return payload.accountId;
        },
        limit: 1,
      },
      handler: async () => undefined,
    });
    const nextasking = createNextasking({ connection: { url: UNREACHABLE_URL }, tasks: [rebuildIndex] });

    const throwingKeyDispatch = nextasking.dispatch("rebuild-index", { accountId: "unknown" });
    const emptyKeyDispatch = nextasking.dispatch("rebuild-index", { accountId: "" });

    await expect(throwingKeyDispatch).rejects.toThrow(
      'Task "rebuild-index" has an invalid concurrency.key: it threw "no account"',
    );
    await expect(emptyKeyDispatch).rejects.toThrow(
      'Task "rebuild-index" has an invalid concurrency.key: it returned an empty string, not a non-empty string',
    );
  });
});

describe("emit", () => {
  test("rejects an unknown event name, listing the valid names", async () => {
    const nextasking = createTestNextasking();

    // @ts-expect-error an unknown event name doesn't compile
    const emitting = nextasking.emit("invoice.payed", { invoiceId: "inv-1", amountInCents: 100 });

    await expect(emitting).rejects.toThrow(UnknownEventError);
    await expect(emitting).rejects.toThrow(
      'Unknown event "invoice.payed". Valid event names: "invoice.paid", "user.signed-up".',
    );
  });

  test("rejects a task name", async () => {
    const nextasking = createTestNextasking();

    // @ts-expect-error a task name is not an event name
    const emitting = nextasking.emit("send-welcome-email", { email: "ada@example.com" });

    await expect(emitting).rejects.toThrow(UnknownEventError);
  });

  test("rejects a payload the event's schema refuses, naming the event", async () => {
    const nextasking = createTestNextasking();

    const emitting = nextasking.emit("invoice.paid", { invoiceId: "inv-1" } as never);

    await expect(emitting).rejects.toThrow(InvalidPayloadError);
    await expect(emitting).rejects.toMatchObject({
      message:
        'Invalid payload for event "invoice.paid": amountInCents: Invalid input: expected number, received undefined',
    });
  });

  test("validates an event with no listeners, warns and opens no connection", async () => {
    const logger = createSpyLogger();
    const nextasking = createTestNextasking(logger);
    const networkResourcesBefore = networkResources();

    const { eventId } = await nextasking.emit("user.signed-up", { userId: "user-7" });
    await setImmediate();

    expect(eventId).toMatch(RANDOM_UUID);
    expect(logger.warn).toHaveBeenCalledExactlyOnceWith(
      { eventName: "user.signed-up", eventId },
      "no listeners for event",
    );
    expect(networkResources()).toEqual(networkResourcesBefore);
    await expect(nextasking.emit("user.signed-up", { userId: 7 } as never)).rejects.toThrow(InvalidPayloadError);
  });

  test("takes the event's input and rejects a wrong payload at compile time", () => {
    const nextasking = createTestNextasking();

    expectTypeOf(nextasking.emit<"invoice.paid">)
      .parameter(1)
      .toEqualTypeOf<{ invoiceId: string; amountInCents: number }>();
    // @ts-expect-error a wrong payload doesn't compile
    void nextasking.emit("invoice.paid", { invoiceId: "inv-1", amountInCents: "100" }).catch(() => undefined);
  });

  test("returns the eventId passed and warns with it when the event has no listeners", async () => {
    const logger = createSpyLogger();
    const nextasking = createTestNextasking(logger);

    const emittedEvent = await nextasking.emit("user.signed-up", { userId: "user-7" }, { eventId: "signup-user-7" });

    expect(emittedEvent).toEqual({ eventId: "signup-user-7" });
    expect(logger.warn).toHaveBeenCalledExactlyOnceWith(
      { eventName: "user.signed-up", eventId: "signup-user-7" },
      "no listeners for event",
    );
  });

  test.each([
    ["", '""'],
    [null as unknown as string, "null"],
  ])("rejects the eventId %j, naming the event, before connecting", async (eventId, shownEventId) => {
    const nextasking = createTestNextasking();

    const emitting = nextasking.emit("invoice.paid", { invoiceId: "inv-1", amountInCents: 100 }, { eventId });

    await expect(emitting).rejects.toThrow(
      `Invalid eventId for event "invoice.paid": ${shownEventId} is not a non-empty string`,
    );
  });

  test("takes an eventId and no delay", () => {
    const nextasking = createTestNextasking();

    expectTypeOf(nextasking.emit<"invoice.paid">)
      .parameter(2)
      .toEqualTypeOf<{ eventId?: string } | undefined>();
  });

  test("takes no name on an instance without events", () => {
    const nextasking = createNextasking({ connection: { url: UNREACHABLE_URL }, tasks: [sendWelcomeEmail] });

    expectTypeOf(nextasking.emit).parameter(0).toBeNever();
  });
});

function createScheduledTaskNextasking(schedule: { cron: string; timezone?: string }) {
  const purgeExpiredSessions = defineScheduledTask("purge-expired-sessions", {
    schedule,
    handler: async () => undefined,
  });
  return createNextasking({ connection: { url: UNREACHABLE_URL }, tasks: [sendWelcomeEmail, purgeExpiredSessions] });
}

describe("createNextasking with a scheduled task", () => {
  test.each([
    ["61 * * * *", '"61 * * * *" is not a cron expression: Constraint error, got value 61 expected range 0-59'],
    ["every day", '"every day" is not a cron expression: Validation error, cannot resolve alias "eve"'],
    ["", '"" is not a cron expression: it is empty'],
    ["  ", '"  " is not a cron expression: it is empty'],
    [42 as unknown as string, "42 is not a cron expression: it is not a string"],
  ])("rejects the cron %j, naming the task", (cron, shownError) => {
    expect(() => createScheduledTaskNextasking({ cron })).toThrow(
      `Task "purge-expired-sessions" has an invalid schedule.cron: ${shownError}`,
    );
  });

  test("rejects a timezone that isn't an IANA time zone, naming the task", () => {
    const creating = () => createScheduledTaskNextasking({ cron: "0 3 * * *", timezone: "Mars/Olympus" });

    expect(creating).toThrow(
      'Task "purge-expired-sessions" has an invalid schedule.timezone: "Mars/Olympus" is not an IANA time zone',
    );
    expect(creating).toThrow(expect.objectContaining({ cause: expect.any(RangeError) }));
  });

  test("names the timezone first when both the cron and the timezone are invalid", () => {
    expect(() => createScheduledTaskNextasking({ cron: "61 * * * *", timezone: "Mars/Olympus" })).toThrow(
      'Task "purge-expired-sessions" has an invalid schedule.timezone: "Mars/Olympus" is not an IANA time zone',
    );
  });

  test("accepts a cron with a seconds field, an IANA alias and no timezone at all", () => {
    expect(() => createScheduledTaskNextasking({ cron: "*/5 * * * * *", timezone: "US/Pacific" })).not.toThrow();
    expect(() => createScheduledTaskNextasking({ cron: "0 3 * * *", timezone: "Europe/Paris" })).not.toThrow();
    expect(() => createScheduledTaskNextasking({ cron: "0 3 * * *" })).not.toThrow();
  });

  test("rejects an invalid schedule before opening any connection", async () => {
    const networkResourcesBefore = networkResources();

    expect(() => createScheduledTaskNextasking({ cron: "0 3 * * *", timezone: "Nowhere" })).toThrow();
    await setImmediate();

    expect(networkResources()).toEqual(networkResourcesBefore);
  });
});

describe("dispatch of a scheduled task", () => {
  test("takes no payload, and an optional runId and delay", () => {
    const nextasking = createScheduledTaskNextasking({ cron: "0 3 * * *" });

    expectTypeOf(nextasking.dispatch<"purge-expired-sessions">).toBeCallableWith("purge-expired-sessions");
    expectTypeOf(nextasking.dispatch<"purge-expired-sessions">).toBeCallableWith("purge-expired-sessions", undefined, {
      runId: "purge-now",
      delay: "5m",
    });
    expectTypeOf(nextasking.dispatch<"send-welcome-email">)
      .parameter(1)
      .toEqualTypeOf<{ email: string; address: { zipCode: string } }>();
  });

  test("rejects a payload in its types and at runtime, before connecting", async () => {
    const nextasking = createScheduledTaskNextasking({ cron: "0 3 * * *" });
    const networkResourcesBefore = networkResources();

    // @ts-expect-error a scheduled task takes no payload
    const dispatching = nextasking.dispatch("purge-expired-sessions", { olderThanDays: 30 });

    await expect(dispatching).rejects.toThrow(
      'Scheduled task "purge-expired-sessions" takes no payload: dispatch it by its name alone to run it now',
    );
    await expect(dispatching).rejects.not.toThrow(InvalidPayloadError);
    await setImmediate();
    expect(networkResources()).toEqual(networkResourcesBefore);
  });

  test("is a task name for isTaskName", () => {
    const nextasking = createScheduledTaskNextasking({ cron: "0 3 * * *" });

    expect(nextasking.isTaskName("purge-expired-sessions")).toBe(true);
    expectTypeOf(nextasking.dispatch).parameter(0).toEqualTypeOf<"send-welcome-email" | "purge-expired-sessions">();
  });
});

describe("isTaskName and isEventName", () => {
  test("tell the instance's task and listener names from its event names and unknown names", () => {
    const nextasking = createTestNextasking();

    expect(
      ["send-welcome-email", "record-invoice-payment", "invoice.paid", "unknown"].map(nextasking.isTaskName),
    ).toEqual([true, true, false, false]);
    expect(["invoice.paid", "user.signed-up", "send-welcome-email", "unknown"].map(nextasking.isEventName)).toEqual([
      true,
      true,
      false,
      false,
    ]);
  });

  test("narrow a runtime string to the names dispatch and emit take", () => {
    const nextasking = createTestNextasking();
    const name: string = "invoice.paid";

    if (nextasking.isEventName(name)) {
      expectTypeOf(name).toEqualTypeOf<"invoice.paid" | "user.signed-up">();
    }
    if (nextasking.isTaskName(name)) {
      expectTypeOf(name).toEqualTypeOf<
        "send-welcome-email" | "generate-invoice-pdf" | "count-page-views" | "record-invoice-payment"
      >();
    }
  });
});

describe("startWorker", () => {
  test.each([
    [0, "0"],
    [-2, "-2"],
    [1.5, "1.5"],
    [Number.POSITIVE_INFINITY, "Infinity"],
    ["4", '"4"'],
  ])("rejects concurrency %s before opening any connection", async (concurrency, shownConcurrency) => {
    const nextasking = createTestNextasking();
    const networkResourcesBefore = networkResources();

    await expect(nextasking.startWorker({ concurrency: concurrency as number })).rejects.toThrow(
      `Invalid startWorker concurrency: ${shownConcurrency} is not a positive integer`,
    );
    await setImmediate();
    expect(networkResources()).toEqual(networkResourcesBefore);
  });

  test("rejects a shutdownTimeout that is not a duration", async () => {
    const nextasking = createTestNextasking();

    await expect(nextasking.startWorker({ shutdownTimeout: "soon" as Duration })).rejects.toThrow(
      'Invalid startWorker shutdownTimeout: "soon" is not a duration',
    );
  });

  test("takes concurrency and a shutdownTimeout duration, and rejects other options at compile time", () => {
    const nextasking = createTestNextasking();

    expectTypeOf(nextasking.startWorker).toBeCallableWith();
    expectTypeOf(nextasking.startWorker).toBeCallableWith({ concurrency: 4, shutdownTimeout: "25s" });
    // @ts-expect-error a shutdownTimeout is a Duration
    expectTypeOf(nextasking.startWorker).toBeCallableWith({ shutdownTimeout: 25_000 });
  });
});
