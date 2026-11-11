import { setImmediate } from "node:timers/promises";
import * as v from "valibot";
import { describe, expect, expectTypeOf, test, vi } from "vitest";
import { z } from "zod";

import { runFailed } from "./built-in-events.js";
import type { RunFailedPayload } from "./built-in-events.js";
import { defineEvent, defineListener, defineScheduledTask, defineTask } from "./definitions.js";
import type { EventDefinition } from "./definitions.js";
import type { StandardSchemaV1 } from "./standard-schema.js";
import { InvalidPayloadError, RedisUnavailableError, UnknownEventError, UnknownTaskError } from "./errors.js";
import type { Duration } from "./durations.js";
import type { Logger } from "./logger.js";
import { createNextasking } from "./registry.js";
import { fakeNextasking } from "./testing.js";
import type { DispatchedRun, DispatchOptions, EmitOptions, EmittedEvent } from "./enqueue-requests.js";
import type { StartWorkerOptions } from "./worker-options.js";

const UNREACHABLE_URL = "redis://nextasking.invalid:6379";

interface DispatchOptionsWithATenant extends DispatchOptions {
  tenant: string;
}

interface StartWorkerOptionsWithATenant extends StartWorkerOptions {
  tenant: string;
}

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

const importedThroughACycle = undefined as unknown as typeof sendWelcomeEmail;

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
    const reservedEvent = defineEvent("nextasking/user.signed-up", { schema: z.object({}) });

    expect(() =>
      createNextasking({ connection: { url: UNREACHABLE_URL }, events: [reservedEvent], tasks: [] }),
    ).toThrow(
      'Event "nextasking/user.signed-up" has a reserved name: names starting with "nextasking/" belong to the built-in events',
    );
  });

  test.each([runFailed])("rejects the built-in event $name listed in events", (builtInEvent) => {
    expect(() => createNextasking({ connection: { url: UNREACHABLE_URL }, events: [builtInEvent], tasks: [] })).toThrow(
      `Event "${builtInEvent.name}" is built in and added automatically: remove it from events, and listen to it as it is`,
    );
  });

  test("rejects a task name starting with nextasking/", () => {
    const reservedTask = defineTask("nextasking/firing", { schema: z.object({}), handler: async () => undefined });

    expect(() => createNextasking({ connection: { url: UNREACHABLE_URL }, tasks: [reservedTask] })).toThrow(
      'Task "nextasking/firing" has a reserved name: names starting with "nextasking/" are reserved for the library',
    );
  });

  test.each([
    { position: "first", tasks: [importedThroughACycle, sendWelcomeEmail], message: "tasks[0]" },
    { position: "last", tasks: [sendWelcomeEmail, generateInvoicePdf, importedThroughACycle], message: "tasks[2]" },
    { position: "alone", tasks: [importedThroughACycle], message: "tasks[0]" },
  ])(
    "rejects an undefined entry of tasks, $position, naming its position and the import cycle",
    ({ tasks, message }) => {
      expect(() => createNextasking({ connection: { url: UNREACHABLE_URL }, tasks })).toThrow(
        new Error(
          `${message} is undefined: an import cycle is the usual cause, the file that defines this task imports the file that calls createNextasking`,
        ),
      );
    },
  );

  test("rejects an undefined entry of events, naming its position and the import cycle", () => {
    const eventImportedThroughACycle = undefined as unknown as typeof userSignedUp;

    expect(() =>
      createNextasking({
        connection: { url: UNREACHABLE_URL },
        events: [invoicePaid, eventImportedThroughACycle],
        tasks: [sendWelcomeEmail],
      }),
    ).toThrow(
      new Error(
        "events[1] is undefined: an import cycle is the usual cause, the file that defines this event imports the file that calls createNextasking",
      ),
    );
  });

  test.each([
    { kind: "task", unnamed: defineTask("", { schema: z.object({}), handler: async () => undefined }) },
    { kind: "listener", unnamed: defineListener("", { on: invoicePaid, handler: async () => undefined }) },
  ])("rejects a $kind with an empty name, naming its position in tasks", ({ unnamed }) => {
    expect(() =>
      createNextasking({
        connection: { url: UNREACHABLE_URL },
        events: [invoicePaid],
        tasks: [sendWelcomeEmail, unnamed],
      }),
    ).toThrow(new Error("tasks[1] has an empty name: every task and event needs a name"));
  });

  test("rejects an event with an empty name, naming its position in events", () => {
    const unnamedEvent = defineEvent("", { schema: z.object({}) });

    expect(() =>
      createNextasking({ connection: { url: UNREACHABLE_URL }, events: [unnamedEvent, invoicePaid], tasks: [] }),
    ).toThrow(new Error("events[0] has an empty name: every task and event needs a name"));
  });

  test("reports two empty names as an empty name, not as a duplicate", () => {
    const unnamedTask = defineTask("", { schema: z.object({}), handler: async () => undefined });
    const unnamedEvent = defineEvent("", { schema: z.object({}) });

    expect(() =>
      createNextasking({ connection: { url: UNREACHABLE_URL }, events: [unnamedEvent], tasks: [unnamedTask] }),
    ).toThrow(new Error("tasks[0] has an empty name: every task and event needs a name"));
  });

  test("refuses a task, a listener or a scheduled task listed in events", () => {
    const purgeExpiredSessions = defineScheduledTask("purge-expired-sessions", {
      schedule: { cron: "0 3 * * *" },
      handler: async () => undefined,
    });
    const createWithEvents = () => {
      // @ts-expect-error a task is not an event
      createNextasking({ connection: { url: UNREACHABLE_URL }, events: [invoicePaid, sendWelcomeEmail], tasks: [] });
      // @ts-expect-error a listener is not an event
      createNextasking({ connection: { url: UNREACHABLE_URL }, events: [recordInvoicePayment], tasks: [] });
      // @ts-expect-error a scheduled task is not an event
      createNextasking({ connection: { url: UNREACHABLE_URL }, events: [purgeExpiredSessions], tasks: [] });
    };

    expect(createWithEvents).toBeTypeOf("function");
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

  test("rejects an invalid connection.giveUpAfter", () => {
    const creating = () =>
      createNextasking({
        connection: { url: UNREACHABLE_URL, giveUpAfter: "half a second" as Duration },
        tasks: [sendWelcomeEmail],
      });

    expect(creating).toThrow(
      'Invalid connection.giveUpAfter: "half a second" is not a duration: a whole number followed by s, m, h or d, such as "30s"',
    );
    expect(creating).toThrow(expect.objectContaining({ cause: expect.any(Error) }));
  });

  test.each([
    ["without a scheme", "localhost:6379", '"localhost:6379"'],
    ["of another scheme, hiding its password", "http://user:s3cr3t@cache.internal", '"http://user:***@cache.internal"'],
    ["that is not a URL", "not a url", '"not a url"'],
  ])("rejects a connection.url %s, before opening any connection", async (_, url, shownUrl) => {
    const networkResourcesBefore = networkResources();

    expect(() => createNextasking({ connection: { url }, tasks: [sendWelcomeEmail] })).toThrow(
      `Invalid connection.url: ${shownUrl} is not a valid redis:// or rediss:// URL, such as "redis://localhost:6379"`,
    );
    await setImmediate();

    expect(networkResources()).toEqual(networkResourcesBefore);
  });

  test("rejects an empty prefix, before opening any connection", async () => {
    const networkResourcesBefore = networkResources();

    expect(() =>
      createNextasking({ connection: { url: UNREACHABLE_URL }, prefix: "", tasks: [sendWelcomeEmail] }),
    ).toThrow('Invalid prefix: "" is empty: leave prefix out for the default "nextasking", or give a non-empty one');
    await setImmediate();

    expect(networkResources()).toEqual(networkResourcesBefore);
  });

  test("accepts a prefix of one character", () => {
    expect(() =>
      createNextasking({ connection: { url: UNREACHABLE_URL }, prefix: "q", tasks: [sendWelcomeEmail] }),
    ).not.toThrow();
  });

  test("accepts a valid connection.giveUpAfter", () => {
    expect(() =>
      createNextasking({ connection: { url: UNREACHABLE_URL, giveUpAfter: "2s" }, tasks: [sendWelcomeEmail] }),
    ).not.toThrow();
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

describe("the options of dispatch and emit", () => {
  test("refuse an option they don't know at compile time, in a literal, through a variable and through a spread", () => {
    const nextasking = createTestNextasking();
    const unknownDispatchOptions = { delayy: "5m" };
    const dispatchOptionsWithOneKnown = { runID: "welcome-ada", delay: "5m" as const };
    const dispatchOptionsWiderThanTheirType: DispatchOptionsWithATenant = { delay: "5m", tenant: "acme" };
    const unknownEmitOptions = { eventID: "paid-1" };
    const emitOptionsWithOneKnown = { eventId: "paid-1", eventID: "paid-1" };
    const invoice = { invoiceId: "inv-1", amountInCents: 100 };
    const sendWithUnknownOptions = async () => {
      // @ts-expect-error delayy is not an option of dispatch
      await nextasking.dispatch("send-welcome-email", VALID_WELCOME_PAYLOAD, { delayy: "5m" });
      // @ts-expect-error delayy is not an option of dispatch
      await nextasking.dispatch("send-welcome-email", VALID_WELCOME_PAYLOAD, unknownDispatchOptions);
      // @ts-expect-error runID is not an option of dispatch
      await nextasking.dispatch("send-welcome-email", VALID_WELCOME_PAYLOAD, dispatchOptionsWithOneKnown);
      // @ts-expect-error delayy is not an option of dispatch
      await nextasking.dispatch("send-welcome-email", VALID_WELCOME_PAYLOAD, { runId: "r", ...unknownDispatchOptions });
      // @ts-expect-error tenant is not an option of dispatch
      await nextasking.dispatch("send-welcome-email", VALID_WELCOME_PAYLOAD, dispatchOptionsWiderThanTheirType);
      // @ts-expect-error a string is not the options
      await nextasking.dispatch("send-welcome-email", VALID_WELCOME_PAYLOAD, "5m");
      // @ts-expect-error a list is not the options
      await nextasking.dispatch("send-welcome-email", VALID_WELCOME_PAYLOAD, ["5m"]);
      // @ts-expect-error null is not the options
      await nextasking.dispatch("send-welcome-email", VALID_WELCOME_PAYLOAD, null);
      // @ts-expect-error a delay is a Duration
      await nextasking.dispatch("send-welcome-email", VALID_WELCOME_PAYLOAD, { delay: 300 });
      // @ts-expect-error eventID is not an option of emit
      await nextasking.emit("invoice.paid", invoice, { eventID: "paid-1" });
      // @ts-expect-error eventID is not an option of emit
      await nextasking.emit("invoice.paid", invoice, unknownEmitOptions);
      // @ts-expect-error eventID is not an option of emit
      await nextasking.emit("invoice.paid", invoice, emitOptionsWithOneKnown);
      // @ts-expect-error eventID is not an option of emit
      await nextasking.emit("invoice.paid", invoice, { eventId: "paid-1", ...unknownEmitOptions });
      // @ts-expect-error a string is not the options
      await nextasking.emit("invoice.paid", invoice, "paid-1");
      // @ts-expect-error a list is not the options
      await nextasking.emit("invoice.paid", invoice, ["paid-1"]);
      // @ts-expect-error null is not the options
      await nextasking.emit("invoice.paid", invoice, null);
    };

    expectTypeOf(sendWithUnknownOptions).toBeFunction();
  });

  test("take their options from a typed variable, a spread, a conditional or an option set to undefined, typed as today", () => {
    const nextasking = createTestNextasking();
    const typedDispatchOptions: DispatchOptions = { delay: "5m" };
    const knownDispatchOptions = { delay: "5m" } as const;
    const isDelayed = Math.random() < 0.5;
    const partialDispatchOptions: Partial<DispatchOptions> = {};
    const readonlyDispatchOptions: Readonly<DispatchOptions> = {};
    const typedEmitOptions: EmitOptions = { eventId: "paid-1" };
    const invoice = { invoiceId: "inv-1", amountInCents: 100 };
    const sendWithKnownOptions = async () => {
      await nextasking.dispatch("send-welcome-email", VALID_WELCOME_PAYLOAD, typedDispatchOptions);
      await nextasking.dispatch("send-welcome-email", VALID_WELCOME_PAYLOAD, { runId: "r", ...knownDispatchOptions });
      await nextasking.dispatch("send-welcome-email", VALID_WELCOME_PAYLOAD, { ...typedDispatchOptions, runId: "r" });
      await nextasking.dispatch("send-welcome-email", VALID_WELCOME_PAYLOAD, isDelayed ? { delay: "5m" } : {});
      await nextasking.dispatch("send-welcome-email", VALID_WELCOME_PAYLOAD, isDelayed ? { delay: "5m" } : undefined);
      await nextasking.dispatch("send-welcome-email", VALID_WELCOME_PAYLOAD, partialDispatchOptions);
      await nextasking.dispatch("send-welcome-email", VALID_WELCOME_PAYLOAD, readonlyDispatchOptions);
      await nextasking.dispatch("send-welcome-email", VALID_WELCOME_PAYLOAD, { delay: undefined, runId: undefined });
      await nextasking.dispatch("record-invoice-payment", invoice, { runId: "r" });
      await nextasking.emit("invoice.paid", invoice, typedEmitOptions);
      await nextasking.emit("invoice.paid", invoice, { ...typedEmitOptions });
      await nextasking.emit("invoice.paid", invoice, isDelayed ? { eventId: "paid-1" } : {});
      await nextasking.emit("invoice.paid", invoice, { eventId: undefined });
    };
    const dispatchWelcomeEmail: (
      name: "send-welcome-email",
      payload: typeof VALID_WELCOME_PAYLOAD,
      options?: DispatchOptions,
    ) => Promise<DispatchedRun> = nextasking.dispatch;
    const emitInvoicePaid: (
      name: "invoice.paid",
      payload: typeof invoice,
      options?: EmitOptions,
    ) => Promise<EmittedEvent> = nextasking.emit;

    expectTypeOf(sendWithKnownOptions).toBeFunction();
    expectTypeOf(dispatchWelcomeEmail).toBeFunction();
    expectTypeOf(emitInvoicePaid).toBeFunction();
    expectTypeOf<Parameters<typeof nextasking.dispatch<"count-page-views">>>().toEqualTypeOf<
      [name: "count-page-views", payload: { views: string }, options?: DispatchOptions]
    >();
    expectTypeOf<Parameters<typeof nextasking.emit<"invoice.paid">>>().toEqualTypeOf<
      [name: "invoice.paid", payload: { invoiceId: string; amountInCents: number }, options?: EmitOptions]
    >();
    expectTypeOf<Parameters<typeof nextasking.emit>[2]>().toEqualTypeOf<EmitOptions | undefined>();
    expectTypeOf(nextasking.dispatch<"count-page-views", { [Member in keyof string]: unknown }>)
      .parameter(2)
      .toEqualTypeOf<DispatchOptions | undefined>();
    expectTypeOf(nextasking.emit<"invoice.paid", string[]>)
      .parameter(2)
      .toEqualTypeOf<EmitOptions | undefined>();
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

const MISSING_URL_MESSAGE =
  "Missing connection.url: it is undefined or empty, set the variable your app reads it from, such as REDIS_URL";

function createNextaskingWithUrl(url: string | undefined) {
  return createNextasking({
    connection: { url: url as string },
    events: [invoicePaid],
    tasks: [sendWelcomeEmail, recordInvoicePayment],
  });
}

async function expectRejectedAtOnceWithTheMissingUrl(call: () => Promise<unknown>): Promise<void> {
  vi.useFakeTimers();
  try {
    const calling = call();
    await expect(calling).rejects.toThrow(MISSING_URL_MESSAGE);
    await expect(calling).rejects.not.toBeInstanceOf(RedisUnavailableError);
    expect(vi.getTimerCount()).toBe(0);
  } finally {
    vi.useRealTimers();
  }
}

describe.each([
  ["undefined", undefined],
  ["empty", ""],
])("a connection.url that is %s", (_, url) => {
  test("is accepted at creation, opening no connection, and the definitions are read", async () => {
    const networkResourcesBefore = networkResources();

    const nextasking = createNextaskingWithUrl(url);
    await setImmediate();

    expect(nextasking.taskDefinitions.map(({ name }) => name)).toEqual([
      "send-welcome-email",
      "record-invoice-payment",
    ]);
    expect(nextasking.eventDefinitions.map(({ name }) => name)).toContain("invoice.paid");
    expect(networkResources()).toEqual(networkResourcesBefore);
  });

  test("makes dispatch, emit, each read and startWorker throw at once that it is missing, every time", async () => {
    const nextasking = createNextaskingWithUrl(url);
    const calls = [
      () => nextasking.dispatch("send-welcome-email", VALID_WELCOME_PAYLOAD),
      () => nextasking.emit("invoice.paid", { invoiceId: "inv-1", amountInCents: 1200 }),
      () => nextasking.getRun("send-welcome-email", "run-1"),
      () => nextasking.listRuns(),
      () => nextasking.countRuns(),
      () => nextasking.listSchedules(),
      () => nextasking.startWorker(),
    ];

    for (const call of [...calls, ...calls]) {
      await expectRejectedAtOnceWithTheMissingUrl(call);
    }
  });

  test("leaves no worker started after startWorker threw: a second one throws the same, and the instance can be faked", async () => {
    const nextasking = createNextaskingWithUrl(url);

    await expectRejectedAtOnceWithTheMissingUrl(() => nextasking.startWorker());
    await expectRejectedAtOnceWithTheMissingUrl(() => createNextaskingWithUrl(url).startWorker());
    using fake = fakeNextasking(nextasking);
    await nextasking.dispatch("send-welcome-email", VALID_WELCOME_PAYLOAD);

    expect(fake.dispatches.map(({ task }) => task)).toEqual(["send-welcome-email"]);
  });

  test("throws nothing on a faked instance, which never touches Redis", async () => {
    const nextasking = createNextaskingWithUrl(url);
    using fake = fakeNextasking(nextasking);

    await nextasking.dispatch("send-welcome-email", VALID_WELCOME_PAYLOAD);
    await nextasking.emit("invoice.paid", { invoiceId: "inv-1", amountInCents: 1200 });

    expect(fake.dispatches.map(({ task }) => task)).toEqual(["send-welcome-email"]);
    expect(fake.emits.map(({ event }) => event)).toEqual(["invoice.paid"]);
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
      expectTypeOf(nextasking.emit).toBeCallableWith(name, { invoiceId: "inv-7", amountInCents: 100 });
    }
    if (nextasking.isTaskName(name)) {
      expectTypeOf(name).toEqualTypeOf<
        "send-welcome-email" | "generate-invoice-pdf" | "count-page-views" | "record-invoice-payment"
      >();
    }
  });
});

describe("startWorker", () => {
  test("leaves process.exitCode untouched when it rejects", async () => {
    const nextasking = createTestNextasking();
    const exitCodeBefore = process.exitCode;

    await expect(nextasking.startWorker({ concurrency: 0 })).rejects.toThrow("Invalid startWorker concurrency");

    expect(process.exitCode).toBe(exitCodeBefore);
  });

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

  test.each([
    ["a string", "exit", '"exit"'],
    ["null", null, "null"],
  ])("rejects an onWorkerNotStarted that is %s, before opening any connection", async (_, value, shownValue) => {
    const nextasking = createTestNextasking();
    const networkResourcesBefore = networkResources();

    await expect(nextasking.startWorker({ onWorkerNotStarted: value as unknown as () => void })).rejects.toThrow(
      `Invalid startWorker onWorkerNotStarted: ${shownValue} is not a function`,
    );
    await setImmediate();
    expect(networkResources()).toEqual(networkResourcesBefore);
  });

  test("takes concurrency, a shutdownTimeout duration and an onWorkerNotStarted callback, and rejects other options at compile time", () => {
    const nextasking = createTestNextasking();

    expectTypeOf(nextasking.startWorker).toBeCallableWith();
    expectTypeOf(nextasking.startWorker).toBeCallableWith({ concurrency: 4, shutdownTimeout: "25s" });
    expectTypeOf(nextasking.startWorker).toBeCallableWith({
      onWorkerNotStarted: (error: unknown) => console.error(error),
    });
    expectTypeOf(nextasking.startWorker).toBeCallableWith({ onWorkerNotStarted: async () => undefined });
    // @ts-expect-error onWorkerNotStarted is a function
    expectTypeOf(nextasking.startWorker).toBeCallableWith({ onWorkerNotStarted: "exit" });
    // @ts-expect-error a shutdownTimeout is a Duration
    expectTypeOf(nextasking.startWorker).toBeCallableWith({ shutdownTimeout: 25_000 });
  });

  test("refuses an option it doesn't know at compile time, in a literal, through a variable and through a spread", () => {
    const nextasking = createTestNextasking();
    const unknownOptions = { concurency: 1 };
    const optionsWithOneKnown = { concurency: 1, shutdownTimeout: "10s" as const };
    const optionsWiderThanItsType: StartWorkerOptionsWithATenant = { concurrency: 1, tenant: "acme" };
    const startWithUnknownOptions = async () => {
      // @ts-expect-error concurency is not an option of startWorker
      await nextasking.startWorker({ concurency: 1 });
      // @ts-expect-error concurency is not an option of startWorker
      await nextasking.startWorker(unknownOptions);
      // @ts-expect-error concurency is not an option of startWorker
      await nextasking.startWorker(optionsWithOneKnown);
      // @ts-expect-error concurency is not an option of startWorker
      await nextasking.startWorker({ shutdownTimeout: "10s", ...unknownOptions });
      // @ts-expect-error tenant is not an option of startWorker
      await nextasking.startWorker(optionsWiderThanItsType);
      // @ts-expect-error a string is not the options
      await nextasking.startWorker("10s");
      // @ts-expect-error a list is not the options
      await nextasking.startWorker(["10s"]);
      // @ts-expect-error null is not the options
      await nextasking.startWorker(null);
      // @ts-expect-error concurrency is a number
      await nextasking.startWorker({ concurrency: "5" });
    };

    expectTypeOf(startWithUnknownOptions).toBeFunction();
  });

  test("takes its options from a typed variable, a spread, a conditional or an option set to undefined, typed as today", () => {
    const nextasking = createTestNextasking();
    const typedOptions: StartWorkerOptions = { concurrency: 5 };
    const knownOptions = { shutdownTimeout: "10s" } as const;
    const isDraining = Math.random() < 0.5;
    const partialOptions: Partial<StartWorkerOptions> = {};
    const readonlyOptions: Readonly<StartWorkerOptions> = {};
    const startWithKnownOptions = async () => {
      await nextasking.startWorker(typedOptions);
      await nextasking.startWorker({ concurrency: 1, ...knownOptions });
      await nextasking.startWorker({ ...typedOptions, concurrency: 2 });
      await nextasking.startWorker(isDraining ? { shutdownTimeout: "1m" } : {});
      await nextasking.startWorker(partialOptions);
      await nextasking.startWorker(readonlyOptions);
      await nextasking.startWorker({ concurrency: undefined, onWorkerNotStarted: (error) => console.error(error) });
      await nextasking.startWorker(undefined);
    };
    const start: (options?: StartWorkerOptions) => Promise<void> = nextasking.startWorker;

    expectTypeOf(startWithKnownOptions).toBeFunction();
    expectTypeOf(start).toBeFunction();
    expectTypeOf<Parameters<typeof nextasking.startWorker>>().toEqualTypeOf<[options?: StartWorkerOptions]>();
    expectTypeOf(nextasking.startWorker<StartWorkerOptions>).parameters.toEqualTypeOf<[options?: StartWorkerOptions]>();
    expectTypeOf(nextasking.startWorker<{ [Member in keyof string]: unknown }>)
      .parameter(0)
      .toEqualTypeOf<StartWorkerOptions | undefined>();
    expectTypeOf(nextasking.startWorker<string[]>)
      .parameter(0)
      .toEqualTypeOf<StartWorkerOptions | undefined>();
  });
});

describe("replay", () => {
  test("rejects an unknown task name, listing the valid names, before opening any connection", async () => {
    const nextasking = createTestNextasking();
    const networkResourcesBefore = networkResources();

    // @ts-expect-error an unknown task name doesn't compile
    const replaying = nextasking.replay("send-welcom-email", "run-7");

    await expect(replaying).rejects.toThrow(UnknownTaskError);
    await expect(replaying).rejects.toThrow(
      'Unknown task "send-welcom-email". Valid task names: "send-welcome-email", "generate-invoice-pdf", "count-page-views", "record-invoice-payment".',
    );
    await setImmediate();
    expect(networkResources()).toEqual(networkResourcesBefore);
  });

  test("takes a task name and a run id and resolves with the run id", () => {
    const nextasking = createTestNextasking();

    expectTypeOf(nextasking.replay<"record-invoice-payment">).parameters.toEqualTypeOf<
      ["record-invoice-payment", string]
    >();
    expectTypeOf(nextasking.replay).returns.resolves.toEqualTypeOf<{ runId: string }>();
  });
});

describe("built-in events", () => {
  const alertOnRunFailed = defineListener("alert-on-run-failed", {
    on: runFailed,
    handler: async ({ payload }) => {
      expectTypeOf(payload).toEqualTypeOf<RunFailedPayload>();
    },
  });

  function createAlertingNextasking() {
    return createNextasking({
      connection: { url: UNREACHABLE_URL },
      events: [invoicePaid],
      tasks: [recordInvoicePayment, alertOnRunFailed],
    });
  }

  test("type runFailed as an event definition named nextasking/run.failed, whose schema gives the notice", () => {
    expect(Object.keys(runFailed).sort()).toEqual(["name", "schema"]);
    expectTypeOf(runFailed).toEqualTypeOf<
      EventDefinition<"nextasking/run.failed", StandardSchemaV1<RunFailedPayload, RunFailedPayload>>
    >();
  });

  test("are added to the registry automatically, with their listeners, and left out of isEventName", () => {
    const nextasking = createAlertingNextasking();

    expect(nextasking.eventDefinitions).toEqual([
      { name: "invoice.paid", listenerNames: ["record-invoice-payment"] },
      { name: "nextasking/run.failed", listenerNames: ["alert-on-run-failed"] },
    ]);
    expect(nextasking.isEventName("nextasking/run.failed")).toBe(false);
  });

  test.each(["nextasking/run.failed"])(
    "can't be emitted: emit rejects %s before opening any connection",
    async (builtInEventName) => {
      const nextasking = createAlertingNextasking();
      const networkResourcesBefore = networkResources();
      const emitByName = nextasking.emit as (name: string, payload: unknown) => Promise<unknown>;

      await expect(emitByName(builtInEventName, {})).rejects.toThrow(
        `Event "${builtInEventName}" is built in: names starting with "nextasking/" belong to the built-in events, which only the library emits`,
      );
      await setImmediate();
      expect(networkResources()).toEqual(networkResourcesBefore);
    },
  );

  test("can't be emitted in the types, and an unknown event lists only the app's events", async () => {
    const nextasking = createAlertingNextasking();

    expectTypeOf(nextasking.emit).parameter(0).toEqualTypeOf<"invoice.paid">();
    // @ts-expect-error a built-in event can't be emitted
    const emitting = nextasking.emit("nextasking/run.failed", {});

    await expect(emitting).rejects.toThrow("is built in");
    await expect(
      nextasking.emit("invoice.payed" as "invoice.paid", { invoiceId: "inv-7", amountInCents: 100 }),
    ).rejects.toThrow('Unknown event "invoice.payed". Valid event names: "invoice.paid".');
  });
});
