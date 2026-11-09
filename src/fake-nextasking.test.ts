import { describe, expect, expectTypeOf, test } from "vitest";
import { z } from "zod";

import { createRecordingLogger } from "../tests/recording-logger.js";
import {
  createNextasking,
  defineEvent,
  defineListener,
  defineScheduledTask,
  defineTask,
  fanOutFailed,
  InvalidPayloadError,
  RedisUnavailableError,
  runFailed,
  UnknownEventError,
  UnknownTaskError,
} from "./index.js";
import type { Duration, EventId, Logger, RunId, TaskName } from "./index.js";
import { closeNextasking } from "./registry.js";
import { fakeNextasking } from "./testing.js";
import type { NextaskingFake } from "./testing.js";

const sendInvoice = defineTask("send-invoice", {
  schema: z.object({ accountId: z.string(), currency: z.string().default("EUR") }),
  concurrency: { key: (payload) => (payload.accountId === "broken" ? "" : payload.accountId), limit: 1 },
  handler: async () => undefined,
});

const orderPlaced = defineEvent("order.placed", {
  schema: z.object({ orderId: z.string(), quantity: z.number().default(1) }),
});

const orderCancelled = defineEvent("order.cancelled", {
  schema: z.object({ orderId: z.string() }),
});

const reserveStock = defineListener("reserve-stock", {
  on: orderPlaced,
  handler: async () => undefined,
});

const alertOnRunFailed = defineListener("alert-on-run-failed", {
  on: runFailed,
  handler: async () => undefined,
});

const closeBooks = defineScheduledTask("close-books", {
  schedule: { cron: "0 0 1 * *" },
  handler: async () => undefined,
});

function createShopNextasking({ logger }: { logger?: Logger } = {}) {
  return createNextasking({
    connection: { url: "redis://nextasking.invalid:6379", giveUpAfter: "1s" },
    ...(logger === undefined ? {} : { logger }),
    events: [orderPlaced, orderCancelled],
    tasks: [sendInvoice, reserveStock, alertOnRunFailed, closeBooks],
  });
}

type NextaskingCalledWithAnyName = {
  dispatch: (name: string, payload?: unknown, options?: object) => Promise<unknown>;
  emit: (name: string, payload: unknown, options?: object) => Promise<unknown>;
  replay: (name: string, runId: RunId) => Promise<unknown>;
};

describe("fakeNextasking", () => {
  test("records a dispatch with the payload the schema returns, its run id and its delay as given", async () => {
    const nextasking = createShopNextasking();
    using fake = fakeNextasking(nextasking);

    const { runId } = await nextasking.dispatch("send-invoice", { accountId: "acme" }, { delay: "5m" });
    await nextasking.dispatch("close-books", undefined, { runId: "close-now" });

    expect(fake.dispatches).toEqual([
      { task: "send-invoice", payload: { accountId: "acme", currency: "EUR" }, runId, delay: "5m" },
      { task: "close-books", payload: undefined, runId: "close-now" },
    ]);
    expect(fake.dispatches[1]).not.toHaveProperty("delay");
  });

  test("records an emit with the payload the schema returns, and no emit of an event without listeners", async () => {
    const { logger, lines } = createRecordingLogger();
    const nextasking = createShopNextasking({ logger });
    using fake = fakeNextasking(nextasking);

    const { eventId } = await nextasking.emit("order.placed", { orderId: "order-1" });
    await nextasking.emit("order.cancelled", { orderId: "order-1" }, { eventId: "cancelled-order-1" });

    expect(fake.emits).toEqual([{ event: "order.placed", payload: { orderId: "order-1", quantity: 1 }, eventId }]);
    expect(lines).toContainEqual({
      level: "warn",
      fields: { eventName: "order.cancelled", eventId: "cancelled-order-1" },
      message: "no listeners for event",
    });
  });

  test("records a repeated run id or event id once and logs the dropped duplicate as production does", async () => {
    const { logger, lines } = createRecordingLogger();
    const nextasking = createShopNextasking({ logger });
    using fake = fakeNextasking(nextasking);

    await nextasking.dispatch("send-invoice", { accountId: "acme" }, { runId: "invoice-1" });
    await nextasking.dispatch("send-invoice", { accountId: "globex" }, { runId: "invoice-1" });
    await nextasking.emit("order.placed", { orderId: "order-1" }, { eventId: "placed-1" });
    await nextasking.emit("order.placed", { orderId: "order-1" }, { eventId: "placed-1" });

    expect(fake.dispatches).toEqual([
      { task: "send-invoice", payload: { accountId: "acme", currency: "EUR" }, runId: "invoice-1" },
    ]);
    expect(fake.emits).toEqual([
      { event: "order.placed", payload: { orderId: "order-1", quantity: 1 }, eventId: "placed-1" },
    ]);
    expect(lines.filter((line) => line.message === "duplicate dispatch dropped")).toEqual([
      { level: "debug", fields: { task: "send-invoice", runId: "invoice-1" }, message: "duplicate dispatch dropped" },
      {
        level: "debug",
        fields: { eventName: "order.placed", eventId: "placed-1" },
        message: "duplicate dispatch dropped",
      },
    ]);
  });

  test("records a replayed run without checking that it failed", async () => {
    const nextasking = createShopNextasking();
    using fake = fakeNextasking(nextasking);

    await nextasking.replay("send-invoice", "invoice-1");

    expect(fake.replays).toEqual([{ task: "send-invoice", runId: "invoice-1" }]);
  });

  test("checks names, payloads, ids, delays and concurrency keys as production does, and records nothing it refuses", async () => {
    const shopNextasking = createShopNextasking();
    using fake = fakeNextasking(shopNextasking);
    const nextasking = shopNextasking as unknown as NextaskingCalledWithAnyName;

    await expect(nextasking.dispatch("send-receipt", {})).rejects.toThrow(UnknownTaskError);
    await expect(nextasking.dispatch("send-invoice", { accountId: 42 })).rejects.toThrow(InvalidPayloadError);
    await expect(nextasking.dispatch("send-invoice", { accountId: "acme" }, { runId: "" })).rejects.toThrow(
      'Invalid runId for task "send-invoice": "" is not a non-empty string',
    );
    await expect(nextasking.dispatch("send-invoice", { accountId: "acme" }, { delay: "5" })).rejects.toThrow(
      'Invalid delay for task "send-invoice"',
    );
    await expect(nextasking.dispatch("send-invoice", { accountId: "broken" })).rejects.toThrow(
      'Task "send-invoice" has an invalid concurrency.key: it returned an empty string, not a non-empty string',
    );
    await expect(nextasking.dispatch("close-books", { month: 1 })).rejects.toThrow(
      'Scheduled task "close-books" takes no payload',
    );
    await expect(nextasking.emit("order.shipped", { orderId: "order-1" })).rejects.toThrow(UnknownEventError);
    await expect(nextasking.emit("order.placed", { quantity: 2 })).rejects.toThrow(InvalidPayloadError);
    await expect(nextasking.emit("nextasking/run.failed", {})).rejects.toThrow(
      'Event "nextasking/run.failed" is built in',
    );
    await expect(nextasking.replay("send-receipt", "receipt-1")).rejects.toThrow(UnknownTaskError);

    expect([fake.dispatches, fake.emits, fake.replays]).toEqual([[], [], []]);
  });

  test("rejects every Redis read and startWorker, naming the call, while the definitions keep working", async () => {
    const nextasking = createShopNextasking();
    fakeNextasking(nextasking);

    const fakedCalls = {
      getRun: () => nextasking.getRun("send-invoice", "invoice-1"),
      listRuns: () => nextasking.listRuns({ state: "failed" }),
      countRuns: () => nextasking.countRuns(),
      getEvent: () => nextasking.getEvent("order.placed", "placed-1"),
      listSchedules: () => nextasking.listSchedules(),
      startWorker: () => nextasking.startWorker(),
    };

    for (const [callName, call] of Object.entries(fakedCalls)) {
      await expect(call()).rejects.toThrow(`This nextasking instance is faked: ${callName} needs Redis`);
    }
    expect(nextasking.isTaskName("send-invoice")).toBe(true);
    expect(nextasking.isEventName("order.placed")).toBe(true);
    expect(nextasking.taskDefinitions.map((task) => task.name)).toEqual([
      "send-invoice",
      "reserve-stock",
      "alert-on-run-failed",
      "close-books",
    ]);
    expect(nextasking.eventDefinitions.map((event) => event.name)).toContain("order.placed");
  });

  test("throws when the instance is faked already, and fakes it again once restored", async () => {
    const nextasking = createShopNextasking();
    const firstFake = fakeNextasking(nextasking);

    expect(() => fakeNextasking(nextasking)).toThrow("This nextasking instance is already faked");
    firstFake.restore();
    firstFake.restore();
    using secondFake = fakeNextasking(nextasking);
    await nextasking.dispatch("close-books");

    expect(firstFake.dispatches).toEqual([]);
    expect(secondFake.dispatches).toEqual([{ task: "close-books", payload: undefined, runId: expect.any(String) }]);
  });

  test("returns the instance to Redis when the fake is disposed", async () => {
    const nextasking = createShopNextasking();
    {
      using fake = fakeNextasking(nextasking);
      await nextasking.dispatch("close-books", undefined, { runId: "while-faked" });
      expect(fake.dispatches).toHaveLength(1);
    }

    try {
      await expect(nextasking.dispatch("close-books", undefined, { runId: "after-disposal" })).rejects.toThrow(
        RedisUnavailableError,
      );
    } finally {
      await closeNextasking(nextasking);
    }
  });

  test("throws when the instance has started its worker", async () => {
    const nextasking = createShopNextasking();

    try {
      await nextasking.startWorker();

      expect(() => fakeNextasking(nextasking)).toThrow("This nextasking instance has started its worker");
    } finally {
      await closeNextasking(nextasking);
    }
  });

  test("takes its record types from the instance passed, not from Register", () => {
    const nextasking = createShopNextasking();
    using fake = fakeNextasking(nextasking);
    const describeDispatch = (dispatch: (typeof fake.dispatches)[number]) => {
      if (dispatch.task === "send-invoice") {
        expectTypeOf(dispatch.payload).toEqualTypeOf<{ accountId: string; currency: string }>();
      }
      if (dispatch.task === "reserve-stock") {
        expectTypeOf(dispatch.payload).toEqualTypeOf<{ orderId: string; quantity: number }>();
      }
      if (dispatch.task === "close-books") {
        expectTypeOf(dispatch.payload).toBeUndefined();
      }
      return dispatch.delay;
    };
    type RecordedEmitOfShop = (typeof fake.emits)[number];
    type RecordedReplayOfShop = (typeof fake.replays)[number];

    expectTypeOf(describeDispatch).returns.toEqualTypeOf<Duration | undefined>();
    expectTypeOf<(typeof fake.dispatches)[number]["task"]>().toEqualTypeOf<ShopTaskName>();
    expectTypeOf<(typeof fake.dispatches)[number]["task"]>().not.toEqualTypeOf<TaskName>();
    expectTypeOf<(typeof fake.dispatches)[number]["runId"]>().toEqualTypeOf<RunId>();
    expectTypeOf<RecordedEmitOfShop["event"]>().toEqualTypeOf<ShopEventName>();
    expectTypeOf<Extract<RecordedEmitOfShop, { event: "order.cancelled" }>["payload"]>().toEqualTypeOf<{
      orderId: string;
    }>();
    expectTypeOf<RecordedEmitOfShop["eventId"]>().toEqualTypeOf<EventId>();
    expectTypeOf<RecordedReplayOfShop>().toEqualTypeOf<{ task: ShopTaskName; runId: RunId }>();
    expectTypeOf(fake.restore).toEqualTypeOf<() => void>();
    expectTypeOf(fake[Symbol.dispose]).toEqualTypeOf<() => void>();
    expectTypeOf(fake).toEqualTypeOf<NextaskingFake<ShopTask, ShopEvent>>();
  });
});

type ShopTaskName = "send-invoice" | "reserve-stock" | "alert-on-run-failed" | "close-books";

type ShopEventName = "order.placed" | "order.cancelled";

type ShopTask = typeof sendInvoice | typeof reserveStock | typeof alertOnRunFailed | typeof closeBooks;

type ShopEvent = typeof orderPlaced | typeof orderCancelled | typeof runFailed | typeof fanOutFailed;
