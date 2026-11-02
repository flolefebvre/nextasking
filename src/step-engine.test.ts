import { describe, expect, expectTypeOf, test, vi } from "vitest";

import type { Duration } from "./durations.js";
import { StepMisuseError } from "./errors.js";
import { deriveHandlerSendId } from "./ids.js";
import type { JsonValue } from "./json.js";
import type { DispatchByName, DispatchOptions, EmitByName, EmitOptions } from "./registry.js";
import { openStepEngine } from "./step-engine.js";
import type { StepStore } from "./step-engine.js";

const ONE_MEBIBYTE = 1024 * 1024;

function createInMemoryStepStore(serializedResultsByStepId: Record<string, string> = {}): StepStore {
  const savedResultsByStepId = new Map(Object.entries(serializedResultsByStepId));
  return {
    load: async () => Object.fromEntries(savedResultsByStepId),
    save: async (stepId, serializedResult) => {
      savedResultsByStepId.set(stepId, serializedResult);
    },
  };
}

type RecordedSend = {
  kind: "dispatch" | "emit";
  name: string;
  payload: unknown;
  options: DispatchOptions & EmitOptions;
};

type RecordingSends = {
  sends: RecordedSend[];
  dispatch: DispatchByName;
  emit: EmitByName;
};

function createRecordingSends(): RecordingSends {
  const sends: RecordedSend[] = [];
  return {
    sends,
    dispatch: async (name, payload, options = {}) => {
      sends.push({ kind: "dispatch", name, payload, options });
      return { runId: options.runId ?? "random-run-id" };
    },
    emit: async (name, payload, options = {}) => {
      sends.push({ kind: "emit", name, payload, options });
      return { eventId: options.eventId ?? "random-event-id" };
    },
  };
}

function openAttempt(store: StepStore, attempt = 1, recordingSends: RecordingSends = createRecordingSends()) {
  return openStepEngine({
    taskName: "send-invoice",
    runId: "run-7",
    attempt,
    store,
    dispatch: recordingSends.dispatch,
    emit: recordingSends.emit,
  });
}

function stringSerializedInBytes(sizeInBytes: number): string {
  return "x".repeat(sizeInBytes - '[""]'.length);
}

describe("step.run", () => {
  test("runs fn and resolves with its JSON round-tripped result", async () => {
    const { step } = await openAttempt(createInMemoryStepStore());

    const invoice = await step.run("load-invoice", () => ({ invoiceId: "inv-1", note: undefined, total: Number.NaN }));

    expect(invoice).toStrictEqual({ invoiceId: "inv-1", total: null });
  });

  test("serves a later attempt the saved result without running fn again", async () => {
    const store = createInMemoryStepStore();
    const firstAttempt = await openAttempt(store);
    await firstAttempt.step.run("charge-card", async () => ({ chargeId: "ch-1" }));
    const chargeCard = vi.fn(async () => ({ chargeId: "ch-2" }));
    const secondAttempt = await openAttempt(store);

    const charge = await secondAttempt.step.run("charge-card", chargeCard);

    expect(charge).toStrictEqual({ chargeId: "ch-1" });
    expect(chargeCard).not.toHaveBeenCalled();
  });

  test("saves an undefined result, so a later attempt gets undefined without running fn", async () => {
    const store = createInMemoryStepStore();
    const firstAttempt = await openAttempt(store);
    await firstAttempt.step.run("send-email", async () => undefined);
    const sendEmail = vi.fn(async () => undefined);
    const secondAttempt = await openAttempt(store);

    const sent = await secondAttempt.step.run("send-email", sendEmail);

    expect(sent).toBeUndefined();
    expect(sendEmail).not.toHaveBeenCalled();
  });

  test("ignores a saved step whose id no longer appears in the code", async () => {
    const store = createInMemoryStepStore();
    const firstAttempt = await openAttempt(store);
    await firstAttempt.step.run("old-step", () => "old result");
    const { step, throwRecordedMisuse } = await openAttempt(store);

    const result = await step.run("new-step", () => "new result");

    expect(result).toBe("new result");
    expect(throwRecordedMisuse).not.toThrow();
  });

  test("does not save a step whose fn throws, and lets its error through", async () => {
    const store = createInMemoryStepStore();
    const firstAttempt = await openAttempt(store);
    const upstreamError = new Error("upstream unavailable");
    const secondAttempt = await openAttempt(store);

    await expect(
      firstAttempt.step.run("call-upstream", () => {
        throw upstreamError;
      }),
    ).rejects.toBe(upstreamError);
    expect(await secondAttempt.step.run("call-upstream", () => "called")).toBe("called");
    expect(firstAttempt.throwRecordedMisuse).not.toThrow();
  });

  test("saves each of parallel steps as it settles", async () => {
    const store = createInMemoryStepStore();
    const { step } = await openAttempt(store);
    let finishSlowStep: (value: string) => void = () => undefined;
    const slowStepResult = new Promise<string>((resolve) => {
      finishSlowStep = resolve;
    });

    const bothSteps = Promise.all([
      step.run("fast-step", async () => "fast"),
      step.run("slow-step", () => slowStepResult),
    ]);
    await vi.waitFor(async () => {
      expect(await store.load()).toHaveProperty("fast-step");
    });
    const savedBeforeTheSlowStepSettles = await store.load();
    finishSlowStep("slow");

    expect(savedBeforeTheSlowStepSettles).not.toHaveProperty("slow-step");
    expect(await bothSteps).toEqual(["fast", "slow"]);
    expect(await store.load()).toHaveProperty("slow-step");
  });
});

describe("step.run idempotency key", () => {
  test("hands fn the same idempotency key on every attempt, and another key to another step", async () => {
    const store = createInMemoryStepStore();
    const firstAttempt = await openAttempt(store);
    const secondAttempt = await openAttempt(store);
    const keys: string[] = [];

    await firstAttempt.step
      .run("charge-card", ({ idempotencyKey }) => {
        keys.push(idempotencyKey);
        throw new Error("timeout");
      })
      .catch(() => undefined);
    await secondAttempt.step.run("charge-card", ({ idempotencyKey }) => {
      keys.push(idempotencyKey);
    });
    await secondAttempt.step.run("send-receipt", ({ idempotencyKey }) => {
      keys.push(idempotencyKey);
    });

    expect(keys[0]).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    expect(keys[1]).toBe(keys[0]);
    expect(keys[2]).not.toBe(keys[0]);
  });
});

describe("step.run misuse", () => {
  test("rejects a repeated step id, naming it, without running fn again", async () => {
    const { step, throwRecordedMisuse } = await openAttempt(createInMemoryStepStore());
    await step.run("send-email", () => "sent");
    const sendEmailAgain = vi.fn(() => "sent again");

    await expect(step.run("send-email", sendEmailAgain)).rejects.toThrow(
      new StepMisuseError(
        'Step id "send-email" is used twice in one run of task "send-invoice". Give each step its own id.',
      ),
    );
    expect(sendEmailAgain).not.toHaveBeenCalled();
    expect(throwRecordedMisuse).toThrow(StepMisuseError);
  });

  test("records a misuse the handler caught, so the run still fails with it", async () => {
    const { step, throwRecordedMisuse } = await openAttempt(createInMemoryStepStore());
    await step.run("send-email", () => "sent");

    await step.run("send-email", () => "sent").catch(() => undefined);

    expect(throwRecordedMisuse).toThrow('Step id "send-email" is used twice');
  });

  test("rejects a step started inside another step's fn", async () => {
    const store = createInMemoryStepStore();
    const { step, throwRecordedMisuse } = await openAttempt(store);
    const secondAttempt = await openAttempt(store);

    await expect(step.run("outer", () => step.run("inner", () => "inner result"))).rejects.toThrow(
      new StepMisuseError(
        'Step "inner" was started inside the fn of step "outer". Start every step from the handler itself.',
      ),
    );
    expect(throwRecordedMisuse).toThrow(StepMisuseError);
    expect(await secondAttempt.step.run("inner", () => "run again")).toBe("run again");
  });

  test.each([
    ["a BigInt", () => 10n],
    [
      "a circular object",
      () => {
        const circular: Record<string, unknown> = {};
        circular.self = circular;
        return circular;
      },
    ],
    ["a function", () => () => undefined],
    ["a symbol", () => Symbol("result")],
  ])("rejects %s result as not JSON and does not save it", async (_resultKind, nonJsonResult) => {
    const store = createInMemoryStepStore();
    const { step, throwRecordedMisuse } = await openAttempt(store);
    const secondAttempt = await openAttempt(store);

    await expect(step.run("compute", nonJsonResult as () => JsonValue)).rejects.toThrow(
      /^The result of step "compute" is not JSON/,
    );
    expect(throwRecordedMisuse).toThrow(StepMisuseError);
    expect(await secondAttempt.step.run("compute", () => "run again")).toBe("run again");
  });

  test("accepts a result of exactly 1 MiB once serialized", async () => {
    const { step } = await openAttempt(createInMemoryStepStore());

    await expect(step.run("render", () => stringSerializedInBytes(ONE_MEBIBYTE))).resolves.toHaveLength(
      ONE_MEBIBYTE - 4,
    );
  });

  test("rejects a result over 1 MiB once serialized, and does not save it", async () => {
    const store = createInMemoryStepStore();
    const { step, throwRecordedMisuse } = await openAttempt(store);
    const secondAttempt = await openAttempt(store);

    await expect(step.run("render", () => stringSerializedInBytes(ONE_MEBIBYTE + 1))).rejects.toThrow(
      new StepMisuseError(
        'The result of step "render" is 1048577 bytes once serialized, over the 1 MiB limit for one step. Store large data elsewhere, such as your database, and return its id.',
      ),
    );
    expect(throwRecordedMisuse).toThrow(StepMisuseError);
    expect(await secondAttempt.step.run("render", () => "small")).toBe("small");
  });

  test("rejects a step that would bring the run's saved steps over 10 MiB, counting earlier attempts", async () => {
    const savedResultsByStepId = Object.fromEntries(
      Array.from({ length: 9 }, (_unused, index) => [`page-${index}`, `["${stringSerializedInBytes(ONE_MEBIBYTE)}"]`]),
    );
    const { step, throwRecordedMisuse } = await openAttempt(createInMemoryStepStore(savedResultsByStepId));
    await step.run("page-9", () => stringSerializedInBytes(ONE_MEBIBYTE));

    await expect(step.run("summary", () => "s")).rejects.toThrow(
      new StepMisuseError(
        'Saving step "summary" would bring the saved steps of this run of task "send-invoice" to 10485765 bytes, over the 10 MiB limit for one run. Store large data elsewhere, such as your database, and return its id.',
      ),
    );
    expect(throwRecordedMisuse).toThrow(StepMisuseError);
  });

  test("rejects every later step with the recorded misuse, without running its fn", async () => {
    const { step } = await openAttempt(createInMemoryStepStore());
    await step.run("send-email", () => "sent");
    const repeatError = await step.run("send-email", () => "sent").catch((error: unknown) => error);
    const nextStep = vi.fn(() => "next");

    await expect(step.run("next-step", nextStep)).rejects.toBe(repeatError);
    expect(nextStep).not.toHaveBeenCalled();
  });
});

describe("step.run types", () => {
  test("resolves with the type fn returns, sync or async", async () => {
    const { step } = await openAttempt(createInMemoryStepStore());

    const charge = await step.run("charge-card", async () => ({ chargeId: "ch-1", amount: 10 }));
    const nothing = await step.run("log", () => undefined);
    const sent = await step.run("send-email", async () => {});

    expectTypeOf(charge).toEqualTypeOf<{ chargeId: string; amount: number }>();
    expectTypeOf(nothing).toEqualTypeOf<undefined>();
    expectTypeOf(sent).toEqualTypeOf<void>();
  });

  test("refuses a result that isn't JSON at compile time", async () => {
    const { step } = await openAttempt(createInMemoryStepStore());

    await expect(
      // @ts-expect-error a Date isn't JSON
      step.run("read-clock", () => new Date(0)),
    ).resolves.toBe("1970-01-01T00:00:00.000Z");
    await expect(
      // @ts-expect-error a Map isn't JSON
      step.run("build-index", async () => new Map<string, number>()),
    ).resolves.toStrictEqual({});
  });
});

const ONE_HOUR_IN_MILLISECONDS = 60 * 60 * 1000;

function savedSleep(wakeAt: number, attempt = 1): string {
  return JSON.stringify({ kind: "sleep", wakeAt, attempt });
}

describe("step.sleep and step.sleepUntil", () => {
  test("save the wake time, then suspend the run until it, even when the handler catches the suspension", async () => {
    const store = createInMemoryStepStore();
    const { step, executeHandler } = await openAttempt(store, 2);
    const wakeAt = Date.now() + ONE_HOUR_IN_MILLISECONDS;

    const suspendedUntil = await executeHandler(async () => {
      await step.sleepUntil("wait-for-trial-end", new Date(wakeAt)).catch(() => undefined);
    });

    expect(suspendedUntil).toBe(wakeAt);
    expect(await store.load()).toStrictEqual({ "wait-for-trial-end": savedSleep(wakeAt, 2) });
  });

  test("sleep saves a wake time its duration away from now", async () => {
    const { step, executeHandler } = await openAttempt(createInMemoryStepStore());
    const threeDaysInMilliseconds = 3 * 24 * ONE_HOUR_IN_MILLISECONDS;
    const sleepReachedAt = Date.now();

    const suspendedUntil = await executeHandler(() => step.sleep("cool-down", "3d"));

    expect(suspendedUntil).toBeGreaterThanOrEqual(sleepReachedAt + threeDaysInMilliseconds);
    expect(suspendedUntil).toBeLessThanOrEqual(Date.now() + threeDaysInMilliseconds);
  });

  test("returns at once for a time in the past, and still saves it", async () => {
    const store = createInMemoryStepStore();
    const { step, executeHandler } = await openAttempt(store);
    const pastTime = Date.now() - 1000;
    const afterTheSleep = vi.fn();

    const suspendedUntil = await executeHandler(async () => {
      await step.sleepUntil("wait", new Date(pastTime));
      afterTheSleep();
    });

    expect(suspendedUntil).toBeUndefined();
    expect(afterTheSleep).toHaveBeenCalledOnce();
    expect(await store.load()).toStrictEqual({ wait: savedSleep(pastTime) });
  });

  test("takes an ISO date string with Z or an offset", async () => {
    const { step, executeHandler } = await openAttempt(createInMemoryStepStore());

    const suspendedUntil = await executeHandler(async () => {
      await Promise.all([
        step.sleepUntil("wait-utc", "2999-01-01T00:00:00.000Z"),
        step.sleepUntil("wait-offset", "2999-01-01T00:00:00+01:00"),
      ]);
    });

    expect(suspendedUntil).toBe(Date.parse("2998-12-31T23:00:00.000Z"));
  });

  test("returns at once on resume when the saved wake time has passed, whatever the duration says", async () => {
    const pastWakeAt = Date.now() - 1000;
    const store = createInMemoryStepStore({ "cool-down": savedSleep(pastWakeAt) });
    const { step, executeHandler } = await openAttempt(store);

    const suspendedUntil = await executeHandler(() => step.sleep("cool-down", "3d"));

    expect(suspendedUntil).toBeUndefined();
    expect(await store.load()).toStrictEqual({ "cool-down": savedSleep(pastWakeAt) });
  });

  test("suspends again until the saved wake time when the run is picked up before it", async () => {
    const savedWakeAt = Date.now() + ONE_HOUR_IN_MILLISECONDS;
    const { step, executeHandler } = await openAttempt(createInMemoryStepStore({ wait: savedSleep(savedWakeAt) }));

    const suspendedUntil = await executeHandler(() => step.sleepUntil("wait", new Date(0)));

    expect(suspendedUntil).toBe(savedWakeAt);
  });

  test("suspends once the steps in flight settle, so their results are saved", async () => {
    const store = createInMemoryStepStore();
    const { step, executeHandler } = await openAttempt(store);
    let finishSlowStep: (value: string) => void = () => undefined;
    const slowStepResult = new Promise<string>((resolve) => {
      finishSlowStep = resolve;
    });
    let isSuspended = false;

    const suspension = executeHandler(async () => {
      await Promise.all([step.run("slow-step", () => slowStepResult), step.sleep("wait", "1h")]);
    }).then((suspendedUntil) => {
      isSuspended = true;
      return suspendedUntil;
    });
    await vi.waitFor(async () => {
      expect(await store.load()).toHaveProperty("wait");
    });
    const isSuspendedBeforeTheSlowStepSettles = isSuspended;
    finishSlowStep("slow");

    expect(isSuspendedBeforeTheSlowStepSettles).toBe(false);
    expect(await suspension).toBeGreaterThan(Date.now());
    expect(await store.load()).toHaveProperty("slow-step", '["slow"]');
  });

  test("suspends until the earliest of two parallel sleeps", async () => {
    const { step, executeHandler } = await openAttempt(createInMemoryStepStore());
    const earliestWakeAt = Date.now() + ONE_HOUR_IN_MILLISECONDS;

    const suspendedUntil = await executeHandler(async () => {
      await Promise.all([
        step.sleepUntil("wait-long", new Date(earliestWakeAt + ONE_HOUR_IN_MILLISECONDS)),
        step.sleepUntil("wait-short", new Date(earliestWakeAt)),
      ]);
    });

    expect(suspendedUntil).toBe(earliestWakeAt);
  });

  test("suspends even when the handler goes on to throw another error", async () => {
    const { step, executeHandler } = await openAttempt(createInMemoryStepStore());

    const suspendedUntil = await executeHandler(async () => {
      await step.sleep("wait", "1h").catch(() => undefined);
      throw new Error("handler error after the sleep");
    });

    expect(suspendedUntil).toBeGreaterThan(Date.now());
  });

  test("lets the handler's own error through when no sleep suspends the run", async () => {
    const { executeHandler } = await openAttempt(createInMemoryStepStore());
    const handlerError = new Error("upstream unavailable");

    await expect(
      executeHandler(async () => {
        throw handlerError;
      }),
    ).rejects.toBe(handlerError);
  });

  test("rejects every later step of the stretch without running it", async () => {
    const { step, executeHandler } = await openAttempt(createInMemoryStepStore());
    const laterStep = vi.fn(() => "later");
    let laterStepError: unknown;

    await executeHandler(async () => {
      await step.sleep("wait", "1h").catch(() => undefined);
      laterStepError = await step.run("later-step", laterStep).catch((error: unknown) => error);
    });

    expect(laterStep).not.toHaveBeenCalled();
    expect(laterStepError).toBeInstanceOf(Error);
    expect(laterStepError).not.toBeInstanceOf(StepMisuseError);
  });

  test("fails the run with a misuse of the same stretch rather than suspending it", async () => {
    const { step, executeHandler } = await openAttempt(createInMemoryStepStore());

    await expect(
      executeHandler(async () => {
        await step.run("send-email", () => "sent");
        await step.run("send-email", () => "sent").catch(() => undefined);
        await step.sleep("wait", "1h");
      }),
    ).rejects.toThrow(StepMisuseError);
  });

  test("shares the step id namespace, so a sleep reusing a step's id is a misuse", async () => {
    const { step, executeHandler } = await openAttempt(createInMemoryStepStore());

    await expect(
      executeHandler(async () => {
        await step.run("wait", () => "done");
        await step.sleep("wait", "1h");
      }),
    ).rejects.toThrow(
      new StepMisuseError('Step id "wait" is used twice in one run of task "send-invoice". Give each step its own id.'),
    );
  });

  test("rejects a sleep started inside a step's fn", async () => {
    const { step, executeHandler } = await openAttempt(createInMemoryStepStore());

    await expect(
      executeHandler(async () => {
        await step.run("outer", async () => {
          await step.sleep("inner-wait", "1h");
        });
      }),
    ).rejects.toThrow(
      new StepMisuseError(
        'Step "inner-wait" was started inside the fn of step "outer". Start every step from the handler itself.',
      ),
    );
  });

  test.each([
    ["an invalid Date", () => new Date("not a date")],
    ["a string that isn't a date", () => "next tuesday"],
    ["an ISO string without Z or an offset", () => "2999-01-01T00:00:00"],
    ["a number", () => Date.now() as unknown as Date],
  ])("rejects %s as a wake time", async (_timeKind, invalidTime) => {
    const store = createInMemoryStepStore();
    const { step, executeHandler } = await openAttempt(store);

    await expect(executeHandler(() => step.sleepUntil("wait", invalidTime()))).rejects.toThrow(
      /^The wake time of step "wait" is not a Date or an ISO date string with Z or an offset/,
    );
    expect(await store.load()).toStrictEqual({});
  });

  test("rejects an invalid duration that slipped past the types", async () => {
    const { step, executeHandler } = await openAttempt(createInMemoryStepStore());

    await expect(executeHandler(() => step.sleep("wait", "0.5h" as Duration))).rejects.toThrow(
      new StepMisuseError(
        'The duration of step "wait" is invalid: "0.5h" is not a duration: a whole number followed by s, m, h or d, such as "30s"',
      ),
    );
  });
});

describe("resuming after a sleep", () => {
  test("is told apart from a retry by the attempt that saved the sleep", async () => {
    const savedSteps = { wait: savedSleep(Date.now() - 1000, 2) };

    const resumedAttempt = await openAttempt(createInMemoryStepStore(savedSteps), 2);
    const retryAttempt = await openAttempt(createInMemoryStepStore(savedSteps), 3);
    const firstAttempt = await openAttempt(createInMemoryStepStore(), 1);

    expect(resumedAttempt.isResumingAfterSleep).toBe(true);
    expect(retryAttempt.isResumingAfterSleep).toBe(false);
    expect(firstAttempt.isResumingAfterSleep).toBe(false);
  });
});

function topLevelSendId(positionInScope: number): string {
  return deriveHandlerSendId("send-invoice", "run-7", undefined, positionInScope);
}

describe("handler dispatch and emit", () => {
  test("send under ids derived from the run and their position, and save a marker for each", async () => {
    const store = createInMemoryStepStore();
    const recordingSends = createRecordingSends();
    const { dispatch, emit } = await openAttempt(store, 1, recordingSends);

    const dispatchedRun = await dispatch("send-receipt", { invoiceId: "inv-1" });
    const emittedEvent = await emit("invoice.sent", { invoiceId: "inv-1" });

    expect(dispatchedRun).toStrictEqual({ runId: topLevelSendId(0) });
    expect(emittedEvent).toStrictEqual({ eventId: topLevelSendId(1) });
    expect(recordingSends.sends).toStrictEqual([
      {
        kind: "dispatch",
        name: "send-receipt",
        payload: { invoiceId: "inv-1" },
        options: { runId: topLevelSendId(0) },
      },
      { kind: "emit", name: "invoice.sent", payload: { invoiceId: "inv-1" }, options: { eventId: topLevelSendId(1) } },
    ]);
    expect(await store.load()).toStrictEqual({
      [topLevelSendId(0)]: JSON.stringify({ kind: "dispatch", runId: topLevelSendId(0) }),
      [topLevelSendId(1)]: JSON.stringify({ kind: "emit", eventId: topLevelSendId(1) }),
    });
  });

  test("skip a send a previous attempt made, and resolve with what it sent", async () => {
    const store = createInMemoryStepStore();
    const firstAttempt = await openAttempt(store, 1);
    const firstDispatchedRun = await firstAttempt.dispatch("send-receipt", { invoiceId: "inv-1" });
    const firstEmittedEvent = await firstAttempt.emit("invoice.sent", { invoiceId: "inv-1" });
    const recordingSends = createRecordingSends();
    const secondAttempt = await openAttempt(store, 2, recordingSends);

    const secondDispatchedRun = await secondAttempt.dispatch("send-receipt", { invoiceId: "inv-1" });
    const secondEmittedEvent = await secondAttempt.emit("invoice.sent", { invoiceId: "inv-1" });

    expect(secondDispatchedRun).toStrictEqual(firstDispatchedRun);
    expect(secondEmittedEvent).toStrictEqual(firstEmittedEvent);
    expect(recordingSends.sends).toStrictEqual([]);
  });

  test("let an explicit runId or eventId win, keyed by position all the same", async () => {
    const store = createInMemoryStepStore();
    const recordingSends = createRecordingSends();
    const firstAttempt = await openAttempt(store, 1, recordingSends);
    await firstAttempt.dispatch("ship-order", { orderId: "ord-1" }, { runId: "ship-order-ord-1", delay: "5m" });
    await firstAttempt.emit("order.shipped", { orderId: "ord-1" }, { eventId: "order-shipped-ord-1" });
    const secondAttempt = await openAttempt(store, 2, recordingSends);

    const repeatedDispatch = await secondAttempt.dispatch("ship-order", { orderId: "ord-1" }, { runId: "other" });
    const repeatedEmit = await secondAttempt.emit("order.shipped", { orderId: "ord-1" });

    expect(recordingSends.sends.map((send) => send.options)).toStrictEqual([
      { runId: "ship-order-ord-1", delay: "5m" },
      { eventId: "order-shipped-ord-1" },
    ]);
    expect(repeatedDispatch).toStrictEqual({ runId: "ship-order-ord-1" });
    expect(repeatedEmit).toStrictEqual({ eventId: "order-shipped-ord-1" });
    expect(await store.load()).toStrictEqual({
      [topLevelSendId(0)]: JSON.stringify({ kind: "dispatch", runId: "ship-order-ord-1" }),
      [topLevelSendId(1)]: JSON.stringify({ kind: "emit", eventId: "order-shipped-ord-1" }),
    });
  });

  test("number the sends inside a step's fn apart from the top level", async () => {
    const recordingSends = createRecordingSends();
    const { step, emit } = await openAttempt(createInMemoryStepStore(), 1, recordingSends);

    await step.run("notify", async () => {
      await emit("invoice.sent", { invoiceId: "inv-1" });
    });
    await emit("invoice.sent", { invoiceId: "inv-1" });

    expect(recordingSends.sends.map((send) => send.options.eventId)).toStrictEqual([
      deriveHandlerSendId("send-invoice", "run-7", "notify", 0),
      topLevelSendId(0),
    ]);
  });

  test("save no marker for a send that fails, so a later attempt sends again under the same id", async () => {
    const store = createInMemoryStepStore();
    const failingSends = createRecordingSends();
    failingSends.emit = async () => {
      throw new Error("Redis unavailable");
    };
    const firstAttempt = await openAttempt(store, 1, failingSends);
    await firstAttempt.emit("invoice.sent", {}).catch(() => undefined);
    const recordingSends = createRecordingSends();
    const secondAttempt = await openAttempt(store, 2, recordingSends);

    await secondAttempt.emit("invoice.sent", {});

    expect(recordingSends.sends.map((send) => send.options.eventId)).toStrictEqual([topLevelSendId(0)]);
  });

  test("throw without sending once a sleep suspended the stretch", async () => {
    const recordingSends = createRecordingSends();
    const { step, emit, executeHandler } = await openAttempt(createInMemoryStepStore(), 1, recordingSends);
    let emitError: unknown;

    await executeHandler(async () => {
      await step.sleep("wait", "1h").catch(() => undefined);
      emitError = await emit("invoice.sent", {}).catch((error: unknown) => error);
    });

    expect(emitError).toBeInstanceOf(Error);
    expect(recordingSends.sends).toStrictEqual([]);
  });

  test("are awaited by a sleep, so their markers are saved before the run suspends", async () => {
    const store = createInMemoryStepStore();
    const slowSends = createRecordingSends();
    let finishEmit: () => void = () => undefined;
    slowSends.emit = async () => {
      await new Promise<void>((resolve) => {
        finishEmit = resolve;
      });
      return { eventId: "evt-1" };
    };
    const { step, emit, executeHandler } = await openAttempt(store, 1, slowSends);

    const suspension = executeHandler(async () => {
      await Promise.all([emit("invoice.sent", {}), step.sleep("wait", "1h")]);
    });
    await vi.waitFor(async () => {
      expect(await store.load()).toHaveProperty("wait");
    });
    finishEmit();

    expect(await suspension).toBeGreaterThan(Date.now());
    expect(await store.load()).toHaveProperty([topLevelSendId(0)], JSON.stringify({ kind: "emit", eventId: "evt-1" }));
  });
});
