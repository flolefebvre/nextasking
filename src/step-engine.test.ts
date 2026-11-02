import { describe, expect, expectTypeOf, test, vi } from "vitest";

import { StepMisuseError } from "./errors.js";
import type { JsonValue } from "./json.js";
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

function openAttempt(store: StepStore) {
  return openStepEngine({ taskName: "send-invoice", runId: "run-7", store });
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
        'Step id "send-email" is used twice in one run of task "send-invoice". Give each step.run call its own id.',
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
