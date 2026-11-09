import { describe, expect, expectTypeOf, test, vi } from "vitest";

import type { Duration } from "./durations.js";
import { StepMisuseError } from "./errors.js";
import { deriveHandlerSendId } from "./ids.js";
import type { JsonValue } from "./json.js";
import type { DispatchByName, DispatchOptions, EmitByName, EmitOptions } from "./enqueue-requests.js";
import { createInMemoryStepStore } from "./in-memory-step-store.js";
import { LostRunLockError } from "./errors.js";
import { openStepEngine, sendThenSave } from "./step-engine.js";
import type {
  FutureResumeTimeHandling,
  HandlerOutcome,
  HandlerSender,
  ResumeTime,
  SendFence,
  StepEngineConfig,
  StepStore,
} from "./step-engine.js";

const ONE_MEBIBYTE = 1024 * 1024;

const LOCK_TOKEN = "lock-token-7";

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

type AttemptControls = Partial<
  Pick<StepEngineConfig, "abortSignal" | "isWorkerDraining" | "sleepsWokenAt" | "markLockLost" | "sender">
> & {
  futureResumeTime?: FutureResumeTimeHandling;
};

function openAttempt(
  store: StepStore,
  recordingSends: RecordingSends = createRecordingSends(),
  {
    abortSignal = new AbortController().signal,
    isWorkerDraining = () => false,
    futureResumeTime = "suspend the run",
    sender = sendThenSave(store, recordingSends),
    markLockLost = () => undefined,
    ...otherControls
  }: AttemptControls = {},
) {
  return openStepEngine({
    taskName: "send-invoice",
    runId: "run-7",
    lockToken: LOCK_TOKEN,
    store,
    sender,
    abortSignal,
    isWorkerDraining,
    futureResumeTime,
    markLockLost,
    ...otherControls,
  });
}

function stringSerializedInBytes(sizeInBytes: number): string {
  return "x".repeat(sizeInBytes - '[""]'.length);
}

describe("step.run", () => {
  test("runs fn and resolves with its JSON round-tripped result", async () => {
    const { step } = openAttempt(createInMemoryStepStore());

    const invoice = await step.run("load-invoice", () => ({ invoiceId: "inv-1", note: undefined, total: Number.NaN }));

    expect(invoice).toStrictEqual({ invoiceId: "inv-1", total: null });
  });

  test("serves a later attempt the saved result without running fn again", async () => {
    const store = createInMemoryStepStore();
    const firstAttempt = openAttempt(store);
    await firstAttempt.step.run("charge-card", async () => ({ chargeId: "ch-1" }));
    const chargeCard = vi.fn(async () => ({ chargeId: "ch-2" }));
    const secondAttempt = openAttempt(store);

    const charge = await secondAttempt.step.run("charge-card", chargeCard);

    expect(charge).toStrictEqual({ chargeId: "ch-1" });
    expect(chargeCard).not.toHaveBeenCalled();
  });

  test("saves an undefined result, so a later attempt gets undefined without running fn", async () => {
    const store = createInMemoryStepStore();
    const firstAttempt = openAttempt(store);
    await firstAttempt.step.run("send-email", async () => undefined);
    const sendEmail = vi.fn(async () => undefined);
    const secondAttempt = openAttempt(store);

    const sent = await secondAttempt.step.run("send-email", sendEmail);

    expect(sent).toBeUndefined();
    expect(sendEmail).not.toHaveBeenCalled();
  });

  test("ignores a saved step whose id no longer appears in the code", async () => {
    const store = createInMemoryStepStore();
    const firstAttempt = openAttempt(store);
    await firstAttempt.step.run("old-step", () => "old result");
    const { step, throwRecordedMisuse } = openAttempt(store);

    const result = await step.run("new-step", () => "new result");

    expect(result).toBe("new result");
    expect(throwRecordedMisuse).not.toThrow();
  });

  test("does not save a step whose fn throws, and lets its error through", async () => {
    const store = createInMemoryStepStore();
    const firstAttempt = openAttempt(store);
    const upstreamError = new Error("upstream unavailable");
    const secondAttempt = openAttempt(store);

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
    const { step } = openAttempt(store);
    let finishSlowStep: (value: string) => void = () => undefined;
    const slowStepResult = new Promise<string>((resolve) => {
      finishSlowStep = resolve;
    });

    const bothSteps = Promise.all([
      step.run("fast-step", async () => "fast"),
      step.run("slow-step", () => slowStepResult),
    ]);
    await vi.waitFor(async () => {
      expect(await store.savedSteps()).toHaveProperty("fast-step");
    });
    const savedBeforeTheSlowStepSettles = await store.savedSteps();
    finishSlowStep("slow");

    expect(savedBeforeTheSlowStepSettles).not.toHaveProperty("slow-step");
    expect(await bothSteps).toEqual(["fast", "slow"]);
    expect(await store.savedSteps()).toHaveProperty("slow-step");
  });
});

describe("step.run idempotency key", () => {
  test("hands fn the same idempotency key on every attempt, and another key to another step", async () => {
    const store = createInMemoryStepStore();
    const firstAttempt = openAttempt(store);
    const secondAttempt = openAttempt(store);
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
    const { step, throwRecordedMisuse } = openAttempt(createInMemoryStepStore());
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
    const { step, throwRecordedMisuse } = openAttempt(createInMemoryStepStore());
    await step.run("send-email", () => "sent");

    await step.run("send-email", () => "sent").catch(() => undefined);

    expect(throwRecordedMisuse).toThrow('Step id "send-email" is used twice');
  });

  test("rejects a step started inside another step's fn", async () => {
    const store = createInMemoryStepStore();
    const { step, throwRecordedMisuse } = openAttempt(store);
    const secondAttempt = openAttempt(store);

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
    const { step, throwRecordedMisuse } = openAttempt(store);
    const secondAttempt = openAttempt(store);

    await expect(step.run("compute", nonJsonResult as () => JsonValue)).rejects.toThrow(
      /^The result of step "compute" is not JSON/,
    );
    expect(throwRecordedMisuse).toThrow(StepMisuseError);
    expect(await secondAttempt.step.run("compute", () => "run again")).toBe("run again");
  });

  test("accepts a result of exactly 1 MiB once serialized", async () => {
    const { step } = openAttempt(createInMemoryStepStore());

    await expect(step.run("render", () => stringSerializedInBytes(ONE_MEBIBYTE))).resolves.toHaveLength(
      ONE_MEBIBYTE - 4,
    );
  });

  test("rejects a result over 1 MiB once serialized, and does not save it", async () => {
    const store = createInMemoryStepStore();
    const { step, throwRecordedMisuse } = openAttempt(store);
    const secondAttempt = openAttempt(store);

    await expect(step.run("render", () => stringSerializedInBytes(ONE_MEBIBYTE + 1))).rejects.toThrow(
      new StepMisuseError(
        'The result of step "render" is 1048577 bytes once serialized, over the 1 MiB limit for one step. Store large data elsewhere, such as your database, and return its id.',
      ),
    );
    expect(throwRecordedMisuse).toThrow(StepMisuseError);
    expect(await secondAttempt.step.run("render", () => "small")).toBe("small");
  });

  test("rejects a step that would bring the run's saved steps over 10 MiB, counting the earlier attempts' steps it reaches", async () => {
    const pageIds = Array.from({ length: 9 }, (_unused, index) => `page-${index}`);
    const savedResultsByStepId = Object.fromEntries(
      pageIds.map((pageId) => [pageId, `["${stringSerializedInBytes(ONE_MEBIBYTE)}"]`]),
    );
    const { step, throwRecordedMisuse } = openAttempt(createInMemoryStepStore(savedResultsByStepId));
    for (const pageId of pageIds) {
      await step.run(pageId, () => "not run again");
    }
    await step.run("page-9", () => stringSerializedInBytes(ONE_MEBIBYTE));

    await expect(step.run("summary", () => "s")).rejects.toThrow(
      new StepMisuseError(
        'Saving step "summary" would bring the saved steps of this run of task "send-invoice" to 10485765 bytes, over the 10 MiB limit for one run. Store large data elsewhere, such as your database, and return its id.',
      ),
    );
    expect(throwRecordedMisuse).toThrow(StepMisuseError);
  });

  test("rejects every later step with the recorded misuse, without running its fn", async () => {
    const { step } = openAttempt(createInMemoryStepStore());
    await step.run("send-email", () => "sent");
    const repeatError = await step.run("send-email", () => "sent").catch((error: unknown) => error);
    const nextStep = vi.fn(() => "next");

    await expect(step.run("next-step", nextStep)).rejects.toBe(repeatError);
    expect(nextStep).not.toHaveBeenCalled();
  });
});

describe("step.run types", () => {
  test("resolves with the type fn returns, sync or async", async () => {
    const { step } = openAttempt(createInMemoryStepStore());

    const charge = await step.run("charge-card", async () => ({ chargeId: "ch-1", amount: 10 }));
    const nothing = await step.run("log", () => undefined);
    const sent = await step.run("send-email", async () => {});

    expectTypeOf(charge).toEqualTypeOf<{ chargeId: string; amount: number }>();
    expectTypeOf(nothing).toEqualTypeOf<undefined>();
    expectTypeOf(sent).toEqualTypeOf<void>();
  });

  test("refuses a result that isn't JSON at compile time", async () => {
    const { step } = openAttempt(createInMemoryStepStore());

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

function resumesAtOf(outcome: HandlerOutcome): number | undefined {
  return outcome.kind === "suspended" ? outcome.resumesAt : undefined;
}

function savedSleep(resumesAt: number): string {
  return String(resumesAt);
}

describe("step.sleep and step.sleepUntil", () => {
  test("save the resume time, then suspend the run until it, even when the handler catches the suspension", async () => {
    const store = createInMemoryStepStore();
    const { step, executeHandler } = openAttempt(store);
    const resumesAt = Date.now() + ONE_HOUR_IN_MILLISECONDS;

    const outcome = await executeHandler(async () => {
      await step.sleepUntil("wait-for-trial-end", new Date(resumesAt)).catch(() => undefined);
    });

    expect(outcome).toEqual({ kind: "suspended", resumesAt });
    expect(await store.savedSteps()).toStrictEqual({ "wait-for-trial-end": savedSleep(resumesAt) });
  });

  test("save the resume time and let the handler go on when a future resume time continues at once", async () => {
    const store = createInMemoryStepStore();
    const { step, executeHandler } = openAttempt(store, createRecordingSends(), {
      futureResumeTime: "continue at once",
    });
    const resumesAt = Date.now() + ONE_HOUR_IN_MILLISECONDS;
    const stepsReached: string[] = [];

    const outcome = await executeHandler(async () => {
      await step.sleepUntil("wait-for-trial-end", new Date(resumesAt));
      await step.run("send-reminder", () => {
        stepsReached.push("send-reminder");
      });
    });

    expect(outcome).toEqual({ kind: "completed" });
    expect(stepsReached).toEqual(["send-reminder"]);
    expect(await store.savedSteps()).toStrictEqual({
      "wait-for-trial-end": savedSleep(resumesAt),
      "send-reminder": "[]",
    });
  });

  test("sleep saves a resume time its duration away from now", async () => {
    const { step, executeHandler } = openAttempt(createInMemoryStepStore());
    const threeDaysInMilliseconds = 3 * 24 * ONE_HOUR_IN_MILLISECONDS;
    const sleepReachedAt = Date.now();

    const outcome = await executeHandler(() => step.sleep("cool-down", "3d"));

    expect(resumesAtOf(outcome)).toBeGreaterThanOrEqual(sleepReachedAt + threeDaysInMilliseconds);
    expect(resumesAtOf(outcome)).toBeLessThanOrEqual(Date.now() + threeDaysInMilliseconds);
  });

  test("returns at once for a time in the past, and still saves it", async () => {
    const store = createInMemoryStepStore();
    const { step, executeHandler } = openAttempt(store);
    const pastTime = Date.now() - 1000;
    const afterTheSleep = vi.fn();

    const outcome = await executeHandler(async () => {
      await step.sleepUntil("wait", new Date(pastTime));
      afterTheSleep();
    });

    expect(outcome).toEqual({ kind: "completed" });
    expect(afterTheSleep).toHaveBeenCalledOnce();
    expect(await store.savedSteps()).toStrictEqual({ wait: savedSleep(pastTime) });
  });

  test("takes an ISO date string with Z or an offset", async () => {
    const { step, executeHandler } = openAttempt(createInMemoryStepStore());

    const outcome = await executeHandler(async () => {
      await Promise.all([
        step.sleepUntil("wait-utc", "2999-01-01T00:00:00.000Z"),
        step.sleepUntil("wait-offset", "2999-01-01T00:00:00+01:00"),
      ]);
    });

    expect(outcome).toEqual({ kind: "suspended", resumesAt: Date.parse("2998-12-31T23:00:00.000Z") });
  });

  test("returns at once on resume when the saved resume time has passed, whatever the duration says", async () => {
    const pastResumesAt = Date.now() - 1000;
    const store = createInMemoryStepStore({ "cool-down": savedSleep(pastResumesAt) });
    const { step, executeHandler } = openAttempt(store);

    const outcome = await executeHandler(() => step.sleep("cool-down", "3d"));

    expect(outcome).toEqual({ kind: "completed" });
    expect(await store.savedSteps()).toStrictEqual({ "cool-down": savedSleep(pastResumesAt) });
  });

  test("suspends again until the saved resume time when the run is picked up before it", async () => {
    const savedResumesAt = Date.now() + ONE_HOUR_IN_MILLISECONDS;
    const { step, executeHandler } = openAttempt(createInMemoryStepStore({ wait: savedSleep(savedResumesAt) }));

    const outcome = await executeHandler(() => step.sleepUntil("wait", new Date(0)));

    expect(outcome).toEqual({ kind: "suspended", resumesAt: savedResumesAt });
  });

  test("follows the store's clock on whether a sleep is due, not the app's", async () => {
    const pastResumesAt = Date.now() - ONE_HOUR_IN_MILLISECONDS;
    const futureResumesAt = Date.now() + ONE_HOUR_IN_MILLISECONDS;
    const storeAnswering = (resumeTime: ResumeTime): StepStore => ({
      ...createInMemoryStepStore(),
      saveSleep: async () => resumeTime,
    });
    const notYetDue = openAttempt(storeAnswering({ resumesAt: pastResumesAt, isDue: false }));
    const alreadyDue = openAttempt(storeAnswering({ resumesAt: futureResumesAt, isDue: true }));

    const notYetDueOutcome = await notYetDue.executeHandler(() => notYetDue.step.sleep("wait", "1h"));
    const alreadyDueOutcome = await alreadyDue.executeHandler(() => alreadyDue.step.sleep("wait", "1h"));

    expect(notYetDueOutcome).toEqual({ kind: "suspended", resumesAt: pastResumesAt });
    expect(alreadyDueOutcome).toEqual({ kind: "completed" });
  });

  test("suspends once the steps in flight settle, so their results are saved", async () => {
    const store = createInMemoryStepStore();
    const { step, executeHandler } = openAttempt(store);
    let finishSlowStep: (value: string) => void = () => undefined;
    const slowStepResult = new Promise<string>((resolve) => {
      finishSlowStep = resolve;
    });
    let isSuspended = false;

    const suspension = executeHandler(async () => {
      await Promise.all([step.run("slow-step", () => slowStepResult), step.sleep("wait", "1h")]);
    }).then((outcome) => {
      isSuspended = true;
      return outcome;
    });
    await vi.waitFor(async () => {
      expect(await store.savedSteps()).toHaveProperty("wait");
    });
    const isSuspendedBeforeTheSlowStepSettles = isSuspended;
    finishSlowStep("slow");

    expect(isSuspendedBeforeTheSlowStepSettles).toBe(false);
    expect(resumesAtOf(await suspension)).toBeGreaterThan(Date.now());
    expect(await store.savedSteps()).toHaveProperty("slow-step", '["slow"]');
  });

  test("suspends until the earliest of two parallel sleeps", async () => {
    const { step, executeHandler } = openAttempt(createInMemoryStepStore());
    const earliestResumesAt = Date.now() + ONE_HOUR_IN_MILLISECONDS;

    const outcome = await executeHandler(async () => {
      await Promise.all([
        step.sleepUntil("wait-long", new Date(earliestResumesAt + ONE_HOUR_IN_MILLISECONDS)),
        step.sleepUntil("wait-short", new Date(earliestResumesAt)),
      ]);
    });

    expect(outcome).toEqual({ kind: "suspended", resumesAt: earliestResumesAt });
  });

  test("suspends even when the handler goes on to throw another error", async () => {
    const { step, executeHandler } = openAttempt(createInMemoryStepStore());

    const outcome = await executeHandler(async () => {
      await step.sleep("wait", "1h").catch(() => undefined);
      throw new Error("handler error after the sleep");
    });

    expect(resumesAtOf(outcome)).toBeGreaterThan(Date.now());
  });

  test("lets the handler's own error through when no sleep suspends the run", async () => {
    const { executeHandler } = openAttempt(createInMemoryStepStore());
    const handlerError = new Error("upstream unavailable");

    await expect(
      executeHandler(async () => {
        throw handlerError;
      }),
    ).rejects.toBe(handlerError);
  });

  test("rejects every later step of the stretch without running it", async () => {
    const { step, executeHandler } = openAttempt(createInMemoryStepStore());
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
    const { step, executeHandler } = openAttempt(createInMemoryStepStore());

    await expect(
      executeHandler(async () => {
        await step.run("send-email", () => "sent");
        await step.run("send-email", () => "sent").catch(() => undefined);
        await step.sleep("wait", "1h");
      }),
    ).rejects.toThrow(StepMisuseError);
  });

  test("shares the step id namespace, so a sleep reusing a step's id is a misuse", async () => {
    const { step, executeHandler } = openAttempt(createInMemoryStepStore());

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
    const { step, executeHandler } = openAttempt(createInMemoryStepStore());

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
  ])("rejects %s as a resume time", async (_timeKind, invalidTime) => {
    const store = createInMemoryStepStore();
    const { step, executeHandler } = openAttempt(store);

    await expect(executeHandler(() => step.sleepUntil("wait", invalidTime()))).rejects.toThrow(
      /^The resume time of step "wait" is not a Date or an ISO date string with Z or an offset/,
    );
    expect(await store.savedSteps()).toStrictEqual({});
  });

  test("rejects an invalid duration that slipped past the types", async () => {
    const { step, executeHandler } = openAttempt(createInMemoryStepStore());

    await expect(executeHandler(() => step.sleep("wait", "0.5h" as Duration))).rejects.toThrow(
      new StepMisuseError(
        'The duration of step "wait" is invalid: "0.5h" is not a duration: a whole number followed by s, m, h or d, such as "30s"',
      ),
    );
  });
});

describe("an attempt whose sleeps were woken", () => {
  test("returns from every sleep at once, the saved one and those still to come, and saves the new ones", async () => {
    const savedResumesAt = Date.now() + ONE_HOUR_IN_MILLISECONDS;
    const store = createInMemoryStepStore({ "wait-for-trial-end": savedSleep(savedResumesAt) });
    const { step, executeHandler } = openAttempt(store, createRecordingSends(), { sleepsWokenAt: Date.now() });
    const sendFollowUp = vi.fn(() => "sent");

    const outcome = await executeHandler(async () => {
      await step.sleepUntil("wait-for-trial-end", new Date(savedResumesAt));
      await step.sleep("cool-down", "3d");
      await step.run("send-follow-up", sendFollowUp);
    });

    expect(outcome).toEqual({ kind: "completed" });
    expect(sendFollowUp).toHaveBeenCalledOnce();
    expect(Object.keys(store.savedSteps())).toEqual(["wait-for-trial-end", "cool-down", "send-follow-up"]);
  });
});

describe("an attempt that lost its lock", () => {
  const lockLosingStore = (): StepStore => ({
    readStep: async (stepId) => {
      throw new LostRunLockError(`Step "${stepId}" of run "run-7"`);
    },
    saveStep: async (stepId) => {
      throw new LostRunLockError(`Step "${stepId}" of run "run-7"`);
    },
    saveSleep: async (stepId) => {
      throw new LostRunLockError(`Step "${stepId}" of run "run-7"`);
    },
  });

  test("throws LostRunLockError from a step, a sleep and a send, and marks the lock lost each time", async () => {
    const markLockLost = vi.fn();
    const recordingSends = createRecordingSends();
    const { step, dispatch } = openAttempt(lockLosingStore(), recordingSends, { markLockLost });
    const loadInvoice = vi.fn(() => "inv-1");
    const thrownBy = (stepCall: Promise<unknown>) =>
      stepCall.then(
        () => undefined,
        (error: unknown) => error,
      );

    const thrownValues = await Promise.all([
      thrownBy(step.run("load-invoice", loadInvoice)),
      thrownBy(step.sleepUntil("wait", new Date(0))),
      thrownBy(dispatch("send-receipt", {})),
    ]);

    expect(thrownValues.map((thrownValue) => thrownValue instanceof LostRunLockError)).toEqual([true, true, true]);
    expect(markLockLost).toHaveBeenCalledTimes(3);
    expect(loadInvoice).not.toHaveBeenCalled();
    expect(recordingSends.sends).toStrictEqual([]);
  });

  test("does not mark the lock lost on any other error", async () => {
    const markLockLost = vi.fn();
    const store = createInMemoryStepStore();
    const { step } = openAttempt({ ...store, saveStep: () => Promise.reject(new Error("boom")) }, undefined, {
      markLockLost,
    });

    await expect(step.run("charge-card", () => "charged")).rejects.toThrow("boom");
    expect(markLockLost).not.toHaveBeenCalled();
  });
});

function topLevelSendId(positionInScope: number): string {
  return deriveHandlerSendId("send-invoice", "run-7", undefined, positionInScope);
}

describe("handler dispatch and emit", () => {
  test("hand the sender the parent's fence: the run and its task, its lock token and the derived send id", async () => {
    const fences: SendFence[] = [];
    const sender: HandlerSender = {
      dispatch: async (fence, _name, _payload, options) => {
        fences.push(fence);
        return { runId: options.runId ?? "" };
      },
      emit: async (fence, _name, _payload, options) => {
        fences.push(fence);
        return { eventId: options.eventId ?? "" };
      },
    };
    const { step, dispatch, emit } = openAttempt(createInMemoryStepStore(), undefined, { sender });

    await dispatch("send-receipt", {});
    await step.run("notify", async () => {
      await emit("invoice.sent", {});
    });

    expect(fences).toStrictEqual([
      { parentTask: "send-invoice", parentRunId: "run-7", lockToken: LOCK_TOKEN, stepId: topLevelSendId(0) },
      {
        parentTask: "send-invoice",
        parentRunId: "run-7",
        lockToken: LOCK_TOKEN,
        stepId: deriveHandlerSendId("send-invoice", "run-7", "notify", 0),
      },
    ]);
  });

  test("send under ids derived from the run and their position, and save a marker for each", async () => {
    const store = createInMemoryStepStore();
    const recordingSends = createRecordingSends();
    const { dispatch, emit } = openAttempt(store, recordingSends);

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
    expect(await store.savedSteps()).toStrictEqual({
      [topLevelSendId(0)]: JSON.stringify([topLevelSendId(0)]),
      [topLevelSendId(1)]: JSON.stringify([topLevelSendId(1)]),
    });
  });

  test("skip a send a previous attempt made, and resolve with what it sent", async () => {
    const store = createInMemoryStepStore();
    const firstAttempt = openAttempt(store);
    const firstDispatchedRun = await firstAttempt.dispatch("send-receipt", { invoiceId: "inv-1" });
    const firstEmittedEvent = await firstAttempt.emit("invoice.sent", { invoiceId: "inv-1" });
    const recordingSends = createRecordingSends();
    const secondAttempt = openAttempt(store, recordingSends);

    const secondDispatchedRun = await secondAttempt.dispatch("send-receipt", { invoiceId: "inv-1" });
    const secondEmittedEvent = await secondAttempt.emit("invoice.sent", { invoiceId: "inv-1" });

    expect(secondDispatchedRun).toStrictEqual(firstDispatchedRun);
    expect(secondEmittedEvent).toStrictEqual(firstEmittedEvent);
    expect(recordingSends.sends).toStrictEqual([]);
  });

  test("let an explicit runId or eventId win, keyed by position all the same", async () => {
    const store = createInMemoryStepStore();
    const recordingSends = createRecordingSends();
    const firstAttempt = openAttempt(store, recordingSends);
    await firstAttempt.dispatch("ship-order", { orderId: "ord-1" }, { runId: "ship-order-ord-1", delay: "5m" });
    await firstAttempt.emit("order.shipped", { orderId: "ord-1" }, { eventId: "order-shipped-ord-1" });
    const secondAttempt = openAttempt(store, recordingSends);

    const repeatedDispatch = await secondAttempt.dispatch("ship-order", { orderId: "ord-1" }, { runId: "other" });
    const repeatedEmit = await secondAttempt.emit("order.shipped", { orderId: "ord-1" });

    expect(recordingSends.sends.map((send) => send.options)).toStrictEqual([
      { runId: "ship-order-ord-1", delay: "5m" },
      { eventId: "order-shipped-ord-1" },
    ]);
    expect(repeatedDispatch).toStrictEqual({ runId: "ship-order-ord-1" });
    expect(repeatedEmit).toStrictEqual({ eventId: "order-shipped-ord-1" });
    expect(await store.savedSteps()).toStrictEqual({
      [topLevelSendId(0)]: JSON.stringify(["ship-order-ord-1"]),
      [topLevelSendId(1)]: JSON.stringify(["order-shipped-ord-1"]),
    });
  });

  test("number the sends inside a step's fn apart from the top level", async () => {
    const recordingSends = createRecordingSends();
    const { step, emit } = openAttempt(createInMemoryStepStore(), recordingSends);

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
    const firstAttempt = openAttempt(store, failingSends);
    await firstAttempt.emit("invoice.sent", {}).catch(() => undefined);
    const recordingSends = createRecordingSends();
    const secondAttempt = openAttempt(store, recordingSends);

    await secondAttempt.emit("invoice.sent", {});

    expect(recordingSends.sends.map((send) => send.options.eventId)).toStrictEqual([topLevelSendId(0)]);
  });

  test("throw without sending once a sleep suspended the stretch", async () => {
    const recordingSends = createRecordingSends();
    const { step, emit, executeHandler } = openAttempt(createInMemoryStepStore(), recordingSends);
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
    const { step, emit, executeHandler } = openAttempt(store, slowSends);

    const suspension = executeHandler(async () => {
      await Promise.all([emit("invoice.sent", {}), step.sleep("wait", "1h")]);
    });
    await vi.waitFor(async () => {
      expect(await store.savedSteps()).toHaveProperty("wait");
    });
    finishEmit();

    expect(resumesAtOf(await suspension)).toBeGreaterThan(Date.now());
    expect(await store.savedSteps()).toHaveProperty([topLevelSendId(0)], JSON.stringify(["evt-1"]));
  });
});

describe("an aborted attempt", () => {
  test("throws the abort reason from every step, emit and dispatch, without running or saving any", async () => {
    const store = createInMemoryStepStore();
    const recordingSends = createRecordingSends();
    const abortController = new AbortController();
    const abortReason = new Error("the attempt lost its lock");
    const { step, emit, dispatch } = openAttempt(store, recordingSends, {
      abortSignal: abortController.signal,
    });
    const chargeCard = vi.fn(() => "charged");
    abortController.abort(abortReason);

    const thrownValues = await Promise.all([
      step.run("charge-card", chargeCard).catch((error: unknown) => error),
      step.sleep("cool-down", "1h").catch((error: unknown) => error),
      step.sleepUntil("follow-up", new Date(0)).catch((error: unknown) => error),
      emit("invoice.sent", {}).catch((error: unknown) => error),
      dispatch("send-receipt", {}).catch((error: unknown) => error),
    ]);

    expect(thrownValues).toStrictEqual([abortReason, abortReason, abortReason, abortReason, abortReason]);
    expect(chargeCard).not.toHaveBeenCalled();
    expect(recordingSends.sends).toStrictEqual([]);
    expect(await store.savedSteps()).toStrictEqual({});
  });

  test("still serves no saved result once aborted, so a replayed step throws too", async () => {
    const abortController = new AbortController();
    const abortReason = new Error("the worker reached its shutdown deadline");
    const { step } = openAttempt(createInMemoryStepStore({ "charge-card": '["charged"]' }), undefined, {
      abortSignal: abortController.signal,
    });
    abortController.abort(abortReason);

    await expect(step.run("charge-card", () => "charged again")).rejects.toBe(abortReason);
  });
});

describe("an attempt in a draining worker", () => {
  test("yields at its next step once the steps in flight have settled and saved", async () => {
    const store = createInMemoryStepStore();
    let isWorkerDraining = false;
    const { step, executeHandler } = openAttempt(store, undefined, {
      isWorkerDraining: () => isWorkerDraining,
    });
    let finishSlowStep: (value: string) => void = () => undefined;
    const slowStepResult = new Promise<string>((resolve) => {
      finishSlowStep = resolve;
    });
    const sendInvoice = vi.fn(() => "sent");
    let yieldError: unknown;
    let isOutcomeSettled = false;

    const outcome = executeHandler(async () => {
      const slowStep = step.run("render-pdf", () => slowStepResult);
      isWorkerDraining = true;
      yieldError = await step.run("send-invoice", sendInvoice).catch((error: unknown) => error);
      await slowStep;
      await step.run("archive-invoice", () => "archived");
    }).finally(() => {
      isOutcomeSettled = true;
    });
    await vi.waitFor(() => {
      expect(yieldError).toBeInstanceOf(Error);
    });
    const isOutcomeSettledBeforeTheSlowStep = isOutcomeSettled;
    finishSlowStep("rendered");

    expect(await outcome).toEqual({ kind: "yielded" });
    expect(isOutcomeSettledBeforeTheSlowStep).toBe(false);
    expect(yieldError).toMatchObject({ name: "RunYield" });
    expect(sendInvoice).not.toHaveBeenCalled();
    expect(await store.savedSteps()).toStrictEqual({ "render-pdf": '["rendered"]' });
  });

  test("yields at an emit or a dispatch without sending it, even when the handler catches the yield", async () => {
    const recordingSends = createRecordingSends();
    const { emit, dispatch, executeHandler } = openAttempt(createInMemoryStepStore(), recordingSends, {
      isWorkerDraining: () => true,
    });

    const outcome = await executeHandler(async () => {
      await emit("invoice.sent", {}).catch(() => undefined);
      await dispatch("send-receipt", {});
    });

    expect(outcome).toEqual({ kind: "yielded" });
    expect(recordingSends.sends).toStrictEqual([]);
  });

  test("completes when the handler reaches no step boundary", async () => {
    const { executeHandler } = openAttempt(createInMemoryStepStore(), undefined, {
      isWorkerDraining: () => true,
    });

    expect(await executeHandler(async () => undefined)).toEqual({ kind: "completed" });
  });

  test("suspends rather than yields when a sleep came first", async () => {
    let isWorkerDraining = false;
    const { step, executeHandler } = openAttempt(createInMemoryStepStore(), undefined, {
      isWorkerDraining: () => isWorkerDraining,
    });

    const outcome = await executeHandler(async () => {
      await step.sleep("cool-down", "1h").catch(() => undefined);
      isWorkerDraining = true;
      await step.run("send-invoice", () => "sent");
    });

    expect(outcome).toMatchObject({ kind: "suspended" });
  });
});
