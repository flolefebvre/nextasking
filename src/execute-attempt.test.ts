import { describe, expect, expectTypeOf, test } from "vitest";
import { z } from "zod";

import { createRecordingLogger } from "../tests/recording-logger.js";
import {
  InvalidPayloadError,
  NonRetriableError,
  StepMisuseError,
  UnknownEventError,
  UnknownTaskError,
  createNextasking,
  defineEvent,
  defineListener,
  defineScheduledTask,
  defineTask,
} from "./index.js";
import type { EventId, Nextasking, RunId, SavedStepResult, TaskContext, TaskName } from "./index.js";
import { deriveStepIdempotencyKey } from "./ids.js";
import { executeAttempt, fakeNextasking } from "./testing.js";
import type { ExecutedAttempt, ExecuteAttemptOptions, RecordedSleep } from "./testing.js";

const ONE_DAY_IN_MILLISECONDS = 24 * 60 * 60 * 1000;

type SignUpHandler = (context: TaskContext<{ email: string; plan: string }>) => Promise<void>;

const userSignedUp = defineEvent("user.signed-up", {
  schema: z.object({ userId: z.string() }),
});

const sendWelcomeEmail = defineTask("send-welcome-email", {
  schema: z.object({ email: z.email() }),
  handler: async () => undefined,
});

const createDefaultWorkspace = defineListener("create-default-workspace", {
  on: userSignedUp,
  handler: async () => undefined,
});

function createSignUpNextasking(signUpHandler: SignUpHandler, scheduledAtsSeen: (Date | undefined)[] = []) {
  const signUpUser = defineTask("sign-up-user", {
    schema: z.object({ email: z.email(), plan: z.string().default("free") }),
    handler: signUpHandler,
  });
  const purgeExpiredSessions = defineScheduledTask("purge-expired-sessions", {
    schedule: { cron: "0 3 * * *" },
    handler: async ({ scheduledAt, step }) => {
      scheduledAtsSeen.push(scheduledAt);
      await step.run("purge", () => ({ purgedCount: 2 }));
    },
  });
  return createNextasking({
    connection: { url: "redis://nextasking.invalid:6379", giveUpAfter: "1s" },
    events: [userSignedUp],
    tasks: [signUpUser, sendWelcomeEmail, createDefaultWorkspace, purgeExpiredSessions],
  });
}

const signUpThenRemind: SignUpHandler = async ({ payload, step, dispatch, emit }) => {
  const { userId } = await step.run("create-user", () => ({ userId: `user-of-${payload.email}` }));
  await emit("user.signed-up", { userId });
  await step.sleep("wait-a-day", "1d");
  await dispatch("send-welcome-email", { email: payload.email });
  await step.sleepUntil("wait-for-trial-end", "2030-01-01T00:00:00Z");
  await step.run("mark-reminded", () => undefined);
};

describe("executeAttempt", () => {
  test("runs the whole handler in one call, recording its steps, sleeps, dispatches and emits without waiting", async () => {
    const nextasking = createSignUpNextasking(signUpThenRemind);
    const calledAt = Date.now();

    const executedAttempt = await executeAttempt(nextasking, "sign-up-user", { email: "ada@example.com" });

    expect(executedAttempt.steps).toEqual([
      { id: "create-user", result: { userId: "user-of-ada@example.com" } },
      { id: expect.any(String), result: executedAttempt.emits[0]?.eventId },
      { id: expect.any(String), result: executedAttempt.dispatches[0]?.runId },
      { id: "mark-reminded", result: undefined },
    ]);
    expect(executedAttempt.sleeps).toEqual([
      { id: "wait-a-day", resumesAt: expect.any(Date) },
      { id: "wait-for-trial-end", resumesAt: new Date("2030-01-01T00:00:00Z") },
    ]);
    expect(executedAttempt.sleeps[0]?.resumesAt.getTime()).toBeGreaterThanOrEqual(calledAt + ONE_DAY_IN_MILLISECONDS);
    expect(executedAttempt.sleeps[0]?.resumesAt.getTime()).toBeLessThanOrEqual(Date.now() + ONE_DAY_IN_MILLISECONDS);
    expect(executedAttempt.dispatches).toEqual([
      { task: "send-welcome-email", payload: { email: "ada@example.com" }, runId: expect.any(String) },
    ]);
    expect(executedAttempt.emits).toEqual([
      { event: "user.signed-up", payload: { userId: "user-of-ada@example.com" }, eventId: expect.any(String) },
    ]);
  });

  test("hands the handler the payload the schema returns, and rejects a payload the schema refuses before running it", async () => {
    const payloadsSeen: unknown[] = [];
    const nextasking = createSignUpNextasking(async ({ payload }) => {
      payloadsSeen.push(payload);
    });

    await executeAttempt(nextasking, "sign-up-user", { email: "ada@example.com" });
    const refusedAttempt = executeAttempt(nextasking, "sign-up-user", { email: "not an email" });

    await expect(refusedAttempt).rejects.toThrow(InvalidPayloadError);
    expect(payloadsSeen).toEqual([{ email: "ada@example.com", plan: "free" }]);
  });

  test("hands a scheduled task the scheduledAt option, or the time of the call", async () => {
    const scheduledAtsSeen: (Date | undefined)[] = [];
    const nextasking = createSignUpNextasking(async () => undefined, scheduledAtsSeen);
    const plannedInstant = new Date("2030-01-01T03:00:00Z");
    const calledAt = Date.now();

    const executedAttempt = await executeAttempt(nextasking, "purge-expired-sessions", undefined, {
      scheduledAt: plannedInstant,
    });
    await executeAttempt(nextasking, "purge-expired-sessions");

    expect(executedAttempt.steps).toEqual([{ id: "purge", result: { purgedCount: 2 } }]);
    expect(scheduledAtsSeen[0]).toEqual(plannedInstant);
    expect(scheduledAtsSeen[1]?.getTime()).toBeGreaterThanOrEqual(calledAt);
    expect(scheduledAtsSeen[1]?.getTime()).toBeLessThanOrEqual(Date.now());
  });

  test("simulates a retry: saved steps are reused without running their fn, and the attempt is the one passed", async () => {
    let failsOnFirstAttempt = true;
    const fnCallsByStepId = new Map<string, number>();
    const attemptsSeen: number[] = [];
    const countCall = (stepId: string) => {
      fnCallsByStepId.set(stepId, (fnCallsByStepId.get(stepId) ?? 0) + 1);
    };
    const nextasking = createSignUpNextasking(async ({ step, attempt }) => {
      attemptsSeen.push(attempt);
      await step.run("charge-card", () => {
        countCall("charge-card");
        return { chargeId: "charge-1" };
      });
      if (failsOnFirstAttempt) {
        failsOnFirstAttempt = false;
        throw new Error("mail server down");
      }
      await step.run("send-receipt", () => {
        countCall("send-receipt");
        return "sent";
      });
    });
    const failedAttempt = await executeAttempt(nextasking, "sign-up-user", { email: "ada@example.com" }).catch(
      (error: unknown) => error,
    );
    const firstSteps: SavedStepResult[] = [{ id: "charge-card", result: { chargeId: "charge-1" } }];

    const retriedAttempt = await executeAttempt(
      nextasking,
      "sign-up-user",
      { email: "ada@example.com" },
      { savedSteps: firstSteps, attempt: 2 },
    );

    expect(failedAttempt).toEqual(new Error("mail server down"));
    expect(attemptsSeen).toEqual([1, 2]);
    expect(Object.fromEntries(fnCallsByStepId)).toEqual({ "charge-card": 1, "send-receipt": 1 });
    expect(retriedAttempt.steps).toEqual([
      { id: "charge-card", result: { chargeId: "charge-1" } },
      { id: "send-receipt", result: "sent" },
    ]);
  });

  test("chains the steps of one attempt into the next as savedSteps", async () => {
    let fnCallCount = 0;
    const nextasking = createSignUpNextasking(async ({ step }) => {
      await step.run("create-user", () => {
        fnCallCount += 1;
        return { userId: "user-1" };
      });
    });
    const firstAttempt = await executeAttempt(nextasking, "sign-up-user", { email: "ada@example.com" });

    const secondAttempt = await executeAttempt(
      nextasking,
      "sign-up-user",
      { email: "ada@example.com" },
      { savedSteps: firstAttempt.steps, attempt: 2 },
    );

    expect(fnCallCount).toBe(1);
    expect(secondAttempt.steps).toEqual(firstAttempt.steps);
  });

  test("rejects with the handler's own error, unwrapped", async () => {
    const accountClosed = new NonRetriableError("account closed");
    const nextasking = createSignUpNextasking(async () => {
      throw accountClosed;
    });

    const attempt = executeAttempt(nextasking, "sign-up-user", { email: "ada@example.com" });

    await expect(attempt).rejects.toThrow(NonRetriableError);
    await expect(attempt).rejects.toBe(accountClosed);
  });

  test("rejects a step misuse as production does, even when the handler catches it", async () => {
    const nextasking = createSignUpNextasking(async ({ step }) => {
      await step.run("create-user", () => "user-1");
      await step.run("create-user", () => "user-2").catch(() => undefined);
    });

    const attempt = executeAttempt(nextasking, "sign-up-user", { email: "ada@example.com" });

    await expect(attempt).rejects.toThrow(StepMisuseError);
    await expect(attempt).rejects.toThrow('Step id "create-user" is used twice in one run of task "sign-up-user"');
  });

  test("rejects a step result over the 1 MiB ceiling, as production does", async () => {
    const nextasking = createSignUpNextasking(async ({ step }) => {
      await step.run("export-users", () => "x".repeat(1024 * 1024));
    });

    const attempt = executeAttempt(nextasking, "sign-up-user", { email: "ada@example.com" });

    await expect(attempt).rejects.toThrow(StepMisuseError);
  });

  test("hands each step the idempotency key production derives from the task, the run id and the step id", async () => {
    const idempotencyKeysSeen: string[] = [];
    const runIdsSeen: RunId[] = [];
    const nextasking = createSignUpNextasking(async ({ step, runId }) => {
      runIdsSeen.push(runId);
      await step.run("charge-card", ({ idempotencyKey }) => {
        idempotencyKeysSeen.push(idempotencyKey);
      });
    });

    await executeAttempt(nextasking, "sign-up-user", { email: "ada@example.com" }, { runId: "sign-up-ada" });
    await executeAttempt(nextasking, "sign-up-user", { email: "ada@example.com" });

    expect(runIdsSeen[0]).toBe("sign-up-ada");
    expect(runIdsSeen[1]).toEqual(expect.any(String));
    expect(runIdsSeen[1]).not.toBe("sign-up-ada");
    expect(idempotencyKeysSeen).toEqual([
      deriveStepIdempotencyKey("sign-up-user", "sign-up-ada", "charge-card"),
      deriveStepIdempotencyKey("sign-up-user", runIdsSeen[1] ?? "", "charge-card"),
    ]);
  });

  test("records a handler's sends under the ids production derives, so two attempts of one run send under the same ids", async () => {
    const nextasking = createSignUpNextasking(signUpThenRemind);
    const options: ExecuteAttemptOptions = { runId: "sign-up-ada" };

    const firstAttempt = await executeAttempt(nextasking, "sign-up-user", { email: "ada@example.com" }, options);
    const secondAttempt = await executeAttempt(nextasking, "sign-up-user", { email: "ada@example.com" }, options);

    expect(secondAttempt.dispatches).toEqual(firstAttempt.dispatches);
    expect(secondAttempt.emits).toEqual(firstAttempt.emits);
  });

  test("lists a handler's sends among its steps, so passing them back as savedSteps skips the sends with the same ids", async () => {
    const sentIds: string[] = [];
    const nextasking = createSignUpNextasking(async ({ dispatch, emit }) => {
      sentIds.push((await emit("user.signed-up", { userId: "user-1" })).eventId);
      sentIds.push((await dispatch("send-welcome-email", { email: "ada@example.com" })).runId);
    });

    const firstAttempt = await executeAttempt(
      nextasking,
      "sign-up-user",
      { email: "ada@example.com" },
      { runId: "sign-up-ada" },
    );
    const resumedAttempt = await executeAttempt(
      nextasking,
      "sign-up-user",
      { email: "ada@example.com" },
      { runId: "sign-up-ada", savedSteps: firstAttempt.steps },
    );

    expect(firstAttempt.steps.map(({ result }) => result)).toEqual(sentIds.slice(0, 2));
    expect(resumedAttempt.emits).toEqual([]);
    expect(resumedAttempt.dispatches).toEqual([]);
    expect(sentIds.slice(2)).toEqual(sentIds.slice(0, 2));
    expect(resumedAttempt.steps).toEqual(firstAttempt.steps);
  });

  test("throws inside the handler on an unknown name or a refused payload, as production does", async () => {
    const errorsSeen: unknown[] = [];
    const nextasking = createSignUpNextasking(async ({ dispatch, emit }) => {
      const dispatchByName = dispatch as (name: string, payload?: unknown) => Promise<unknown>;
      const emitByName = emit as (name: string, payload: unknown) => Promise<unknown>;
      await dispatchByName("send-welcome-sms", {}).catch((error: unknown) => errorsSeen.push(error));
      await emitByName("user.deleted", {}).catch((error: unknown) => errorsSeen.push(error));
      await dispatchByName("send-welcome-email", { email: "not an email" }).catch((error: unknown) =>
        errorsSeen.push(error),
      );
    });

    const executedAttempt = await executeAttempt(nextasking, "sign-up-user", { email: "ada@example.com" });

    expect(errorsSeen).toEqual([
      expect.any(UnknownTaskError),
      expect.any(UnknownEventError),
      expect.any(InvalidPayloadError),
    ]);
    expect(executedAttempt.dispatches).toEqual([]);
    expect(executedAttempt.emits).toEqual([]);
  });

  test("never reaches the instance's own dispatch and emit, so an active fake records nothing", async () => {
    const nextasking = createSignUpNextasking(signUpThenRemind);
    using fake = fakeNextasking(nextasking);

    const executedAttempt = await executeAttempt(nextasking, "sign-up-user", { email: "ada@example.com" });

    expect(executedAttempt.dispatches).toHaveLength(1);
    expect(executedAttempt.emits).toHaveLength(1);
    expect(fake.dispatches).toEqual([]);
    expect(fake.emits).toEqual([]);
  });

  test("simulates a shutdown: an aborted signal makes the next step throw, and the attempt rejects with its reason", async () => {
    const stepsRun: string[] = [];
    const abortController = new AbortController();
    const shutdown = new Error("worker shutting down");
    const nextasking = createSignUpNextasking(async ({ step }) => {
      await step.run("create-user", () => {
        stepsRun.push("create-user");
        abortController.abort(shutdown);
      });
      await step.run("send-welcome-email", () => {
        stepsRun.push("send-welcome-email");
      });
    });

    const attempt = executeAttempt(
      nextasking,
      "sign-up-user",
      { email: "ada@example.com" },
      { abortSignal: abortController.signal },
    );

    await expect(attempt).rejects.toBe(shutdown);
    expect(stepsRun).toEqual(["create-user"]);
  });

  test("hands the handler the logger passed, with the task, run id and attempt bound as production does", async () => {
    const { logger, lines } = createRecordingLogger();
    const nextasking = createSignUpNextasking(async ({ logger: runLogger }) => {
      runLogger.info({ email: "ada@example.com" }, "welcome email queued");
    });

    await executeAttempt(
      nextasking,
      "sign-up-user",
      { email: "ada@example.com" },
      { logger, runId: "sign-up-ada", attempt: 3 },
    );

    expect(lines).toEqual([
      {
        level: "info",
        fields: { email: "ada@example.com", task: "sign-up-user", runId: "sign-up-ada", attempt: 3 },
        message: "welcome email queued",
      },
    ]);
  });

  test("throws on a task name the instance doesn't list, and on an object that isn't an instance", async () => {
    const nextasking = createSignUpNextasking(async () => undefined);
    const executeAttemptByName = executeAttempt as (nextasking: object, taskName: string) => Promise<unknown>;

    await expect(executeAttemptByName(nextasking, "send-welcome-sms")).rejects.toThrow(UnknownTaskError);
    await expect(executeAttemptByName({}, "sign-up-user")).rejects.toThrow(
      "executeAttempt takes the instance createNextasking returns",
    );
  });

  test("takes its types from the instance passed, not from Register", () => {
    const nextasking = createSignUpNextasking(async () => undefined);
    type SignUpTaskName = "sign-up-user" | "send-welcome-email" | "create-default-workspace" | "purge-expired-sessions";
    const executeTheAttempt = () => executeAttempt(nextasking, "sign-up-user", { email: "ada@example.com" });
    type SignUpAttempt = Awaited<ReturnType<typeof executeTheAttempt>>;

    expectTypeOf<Parameters<typeof executeAttempt<SignUpTask, SignUpEvent, "sign-up-user">>[2]>().toEqualTypeOf<{
      email: string;
      plan?: string | undefined;
    }>();
    expectTypeOf<SignUpAttempt["dispatches"][number]["task"]>().toEqualTypeOf<SignUpTaskName>();
    expectTypeOf<SignUpAttempt["dispatches"][number]["task"]>().not.toEqualTypeOf<TaskName>();
    expectTypeOf<SignUpAttempt["emits"][number]>().toEqualTypeOf<{
      event: "user.signed-up";
      payload: { userId: string };
      eventId: EventId;
    }>();
    expectTypeOf<SignUpAttempt["steps"]>().toEqualTypeOf<readonly SavedStepResult[]>();
    expectTypeOf<SignUpAttempt["sleeps"]>().toEqualTypeOf<readonly RecordedSleep[]>();
    expectTypeOf<RecordedSleep>().toEqualTypeOf<{ id: string; resumesAt: Date }>();
    expectTypeOf<SignUpAttempt>().toEqualTypeOf<ExecutedAttempt<SignUpTask, SignUpEvent>>();
    const checkCalls = () => {
      void executeTheAttempt();
      void executeAttempt(nextasking, "purge-expired-sessions");
      void executeAttempt(nextasking, "purge-expired-sessions", undefined, { scheduledAt: new Date() });
      void executeAttempt(nextasking, "create-default-workspace", { userId: "user-1" }, { attempt: 2 });
      // @ts-expect-error a task name the instance doesn't list
      void executeAttempt(nextasking, "send-welcome-sms", {});
      // @ts-expect-error a payload that doesn't match the task's schema input
      void executeAttempt(nextasking, "sign-up-user", { email: 42 });
      // @ts-expect-error a task with a schema needs its payload
      void executeAttempt(nextasking, "sign-up-user");
      // @ts-expect-error a scheduled task takes no payload
      void executeAttempt(nextasking, "purge-expired-sessions", { force: true });
    };
    expect(checkCalls).toBeTypeOf("function");
  });
});

type SignUpNextasking = ReturnType<typeof createSignUpNextasking>;

type SignUpDefinitions =
  SignUpNextasking extends Nextasking<infer Task, infer Event> ? { task: Task; event: Event } : never;

type SignUpTask = SignUpDefinitions["task"];

type SignUpEvent = SignUpDefinitions["event"];
