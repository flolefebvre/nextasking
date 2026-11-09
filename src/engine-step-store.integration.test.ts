import { expect, vi } from "vitest";

import { engineTest, fetchedLockedRunsOf, storedValuesByKeyOf } from "../tests/engine-test.js";
import type { EngineTestContext } from "../tests/engine-test.js";
import { freshLockTokensOf } from "../tests/lock-tokens.js";
import type { Engine, LockedRun } from "./engine/engine.js";
import { createEngineStepStore, sendFenced } from "./engine-step-store.js";
import { deriveHandlerSendId } from "./ids.js";
import { LostRunLockError } from "./errors.js";
import { openStepEngine } from "./step-engine.js";
import type { HandlerSender, StepEngine } from "./step-engine.js";

const PARENT_TASK = "onboard-user";

function engineSenderOf(engine: Engine): HandlerSender {
  return {
    dispatch: async (fence, name, payload, options) => {
      const runId = options.runId!;
      const runs = [{ runId, task: name, payload: JSON.stringify(payload), retries: 0 }];
      return { runId: (await sendFenced(engine, { runs }, fence, runId)).sentId };
    },
    emit: async (fence, name, payload, options) => {
      const eventId = options.eventId!;
      const sourceEvent = { eventName: name, eventId };
      const runs = [
        { runId: `welcome-${eventId}`, task: "send-welcome-email", payload: "{}", retries: 0, sourceEvent },
      ];
      return { eventId: (await sendFenced(engine, { runs, eventDedup: sourceEvent }, fence, eventId)).sentId };
    },
  };
}

async function lockedParentRunOf({ engine, ...context }: EngineTestContext): Promise<LockedRun> {
  await engine.enqueue({ runs: [{ runId: "parent-run", task: PARENT_TASK, payload: "{}", retries: 0 }] });
  const [lockedRun] = await fetchedLockedRunsOf({ engine, ...context }, 1);
  return lockedRun!;
}

function openAttemptOf(
  { engine }: EngineTestContext,
  lockedRun: LockedRun,
  markLockLost: () => void = () => undefined,
): StepEngine {
  return openStepEngine({
    taskName: lockedRun.task,
    runId: lockedRun.runId,
    lockToken: lockedRun.lockToken,
    store: createEngineStepStore(engine, lockedRun),
    sender: engineSenderOf(engine),
    abortSignal: new AbortController().signal,
    isWorkerDraining: () => false,
    futureResumeTime: "suspend the run",
    sleepsWokenAt: lockedRun.sleepsWokenAt,
    markLockLost,
  });
}

function topLevelSendId(positionInScope: number): string {
  return deriveHandlerSendId(PARENT_TASK, "parent-run", undefined, positionInScope);
}

engineTest(
  "a step's value is saved in the steps hash and served to a re-entered handler without running its fn",
  async (context) => {
    const { redis, keys } = context;
    const lockedRun = await lockedParentRunOf(context);
    await openAttemptOf(context, lockedRun).step.run("create-account", () => ({ accountId: "account-1" }));
    const createAccount = vi.fn(() => ({ accountId: "account-2" }));

    const account = await openAttemptOf(context, lockedRun).step.run("create-account", createAccount);

    expect(account).toEqual({ accountId: "account-1" });
    expect(createAccount).not.toHaveBeenCalled();
    expect(await redis.hGet(keys.runSteps(PARENT_TASK, "parent-run"), "create-account")).toBe(
      '[{"accountId":"account-1"}]',
    );
  },
);

engineTest(
  "a handler resumed with a send among its saved steps sends nothing again and sees the same id",
  async (context) => {
    const { redis, keys } = context;
    const lockedRun = await lockedParentRunOf(context);
    const firstAttempt = openAttemptOf(context, lockedRun);
    const firstDispatch = await firstAttempt.dispatch("send-tips-email", {});
    const firstEmit = await firstAttempt.emit("user.onboarded", {});
    const storedValuesBefore = await storedValuesByKeyOf(redis, context.keyPrefix);

    const resumedAttempt = openAttemptOf(context, lockedRun);
    const resumedDispatch = await resumedAttempt.dispatch("send-tips-email", {});
    const resumedEmit = await resumedAttempt.emit("user.onboarded", {});

    expect(resumedDispatch).toEqual(firstDispatch);
    expect(resumedEmit).toEqual(firstEmit);
    expect(await storedValuesByKeyOf(redis, context.keyPrefix)).toEqual(storedValuesBefore);
    expect(await redis.hGetAll(keys.runSteps(PARENT_TASK, "parent-run"))).toEqual({
      [topLevelSendId(0)]: JSON.stringify([topLevelSendId(0)]),
      [topLevelSendId(1)]: JSON.stringify([topLevelSendId(1)]),
    });
    expect(await redis.exists(keys.runRecord("send-tips-email", topLevelSendId(0)))).toBe(1);
  },
);

engineTest(
  "a send inside a step's fn is saved under its own derived id and skipped by a re-entered handler",
  async (context) => {
    const lockedRun = await lockedParentRunOf(context);
    const sendInStep = async (attempt: StepEngine) =>
      attempt.step.run("notify", async () => (await attempt.emit("user.onboarded", {})).eventId);
    const firstEventId = await sendInStep(openAttemptOf(context, lockedRun));
    await context.redis.hDel(context.keys.runSteps(PARENT_TASK, "parent-run"), "notify");

    const resumedEventId = await sendInStep(openAttemptOf(context, lockedRun));

    expect(resumedEventId).toBe(firstEventId);
    expect(firstEventId).toBe(deriveHandlerSendId(PARENT_TASK, "parent-run", "notify", 0));
  },
);

engineTest("an emit whose event id is already live resolves with that id and saves no step", async (context) => {
  const { engine, redis, keys } = context;
  const lockedRun = await lockedParentRunOf(context);
  const sourceEvent = { eventName: "user.onboarded", eventId: "onboarded-user-1" };
  await engine.enqueue({
    runs: [{ runId: "earlier-welcome", task: "send-welcome-email", payload: "{}", retries: 0, sourceEvent }],
    eventDedup: sourceEvent,
  });

  const emittedEvent = await openAttemptOf(context, lockedRun).emit(
    "user.onboarded",
    {},
    { eventId: "onboarded-user-1" },
  );

  expect(emittedEvent).toEqual({ eventId: "onboarded-user-1" });
  expect(await redis.hGetAll(keys.runSteps(PARENT_TASK, "parent-run"))).toEqual({});
});

async function staleAttemptOf(context: EngineTestContext, markLockLost: () => void): Promise<StepEngine> {
  const lockedRun = await lockedParentRunOf(context);
  await context.engine.saveStep({ ...lockedRun, stepId: "create-account", step: { kind: "value", value: '["a-1"]' } });
  await context.engine.settle({ ...lockedRun, outcome: { kind: "yield" } });
  await fetchedLockedRunsOf(context, 1);
  return openAttemptOf(context, lockedRun, markLockLost);
}

engineTest(
  "a step read, a step write, a sleep and a send from an attempt whose token is stale throw LostRunLockError and write nothing",
  async (context) => {
    const { redis, keyPrefix } = context;
    const markLockLost = vi.fn();
    const staleAttempt = await staleAttemptOf(context, markLockLost);
    const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);
    const createAccount = vi.fn(() => "a-2");
    const thrownBy = (stepCall: Promise<unknown>) =>
      stepCall.then(
        () => undefined,
        (error: unknown) => error,
      );

    const thrownValues = await Promise.all([
      thrownBy(staleAttempt.step.run("create-account", createAccount)),
      thrownBy(staleAttempt.step.sleep("wait-a-day", "1d")),
      thrownBy(staleAttempt.dispatch("send-tips-email", {})),
      thrownBy(staleAttempt.emit("user.onboarded", {})),
    ]);

    expect(thrownValues.map((thrownValue) => thrownValue instanceof LostRunLockError)).toEqual([
      true,
      true,
      true,
      true,
    ]);
    expect(thrownValues[0]).toMatchObject({
      message:
        'Step "create-account" of run "parent-run" was not read: this attempt no longer holds its lock, so another attempt owns it now.',
    });
    expect(createAccount).not.toHaveBeenCalled();
    expect(markLockLost).toHaveBeenCalledTimes(4);
    expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
  },
);

engineTest("a step write with a stale token throws LostRunLockError and writes nothing", async (context) => {
  const { engine, redis, keyPrefix } = context;
  const lockedRun = await lockedParentRunOf(context);
  const store = createEngineStepStore(engine, {
    task: lockedRun.task,
    runId: lockedRun.runId,
    lockToken: freshLockTokensOf(1)[0]!,
  });
  const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

  await expect(store.saveStep("create-account", '["a-1"]')).rejects.toThrow(
    new LostRunLockError('Step "create-account" of run "parent-run"'),
  );
  expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
});

engineTest(
  "a step call on a record rewritten by a newer release throws LostRunLockError naming the version",
  async (context) => {
    const { redis, keys } = context;
    const lockedRun = await lockedParentRunOf(context);
    await redis.hSet(keys.runRecord(PARENT_TASK, "parent-run"), "version", 2);

    await expect(openAttemptOf(context, lockedRun).step.run("create-account", () => "a-1")).rejects.toThrow(
      new LostRunLockError('Step "create-account" of run "parent-run"', {
        refusedCall: "read",
        lockLoss: { kind: "recordRewritten", recordVersion: 2 },
      }),
    );
  },
);

engineTest("a suspended attempt made due returns from every later sleep at once", async (context) => {
  const { engine } = context;
  const firstLock = await lockedParentRunOf(context);
  const sendFollowUp = vi.fn(() => "sent");
  const sleepThenFollowUp = (attempt: StepEngine) =>
    attempt.executeHandler(async () => {
      await attempt.step.sleep("wait-a-day", "1d");
      await attempt.step.sleep("wait-a-week", "7d");
      await attempt.step.run("send-follow-up", sendFollowUp);
    });
  const suspension = await sleepThenFollowUp(openAttemptOf(context, firstLock));
  if (suspension.kind !== "suspended") {
    throw new Error(`expected the first attempt to suspend, got ${JSON.stringify(suspension)}`);
  }
  await engine.settle({ ...firstLock, outcome: { kind: "suspend", resumesAt: suspension.resumesAt } });
  await engine.makeDue();
  const [resumedLock] = await fetchedLockedRunsOf(context, 1);

  const outcome = await sleepThenFollowUp(openAttemptOf(context, resumedLock!));

  expect(resumedLock?.sleepsWokenAt).toBeDefined();
  expect(outcome).toEqual({ kind: "completed" });
  expect(sendFollowUp).toHaveBeenCalledOnce();
});
