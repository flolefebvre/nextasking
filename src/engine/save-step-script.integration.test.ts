import { expect, vi } from "vitest";

import { engineTest, fetchedLockedRunsOf, storedValuesByKeyOf } from "../../tests/engine-test.js";
import type { EngineTestContext } from "../../tests/engine-test.js";
import { freshLockTokensOf } from "../../tests/lock-tokens.js";
import { serverInstantInMillisecondsOf } from "../../tests/server-instant.js";
import type { LockedRun, StepToSave } from "./engine.js";

const ONE_MINUTE_SLEEP: StepToSave = { kind: "sleep", durationInMilliseconds: 60_000 };

const NEW_YEAR_2030 = Date.UTC(2030, 0, 1);

async function lockedRunOf(context: EngineTestContext): Promise<LockedRun> {
  await context.engine.enqueue({ runs: [{ runId: "run-1", task: "send-email", payload: "{}", retries: 0 }] });
  const [lockedRun] = await fetchedLockedRunsOf(context, 1);
  return lockedRun!;
}

async function savedSleepStepOf(context: EngineTestContext, lockedRun: LockedRun, step: StepToSave) {
  const { engine, redis } = context;
  const instantBefore = await serverInstantInMillisecondsOf(redis);
  const reply = await engine.saveStep({ ...lockedRun, stepId: "wait-a-minute", step });
  return { reply, instantBefore, instantAfter: await serverInstantInMillisecondsOf(redis) };
}

engineTest("a value step saved with the current token is stored verbatim and resets stalls", async (context) => {
  const { engine, redis, keys } = context;
  const { task, runId, lockToken } = await lockedRunOf(context);
  await redis.hset(keys.runRecord(task, runId), "stalls", 2);

  const reply = await engine.saveStep({
    task,
    runId,
    lockToken,
    stepId: "charge-card",
    step: { kind: "value", value: '{"chargeId":"ch-1"}' },
  });

  expect(reply).toEqual({ kind: "saved", resumesAt: undefined });
  expect(await redis.hgetall(keys.runSteps(task, runId))).toEqual({ "charge-card": '{"chargeId":"ch-1"}' });
  expect(await redis.hget(keys.runRecord(task, runId), "stalls")).toBe("0");
});

engineTest("a sleep step is saved with its resume instant, the server's TIME plus the duration", async (context) => {
  const { redis, keys } = context;
  const lockedRun = await lockedRunOf(context);
  vi.useFakeTimers({ toFake: ["Date"], now: new Date("2000-01-01T00:00:00Z") });
  try {
    const { reply, instantBefore, instantAfter } = await savedSleepStepOf(context, lockedRun, ONE_MINUTE_SLEEP);

    const resumesAt = reply.kind === "saved" ? Number(reply.resumesAt) : Number.NaN;
    expect(resumesAt).toBeGreaterThanOrEqual(instantBefore + 60_000);
    expect(resumesAt).toBeLessThanOrEqual(instantAfter + 60_000);
    expect(await redis.hget(keys.runSteps("send-email", "run-1"), "wait-a-minute")).toBe(String(resumesAt));
  } finally {
    vi.useRealTimers();
  }
});

engineTest(
  "a sleep step saved twice keeps the first resume time and writes nothing the second time",
  async (context) => {
    const { engine, redis, keys, keyPrefix } = context;
    const lockedRun = await lockedRunOf(context);
    const { reply: firstReply } = await savedSleepStepOf(context, lockedRun, ONE_MINUTE_SLEEP);
    await redis.hset(keys.runRecord("send-email", "run-1"), "stalls", 1);
    const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

    const secondReply = await engine.saveStep({
      ...lockedRun,
      stepId: "wait-a-minute",
      step: { kind: "sleep", durationInMilliseconds: 3_600_000 },
    });

    expect(secondReply).toEqual(firstReply);
    expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
  },
);

engineTest("a sleepUntil step is saved with the instant it was sent", async (context) => {
  const { redis, keys } = context;
  const lockedRun = await lockedRunOf(context);

  const { reply } = await savedSleepStepOf(context, lockedRun, { kind: "sleepUntil", resumesAt: NEW_YEAR_2030 });

  expect(reply).toEqual({ kind: "saved", resumesAt: NEW_YEAR_2030, isDue: false });
  expect(await redis.hget(keys.runSteps("send-email", "run-1"), "wait-a-minute")).toBe(String(NEW_YEAR_2030));
});

engineTest("a sleep tells whether it is due by the server's TIME", async (context) => {
  const lockedRun = await lockedRunOf(context);
  vi.useFakeTimers({ toFake: ["Date"], now: new Date("2100-01-01T00:00:00Z") });
  try {
    const sleepOf = (stepId: string, step: StepToSave) => context.engine.saveStep({ ...lockedRun, stepId, step });

    const pastSleepUntil = await sleepOf("wait-for-1970", { kind: "sleepUntil", resumesAt: 1000 });
    const oneDaySleep = await sleepOf("wait-a-day", { kind: "sleep", durationInMilliseconds: 86_400_000 });
    await context.redis.hset(context.keys.runSteps("send-email", "run-1"), "wait-a-day", 1000);
    const reachedAgainOnceDue = await sleepOf("wait-a-day", { kind: "sleep", durationInMilliseconds: 86_400_000 });

    expect(pastSleepUntil).toEqual({ kind: "saved", resumesAt: 1000, isDue: true });
    expect(oneDaySleep).toMatchObject({ kind: "saved", isDue: false });
    expect(reachedAgainOnceDue).toEqual({ kind: "saved", resumesAt: 1000, isDue: true });
  } finally {
    vi.useRealTimers();
  }
});

engineTest("a value step saved twice keeps the first value and writes nothing the second time", async (context) => {
  const { engine, redis, keys, keyPrefix } = context;
  const lockedRun = await lockedRunOf(context);
  await engine.saveStep({ ...lockedRun, stepId: "charge-card", step: { kind: "value", value: '"first"' } });
  await redis.hset(keys.runRecord("send-email", "run-1"), "stalls", 1);
  const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

  const reply = await engine.saveStep({
    ...lockedRun,
    stepId: "charge-card",
    step: { kind: "value", value: '"second"' },
  });

  expect(reply).toEqual({ kind: "saved", resumesAt: undefined });
  expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
});

engineTest("a sleep saved under the id of a value step is rejected and writes nothing", async (context) => {
  const { engine, redis, keyPrefix } = context;
  const lockedRun = await lockedRunOf(context);
  await engine.saveStep({ ...lockedRun, stepId: "charge-card", step: { kind: "value", value: '"charged"' } });
  const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

  const saveStep = engine.saveStep({ ...lockedRun, stepId: "charge-card", step: ONE_MINUTE_SLEEP });

  await expect(saveStep).rejects.toThrow("invalid argument: step charge-card is saved but is not a sleep");
  expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
});

type Staleness = "another token" | "an ended lock" | "a missing run";

async function staleLockOf(context: EngineTestContext, staleness: Staleness): Promise<LockedRun> {
  const lockedRun = await lockedRunOf(context);
  if (staleness === "another token") {
    return { ...lockedRun, lockToken: freshLockTokensOf(1)[0]! };
  }
  if (staleness === "an ended lock") {
    await context.engine.settle({ ...lockedRun, outcome: { kind: "yield" } });
    return lockedRun;
  }
  return { ...lockedRun, runId: "missing-run" };
}

for (const staleness of ["another token", "an ended lock", "a missing run"] as const) {
  engineTest(`a save or read with ${staleness} replies stale and writes nothing`, async (context) => {
    const { engine, redis, keys, keyPrefix } = context;
    const staleLock = await staleLockOf(context, staleness);
    await redis.hset(keys.runSteps("send-email", "run-1"), "charge-card", '"charged"');
    await redis.hset(keys.runRecord("send-email", "run-1"), "stalls", 1);
    const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

    const saveReply = await engine.saveStep({
      ...staleLock,
      stepId: "send-receipt",
      step: { kind: "value", value: '"sent"' },
    });
    const readReply = await engine.readStep({ ...staleLock, stepId: "charge-card" });

    expect(saveReply).toEqual({ kind: "stale" });
    expect(readReply).toEqual({ kind: "stale" });
    expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
  });
}

engineTest(
  "a save or read on a record of a newer version replies unknownVersion and writes nothing",
  async (context) => {
    const { engine, redis, keys, keyPrefix } = context;
    const lockedRun = await lockedRunOf(context);
    await redis.hset(keys.runRecord("send-email", "run-1"), "version", 2);
    const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

    const saveReply = await engine.saveStep({ ...lockedRun, stepId: "wait-a-minute", step: ONE_MINUTE_SLEEP });
    const readReply = await engine.readStep({ ...lockedRun, stepId: "wait-a-minute" });

    expect(saveReply).toEqual({ kind: "unknownVersion", recordVersion: 2 });
    expect(readReply).toEqual({ kind: "unknownVersion", recordVersion: 2 });
    expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
  },
);

engineTest("a completed run has no steps and a failed run keeps them", async (context) => {
  const { engine, redis, keys } = context;
  await engine.enqueue({
    runs: ["run-1", "run-2"].map((runId) => ({ runId, task: "send-email", payload: "{}", retries: 0 })),
  });
  const [completedRun, failedRun] = await fetchedLockedRunsOf(context, 2);
  for (const lockedRun of [completedRun!, failedRun!]) {
    await engine.saveStep({ ...lockedRun, stepId: "charge-card", step: { kind: "value", value: '"charged"' } });
  }

  await engine.settle({ ...completedRun!, outcome: { kind: "complete" } });
  await engine.settle({
    ...failedRun!,
    outcome: {
      kind: "fail",
      reason: "non-retriable",
      error: { attempt: 1, name: "Error", message: "boom", stack: "", failedAt: "" },
      failureNotice: undefined,
    },
  });

  expect(await redis.exists(keys.runSteps(completedRun!.task, completedRun!.runId))).toBe(0);
  expect(await redis.hgetall(keys.runSteps(failedRun!.task, failedRun!.runId))).toEqual({ "charge-card": '"charged"' });
});
