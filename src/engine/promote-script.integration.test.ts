import { expect } from "vitest";

import { engineTest, expireLock, fetchedLockedRunsOf, storedValuesByKeyOf } from "../../tests/engine-test.js";
import type { EngineTestContext } from "../../tests/engine-test.js";
import { serverInstantInMillisecondsOf } from "../../tests/server-instant.js";
import type { RunState } from "../run-states.js";
import type { LockedRun, PromoteReply, RunToCreate } from "./engine.js";
import { MOST_DUE_RUNS_PROMOTED_PER_PROMOTE } from "./promote-script.js";

const DELAY_IN_MILLISECONDS = 60_000;

const DUE_INSTANT_FIELD_BY_STATE: Readonly<Partial<Record<RunState, string>>> = {
  delayed: "startsAt",
  suspended: "resumesAt",
  backingOff: "nextAttemptAt",
};

const RETRY_ERROR = { attempt: 1, name: "Error", message: "boom", stack: "", failedAt: "" };

function sendEmailRunOf(runId: string, extraFields: Partial<RunToCreate> = {}): RunToCreate {
  return { runId, task: "send-email", payload: "{}", retries: 3, ...extraFields };
}

async function makeDue({ redis, keys }: EngineTestContext, runId: string): Promise<void> {
  const [task, state] = (await redis.hmget(keys.runRecord(runId), "task", "state")) as [string, RunState];
  const dueAt = (await serverInstantInMillisecondsOf(redis)) - 1000;
  await redis.hset(keys.runRecord(runId), DUE_INSTANT_FIELD_BY_STATE[state]!, dueAt);
  await redis.zadd(keys.due, dueAt, runId);
  await redis.zadd(keys.runIndex(task, state), dueAt, runId);
}

async function runOneWithAnExpiredLockOf(context: EngineTestContext): Promise<LockedRun> {
  await context.engine.enqueue({ runs: [sendEmailRunOf("run-1")] });
  const [lockedRun] = await fetchedLockedRunsOf(context, 1);
  await expireLock(context, "run-1");
  return lockedRun!;
}

async function dueRunInStateOf(context: EngineTestContext, state: RunState): Promise<void> {
  const { engine } = context;
  if (state === "delayed") {
    await engine.enqueue({ runs: [sendEmailRunOf("due-run", { delayInMilliseconds: DELAY_IN_MILLISECONDS })] });
  } else {
    await engine.enqueue({ runs: [sendEmailRunOf("due-run")] });
    const [lockedRun] = await fetchedLockedRunsOf(context, 1);
    await engine.settle({
      ...lockedRun!,
      outcome:
        state === "suspended"
          ? { kind: "suspend", resumesAt: Date.UTC(2030, 0, 1) }
          : { kind: "retry", error: RETRY_ERROR, backoffInMilliseconds: DELAY_IN_MILLISECONDS },
    });
  }
  await makeDue(context, "due-run");
}

function promotedReplyOf(reply: PromoteReply) {
  if (reply.kind !== "promoted") {
    throw new Error(`expected a promoted reply, got ${JSON.stringify(reply)}`);
  }
  return reply;
}

for (const state of ["delayed", "suspended", "backingOff"] as const) {
  engineTest(
    `a due ${state} run is moved to the back of pending with a new order and leaves the due index`,
    async (context) => {
      const { engine, redis, keys } = context;
      await dueRunInStateOf(context, state);
      await engine.enqueue({ runs: [sendEmailRunOf("waiting-run")] });
      await redis.del(keys.wakeUpMarker);

      const reply = await engine.promote();

      expect(reply).toEqual({ kind: "promoted", promotedRunCount: 1, stalledRuns: [], thirdStalls: [] });
      expect(await redis.zrange(keys.pending, 0, -1)).toEqual(["waiting-run", "due-run"]);
      expect(Number(await redis.hget(keys.runRecord("due-run"), "order"))).toBeGreaterThan(
        Number(await redis.hget(keys.runRecord("waiting-run"), "order")),
      );
      expect(await redis.hget(keys.runRecord("due-run"), "state")).toBe("pending");
      expect(await redis.zscore(keys.due, "due-run")).toBeNull();
      expect(await redis.zcard(keys.runIndex("send-email", state))).toBe(0);
      expect(await redis.hexists(keys.runRecord("due-run"), "startsAt")).toBe(0);
      expect(await redis.hexists(keys.runRecord("due-run"), "nextAttemptAt")).toBe(0);
      expect(await redis.hexists(keys.runRecord("due-run"), "resumesAt")).toBe(state === "suspended" ? 1 : 0);
      expect(await redis.zcard(keys.wakeUpMarker)).toBe(1);
    },
  );
}

engineTest("a promoted suspended run is fetched as a resume", async (context) => {
  const { engine } = context;
  await dueRunInStateOf(context, "suspended");
  await engine.promote();

  const [lockedRun] = await fetchedLockedRunsOf(context, 1);

  expect(lockedRun).toMatchObject({ runId: "due-run", isResume: true });
});

engineTest("a run not yet due stays where it is", async (context) => {
  const { engine, redis, keys, keyPrefix } = context;
  await engine.enqueue({ runs: [sendEmailRunOf("later-run", { delayInMilliseconds: DELAY_IN_MILLISECONDS })] });
  const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

  const reply = await engine.promote();

  expect(reply).toEqual({ kind: "promoted", promotedRunCount: 0, stalledRuns: [], thirdStalls: [] });
  expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
  expect(await redis.hget(keys.runRecord("later-run"), "state")).toBe("delayed");
});

engineTest(`a promote moves at most ${String(MOST_DUE_RUNS_PROMOTED_PER_PROMOTE)} due runs`, async (context) => {
  const { engine, redis, keys } = context;
  const runIds = Array.from(
    { length: MOST_DUE_RUNS_PROMOTED_PER_PROMOTE + 1 },
    (_, position) => `run-${String(position)}`,
  );
  await engine.enqueue({
    runs: runIds.map((runId) => sendEmailRunOf(runId, { delayInMilliseconds: DELAY_IN_MILLISECONDS })),
  });
  for (const runId of runIds) {
    await makeDue(context, runId);
  }

  const reply = promotedReplyOf(await engine.promote());

  expect(reply.promotedRunCount).toBe(MOST_DUE_RUNS_PROMOTED_PER_PROMOTE);
  expect(await redis.zcard(keys.due)).toBe(1);
});

engineTest(
  "an executing run whose lock expired goes back to the front of pending with stalls 1, its lock and slot ended",
  async (context) => {
    const { engine, redis, keys } = context;
    await engine.enqueue({ runs: [sendEmailRunOf("run-1"), sendEmailRunOf("run-2")] });
    const [lockedRun] = await fetchedLockedRunsOf(context, 1);
    const orderBefore = await redis.hget(keys.runRecord("run-1"), "order");
    await expireLock(context, "run-1");

    const reply = await engine.promote();
    const lateSettleReply = await engine.settle({ ...lockedRun!, outcome: { kind: "complete" } });

    expect(reply).toEqual({
      kind: "promoted",
      promotedRunCount: 0,
      stalledRuns: [{ runId: "run-1", task: "send-email", attempt: 1, stalls: 1 }],
      thirdStalls: [],
    });
    expect(lateSettleReply).toEqual({ kind: "stale" });
    expect(await redis.zrange(keys.pending, 0, -1)).toEqual(["run-1", "run-2"]);
    expect(await redis.hmget(keys.runRecord("run-1"), "state", "order", "stalls", "attempt", "lockToken")).toEqual([
      "pending",
      orderBefore,
      "1",
      "1",
      null,
    ]);
    expect(await redis.exists(keys.lock(lockedRun!.lockToken))).toBe(0);
    expect(await redis.zcard(keys.locks)).toBe(0);
    expect(await redis.scard(keys.slotSet("send-email", undefined))).toBe(0);
  },
);

engineTest("a second expiry of the same run counts stalls 2", async (context) => {
  const { engine, redis, keys } = context;
  await runOneWithAnExpiredLockOf(context);
  await engine.promote();
  await fetchedLockedRunsOf(context, 1);
  await expireLock(context, "run-1");

  const reply = promotedReplyOf(await engine.promote());

  expect(reply.stalledRuns).toEqual([{ runId: "run-1", task: "send-email", attempt: 1, stalls: 2 }]);
  expect(await redis.hget(keys.runRecord("run-1"), "stalls")).toBe("2");
});

engineTest("an expired lock wakes every parked run of its key, as no limit is known", async (context) => {
  const { engine, redis, keys } = context;
  await engine.enqueue({
    runs: ["run-1", "run-2", "run-3"].map((runId) => sendEmailRunOf(runId, { concurrencyKey: "account-7" })),
  });
  await fetchedLockedRunsOf(context, 3, { "send-email": 1 });
  await expireLock(context, "run-1");

  await engine.promote();

  expect(await redis.zrange(keys.pending, 0, -1)).toEqual(["run-1", "run-2", "run-3"]);
  expect(await redis.zcard(keys.waitingList("send-email", "account-7"))).toBe(0);
});

engineTest(
  "a third expiry is reported with the run's expired token, writes nothing, and is reported again on the next call",
  async (context) => {
    const { engine, redis, keys, keyPrefix } = context;
    const lockedRun = await runOneWithAnExpiredLockOf(context);
    await redis.hset(keys.runRecord("run-1"), "stalls", 2);
    const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

    const reply = await engine.promote();
    const nextReply = await engine.promote();

    const expectedReply = {
      kind: "promoted",
      promotedRunCount: 0,
      stalledRuns: [],
      thirdStalls: [{ runId: "run-1", task: "send-email", attempt: 1, expiredLockToken: lockedRun.lockToken }],
    };
    expect(reply).toEqual(expectedReply);
    expect(nextReply).toEqual(expectedReply);
    expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
  },
);

engineTest("an expired lock on a record of a newer version is left in place", async (context) => {
  const { engine, redis, keys } = context;
  await runOneWithAnExpiredLockOf(context);
  await redis.hset(keys.runRecord("run-1"), "version", 2);

  const reply = await engine.promote();

  expect(reply).toEqual({ kind: "promoted", promotedRunCount: 0, stalledRuns: [], thirdStalls: [] });
  expect(await redis.hget(keys.runRecord("run-1"), "state")).toBe("executing");
});

engineTest("a due record of a newer version is passed over and the due run behind it promoted", async (context) => {
  const { engine, redis, keys } = context;
  await engine.enqueue({
    runs: ["newer-run", "current-run"].map((runId) =>
      sendEmailRunOf(runId, { delayInMilliseconds: DELAY_IN_MILLISECONDS }),
    ),
  });
  await makeDue(context, "newer-run");
  await makeDue(context, "current-run");
  await redis.hset(keys.runRecord("newer-run"), "version", 2);

  const reply = await engine.promote();

  expect(reply).toEqual({ kind: "promoted", promotedRunCount: 1, stalledRuns: [], thirdStalls: [] });
  expect(await redis.zrange(keys.due, 0, -1)).toEqual(["newer-run"]);
  expect(await redis.zrange(keys.pending, 0, -1)).toEqual(["current-run"]);
});

engineTest(
  "ten due records of a newer version and nothing else reply unknownVersion and write nothing",
  async (context) => {
    const { engine, redis, keys, keyPrefix } = context;
    const runIds = Array.from({ length: 11 }, (_, position) => `newer-run-${String(position)}`);
    await engine.enqueue({
      runs: runIds.map((runId) => sendEmailRunOf(runId, { delayInMilliseconds: DELAY_IN_MILLISECONDS })),
    });
    for (const runId of runIds) {
      await makeDue(context, runId);
      await redis.hset(keys.runRecord(runId), "version", 2);
    }
    const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

    const reply = await engine.promote();

    expect(reply).toEqual({ kind: "unknownVersion", recordVersion: 2 });
    expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
  },
);
