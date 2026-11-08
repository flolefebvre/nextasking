import { expect } from "vitest";

import { engineTest, storedValuesByKeyOf } from "../../tests/engine-test.js";
import type { EngineTestContext } from "../../tests/engine-test.js";
import { freshLockTokensOf } from "../../tests/lock-tokens.js";
import { serverInstantInMillisecondsOf } from "../../tests/server-instant.js";
import { DEDUP_WINDOW_IN_MILLISECONDS } from "./engine.js";
import type { ImplementedSettleOutcome, LockedRun } from "./engine.js";

const LOCK_DURATION_IN_MILLISECONDS = 120_000;

const COMPLETE: ImplementedSettleOutcome = { kind: "complete" };

const YIELD: ImplementedSettleOutcome = { kind: "yield" };

const HAND_BACK: ImplementedSettleOutcome = {
  kind: "handBack",
  error: { attempt: 1, name: "UnknownTaskError", message: "unknown task", stack: "", failedAt: "" },
  failureNotice: undefined,
};

const OUTCOMES = [COMPLETE, YIELD, HAND_BACK];

async function enqueueSendEmailRuns(
  { engine }: EngineTestContext,
  count: number,
  concurrencyKey?: string,
): Promise<void> {
  await engine.enqueue({
    runs: Array.from({ length: count }, (_, position) => ({
      runId: `run-${String(position + 1)}`,
      task: "send-email",
      payload: "{}",
      retries: 0,
      ...(concurrencyKey === undefined ? {} : { concurrencyKey }),
    })),
  });
}

async function fetchedLockedRunsOf(
  { engine }: EngineTestContext,
  lockTokenCount: number,
  concurrencyLimitsByTask: Record<string, number> = {},
): Promise<LockedRun[]> {
  const reply = await engine.fetch({
    lockTokens: freshLockTokensOf(lockTokenCount),
    concurrencyLimitsByTask,
    lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS,
  });
  if (reply.kind !== "fetched") {
    throw new Error(`expected a fetched reply, got ${JSON.stringify(reply)}`);
  }
  return reply.lockedRuns;
}

async function lockedRunOf(context: EngineTestContext): Promise<LockedRun> {
  await enqueueSendEmailRuns(context, 1);
  const [lockedRun] = await fetchedLockedRunsOf(context, 1);
  return lockedRun!;
}

engineTest(
  "a completion drops the steps, writes the dedup entry, moves the run to completed and ends its lock and slot",
  async (context) => {
    const { engine, redis, keys } = context;
    const { runId, lockToken } = await lockedRunOf(context);
    await redis.hset(keys.runSteps(runId), "step-1", '"saved"');
    const instantBefore = await serverInstantInMillisecondsOf(redis);

    const reply = await engine.settle({ runId, lockToken, outcome: COMPLETE });

    const instantAfter = await serverInstantInMillisecondsOf(redis);
    const finishedAt = reply.kind === "completed" ? reply.finishedAt : Number.NaN;
    expect(reply).toEqual({ kind: "completed", finishedAt: expect.any(Number) });
    expect(finishedAt).toBeGreaterThanOrEqual(instantBefore);
    expect(finishedAt).toBeLessThanOrEqual(instantAfter);
    expect(
      await redis.hmget(
        keys.runRecord(runId),
        "state",
        "finishedAt",
        "endedLockToken",
        "endedAs",
        "lockToken",
        "lockedUntil",
      ),
    ).toEqual(["completed", String(finishedAt), lockToken, "complete", null, null]);
    expect(Number(await redis.zscore(keys.runIndex("send-email", "completed"), runId))).toBe(finishedAt);
    expect(Number(await redis.zscore(keys.runDedup("send-email"), runId))).toBe(
      finishedAt + DEDUP_WINDOW_IN_MILLISECONDS,
    );
    expect(await redis.exists(keys.runSteps(runId), keys.lock(lockToken))).toBe(0);
    expect(await redis.zscore(keys.locks, runId)).toBeNull();
    expect(await redis.sismember(keys.slotSet("send-email", undefined), runId)).toBe(0);
  },
);

engineTest(
  "a yield returns the run to the front of pending with its order kept and ends its lock and slot",
  async (context) => {
    const { engine, redis, keys } = context;
    await enqueueSendEmailRuns(context, 2);
    const [lockedRun] = await fetchedLockedRunsOf(context, 1);
    const orderBefore = await redis.hget(keys.runRecord("run-1"), "order");
    await redis.del(keys.wakeUpMarker);

    const reply = await engine.settle({ runId: "run-1", lockToken: lockedRun!.lockToken, outcome: YIELD });

    expect(reply).toEqual({ kind: "yielded" });
    expect(await redis.zrange(keys.pending, 0, -1)).toEqual(["run-1", "run-2"]);
    expect(await redis.hmget(keys.runRecord("run-1"), "state", "order", "endedAs")).toEqual([
      "pending",
      orderBefore,
      "yield",
    ]);
    expect(await redis.exists(keys.lock(lockedRun!.lockToken))).toBe(0);
    expect(await redis.scard(keys.slotSet("send-email", undefined))).toBe(0);
    expect(await redis.zcard(keys.wakeUpMarker)).toBe(1);
  },
);

engineTest(
  "a hand-back returns the run to the front of pending and keeps the instant it was first found unknown",
  async (context) => {
    const { engine, redis, keys } = context;
    await enqueueSendEmailRuns(context, 2);
    const [firstLock] = await fetchedLockedRunsOf(context, 1);
    const instantBefore = await serverInstantInMillisecondsOf(redis);

    const firstReply = await engine.settle({ runId: "run-1", lockToken: firstLock!.lockToken, outcome: HAND_BACK });
    const firstFoundUnknownAt = await redis.hget(keys.runRecord("run-1"), "firstFoundUnknownAt");
    const [secondLock] = await fetchedLockedRunsOf(context, 1);
    await engine.settle({ runId: "run-1", lockToken: secondLock!.lockToken, outcome: HAND_BACK });

    expect(firstReply).toEqual({ kind: "handedBack" });
    expect(Number(firstFoundUnknownAt)).toBeGreaterThanOrEqual(instantBefore);
    expect(await redis.hget(keys.runRecord("run-1"), "firstFoundUnknownAt")).toBe(firstFoundUnknownAt);
    expect(await redis.zrange(keys.pending, 0, -1)).toEqual(["run-1", "run-2"]);
    expect(await redis.hget(keys.runRecord("run-1"), "endedAs")).toBe("handBack");
  },
);

for (const outcome of [COMPLETE, YIELD]) {
  engineTest(
    `a ${outcome.kind} after a hand-back clears the instant the run was first found unknown`,
    async (context) => {
      const { engine, redis, keys } = context;
      const handedBackLock = await lockedRunOf(context);
      await engine.settle({ ...handedBackLock, outcome: HAND_BACK });
      const [knownTaskLock] = await fetchedLockedRunsOf(context, 1);

      await engine.settle({ ...knownTaskLock!, outcome });

      expect(await redis.hexists(keys.runRecord(handedBackLock.runId), "firstFoundUnknownAt")).toBe(0);
    },
  );
}

for (const outcome of OUTCOMES) {
  engineTest(
    `a ${outcome.kind} with a token that is not the record's replies stale and writes nothing`,
    async (context) => {
      const { engine, redis, keyPrefix } = context;
      const { runId } = await lockedRunOf(context);
      const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

      const reply = await engine.settle({ runId, lockToken: "stale-token", outcome });

      expect(reply).toEqual({ kind: "stale" });
      expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
    },
  );

  engineTest(`a resent ${outcome.kind} with the ended token replies already and writes nothing`, async (context) => {
    const { engine, redis, keyPrefix } = context;
    const { runId, lockToken } = await lockedRunOf(context);
    await engine.settle({ runId, lockToken, outcome });
    const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

    const reply = await engine.settle({ runId, lockToken, outcome });

    expect(reply).toEqual({ kind: "already" });
    expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
  });
}

engineTest("a settle of another kind with the ended token replies stale", async (context) => {
  const { engine } = context;
  const { runId, lockToken } = await lockedRunOf(context);
  await engine.settle({ runId, lockToken, outcome: YIELD });

  const reply = await engine.settle({ runId, lockToken, outcome: COMPLETE });

  expect(reply).toEqual({ kind: "stale" });
});

engineTest("a settle of a run with no record replies stale and writes nothing", async (context) => {
  const { engine, redis, keyPrefix } = context;
  const { lockToken } = await lockedRunOf(context);
  const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

  const reply = await engine.settle({ runId: "missing-run", lockToken, outcome: COMPLETE });

  expect(reply).toEqual({ kind: "stale" });
  expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
});

engineTest("a settle of a record of a newer version replies unknownVersion and writes nothing", async (context) => {
  const { engine, redis, keys, keyPrefix } = context;
  const { runId, lockToken } = await lockedRunOf(context);
  await redis.hset(keys.runRecord(runId), "version", 2);
  const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

  const reply = await engine.settle({ runId, lockToken, outcome: COMPLETE });

  expect(reply).toEqual({ kind: "unknownVersion", recordVersion: 2 });
  expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
});

engineTest("a completion wakes as many parked runs as there are free slots, in waiting-list order", async (context) => {
  const { engine, redis, keys } = context;
  await enqueueSendEmailRuns(context, 5, "account-7");
  const [firstHolder, secondHolder] = await fetchedLockedRunsOf(context, 5, { "send-email": 2 });

  await engine.settle({ ...firstHolder!, outcome: COMPLETE, concurrencyLimit: 2 });
  const pendingAfterFirstCompletion = await redis.zrange(keys.pending, 0, -1);
  await engine.settle({ ...secondHolder!, outcome: COMPLETE, concurrencyLimit: 3 });

  expect(pendingAfterFirstCompletion).toEqual(["run-3"]);
  expect(await redis.zrange(keys.pending, 0, -1)).toEqual(["run-3", "run-4", "run-5"]);
  expect(await redis.hget(keys.runRecord("run-4"), "state")).toBe("pending");
  expect(await redis.zcard(keys.waitingList("send-email", "account-7"))).toBe(0);
});

engineTest("a settle with no limit wakes every parked run of the key", async (context) => {
  const { engine, redis, keys } = context;
  await enqueueSendEmailRuns(context, 4, "account-7");
  const [holder] = await fetchedLockedRunsOf(context, 4, { "send-email": 1 });

  await engine.settle({ ...holder!, outcome: YIELD });

  expect(await redis.zrange(keys.pending, 0, -1)).toEqual(["run-1", "run-2", "run-3", "run-4"]);
  expect(await redis.zcard(keys.waitingList("send-email", "account-7"))).toBe(0);
});

engineTest("a settle with no free slot left wakes no parked run", async (context) => {
  const { engine, redis, keys } = context;
  await enqueueSendEmailRuns(context, 3, "account-7");
  const [holder] = await fetchedLockedRunsOf(context, 3, { "send-email": 2 });

  await engine.settle({ ...holder!, outcome: HAND_BACK, concurrencyLimit: 1 });

  expect(await redis.zrange(keys.waitingList("send-email", "account-7"), 0, -1)).toEqual(["run-3"]);
});

engineTest("a settle of an unknown kind is rejected and writes nothing", async (context) => {
  const { engine, redis, keyPrefix } = context;
  const { runId, lockToken } = await lockedRunOf(context);
  const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

  const settle = engine.settle({
    runId,
    lockToken,
    outcome: { kind: "explode" } as unknown as ImplementedSettleOutcome,
  });

  await expect(settle).rejects.toThrow("invalid argument: unknown settle kind explode");
  expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
});
