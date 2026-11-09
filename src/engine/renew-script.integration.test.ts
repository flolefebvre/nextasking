import { expect } from "vitest";

import { engineTest, storedValuesByKeyOf } from "../../tests/engine-test.js";
import type { EngineTestContext } from "../../tests/engine-test.js";
import { freshLockTokensOf } from "../../tests/lock-tokens.js";
import { serverInstantInMillisecondsOf } from "../../tests/server-instant.js";
import { storedRunIdOf } from "./engine-keys.js";
import type { HeldLock } from "./engine.js";

const SHORT_LOCK_DURATION_IN_MILLISECONDS = 5_000;

const LOCK_DURATION_IN_MILLISECONDS = 120_000;

async function heldLocksOf({ engine }: EngineTestContext, runCount: number): Promise<HeldLock[]> {
  await engine.enqueue({
    runs: Array.from({ length: runCount }, (_, position) => ({
      runId: `run-${String(position + 1)}`,
      task: "send-email",
      payload: "{}",
      retries: 0,
    })),
  });
  const reply = await engine.fetch({
    lockTokens: freshLockTokensOf(runCount),
    concurrencyLimitsByTask: {},
    lockDurationInMilliseconds: SHORT_LOCK_DURATION_IN_MILLISECONDS,
  });
  if (reply.kind !== "fetched") {
    throw new Error(`expected a fetched reply, got ${JSON.stringify(reply)}`);
  }
  return reply.lockedRuns.map(({ task, runId, lockToken }) => ({ task, runId, lockToken }));
}

engineTest(
  "a renew of current locks moves lockedUntil, the locks score and the lock key's expiry to TIME plus the duration",
  async (context) => {
    const { engine, redis, keys } = context;
    const heldLocks = await heldLocksOf(context, 2);
    const instantBefore = await serverInstantInMillisecondsOf(redis);

    const renewals = await engine.renew({ heldLocks, lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS });

    const instantAfter = await serverInstantInMillisecondsOf(redis);
    expect(renewals).toEqual([
      { kind: "renewed", lockedUntil: expect.any(Number) },
      { kind: "renewed", lockedUntil: expect.any(Number) },
    ]);
    for (const [position, { task, runId, lockToken }] of heldLocks.entries()) {
      const renewal = renewals[position];
      const lockedUntil = renewal?.kind === "renewed" ? renewal.lockedUntil : Number.NaN;
      expect(lockedUntil).toBeGreaterThanOrEqual(instantBefore + LOCK_DURATION_IN_MILLISECONDS);
      expect(lockedUntil).toBeLessThanOrEqual(instantAfter + LOCK_DURATION_IN_MILLISECONDS);
      expect(await redis.hget(keys.runRecord(task, runId), "lockedUntil")).toBe(String(lockedUntil));
      expect(Number(await redis.zscore(keys.locks, storedRunIdOf(task, runId)))).toBe(lockedUntil);
      expect(await redis.pttl(keys.lock(lockToken))).toBeGreaterThan(SHORT_LOCK_DURATION_IN_MILLISECONDS);
    }
  },
);

engineTest(
  "a renew with a stale token replies stale and writes nothing, while the current locks of the batch are renewed",
  async (context) => {
    const { engine, redis, keys } = context;
    const [currentLock, otherLock] = await heldLocksOf(context, 2);
    const otherRecordBefore = await redis.hgetall(keys.runRecord(otherLock!.task, otherLock!.runId));

    const renewals = await engine.renew({
      heldLocks: [currentLock!, { task: otherLock!.task, runId: otherLock!.runId, lockToken: "stale-token" }],
      lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS,
    });

    expect(renewals.map(({ kind }) => kind)).toEqual(["renewed", "stale"]);
    expect(await redis.hgetall(keys.runRecord(otherLock!.task, otherLock!.runId))).toEqual(otherRecordBefore);
    expect(await redis.exists(keys.lock("stale-token"))).toBe(0);
  },
);

const STALE_RENEWALS = [
  {
    name: "a token that is not the record's",
    heldLockOf: ({ task, runId }: HeldLock) => ({ task, runId, lockToken: "stale-token" }),
    prepare: async () => undefined,
  },
  {
    name: "a run with no record",
    heldLockOf: ({ task, lockToken }: HeldLock) => ({ task, runId: "missing-run", lockToken }),
    prepare: async () => undefined,
  },
  {
    name: "a record of a newer version",
    heldLockOf: (heldLock: HeldLock) => heldLock,
    prepare: async ({ redis, keys }: EngineTestContext, { task, runId }: HeldLock) => {
      await redis.hset(keys.runRecord(task, runId), "version", 2);
    },
  },
];

for (const { name, heldLockOf, prepare } of STALE_RENEWALS) {
  engineTest(`a renew of ${name} replies stale and writes nothing`, async (context) => {
    const { engine, redis, keyPrefix } = context;
    const [heldLock] = await heldLocksOf(context, 1);
    await prepare(context, heldLock!);
    const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

    const renewals = await engine.renew({
      heldLocks: [heldLockOf(heldLock!)],
      lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS,
    });

    expect(renewals).toEqual([{ kind: "stale" }]);
    expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
  });
}

engineTest(
  "a renew of a lock whose key expired while its token is still the record's renews it and sets the key again",
  async (context) => {
    const { engine, redis, keys } = context;
    const [heldLock] = await heldLocksOf(context, 1);
    await redis.del(keys.lock(heldLock!.lockToken));

    const renewals = await engine.renew({
      heldLocks: [heldLock!],
      lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS,
    });

    expect(renewals.map(({ kind }) => kind)).toEqual(["renewed"]);
    expect(await redis.get(keys.lock(heldLock!.lockToken))).toBe(storedRunIdOf(heldLock!.task, heldLock!.runId));
  },
);

engineTest("a renew of no lock replies with no renewal", async ({ engine }) => {
  const renewals = await engine.renew({ heldLocks: [], lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS });

  expect(renewals).toEqual([]);
});
