import type { Redis } from "ioredis";

import type { DurationInMilliseconds } from "../src/durations.js";
import { engineKeysOf, storedRunIdOf } from "../src/engine/engine-keys.js";
import type { EngineKeys } from "../src/engine/engine-keys.js";
import { createEngine } from "../src/engine/engine.js";
import type { Engine, LockedRun } from "../src/engine/engine.js";
import type { RunId } from "../src/ids.js";
import { integrationTest } from "./integration-test.js";
import { expectNoInvariantViolations } from "./invariant-violations.js";
import { freshLockTokensOf } from "./lock-tokens.js";
import { serverInstantInMillisecondsOf } from "./server-instant.js";
import { withRedis } from "./with-redis.js";

export type EngineTestContext = { engine: Engine; redis: Redis; keys: EngineKeys; keyPrefix: string };

export function engineTest(name: string, body: (context: EngineTestContext) => Promise<void>): void {
  integrationTest(name, async ({ redisUrl, keyPrefix }) => {
    await withRedis(redisUrl, async (redis) => {
      await body({ engine: createEngine(redis, keyPrefix), redis, keys: engineKeysOf(keyPrefix), keyPrefix });
      await expectNoInvariantViolations(redis, keyPrefix);
    });
  });
}

export const ENGINE_TEST_LOCK_DURATION_IN_MILLISECONDS = 120_000;

export async function fetchedLockedRunsOf(
  { engine }: EngineTestContext,
  lockTokenCount: number,
  concurrencyLimitsByTask: Record<string, number> = {},
): Promise<LockedRun[]> {
  const reply = await engine.fetch({
    lockTokens: freshLockTokensOf(lockTokenCount),
    concurrencyLimitsByTask,
    lockDurationInMilliseconds: ENGINE_TEST_LOCK_DURATION_IN_MILLISECONDS,
  });
  if (reply.kind !== "fetched") {
    throw new Error(`expected a fetched reply, got ${JSON.stringify(reply)}`);
  }
  return reply.lockedRuns;
}

export async function expireLock({ redis, keys }: EngineTestContext, task: string, runId: RunId): Promise<void> {
  const lockedUntil = (await serverInstantInMillisecondsOf(redis)) - 1;
  await redis.hset(keys.runRecord(task, runId), "lockedUntil", lockedUntil);
  await redis.zadd(keys.locks, lockedUntil, storedRunIdOf(task, runId));
}

export async function backdateEndedRun(
  { redis, keys }: EngineTestContext,
  task: string,
  runId: RunId,
  durationInMilliseconds: DurationInMilliseconds,
): Promise<void> {
  const [state, finishedAt] = await redis.hmget(keys.runRecord(task, runId), "state", "finishedAt");
  if ((state !== "completed" && state !== "failed") || finishedAt == null) {
    throw new Error(`run "${runId}" has not ended`);
  }
  const backdatedFinishedAt = Number(finishedAt) - durationInMilliseconds;
  await redis.hset(keys.runRecord(task, runId), "finishedAt", backdatedFinishedAt);
  await redis.zadd(keys.runIndex(task, state), backdatedFinishedAt, storedRunIdOf(task, runId));
  const dedupExpiresAt = await redis.zscore(keys.runDedup(task), storedRunIdOf(task, runId));
  if (dedupExpiresAt !== null) {
    await redis.zadd(keys.runDedup(task), Number(dedupExpiresAt) - durationInMilliseconds, storedRunIdOf(task, runId));
  }
}

export async function backdateFirstFoundUnknownAt(
  { redis, keys }: EngineTestContext,
  task: string,
  runId: RunId,
  durationInMilliseconds: DurationInMilliseconds,
): Promise<void> {
  const firstFoundUnknownAt = await redis.hget(keys.runRecord(task, runId), "firstFoundUnknownAt");
  if (firstFoundUnknownAt === null) {
    throw new Error(`run "${runId}" was never found unknown`);
  }
  await redis.hset(
    keys.runRecord(task, runId),
    "firstFoundUnknownAt",
    Number(firstFoundUnknownAt) - durationInMilliseconds,
  );
}

export async function storedValuesByKeyOf(redis: Redis, keyPrefix: string): Promise<Map<string, Buffer | null>> {
  const storedKeys = (await redis.keys(`{${keyPrefix}}:*`)).sort();
  const storedValuesByKey = new Map<string, Buffer | null>();
  for (const storedKey of storedKeys) {
    storedValuesByKey.set(storedKey, await redis.dumpBuffer(storedKey));
  }
  return storedValuesByKey;
}
