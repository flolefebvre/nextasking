import type { Redis } from "ioredis";

import type { DurationInMilliseconds } from "../src/durations.js";
import { engineKeysOf } from "../src/engine/engine-keys.js";
import type { EngineKeys } from "../src/engine/engine-keys.js";
import { createEngine } from "../src/engine/engine.js";
import type { Engine, LockedRun } from "../src/engine/engine.js";
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

export async function expireLock({ redis, keys }: EngineTestContext, runId: string): Promise<void> {
  const lockedUntil = (await serverInstantInMillisecondsOf(redis)) - 1;
  await redis.hset(keys.runRecord(runId), "lockedUntil", lockedUntil);
  await redis.zadd(keys.locks, lockedUntil, runId);
}

export async function backdateEndedRun(
  { redis, keys }: EngineTestContext,
  runId: string,
  durationInMilliseconds: DurationInMilliseconds,
): Promise<void> {
  const [task, state, finishedAt] = await redis.hmget(keys.runRecord(runId), "task", "state", "finishedAt");
  if (task == null || (state !== "completed" && state !== "failed") || finishedAt == null) {
    throw new Error(`run "${runId}" has not ended`);
  }
  const backdatedFinishedAt = Number(finishedAt) - durationInMilliseconds;
  await redis.hset(keys.runRecord(runId), "finishedAt", backdatedFinishedAt);
  await redis.zadd(keys.runIndex(task, state), backdatedFinishedAt, runId);
  const dedupExpiresAt = await redis.zscore(keys.runDedup(task), runId);
  if (dedupExpiresAt !== null) {
    await redis.zadd(keys.runDedup(task), Number(dedupExpiresAt) - durationInMilliseconds, runId);
  }
}

export async function storedValuesByKeyOf(redis: Redis, keyPrefix: string): Promise<Map<string, Buffer | null>> {
  const storedKeys = (await redis.keys(`{${keyPrefix}}:*`)).sort();
  const storedValuesByKey = new Map<string, Buffer | null>();
  for (const storedKey of storedKeys) {
    storedValuesByKey.set(storedKey, await redis.dumpBuffer(storedKey));
  }
  return storedValuesByKey;
}
