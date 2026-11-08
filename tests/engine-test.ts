import type { Redis } from "ioredis";

import { engineKeysOf } from "../src/engine/engine-keys.js";
import type { EngineKeys } from "../src/engine/engine-keys.js";
import { createEngine } from "../src/engine/engine.js";
import type { ImplementedEngine, LockedRun } from "../src/engine/engine.js";
import { integrationTest } from "./integration-test.js";
import { expectNoInvariantViolations } from "./invariant-violations.js";
import { freshLockTokensOf } from "./lock-tokens.js";
import { serverInstantInMillisecondsOf } from "./server-instant.js";
import { withRedis } from "./with-redis.js";

export type EngineTestContext = { engine: ImplementedEngine; redis: Redis; keys: EngineKeys; keyPrefix: string };

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

export async function storedValuesByKeyOf(redis: Redis, keyPrefix: string): Promise<Map<string, Buffer | null>> {
  const storedKeys = (await redis.keys(`{${keyPrefix}}:*`)).sort();
  const storedValuesByKey = new Map<string, Buffer | null>();
  for (const storedKey of storedKeys) {
    storedValuesByKey.set(storedKey, await redis.dumpBuffer(storedKey));
  }
  return storedValuesByKey;
}
