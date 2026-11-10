import { RESP_TYPES } from "@redis/client";

import type { DurationInMilliseconds } from "../src/durations.js";
import { engineKeysOf, storedRunIdOf } from "../src/engine/engine-keys.js";
import type { EngineKeys } from "../src/engine/engine-keys.js";
import { createEngine } from "../src/engine/engine.js";
import type { Engine, LockedRun } from "../src/engine/engine.js";
import type { RunId } from "../src/ids.js";
import type { RedisClient } from "../src/redis-client.js";
import { integrationTest } from "./integration-test.js";
import type { ExpectInvariantViolation } from "./invariant-violations.js";
import { freshLockTokensOf } from "./lock-tokens.js";
import { serverInstantInMillisecondsOf } from "./server-instant.js";
import { withRedis } from "./with-redis.js";
import type { TestClient } from "./with-redis.js";

export type EngineTestContext = {
  engine: Engine;
  client: RedisClient;
  redis: TestClient;
  keys: EngineKeys;
  keyPrefix: string;
};

export function engineTestContextOf(redis: TestClient, client: RedisClient, keyPrefix: string): EngineTestContext {
  return { engine: createEngine(client, keyPrefix), client, redis, keys: engineKeysOf(keyPrefix), keyPrefix };
}

export function engineTest(
  name: string,
  body: (
    context: EngineTestContext & { redisUrl: string; expectInvariantViolation: ExpectInvariantViolation },
  ) => Promise<void>,
  { timeout }: { timeout?: number } = {},
): void {
  integrationTest(name, { timeout }, async ({ redisUrl, keyPrefix, expectInvariantViolation }) => {
    await withRedis(redisUrl, async (redis, client) => {
      await body({ ...engineTestContextOf(redis, client, keyPrefix), redisUrl, expectInvariantViolation });
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
  await redis.hSet(keys.runRecord(task, runId), "lockedUntil", lockedUntil);
  await redis.zAdd(keys.locks, { score: lockedUntil, value: storedRunIdOf(task, runId) });
}

export async function backdateEndedRun(
  { redis, keys }: EngineTestContext,
  task: string,
  runId: RunId,
  durationInMilliseconds: DurationInMilliseconds,
): Promise<void> {
  const [state, finishedAt] = await redis.hmGet(keys.runRecord(task, runId), ["state", "finishedAt"]);
  if ((state !== "completed" && state !== "failed") || finishedAt == null) {
    throw new Error(`run "${runId}" has not ended`);
  }
  const backdatedFinishedAt = Number(finishedAt) - durationInMilliseconds;
  await redis.hSet(keys.runRecord(task, runId), "finishedAt", backdatedFinishedAt);
  await redis.zAdd(keys.runIndex(task, state), { score: backdatedFinishedAt, value: storedRunIdOf(task, runId) });
  const dedupExpiresAt = await redis.zScore(keys.runDedup(task), storedRunIdOf(task, runId));
  if (dedupExpiresAt !== null) {
    await redis.zAdd(keys.runDedup(task), {
      score: dedupExpiresAt - durationInMilliseconds,
      value: storedRunIdOf(task, runId),
    });
  }
}

export async function backdateFirstFoundUnknownAt(
  { redis, keys }: EngineTestContext,
  task: string,
  runId: RunId,
  durationInMilliseconds: DurationInMilliseconds,
): Promise<void> {
  const firstFoundUnknownAt = await redis.hGet(keys.runRecord(task, runId), "firstFoundUnknownAt");
  if (firstFoundUnknownAt === null) {
    throw new Error(`run "${runId}" was never found unknown`);
  }
  await redis.hSet(
    keys.runRecord(task, runId),
    "firstFoundUnknownAt",
    Number(firstFoundUnknownAt) - durationInMilliseconds,
  );
}

export function serializedValueOf(redis: TestClient, key: string): Promise<Buffer | null> {
  return redis.withTypeMapping({ [RESP_TYPES.BLOB_STRING]: Buffer }).dump(key);
}

export async function storedValuesByKeyOf(redis: TestClient, keyPrefix: string): Promise<Map<string, Buffer | null>> {
  const storedKeys = (await redis.keys(`{${keyPrefix}}:*`)).sort();
  const storedValuesByKey = new Map<string, Buffer | null>();
  for (const storedKey of storedKeys) {
    storedValuesByKey.set(storedKey, await serializedValueOf(redis, storedKey));
  }
  return storedValuesByKey;
}
