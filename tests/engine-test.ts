import type { Redis } from "ioredis";

import { engineKeysOf } from "../src/engine/engine-keys.js";
import type { EngineKeys } from "../src/engine/engine-keys.js";
import { createEngine } from "../src/engine/engine.js";
import type { ImplementedEngine } from "../src/engine/engine.js";
import { integrationTest } from "./integration-test.js";
import { expectNoInvariantViolations } from "./invariant-violations.js";
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

export async function storedValuesByKeyOf(redis: Redis, keyPrefix: string): Promise<Map<string, Buffer | null>> {
  const storedKeys = (await redis.keys(`{${keyPrefix}}:*`)).sort();
  const storedValuesByKey = new Map<string, Buffer | null>();
  for (const storedKey of storedKeys) {
    storedValuesByKey.set(storedKey, await redis.dumpBuffer(storedKey));
  }
  return storedValuesByKey;
}
