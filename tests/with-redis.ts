import { Redis } from "ioredis";

import { createNodeRedisDriver } from "../src/node-redis-driver.js";
import type { RedisClient } from "../src/redis-client.js";

export async function withRedisClient<Result>(
  redisUrl: string,
  use: (client: RedisClient) => Promise<Result>,
): Promise<Result> {
  let markReady = () => undefined as void;
  const ready = new Promise<void>((resolve) => {
    markReady = resolve;
  });
  const driver = createNodeRedisDriver(redisUrl, {
    onConnect: () => undefined,
    onReady: markReady,
    onError: () => undefined,
  });
  driver.connect();
  try {
    await ready;
    return await use(driver);
  } finally {
    await driver.close().catch(() => undefined);
    driver.destroy();
  }
}

export async function withRedis<Result>(
  redisUrl: string,
  use: (redis: Redis, client: RedisClient) => Promise<Result>,
): Promise<Result> {
  const redis = new Redis(redisUrl, { lazyConnect: true });
  redis.on("error", () => undefined);
  try {
    await redis.connect();
    return await withRedisClient(redisUrl, (client) => use(redis, client));
  } finally {
    await redis.quit().catch(() => undefined);
  }
}
