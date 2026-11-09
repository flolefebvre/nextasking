import { Redis } from "ioredis";

import { ioredisClientOf } from "../src/ioredis-client.js";
import type { RedisClient } from "../src/redis-client.js";

export async function withRedis<Result>(redisUrl: string, use: (redis: Redis) => Promise<Result>): Promise<Result> {
  const redis = new Redis(redisUrl, { lazyConnect: true });
  redis.on("error", () => undefined);
  try {
    await redis.connect();
    return await use(redis);
  } finally {
    await redis.quit().catch(() => undefined);
  }
}

export function redisClientOf(redis: Redis): RedisClient {
  return ioredisClientOf(redis);
}
