import { Redis } from "ioredis";

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
