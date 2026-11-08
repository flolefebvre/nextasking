import type { Redis } from "ioredis";

import type { EpochMilliseconds } from "../src/durations.js";

export async function serverInstantInMillisecondsOf(redis: Redis): Promise<EpochMilliseconds> {
  const [seconds, microseconds] = await redis.time();
  return Number(seconds) * 1000 + Math.floor(Number(microseconds) / 1000);
}
