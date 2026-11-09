import type { EpochMilliseconds } from "../src/durations.js";
import type { TestClient } from "./with-redis.js";

export async function serverInstantInMillisecondsOf(redis: TestClient): Promise<EpochMilliseconds> {
  const [seconds, microseconds] = await redis.time();
  return Number(seconds) * 1000 + Math.floor(Number(microseconds) / 1000);
}
