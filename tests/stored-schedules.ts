import type { EpochMilliseconds } from "../src/durations.js";
import { engineKeysOf } from "../src/engine/engine-keys.js";
import { withRedis } from "./with-redis.js";

export type StoredSchedule = { task: string } & Record<string, string>;

export async function storedSchedules(redisUrl: string, keyPrefix: string): Promise<StoredSchedule[]> {
  const keys = engineKeysOf(keyPrefix);
  return withRedis(redisUrl, async (redis) => {
    const tasks = (await redis.zRange(keys.schedules, 0, -1)).sort();
    return Promise.all(tasks.map(async (task) => ({ ...(await redis.hGetAll(keys.schedule(task))), task })));
  });
}

export async function moveNextFiringTo(
  redisUrl: string,
  keyPrefix: string,
  task: string,
  plannedInstant: EpochMilliseconds,
): Promise<void> {
  const keys = engineKeysOf(keyPrefix);
  await withRedis(redisUrl, async (redis) => {
    if ((await redis.exists(keys.schedule(task))) === 0) {
      throw new Error(`No schedule of "${task}" is stored`);
    }
    await redis.hSet(keys.schedule(task), "nextFiringAt", plannedInstant);
    await redis.zAdd(keys.schedules, { score: plannedInstant, value: task });
  });
}
