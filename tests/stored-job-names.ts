import { Redis } from "ioredis";

const RUN_QUEUE_KEY_SEGMENT = ":runs:";

const JOB_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

export async function storedJobNames(redisUrl: string, keyPrefix: string): Promise<string[]> {
  const redis = new Redis(redisUrl, { lazyConnect: true });
  redis.on("error", () => undefined);
  try {
    await redis.connect();
    const queueKeyPrefix = `${keyPrefix}${RUN_QUEUE_KEY_SEGMENT}`;
    const keys = await redis.keys(`${queueKeyPrefix}*`);
    const jobKeys = keys.filter((key) => JOB_ID.test(key.slice(queueKeyPrefix.length)));
    const jobNames = await Promise.all(jobKeys.map((jobKey) => redis.hget(jobKey, "name")));
    return jobNames.filter((jobName) => jobName !== null).sort();
  } finally {
    await redis.quit().catch(() => undefined);
  }
}
