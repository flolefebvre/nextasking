import { runQueueKeyPrefix } from "./run-job-key.js";
import { withRedis } from "./with-redis.js";

const JOB_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

export async function storedJobNames(redisUrl: string, keyPrefix: string): Promise<string[]> {
  return withRedis(redisUrl, async (redis) => {
    const queueKeyPrefix = runQueueKeyPrefix(keyPrefix);
    const keys = await redis.keys(`${queueKeyPrefix}*`);
    const jobKeys = keys.filter((key) => JOB_ID.test(key.slice(queueKeyPrefix.length)));
    const jobNames = await Promise.all(jobKeys.map((jobKey) => redis.hget(jobKey, "name")));
    return jobNames.filter((jobName) => jobName !== null).sort();
  });
}
