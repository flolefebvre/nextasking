import { withRedis } from "../with-redis.js";

export async function fixtureRecordsOf(redisUrl: string, keyPrefix: string, taskName: string): Promise<unknown[]> {
  return withRedis(redisUrl, async (redis) => {
    const serializedRecords = await redis.lrange(`${keyPrefix}records:${taskName}`, 0, -1);
    return serializedRecords.map((serializedRecord) => JSON.parse(serializedRecord) as unknown);
  });
}
