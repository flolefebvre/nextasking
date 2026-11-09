import { withRedis } from "./with-redis.js";

export function fixtureRecordsKey(keyPrefix: string, taskName: string): string {
  return `${keyPrefix}records:${taskName}`;
}

export async function fixtureRecordsOf(redisUrl: string, keyPrefix: string, taskName: string): Promise<unknown[]> {
  return withRedis(redisUrl, async (redis) => {
    const serializedRecords = await redis.lRange(fixtureRecordsKey(keyPrefix, taskName), 0, -1);
    return serializedRecords.map((serializedRecord) => JSON.parse(serializedRecord) as unknown);
  });
}
