import type { RunId } from "../src/ids.js";
import { runJobKey } from "./run-job-key.js";
import { withRedis } from "./with-redis.js";

export async function storedRun(
  redisUrl: string,
  keyPrefix: string,
  taskName: string,
  runId: RunId,
): Promise<Record<string, unknown> | undefined> {
  return withRedis(redisUrl, async (redis) => {
    const storedData = await redis.hget(runJobKey(keyPrefix, taskName, runId), "data");
    return storedData === null ? undefined : (JSON.parse(storedData) as Record<string, unknown>);
  });
}
