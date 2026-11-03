import { deriveJobId } from "../src/ids.js";
import type { RunId } from "../src/ids.js";
import { runJobKey, runQueueKeyPrefix } from "./run-job-key.js";
import { withRedis } from "./with-redis.js";

export type RunQueuePosition = {
  isWaiting: boolean;
  attemptsMade: number;
  stallCount: number;
};

export async function runQueuePosition(
  redisUrl: string,
  keyPrefix: string,
  taskName: string,
  runId: RunId,
): Promise<RunQueuePosition> {
  return withRedis(redisUrl, async (redis) => {
    const jobKey = runJobKey(keyPrefix, taskName, runId);
    const [waitingJobIds, attemptsMade, stallCount] = await Promise.all([
      redis.lrange(`${runQueueKeyPrefix(keyPrefix)}wait`, 0, -1),
      redis.hget(jobKey, "atm"),
      redis.hget(jobKey, "stc"),
    ]);
    return {
      isWaiting: waitingJobIds.includes(deriveJobId(taskName, runId)),
      attemptsMade: Number(attemptsMade ?? 0),
      stallCount: Number(stallCount ?? 0),
    };
  });
}
