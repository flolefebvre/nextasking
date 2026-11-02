import type { RunId } from "../src/ids.js";
import { runJobKey } from "./run-job-key.js";
import { withRedis } from "./with-redis.js";

export type SavedSteps = {
  serializedResultsByStepId: Record<string, string>;
  timeToLiveInMilliseconds: number;
};

export function savedStepsKey(keyPrefix: string, taskName: string, runId: RunId): string {
  return `${runJobKey(keyPrefix, taskName, runId)}:steps`;
}

export async function savedSteps(
  redisUrl: string,
  keyPrefix: string,
  taskName: string,
  runId: RunId,
): Promise<SavedSteps> {
  return withRedis(redisUrl, async (redis) => {
    const stepsKey = savedStepsKey(keyPrefix, taskName, runId);
    const [serializedResultsByStepId, timeToLiveInMilliseconds] = await Promise.all([
      redis.hgetall(stepsKey),
      redis.pttl(stepsKey),
    ]);
    return { serializedResultsByStepId, timeToLiveInMilliseconds };
  });
}
