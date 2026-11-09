import { engineKeysOf } from "../src/engine/engine-keys.js";
import type { RunId } from "../src/ids.js";
import { withRedis } from "./with-redis.js";

export type SavedSteps = {
  serializedResultsByStepId: Record<string, string>;
  timeToLiveInMilliseconds: number;
};

export async function savedSteps(redisUrl: string, keyPrefix: string, task: string, runId: RunId): Promise<SavedSteps> {
  const stepsKey = engineKeysOf(keyPrefix).runSteps(task, runId);
  return withRedis(redisUrl, async (redis) => {
    const [serializedResultsByStepId, timeToLiveInMilliseconds] = await Promise.all([
      redis.hgetall(stepsKey),
      redis.pttl(stepsKey),
    ]);
    return { serializedResultsByStepId, timeToLiveInMilliseconds };
  });
}
