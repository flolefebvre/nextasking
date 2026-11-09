import { compareInSortedSetMemberOrder, engineKeysOf, runIdentityOf } from "../src/engine/engine-keys.js";
import type { RunIdentity } from "../src/engine/engine-keys.js";
import { withRedis } from "./with-redis.js";

export async function storedRunIdentities(redisUrl: string, keyPrefix: string): Promise<RunIdentity[]> {
  const recordKeyStart = engineKeysOf(keyPrefix)
    .runRecord("t", "r")
    .replace(/[^:]+$/, "");
  return withRedis(redisUrl, async (redis) => {
    const keys = await redis.keys(`${recordKeyStart.replace(/[*?[\]\\]/g, "\\$&")}*`);
    return keys
      .map((key) => key.slice(recordKeyStart.length))
      .filter((encodedStoredRunId) => !encodedStoredRunId.includes(":"))
      .flatMap((encodedStoredRunId) => {
        const runIdentity = runIdentityOf(decodeURIComponent(encodedStoredRunId));
        return runIdentity === undefined ? [] : [runIdentity];
      })
      .sort(
        (left, right) =>
          compareInSortedSetMemberOrder(left.task, right.task) ||
          compareInSortedSetMemberOrder(left.runId, right.runId),
      );
  });
}
