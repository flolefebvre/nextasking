import { engineKeysOf } from "../src/engine/engine-keys.js";
import { deserializePayload } from "../src/engine/engine.js";
import type { RunId } from "../src/ids.js";
import { withRedis } from "./with-redis.js";

export type StoredRun = {
  record: Record<string, string>;
  payload: unknown;
  errors: unknown[];
};

export async function storedRun(
  redisUrl: string,
  keyPrefix: string,
  task: string,
  runId: RunId,
): Promise<StoredRun | undefined> {
  const keys = engineKeysOf(keyPrefix);
  return withRedis(redisUrl, async (redis) => {
    const [record, serializedPayload, serializedErrors] = await Promise.all([
      redis.hgetall(keys.runRecord(task, runId)),
      redis.get(keys.runPayload(task, runId)),
      redis.lrange(keys.runErrors(task, runId), 0, -1),
    ]);
    if (Object.keys(record).length === 0) {
      return undefined;
    }
    return {
      record,
      payload: serializedPayload === null ? undefined : deserializePayload(serializedPayload),
      errors: serializedErrors.map((serializedError) => JSON.parse(serializedError) as unknown),
    };
  });
}
