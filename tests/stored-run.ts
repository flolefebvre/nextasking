import { Redis } from "ioredis";

import { deriveJobId } from "../src/ids.js";
import type { RunId } from "../src/ids.js";

const RUN_QUEUE_KEY_SEGMENT = ":runs:";

export async function storedRun(
  redisUrl: string,
  keyPrefix: string,
  taskName: string,
  runId: RunId,
): Promise<Record<string, unknown> | undefined> {
  const redis = new Redis(redisUrl, { lazyConnect: true });
  redis.on("error", () => undefined);
  try {
    await redis.connect();
    const jobKey = `${keyPrefix}${RUN_QUEUE_KEY_SEGMENT}${deriveJobId(taskName, runId)}`;
    const storedData = await redis.hget(jobKey, "data");
    return storedData === null ? undefined : (JSON.parse(storedData) as Record<string, unknown>);
  } finally {
    await redis.quit().catch(() => undefined);
  }
}
