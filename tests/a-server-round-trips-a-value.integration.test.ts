import { Redis } from "ioredis";
import { expect } from "vitest";

import { integrationTest } from "./integration-test.js";

integrationTest("a server round-trips a value", async ({ redisUrl, keyPrefix }) => {
  const redis = new Redis(redisUrl, { lazyConnect: true });
  redis.on("error", () => undefined);
  const key = `${keyPrefix}greeting`;

  try {
    await redis.connect();

    await redis.set(key, "hello");
    const storedValue = await redis.get(key);

    expect(storedValue).toBe("hello");
  } finally {
    await redis.del(key).catch(() => undefined);
    await redis.quit().catch(() => undefined);
  }
});
