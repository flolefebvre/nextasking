import { expect } from "vitest";

import { integrationTest } from "./integration-test.js";
import { withRedis } from "./with-redis.js";

integrationTest("a server round-trips a value", async ({ redisUrl, keyPrefix }) => {
  await withRedis(redisUrl, async (redis) => {
    const key = `${keyPrefix}greeting`;

    try {
      await redis.set(key, "hello");
      const storedValue = await redis.get(key);

      expect(storedValue).toBe("hello");
    } finally {
      await redis.del(key).catch(() => undefined);
    }
  });
});
