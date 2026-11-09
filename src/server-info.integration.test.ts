import { Redis } from "ioredis";
import { expect } from "vitest";

import { readServerInfo, serverFloorRejectionOf } from "./server-info.js";
import { integrationTest } from "../tests/integration-test.js";

integrationTest(
  "reads the policy and a version at the floor from the test server in one INFO",
  async ({ redisUrl }) => {
    const redis = new Redis(redisUrl, { lazyConnect: true });
    redis.on("error", () => undefined);

    try {
      await redis.connect();

      const { maxmemoryPolicyReading, serverVersionReading } = await readServerInfo(redis);

      expect(maxmemoryPolicyReading).toEqual({ kind: "read", policy: "noeviction" });
      expect(serverVersionReading).toEqual({
        kind: "read",
        serverName: expect.stringMatching(/^(Valkey|Redis)$/),
        version: expect.stringMatching(/^\d+\.\d+/),
      });
      expect(serverVersionReading.kind === "read" && serverFloorRejectionOf(serverVersionReading)).toBeUndefined();
    } finally {
      await redis.quit().catch(() => undefined);
    }
  },
);
