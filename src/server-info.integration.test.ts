import { expect } from "vitest";

import { readServerInfo, serverFloorRejectionOf } from "./server-info.js";
import { integrationTest } from "../tests/integration-test.js";
import { withRedisClient } from "../tests/with-redis.js";

integrationTest(
  "reads the policy and a version at the floor from the test server in one INFO",
  async ({ redisUrl }) => {
    await withRedisClient(redisUrl, async (client) => {
      const { maxmemoryPolicyReading, serverVersionReading } = await readServerInfo(client);

      expect(maxmemoryPolicyReading).toEqual({ kind: "read", policy: "noeviction" });
      expect(serverVersionReading).toEqual({
        kind: "read",
        serverName: expect.stringMatching(/^(Valkey|Redis)$/),
        version: expect.stringMatching(/^\d+\.\d+/),
      });
      expect(serverVersionReading.kind === "read" && serverFloorRejectionOf(serverVersionReading)).toBeUndefined();
    });
  },
);
