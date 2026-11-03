import { GenericContainer, Wait } from "testcontainers";
import { expect } from "vitest";

import { closeNextasking } from "../src/registry.js";
import { createGiveUpAfterOneSecondNextasking } from "./give-up-after-one-second-nextasking.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";
import { startTcpProxy } from "./tcp-proxy.js";
import { providedRedisUrl, testImage } from "./valkey-container.js";
import { withRedis } from "./with-redis.js";

const REDIS_CONTAINER_PORT = 6379;

const EVICTING_POLICY_REJECTION =
  'maxmemory-policy is "allkeys-lru": the Valkey or Redis holding the queues must use "noeviction", or it evicts locks and runs';

integrationTest(
  "a worker connecting as a user denied CONFIG warns that it can't read the maxmemory-policy, and starts",
  async ({ redisUrl, keyPrefix }) => {
    const userDeniedConfig = `${keyPrefix}denied-config`;
    const userUrl = new URL(redisUrl);
    userUrl.username = userDeniedConfig;
    userUrl.password = "denied-config-password";
    const { logger, lines } = createRecordingLogger();
    const nextasking = createGiveUpAfterOneSecondNextasking(userUrl.toString(), keyPrefix, { logger });

    try {
      await withRedis(redisUrl, (redis) =>
        redis.call("ACL", "SETUSER", userDeniedConfig, "on", ">denied-config-password", "~*", "&*", "+@all", "-config"),
      );
      await nextasking.startWorker();

      expect(lines.filter((line) => line.message === "maxmemory-policy unreadable")).toEqual([
        { level: "warn", fields: { err: expect.any(Error) }, message: "maxmemory-policy unreadable" },
      ]);
      expect(lines.map((line) => line.message)).toContain("worker started");
    } finally {
      await closeNextasking(nextasking);
      await withRedis(redisUrl, (redis) => redis.call("ACL", "DELUSER", userDeniedConfig));
    }
  },
);

integrationTest.skipIf(providedRedisUrl() !== undefined)(
  "a worker refuses a server whose maxmemory-policy evicts: startWorker() rejects, or logs worker not started when it finds it in the background",
  async ({ keyPrefix }) => {
    const evictingContainer = await new GenericContainer(testImage())
      .withCommand(["--maxmemory-policy", "allkeys-lru"])
      .withExposedPorts(REDIS_CONTAINER_PORT)
      .withWaitStrategy(Wait.forLogMessage(/Ready to accept connections/))
      .start();
    const evictingUrl = `redis://${evictingContainer.getHost()}:${String(evictingContainer.getMappedPort(REDIS_CONTAINER_PORT))}`;
    const proxy = await startTcpProxy(evictingUrl);
    const backgroundBoot = createRecordingLogger();
    const foregroundNextasking = createGiveUpAfterOneSecondNextasking(evictingUrl, keyPrefix);
    const backgroundNextasking = createGiveUpAfterOneSecondNextasking(proxy.url, keyPrefix, {
      logger: backgroundBoot.logger,
    });

    try {
      await expect(foregroundNextasking.startWorker()).rejects.toThrow(EVICTING_POLICY_REJECTION);
      await closeNextasking(foregroundNextasking);
      await proxy.cut();
      await backgroundNextasking.startWorker();
      await proxy.restore();
      await waitForLogMessage(backgroundBoot.lines, "worker not started", { timeout: 15_000 });

      expect(backgroundBoot.lines.filter((line) => line.message === "worker not started")).toEqual([
        {
          level: "error",
          fields: { err: expect.objectContaining({ message: EVICTING_POLICY_REJECTION }) },
          message: "worker not started",
        },
      ]);
      expect(backgroundBoot.lines.map((line) => line.message)).not.toContain("worker started");
    } finally {
      await closeNextasking(foregroundNextasking);
      await closeNextasking(backgroundNextasking);
      await proxy.stop();
      await evictingContainer.stop();
    }
  },
  120_000,
);
