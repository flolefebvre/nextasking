import { expect, vi } from "vitest";

import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { EVICTING_POLICY_REJECTION, startEvictingServer } from "./evicting-server.js";
import { createGiveUpAfterOneSecondNextasking } from "./give-up-after-one-second-nextasking.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";
import { providedRedisUrl } from "./valkey-container.js";
import { withRedis } from "./with-redis.js";

integrationTest(
  "a worker connecting as a user denied CONFIG reads the maxmemory-policy from INFO, and starts without warning",
  async ({ redisUrl, keyPrefix }) => {
    const userDeniedConfig = `${keyPrefix}denied-config`;
    const userUrl = new URL(redisUrl);
    userUrl.username = userDeniedConfig;
    userUrl.password = "denied-config-password";
    const { logger, lines } = createRecordingLogger();
    const nextasking = createGiveUpAfterOneSecondNextasking(userUrl.toString(), keyPrefix, { logger });

    try {
      await withRedis(redisUrl, (redis) =>
        redis.sendCommand([
          "ACL",
          "SETUSER",
          userDeniedConfig,
          "on",
          ">denied-config-password",
          "~*",
          "&*",
          "+@all",
          "-config",
        ]),
      );
      await nextasking.startWorker();

      expect(lines.map((line) => line.message)).not.toContain("maxmemory-policy unreadable");
      expect(lines.map((line) => line.message)).toContain("worker started");
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
      await withRedis(redisUrl, (redis) => redis.sendCommand(["ACL", "DELUSER", userDeniedConfig]));
    }
  },
);

integrationTest.skipIf(providedRedisUrl() !== undefined)(
  "a worker refuses a server with CONFIG disabled whose maxmemory-policy evicts: startWorker() rejects, or logs worker not started then calls onWorkerNotStarted when it finds it in the background",
  async ({ keyPrefix }) => {
    const evictingServer = await startEvictingServer({ isConfigDisabled: true });
    const backgroundBoot = createRecordingLogger();
    const foregroundNextasking = createGiveUpAfterOneSecondNextasking(evictingServer.url, keyPrefix);
    const backgroundNextasking = createGiveUpAfterOneSecondNextasking(evictingServer.proxy.url, keyPrefix, {
      logger: backgroundBoot.logger,
    });

    const foregroundOnWorkerNotStarted = vi.fn();
    const messagesWrittenWhenCalledBack: string[][] = [];
    const backgroundOnWorkerNotStarted = vi.fn(() => {
      messagesWrittenWhenCalledBack.push(backgroundBoot.lines.map((line) => line.message));
    });
    const exitCodeBefore = process.exitCode;

    try {
      await expect(
        foregroundNextasking.startWorker({ onWorkerNotStarted: foregroundOnWorkerNotStarted }),
      ).rejects.toThrow(EVICTING_POLICY_REJECTION);
      await closeNextaskingAndCheckItsDuration(foregroundNextasking);
      await evictingServer.proxy.cut();
      await backgroundNextasking.startWorker({ onWorkerNotStarted: backgroundOnWorkerNotStarted });
      await evictingServer.proxy.restore();
      await vi.waitFor(() => expect(backgroundOnWorkerNotStarted).toHaveBeenCalled(), { timeout: 15_000 });

      expect(backgroundBoot.lines.filter((line) => line.message === "worker not started")).toEqual([
        {
          level: "error",
          fields: { err: expect.objectContaining({ message: EVICTING_POLICY_REJECTION }) },
          message: "worker not started",
        },
      ]);
      expect(backgroundBoot.lines.map((line) => line.message)).not.toContain("worker started");
      expect(backgroundOnWorkerNotStarted.mock.calls).toEqual([
        [expect.objectContaining({ message: EVICTING_POLICY_REJECTION })],
      ]);
      expect(messagesWrittenWhenCalledBack[0]).toContain("worker not started");
      expect(foregroundOnWorkerNotStarted).not.toHaveBeenCalled();
      expect(process.exitCode).toBe(exitCodeBefore);
    } finally {
      await closeNextaskingAndCheckItsDuration(foregroundNextasking);
      await closeNextaskingAndCheckItsDuration(backgroundNextasking);
      await evictingServer.stop();
    }
  },
  120_000,
);
