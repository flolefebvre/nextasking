import { setTimeout as sleep } from "node:timers/promises";

import { expect } from "vitest";

import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { createGiveUpAfterOneSecondNextasking } from "./give-up-after-one-second-nextasking.js";
import { integrationTest } from "./integration-test.js";
import { SERVER_CONTAINER_START_BOUND_IN_MILLISECONDS, startOwnServer } from "./own-server.js";
import { providedRedisUrl } from "./valkey-container.js";
import { createRecordingLogger } from "./recording-logger.js";
import { withRedis } from "./with-redis.js";

const READ_ONLY_STRETCH_IN_MILLISECONDS = 500;

const UNREACHABLE_PRIMARY_PORT = "1";

integrationTest.skipIf(providedRedisUrl() !== undefined)(
  "a write answered READONLY while the server is a replica succeeds once it is a primary again, the caller seeing no error",
  { timeout: SERVER_CONTAINER_START_BOUND_IN_MILLISECONDS + READ_ONLY_STRETCH_IN_MILLISECONDS + 5_000 },
  async ({ keyPrefix }) => {
    const ownServer = await startOwnServer();
    const { logger, lines } = createRecordingLogger();
    const nextasking = createGiveUpAfterOneSecondNextasking(ownServer.url, keyPrefix, { logger });
    try {
      await withRedis(ownServer.url, async (admin) => {
        await nextasking.dispatch("send-welcome-email", { email: "ada@example.com" }, { runId: "welcome-ada" });
        await admin.sendCommand(["REPLICAOF", "127.0.0.1", UNREACHABLE_PRIMARY_PORT]);
        const promotedAgain = sleep(READ_ONLY_STRETCH_IN_MILLISECONDS).then(() =>
          admin.sendCommand(["REPLICAOF", "NO", "ONE"]),
        );

        const dispatched = await nextasking.dispatch(
          "send-welcome-email",
          { email: "grace@example.com" },
          { runId: "welcome-grace" },
        );
        await promotedAgain;
        const readBack = await nextasking.getRun("send-welcome-email", "welcome-grace");

        expect(dispatched).toEqual({ runId: "welcome-grace" });
        expect(readBack).toMatchObject({ runId: "welcome-grace", state: "pending" });
        expect(lines.filter(({ level }) => level !== "debug")).toEqual([]);
      });
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
      await ownServer.stop();
    }
  },
);
