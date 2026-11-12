import { setTimeout as sleep } from "node:timers/promises";

import { expect } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { integrationTest, TEST_TIMEOUT_PAST_ITS_WAITS_IN_MILLISECONDS } from "./integration-test.js";
import { STORED_KEYS_READ_BOUND_IN_MILLISECONDS } from "./invariant-violations.js";
import { SERVER_CONTAINER_START_BOUND_IN_MILLISECONDS, startOwnServer } from "./own-server.js";
import { createRecordingLogger } from "./recording-logger.js";
import { providedRedisUrl } from "./valkey-container.js";

const SERVER_QUIET_CLIENT_TIMEOUT_IN_SECONDS = 15;

const QUIET_WINDOW_IN_MILLISECONDS = 3 * SERVER_QUIET_CLIENT_TIMEOUT_IN_SECONDS * 1000 + 2_000;

const GIVE_UP_AFTER_IN_MILLISECONDS = 5_000;

integrationTest.skipIf(providedRedisUrl() !== undefined)(
  "an instance that only dispatches, on a server closing clients quiet for 15 s, writes no connection error across three of those periods and answers its next dispatch on the same connection",
  {
    timeout:
      SERVER_CONTAINER_START_BOUND_IN_MILLISECONDS +
      2 * GIVE_UP_AFTER_IN_MILLISECONDS +
      QUIET_WINDOW_IN_MILLISECONDS +
      STORED_KEYS_READ_BOUND_IN_MILLISECONDS +
      TEST_TIMEOUT_PAST_ITS_WAITS_IN_MILLISECONDS,
  },
  async ({ keyPrefix }) => {
    const ownServer = await startOwnServer({
      serverArguments: ["--timeout", String(SERVER_QUIET_CLIENT_TIMEOUT_IN_SECONDS)],
    });
    const { logger, lines } = createRecordingLogger();
    const nextasking = createNextasking({
      connection: { url: ownServer.proxy.url },
      prefix: keyPrefix,
      logger,
      tasks: [
        defineTask("send-welcome-email", {
          schema: z.object({ email: z.string() }),
          handler: async () => undefined,
        }),
      ],
    });
    try {
      await nextasking.dispatch("send-welcome-email", { email: "ada@example.com" }, { runId: "welcome-ada" });

      await sleep(QUIET_WINDOW_IN_MILLISECONDS);
      const { runId } = await nextasking.dispatch(
        "send-welcome-email",
        { email: "grace@example.com" },
        { runId: "welcome-grace" },
      );

      expect(runId).toBe("welcome-grace");
      expect(lines.filter(({ level }) => level !== "debug")).toEqual([]);
      expect(ownServer.proxy.acceptedConnectionCount()).toBe(1);
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
      await ownServer.stop();
    }
  },
);
