import { setTimeout as sleep } from "node:timers/promises";

import { expect } from "vitest";

import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { createGiveUpAfterOneSecondNextasking } from "./give-up-after-one-second-nextasking.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";

const IDLE_LONGER_THAN_GIVE_UP_AFTER_IN_MILLISECONDS = 2_500;

integrationTest(
  "an instance left idle for longer than giveUpAfter logs no connection error and answers the next call",
  { timeout: 20_000 },
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const nextasking = createGiveUpAfterOneSecondNextasking(redisUrl, keyPrefix, { logger });
    try {
      await nextasking.dispatch("send-welcome-email", { email: "ada@example.com" }, { runId: "welcome-ada" });

      await sleep(IDLE_LONGER_THAN_GIVE_UP_AFTER_IN_MILLISECONDS);
      const nextCall = await nextasking.getRun("send-welcome-email", "welcome-ada");

      expect(nextCall).toMatchObject({ runId: "welcome-ada", state: "pending" });
      expect(lines.filter(({ level }) => level !== "debug")).toEqual([]);
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);
