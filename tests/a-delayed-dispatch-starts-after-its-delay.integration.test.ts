import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";

const DELAY_IN_MILLISECONDS = 2000;

integrationTest(
  "a delayed dispatch starts its run only once the delay has passed",
  { timeout: 15_000 },
  async ({ redisUrl, keyPrefix }) => {
    let handlerStartedAt: number | undefined;
    const sendReminder = defineTask("send-reminder", {
      schema: z.object({}),
      handler: async () => {
        handlerStartedAt = Date.now();
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger: createRecordingLogger().logger,
      tasks: [sendReminder],
    });

    try {
      await nextasking.startWorker();
      const dispatchedAt = Date.now();

      await nextasking.dispatch("send-reminder", {}, { delay: "2s" });

      const startedAt = await vi.waitUntil(() => handlerStartedAt, { timeout: 10_000 });
      expect(startedAt - dispatchedAt).toBeGreaterThanOrEqual(DELAY_IN_MILLISECONDS);
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);
