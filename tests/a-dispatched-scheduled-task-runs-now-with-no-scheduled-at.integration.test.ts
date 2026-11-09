import { expect, vi } from "vitest";

import { createNextasking, defineScheduledTask } from "../src/index.js";
import type { ScheduledTaskContext } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";

integrationTest(
  "a scheduled task dispatched with no payload runs now, with scheduledAt undefined and no payload in its context",
  { timeout: 15_000 },
  async ({ redisUrl, keyPrefix }) => {
    const { logger } = createRecordingLogger();
    const recordedContexts: ScheduledTaskContext[] = [];
    const purgeExpiredSessions = defineScheduledTask("purge-expired-sessions", {
      schedule: { cron: "0 0 1 1 *" },
      handler: async (context) => {
        recordedContexts.push(context);
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [purgeExpiredSessions],
    });

    try {
      await nextasking.startWorker();

      const { runId } = await nextasking.dispatch("purge-expired-sessions", undefined, { runId: "purge-now" });

      await vi.waitFor(
        () => {
          expect(recordedContexts).toHaveLength(1);
        },
        { timeout: 10_000 },
      );
      expect(runId).toBe("purge-now");
      expect(recordedContexts[0]).toMatchObject({ runId: "purge-now", attempt: 1, scheduledAt: undefined });
      expect(recordedContexts[0]).toHaveProperty("scheduledAt");
      expect(recordedContexts[0]).not.toHaveProperty("payload");
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);
