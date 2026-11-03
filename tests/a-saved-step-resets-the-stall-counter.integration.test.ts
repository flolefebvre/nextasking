import { expect } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";
import { runJobKey } from "./run-job-key.js";
import { withRedis } from "./with-redis.js";

integrationTest(
  "a saved step resets BullMQ's stall counter, the stc field of the job hash <prefix>:runs:<jobId>",
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const stallCountsBeforeEachStep: (string | null)[] = [];
    const syncCalendar = defineTask("sync-calendar", {
      schema: z.object({ calendarId: z.string() }),
      handler: async ({ step, runId }) => {
        const jobKey = runJobKey(keyPrefix, "sync-calendar", runId);
        await withRedis(redisUrl, (redis) => redis.hset(jobKey, "stc", 2));
        stallCountsBeforeEachStep.push(await withRedis(redisUrl, (redis) => redis.hget(jobKey, "stc")));
        await step.run("fetch-events", () => [{ eventId: "event-1" }]);
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [syncCalendar],
    });

    try {
      await nextasking.startWorker();
      const { runId } = await nextasking.dispatch("sync-calendar", { calendarId: "calendar-1" });
      await waitForLogMessage(lines, "run completed");
      const stallCountAfterTheStep = await withRedis(redisUrl, (redis) =>
        redis.hget(runJobKey(keyPrefix, "sync-calendar", runId), "stc"),
      );

      expect(stallCountsBeforeEachStep).toEqual(["2"]);
      expect(stallCountAfterTheStep).toBe("0");
    } finally {
      await closeNextasking(nextasking);
    }
  },
);
