import { expect } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";
import { engineKeysOf } from "../src/engine/engine-keys.js";
import { withRedis } from "./with-redis.js";

integrationTest(
  "a saved step resets the stall counter, the stalls field of the run's record",
  { timeout: 15_000 },
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const stallCountsBeforeEachStep: (string | null)[] = [];
    const syncCalendar = defineTask("sync-calendar", {
      schema: z.object({ calendarId: z.string() }),
      handler: async ({ step, runId }) => {
        const recordKey = engineKeysOf(keyPrefix).runRecord("sync-calendar", runId);
        await withRedis(redisUrl, (redis) => redis.hSet(recordKey, "stalls", 2));
        stallCountsBeforeEachStep.push(await withRedis(redisUrl, (redis) => redis.hGet(recordKey, "stalls")));
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
        redis.hGet(engineKeysOf(keyPrefix).runRecord("sync-calendar", runId), "stalls"),
      );

      expect(stallCountsBeforeEachStep).toEqual(["2"]);
      expect(stallCountAfterTheStep).toBe("0");
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);
