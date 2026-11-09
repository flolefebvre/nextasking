import { expect } from "vitest";
import { z } from "zod";

import { createEngine } from "../src/engine/engine.js";
import { createNextasking, defineTask } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";
import { storedRunIdentities } from "./stored-run-identities.js";
import { storedSchedules } from "./stored-schedules.js";
import { withRedisClient } from "./with-redis.js";

integrationTest(
  "a due schedule left by an earlier release for a task this release no longer schedules is removed at boot and starts no run",
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const handledPayloads: unknown[] = [];
    const rebuildIndex = defineTask("rebuild-index", {
      schema: z.object({}),
      handler: async ({ payload }) => {
        handledPayloads.push(payload);
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [rebuildIndex],
    });
    await withRedisClient(redisUrl, (client) =>
      createEngine(client, keyPrefix).syncSchedules({
        schedulesToSync: [
          {
            task: "rebuild-index",
            cron: "0 2 * * *",
            timezone: "UTC",
            nextFiringAt: Date.UTC(2026, 10, 3, 2, 0, 0),
            retries: 5,
          },
        ],
      }),
    );

    try {
      await nextasking.startWorker();
      await waitForLogMessage(lines, "worker started");

      expect(lines.filter((line) => line.message.startsWith("schedule "))).toEqual([
        {
          level: "info",
          fields: { task: "rebuild-index", cron: "0 2 * * *", timezone: "UTC" },
          message: "schedule removed",
        },
      ]);
      expect(await storedSchedules(redisUrl, keyPrefix)).toEqual([]);
      expect(await storedRunIdentities(redisUrl, keyPrefix)).toEqual([]);
      expect(handledPayloads).toEqual([]);
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);
