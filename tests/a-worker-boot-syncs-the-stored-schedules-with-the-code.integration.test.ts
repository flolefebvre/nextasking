import { expect } from "vitest";
import { z } from "zod";

import { createNextasking, defineScheduledTask, defineTask } from "../src/index.js";
import type { Schedule } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { engineKeysOf } from "../src/engine/engine-keys.js";
import { RECORD_VERSION } from "../src/engine/engine.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";
import type { RecordedLogLine } from "./recording-logger.js";
import { storedSchedules } from "./stored-schedules.js";
import { withRedis } from "./with-redis.js";

const LEFT_UNTOUCHED_INSTANT = Date.UTC(2040, 0, 1, 3);

function scheduledTask(name: string, schedule: Schedule) {
  return defineScheduledTask(name, { schedule, handler: async () => undefined });
}

function scheduleLinesOf(lines: readonly RecordedLogLine[]): RecordedLogLine[] {
  return lines
    .filter((line) => line.message.startsWith("schedule "))
    .sort((first, second) => String(first.fields.task).localeCompare(String(second.fields.task)));
}

integrationTest(
  "each worker boot adds, changes and removes the stored schedules to match the code, updates a retries change alone, and leaves the others untouched",
  async ({ redisUrl, keyPrefix }) => {
    const firstRelease = createRecordingLogger();
    const secondRelease = createRecordingLogger();
    const keys = engineKeysOf(keyPrefix);
    const firstReleaseNextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger: firstRelease.logger,
      tasks: [
        scheduledTask("purge-expired-sessions", { cron: "0 3 * * *" }),
        scheduledTask("send-digest", { cron: "0 8 * * 1" }),
        scheduledTask("send-weekly-report", { cron: "0 9 * * 1" }),
        scheduledTask("rebuild-index", { cron: "0 2 * * *" }),
        scheduledTask("archive-invoices", { cron: "0 1 * * *", timezone: "Europe/Paris" }),
      ],
    });
    const secondReleaseNextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger: secondRelease.logger,
      tasks: [
        scheduledTask("purge-expired-sessions", { cron: "0 3 * * *" }),
        scheduledTask("send-digest", { cron: "0 7 * * 1" }),
        scheduledTask("send-weekly-report", { cron: "0 9 * * 1", timezone: "Europe/Paris" }),
        defineTask("rebuild-index", { schema: z.object({}), handler: async () => undefined }),
        scheduledTask("refresh-sitemap", { cron: "*/30 * * * *" }),
        scheduledTask("compact-logs", { cron: "0 4 * * *" }),
      ],
    });

    try {
      await firstReleaseNextasking.startWorker();
      await closeNextasking(firstReleaseNextasking);
      await withRedis(redisUrl, async (redis) => {
        await redis.hset(keys.schedule("purge-expired-sessions"), "nextFiringAt", LEFT_UNTOUCHED_INSTANT, "retries", 2);
        await redis.zadd(keys.schedules, LEFT_UNTOUCHED_INSTANT, "purge-expired-sessions");
        await redis.hset(keys.schedule("compact-logs"), {
          version: RECORD_VERSION,
          cron: "0 4 * * *",
          timezone: "Europe/Paris",
          nextFiringAt: LEFT_UNTOUCHED_INSTANT,
          retries: 5,
        });
        await redis.zadd(keys.schedules, LEFT_UNTOUCHED_INSTANT, "compact-logs");
      });
      await secondReleaseNextasking.startWorker();

      expect(scheduleLinesOf(firstRelease.lines)).toEqual([
        {
          level: "info",
          fields: { task: "archive-invoices", cron: "0 1 * * *", timezone: "Europe/Paris" },
          message: "schedule added",
        },
        {
          level: "info",
          fields: { task: "purge-expired-sessions", cron: "0 3 * * *", timezone: "UTC" },
          message: "schedule added",
        },
        {
          level: "info",
          fields: { task: "rebuild-index", cron: "0 2 * * *", timezone: "UTC" },
          message: "schedule added",
        },
        {
          level: "info",
          fields: { task: "send-digest", cron: "0 8 * * 1", timezone: "UTC" },
          message: "schedule added",
        },
        {
          level: "info",
          fields: { task: "send-weekly-report", cron: "0 9 * * 1", timezone: "UTC" },
          message: "schedule added",
        },
      ]);
      expect(scheduleLinesOf(secondRelease.lines)).toEqual([
        {
          level: "info",
          fields: { task: "archive-invoices", cron: "0 1 * * *", timezone: "Europe/Paris" },
          message: "schedule removed",
        },
        {
          level: "info",
          fields: { task: "compact-logs", cron: "0 4 * * *", timezone: "UTC" },
          message: "schedule changed",
        },
        {
          level: "info",
          fields: { task: "rebuild-index", cron: "0 2 * * *", timezone: "UTC" },
          message: "schedule removed",
        },
        {
          level: "info",
          fields: { task: "refresh-sitemap", cron: "*/30 * * * *", timezone: "UTC" },
          message: "schedule added",
        },
        {
          level: "info",
          fields: { task: "send-digest", cron: "0 7 * * 1", timezone: "UTC" },
          message: "schedule changed",
        },
        {
          level: "info",
          fields: { task: "send-weekly-report", cron: "0 9 * * 1", timezone: "Europe/Paris" },
          message: "schedule changed",
        },
      ]);
      expect(secondRelease.lines.map((line) => line.message).indexOf("worker started")).toBeGreaterThan(
        secondRelease.lines.map((line) => line.message).lastIndexOf("schedule added"),
      );
      const nextFiringAfterTheBoot = expect.not.stringMatching(`^${String(LEFT_UNTOUCHED_INSTANT)}$`);
      expect(await storedSchedules(redisUrl, keyPrefix)).toEqual([
        expect.objectContaining({
          task: "compact-logs",
          cron: "0 4 * * *",
          timezone: "UTC",
          nextFiringAt: nextFiringAfterTheBoot,
        }),
        expect.objectContaining({
          task: "purge-expired-sessions",
          cron: "0 3 * * *",
          timezone: "UTC",
          nextFiringAt: String(LEFT_UNTOUCHED_INSTANT),
          retries: "5",
        }),
        expect.objectContaining({ task: "refresh-sitemap", cron: "*/30 * * * *", timezone: "UTC" }),
        expect.objectContaining({ task: "send-digest", cron: "0 7 * * 1", timezone: "UTC" }),
        expect.objectContaining({ task: "send-weekly-report", cron: "0 9 * * 1", timezone: "Europe/Paris" }),
      ]);
    } finally {
      await closeNextasking(firstReleaseNextasking);
      await closeNextasking(secondReleaseNextasking);
    }
  },
);
