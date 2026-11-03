import { expect } from "vitest";
import { z } from "zod";

import { createNextasking, defineScheduledTask, defineTask } from "../src/index.js";
import type { Schedule } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { FIRING_JOB_NAME } from "../src/run-queue.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";
import type { RecordedLogLine } from "./recording-logger.js";
import { storedSchedulers, withRunQueue } from "./with-run-queue.js";

function scheduledTask(name: string, schedule: Schedule) {
  return defineScheduledTask(name, { schedule, handler: async () => undefined });
}

function scheduleLinesOf(lines: readonly RecordedLogLine[]): RecordedLogLine[] {
  return lines
    .filter((line) => line.message.startsWith("schedule "))
    .sort((first, second) => String(first.fields.task).localeCompare(String(second.fields.task)));
}

integrationTest(
  "each worker boot adds, changes and removes the stored schedules to match the code, and leaves the others untouched",
  async ({ redisUrl, keyPrefix }) => {
    const firstRelease = createRecordingLogger();
    const secondRelease = createRecordingLogger();
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
      await withRunQueue(redisUrl, keyPrefix, async (runQueue) => {
        await runQueue.upsertJobScheduler(
          "purge-expired-sessions",
          { pattern: "0 3 * * *", tz: "UTC" },
          { name: FIRING_JOB_NAME, data: { taskName: "left untouched" } },
        );
        await runQueue.upsertJobScheduler(
          "compact-logs",
          { pattern: "0 4 * * *" },
          { name: FIRING_JOB_NAME, data: { taskName: "compact-logs" } },
        );
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
      expect(await storedSchedulers(redisUrl, keyPrefix)).toEqual([
        {
          taskName: "compact-logs",
          cron: "0 4 * * *",
          timezone: "UTC",
          firingTemplateData: { taskName: "compact-logs" },
        },
        {
          taskName: "purge-expired-sessions",
          cron: "0 3 * * *",
          timezone: "UTC",
          firingTemplateData: { taskName: "left untouched" },
        },
        {
          taskName: "refresh-sitemap",
          cron: "*/30 * * * *",
          timezone: "UTC",
          firingTemplateData: { taskName: "refresh-sitemap" },
        },
        {
          taskName: "send-digest",
          cron: "0 7 * * 1",
          timezone: "UTC",
          firingTemplateData: { taskName: "send-digest" },
        },
        {
          taskName: "send-weekly-report",
          cron: "0 9 * * 1",
          timezone: "Europe/Paris",
          firingTemplateData: { taskName: "send-weekly-report" },
        },
      ]);
    } finally {
      await closeNextasking(firstReleaseNextasking);
      await closeNextasking(secondReleaseNextasking);
    }
  },
);
