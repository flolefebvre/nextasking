import { expect, vi } from "vitest";
import { CronExpressionParser } from "cron-parser";
import { z } from "zod";

import { createNextasking, defineScheduledTask, defineTask } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";
import { moveNextFiringTo } from "./stored-schedules.js";

integrationTest(
  "listSchedules shows each scheduled task's next firing, and once it has fired, the run id of its previous firing",
  { timeout: 20_000 },
  async ({ redisUrl, keyPrefix }) => {
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      tasks: [
        defineScheduledTask("purge-expired-sessions", {
          schedule: { cron: "0 3 * * *", timezone: "Europe/Paris" },
          handler: async () => undefined,
        }),
        defineScheduledTask("send-digest", { schedule: { cron: "0 8 * * 1" }, handler: async () => undefined }),
        defineTask("send-reminder", { schema: z.object({}), handler: async () => undefined }),
      ],
    });

    try {
      const schedulesBeforeAnyWorker = await nextasking.listSchedules();
      await nextasking.startWorker();
      const schedulesBeforeAFiring = await nextasking.listSchedules();
      const plannedInstant = CronExpressionParser.parse("0 3 * * *", { tz: "Europe/Paris", currentDate: new Date() })
        .prev()
        .getTime();
      await moveNextFiringTo(redisUrl, keyPrefix, "purge-expired-sessions", plannedInstant);
      await vi.waitFor(
        async () => {
          const [purgeSchedule] = await nextasking.listSchedules();
          expect(purgeSchedule?.previousFiring).toBeDefined();
          expect(
            (await nextasking.getRun("purge-expired-sessions", purgeSchedule?.previousFiring?.runId ?? ""))?.state,
          ).toBe("completed");
        },
        { timeout: 10_000 },
      );

      expect(schedulesBeforeAnyWorker).toEqual([
        { task: "purge-expired-sessions", cron: "0 3 * * *", timezone: "Europe/Paris" },
        { task: "send-digest", cron: "0 8 * * 1", timezone: "UTC" },
      ]);
      expect(schedulesBeforeAFiring).toEqual([
        {
          task: "purge-expired-sessions",
          cron: "0 3 * * *",
          timezone: "Europe/Paris",
          nextFiringAt: expect.any(Date),
        },
        { task: "send-digest", cron: "0 8 * * 1", timezone: "UTC", nextFiringAt: expect.any(Date) },
      ]);
      const [purgeSchedule] = await nextasking.listSchedules();
      expect(purgeSchedule?.previousFiring?.scheduledAt).toEqual(new Date(plannedInstant));
      expect(purgeSchedule?.nextFiringAt?.getTime()).toBeGreaterThan(Date.now());
      expect(schedulesBeforeAFiring[0]?.nextFiringAt?.getTime()).toBeGreaterThan(plannedInstant);
    } finally {
      await closeNextasking(nextasking);
    }
  },
);
