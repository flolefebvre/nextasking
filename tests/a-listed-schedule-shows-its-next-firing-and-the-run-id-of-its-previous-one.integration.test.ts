import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineScheduledTask, defineTask } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";
import { promoteNextFiring } from "./with-run-queue.js";

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
      const plannedInstant = await promoteNextFiring(redisUrl, keyPrefix, "purge-expired-sessions");
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
          nextFiringAt: new Date(plannedInstant),
        },
        { task: "send-digest", cron: "0 8 * * 1", timezone: "UTC", nextFiringAt: expect.any(Date) },
      ]);
      const [purgeSchedule] = await nextasking.listSchedules();
      expect(purgeSchedule?.previousFiring?.scheduledAt).toEqual(new Date(plannedInstant));
      expect(purgeSchedule?.nextFiringAt?.getTime()).toBeGreaterThan(plannedInstant);
    } finally {
      await closeNextasking(nextasking);
    }
  },
);
