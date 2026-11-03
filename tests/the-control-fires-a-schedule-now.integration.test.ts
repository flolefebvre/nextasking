import { expect } from "vitest";

import { createNextasking, defineScheduledTask } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { createNextaskingControl } from "../src/testing.js";
import { integrationTest } from "./integration-test.js";

type FiredRun = { runId: string; scheduledAt: Date | undefined; attempt: number };

integrationTest(
  "fireSchedule starts one run of a scheduled task now, under the id and scheduledAt of its instant, and throws for a task with no stored schedule",
  async ({ redisUrl, keyPrefix }) => {
    const firedRuns: FiredRun[] = [];
    const purgeExpiredSessions = defineScheduledTask("purge:expired-sessions", {
      retries: 0,
      schedule: { cron: "0 0 1 1 *" },
      handler: async (context) => {
        firedRuns.push({ runId: context.runId, scheduledAt: context.scheduledAt, attempt: context.attempt });
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      tasks: [purgeExpiredSessions],
    });
    await using control = createNextaskingControl({ connection: { url: redisUrl }, prefix: keyPrefix });

    try {
      await nextasking.startWorker();
      const firedAt = Date.now();
      const { runId } = await control.fireSchedule("purge:expired-sessions");
      await control.waitForIdle({ timeout: "10s" });
      const unscheduledFiring = control.fireSchedule("send-welcome-email");

      await expect(unscheduledFiring).rejects.toThrow('No schedule is stored for task "send-welcome-email"');
      expect(firedRuns).toEqual([{ runId, scheduledAt: expect.any(Date), attempt: 1 }]);
      expect(firedRuns[0]?.scheduledAt?.getTime()).toBeGreaterThanOrEqual(firedAt);
      expect(firedRuns[0]?.scheduledAt?.getTime()).toBeLessThanOrEqual(Date.now());
      expect(await nextasking.getRun("purge:expired-sessions", runId)).toMatchObject({ state: "completed" });
      expect((await nextasking.listSchedules())[0]?.nextFiringAt?.getTime()).toBeGreaterThan(Date.now());
    } finally {
      await closeNextasking(nextasking);
    }
  },
);
