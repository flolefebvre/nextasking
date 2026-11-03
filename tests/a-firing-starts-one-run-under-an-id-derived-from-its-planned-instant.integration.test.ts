import { expect, vi } from "vitest";

import { createNextasking, defineScheduledTask } from "../src/index.js";
import { deriveFiringRunId } from "../src/ids.js";
import { closeNextasking } from "../src/registry.js";
import { FIRING_JOB_NAME } from "../src/run-queue.js";
import { integrationTest } from "./integration-test.js";
import { storedJobNames } from "./stored-job-names.js";
import { storedRun } from "./stored-run.js";
import { promoteNextFiring, withRunQueue } from "./with-run-queue.js";

type RecordedFiringRun = { runId: string; scheduledAt: Date | undefined; attempt: number };

integrationTest(
  "a firing starts one run whose id and scheduledAt come from the planned instant, not the time it was processed, and a repeat of it starts no other run",
  async ({ redisUrl, keyPrefix }) => {
    const recordedRuns: RecordedFiringRun[] = [];
    const purgeExpiredSessions = defineScheduledTask("purge-expired-sessions", {
      schedule: { cron: "0 0 1 1 *", timezone: "Europe/Paris" },
      handler: async ({ runId, scheduledAt, attempt }) => {
        recordedRuns.push({ runId, scheduledAt, attempt });
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      tasks: [purgeExpiredSessions],
    });

    try {
      await nextasking.startWorker();
      const plannedInstant = await promoteNextFiring(redisUrl, keyPrefix, "purge-expired-sessions");
      await vi.waitFor(
        () => {
          expect(recordedRuns).toHaveLength(1);
        },
        { timeout: 10_000 },
      );
      const firingRunId = deriveFiringRunId("purge-expired-sessions", plannedInstant);
      const repeatedFiringFinishedOn = await withRunQueue(redisUrl, keyPrefix, async (runQueue) => {
        const completedJobs = await runQueue.getCompleted(0, -1);
        const firingJob = completedJobs.find((completedJob) => completedJob.name === FIRING_JOB_NAME);
        const firstFinishedOn = firingJob?.finishedOn ?? 0;
        await firingJob?.retry("completed");
        await vi.waitFor(
          async () => {
            const repeatedFiringJob = await runQueue.getJob(firingJob?.id ?? "");
            expect(repeatedFiringJob?.finishedOn).toBeGreaterThan(firstFinishedOn);
          },
          { timeout: 10_000 },
        );
        return firstFinishedOn;
      });

      expect(plannedInstant).toBeGreaterThan(Date.now());
      expect(new Date(plannedInstant).toISOString()).toMatch(/^\d{4}-12-31T23:00:00\.000Z$/);
      expect(recordedRuns).toEqual([{ runId: firingRunId, scheduledAt: new Date(plannedInstant), attempt: 1 }]);
      expect(await storedRun(redisUrl, keyPrefix, "purge-expired-sessions", firingRunId)).toMatchObject({
        runId: firingRunId,
        scheduledAt: new Date(plannedInstant).toISOString(),
      });
      expect(repeatedFiringFinishedOn).toBeGreaterThan(0);
      expect(await storedJobNames(redisUrl, keyPrefix)).toEqual(["purge-expired-sessions"]);
    } finally {
      await closeNextasking(nextasking);
    }
  },
);
