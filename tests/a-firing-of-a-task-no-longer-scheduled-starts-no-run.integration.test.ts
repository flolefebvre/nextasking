import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { FIRING_JOB_NAME } from "../src/run-queue.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";
import { storedJobNames } from "./stored-job-names.js";
import { withRunQueue } from "./with-run-queue.js";

integrationTest(
  "a firing left by an earlier release for a task this release no longer schedules completes, starts no run and logs at debug",
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
    const plannedInstant = Date.UTC(2026, 10, 3, 2, 0, 0);

    try {
      await nextasking.startWorker();
      const firingJobState = await withRunQueue(redisUrl, keyPrefix, async (runQueue) => {
        const firingJob = await runQueue.add(
          FIRING_JOB_NAME,
          { taskName: "rebuild-index" },
          { jobId: `repeat:rebuild-index:${String(plannedInstant)}` },
        );
        await vi.waitFor(
          async () => {
            expect(await firingJob.getState()).toBe("completed");
          },
          { timeout: 10_000 },
        );
        return firingJob.getState();
      });

      expect(firingJobState).toBe("completed");
      expect(lines.filter((line) => line.message === "firing dropped: task not scheduled")).toEqual([
        {
          level: "debug",
          fields: { task: "rebuild-index", scheduledAt: "2026-11-03T02:00:00.000Z" },
          message: "firing dropped: task not scheduled",
        },
      ]);
      expect(await storedJobNames(redisUrl, keyPrefix)).toEqual([]);
      expect(handledPayloads).toEqual([]);
    } finally {
      await closeNextasking(nextasking);
    }
  },
);
