import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineScheduledTask, defineTask } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { createNextaskingControl } from "../src/testing.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";
import { withRedis } from "./with-redis.js";

function createReportNextasking(redisUrl: string, keyPrefix: string, sendReportHandler: () => Promise<void>) {
  const { logger, lines } = createRecordingLogger();
  const sendReport = defineTask("send-report", {
    schema: z.object({ reportId: z.string() }),
    handler: sendReportHandler,
  });
  const purgeExpiredSessions = defineScheduledTask("purge-expired-sessions", {
    schedule: { cron: "0 0 1 1 *" },
    handler: async () => undefined,
  });
  const nextasking = createNextasking({
    connection: { url: redisUrl },
    prefix: keyPrefix,
    logger,
    tasks: [sendReport, purgeExpiredSessions],
  });
  return { nextasking, lines };
}

integrationTest(
  "reset removes every run under the prefix, keeps the schedules with their next firing, and leaves the worker running",
  async ({ redisUrl, keyPrefix }) => {
    const neighbourKey = `${keyPrefix}neighbour:runs:kept`;
    const { nextasking } = createReportNextasking(redisUrl, keyPrefix, async () => undefined);
    await using control = createNextaskingControl({ connection: { url: redisUrl }, prefix: keyPrefix });

    try {
      await withRedis(redisUrl, (redis) => redis.set(neighbourKey, "kept"));
      await nextasking.startWorker();
      const nextFiringBeforeTheReset = (await nextasking.listSchedules())[0]?.nextFiringAt;
      const completedReport = await nextasking.dispatch("send-report", { reportId: "report-1" });
      await nextasking.dispatch("send-report", { reportId: "report-2" }, { delay: "1h" });
      await control.waitForIdle({ timeout: "10s" });
      await control.reset();
      const runCountsAfterTheReset = await nextasking.countRuns();
      const schedulesAfterTheReset = await nextasking.listSchedules();
      const { runId: firingRunId } = await control.fireSchedule("purge-expired-sessions");
      await control.waitForIdle({ timeout: "10s" });

      expect(await nextasking.getRun("send-report", completedReport.runId)).toBeUndefined();
      expect(runCountsAfterTheReset["send-report"]).toEqual({
        delayed: 0,
        pending: 0,
        waitingForConcurrencySlot: 0,
        executing: 0,
        suspended: 0,
        backingOff: 0,
        completed: 0,
        failed: 0,
      });
      expect(schedulesAfterTheReset).toEqual([
        { task: "purge-expired-sessions", cron: "0 0 1 1 *", timezone: "UTC", nextFiringAt: nextFiringBeforeTheReset },
      ]);
      expect(await nextasking.getRun("purge-expired-sessions", firingRunId)).toMatchObject({ state: "completed" });
      expect(await withRedis(redisUrl, (redis) => redis.get(neighbourKey))).toBe("kept");
    } finally {
      await withRedis(redisUrl, (redis) => redis.unlink(neighbourKey));
      await closeNextasking(nextasking);
    }
  },
);

integrationTest(
  "reset refuses to run under NODE_ENV=production, and waits for the attempts executing until its timeout",
  async ({ redisUrl, keyPrefix }) => {
    let releaseReport = () => undefined as void;
    const reportReleased = new Promise<void>((resolve) => {
      releaseReport = resolve;
    });
    const { nextasking, lines } = createReportNextasking(redisUrl, keyPrefix, () => reportReleased);
    await using control = createNextaskingControl({ connection: { url: redisUrl }, prefix: keyPrefix });

    try {
      await nextasking.startWorker();
      const { runId } = await nextasking.dispatch("send-report", { reportId: "report-1" });
      await waitForLogMessage(lines, "attempt started");
      vi.stubEnv("NODE_ENV", "production");
      const productionReset = await control.reset().catch((error: unknown) => error);
      vi.unstubAllEnvs();
      const busyReset = await control.reset({ timeout: "1s" }).catch((error: unknown) => error);
      const runAfterTheRefusals = await nextasking.getRun("send-report", runId);
      releaseReport();
      await control.reset({ timeout: "10s" });

      expect(productionReset).toEqual(
        new Error(
          "reset() removes every key under the prefix, so it refuses to run in a process whose NODE_ENV is production",
        ),
      );
      expect(busyReset).toEqual(new Error("reset() found attempts still executing after 1s: 1 executing"));
      expect(runAfterTheRefusals).toMatchObject({ state: "executing" });
      expect(await nextasking.getRun("send-report", runId)).toBeUndefined();
    } finally {
      vi.unstubAllEnvs();
      releaseReport();
      await closeNextasking(nextasking);
    }
  },
);
