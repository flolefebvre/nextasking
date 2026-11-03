import { expect, vi } from "vitest";
import { z } from "zod";

import { NonRetriableError, createNextasking, defineListener, defineTask, runFailed } from "../src/index.js";
import type { RunFailedPayload } from "../src/index.js";
import { deriveFailureNoticeEventId, deriveJobId } from "../src/ids.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";
import { runJobKey, runQueueKeyPrefix } from "./run-job-key.js";
import { storedRun } from "./stored-run.js";
import { withRedis } from "./with-redis.js";

const MORE_THAN_A_DAY_IN_MILLISECONDS = 25 * 60 * 60 * 1000;

const syncAccount = defineTask("sync-account", {
  schema: z.object({ accountId: z.string() }),
  retries: 0,
  handler: async () => {
    throw new NonRetriableError("account deleted upstream");
  },
});

async function forgetFailureNotice(redisUrl: string, keyPrefix: string, runId: string, failedAt?: number) {
  await withRedis(redisUrl, async (redis) => {
    const jobKey = runJobKey(keyPrefix, "sync-account", runId);
    const { notifiedFailureCount, ...failedRunWithoutMarker } = JSON.parse(
      (await redis.hget(jobKey, "data")) ?? "{}",
    ) as {
      notifiedFailureCount?: number;
    };
    expect(notifiedFailureCount).toBe(1);
    await redis.hset(jobKey, "data", JSON.stringify(failedRunWithoutMarker));
    if (failedAt !== undefined) {
      await redis.zadd(`${runQueueKeyPrefix(keyPrefix)}failed`, "XX", failedAt, deriveJobId("sync-account", runId));
    }
  });
}

integrationTest(
  "a worker boot re-sends the unsent notices of the last 24 hours of failures, and only those",
  async ({ redisUrl, keyPrefix }) => {
    const { logger } = createRecordingLogger();
    const runFailedNotices: RunFailedPayload[] = [];
    const crashedWorker = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [syncAccount],
    });
    const bootedWorker = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [
        syncAccount,
        defineListener("alert-on-run-failed", {
          on: runFailed,
          handler: async ({ payload }) => {
            runFailedNotices.push(payload);
          },
        }),
      ],
    });
    const runIds = ["recent-unsent", "old-unsent", "recent-sent"];

    try {
      await crashedWorker.startWorker();
      for (const runId of runIds) {
        await crashedWorker.dispatch("sync-account", { accountId: runId }, { runId });
      }
      await vi.waitFor(
        async () => {
          for (const runId of runIds) {
            expect(await storedRun(redisUrl, keyPrefix, "sync-account", runId)).toMatchObject({
              notifiedFailureCount: 1,
            });
          }
        },
        { timeout: 10_000 },
      );
      await closeNextasking(crashedWorker);
      await forgetFailureNotice(redisUrl, keyPrefix, "recent-unsent");
      await forgetFailureNotice(redisUrl, keyPrefix, "old-unsent", Date.now() - MORE_THAN_A_DAY_IN_MILLISECONDS);

      await bootedWorker.startWorker();
      await vi.waitFor(
        () => {
          expect(runFailedNotices).toHaveLength(1);
        },
        { timeout: 10_000 },
      );

      expect(runFailedNotices).toEqual([expect.objectContaining({ task: "sync-account", runId: "recent-unsent" })]);
      expect(await storedRun(redisUrl, keyPrefix, "sync-account", "recent-unsent")).toMatchObject({
        notifiedFailureCount: 1,
      });
      for (const runId of ["old-unsent", "recent-sent"]) {
        const noticeEvent = await bootedWorker.getEvent(
          "nextasking/run.failed",
          deriveFailureNoticeEventId("sync-account", runId, 1),
        );
        expect(noticeEvent.fanOut).toBeUndefined();
      }
    } finally {
      await closeNextasking(crashedWorker);
      await closeNextasking(bootedWorker);
    }
  },
);
