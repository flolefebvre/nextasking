import { expect } from "vitest";
import { z } from "zod";

import { deriveJobId } from "../src/ids.js";
import { createNextasking, defineTask } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";
import { runJobKey } from "./run-job-key.js";
import { storedRun } from "./stored-run.js";
import { withRedis } from "./with-redis.js";

integrationTest(
  "a failed attempt whose lock key <prefix>:runs:<jobId>:lock holds another attempt's token leaves that attempt's run data untouched",
  { timeout: 15_000 },
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const newerAttemptError = {
      attempt: 1,
      name: "Error",
      message: "the newer attempt failed",
      stack: "Error: the newer attempt failed",
      failedAt: "2026-11-03T10:00:00.000Z",
    };
    const newerAttemptDelayedRunState = { state: "backingOff", nextAttemptAt: "2026-11-03T10:00:30.000Z" };
    const sendNewsletter = defineTask("send-newsletter", {
      schema: z.object({ issueId: z.string() }),
      retries: 1,
      handler: async ({ runId }) => {
        const lockKey = `${keyPrefix}:runs:${deriveJobId("send-newsletter", runId)}:lock`;
        await withRedis(redisUrl, async (redis) => {
          const jobKey = runJobKey(keyPrefix, "send-newsletter", runId);
          const storedData = JSON.parse((await redis.hget(jobKey, "data")) ?? "{}") as Record<string, unknown>;
          await redis.set(lockKey, "token-of-the-newer-attempt", "PX", 60_000);
          await redis.hset(
            jobKey,
            "data",
            JSON.stringify({
              ...storedData,
              errors: [newerAttemptError],
              delayedRunState: newerAttemptDelayedRunState,
            }),
          );
        });
        throw new Error("the stale attempt failed");
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [sendNewsletter],
    });

    try {
      await nextasking.startWorker();
      const { runId } = await nextasking.dispatch("send-newsletter", { issueId: "issue-7" });
      await waitForLogMessage(lines, "attempt aborted: lock lost");
      const run = await storedRun(redisUrl, keyPrefix, "send-newsletter", runId);

      expect(run).toMatchObject({ errors: [newerAttemptError], delayedRunState: newerAttemptDelayedRunState });
      expect(lines).toContainEqual({
        level: "debug",
        fields: { task: "send-newsletter", runId, attempt: 1 },
        message: "attempt aborted: lock lost",
      });
      expect(lines.map((line) => line.message)).not.toContain("attempt failed");
    } finally {
      await closeNextasking(nextasking);
    }
  },
);
