import { expect } from "vitest";
import { z } from "zod";

import { engineKeysOf } from "../src/engine/engine-keys.js";
import { createNextasking, defineTask } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";
import { storedRun } from "./stored-run.js";
import { withRedis } from "./with-redis.js";

integrationTest(
  "a failed attempt whose run's record holds another attempt's lock token leaves that attempt's errors and state untouched",
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
    const keys = engineKeysOf(keyPrefix);
    const sendNewsletter = defineTask("send-newsletter", {
      schema: z.object({ issueId: z.string() }),
      retries: 1,
      handler: async ({ runId }) => {
        await withRedis(redisUrl, async (redis) => {
          await redis.hset(keys.runRecord("send-newsletter", runId), "lockToken", "token-of-the-newer-attempt");
          await redis.rpush(keys.runErrors("send-newsletter", runId), JSON.stringify(newerAttemptError));
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
      await waitForLogMessage(lines, "attempt outcome discarded: lock lost");
      const run = await storedRun(redisUrl, keyPrefix, "send-newsletter", runId);

      expect(run).toMatchObject({
        state: "executing",
        lockToken: "token-of-the-newer-attempt",
        errors: [newerAttemptError],
      });
      expect(lines).toContainEqual({
        level: "warn",
        fields: { task: "send-newsletter", runId, attempt: 1, outcome: "retry" },
        message: "attempt outcome discarded: lock lost",
      });
      expect(lines.map((line) => line.message)).not.toContain("attempt failed");
    } finally {
      await closeNextasking(nextasking);
    }
  },
);
