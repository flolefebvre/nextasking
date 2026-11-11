import { expect, vi } from "vitest";
import { z } from "zod";

import { engineKeysOf } from "../src/engine/engine-keys.js";
import { createNextasking, defineTask } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";
import { savedSteps } from "./saved-steps.js";
import { runLeftExecutingSentenceOf } from "./stored-key-violations.js";
import { withRedis } from "./with-redis.js";

integrationTest(
  "a step is refused, read or write, once the run's record holds another attempt's lock token",
  { timeout: 15_000 },
  async ({ redisUrl, keyPrefix, expectInvariantViolation }) => {
    const stepErrors: unknown[] = [];
    const sendNewsletterTask = defineTask("send-newsletter", {
      schema: z.object({ issueId: z.string() }),
      retries: 0,
      handler: async ({ step, runId }) => {
        await step.run("pick-recipients", () => ["ada@example.com"]);
        const recordKey = engineKeysOf(keyPrefix).runRecord("send-newsletter", runId);
        expectInvariantViolation("run left executing", runLeftExecutingSentenceOf("send-newsletter", runId));
        await withRedis(redisUrl, (redis) => redis.hSet(recordKey, "lockToken", "token-of-another-attempt"));
        await step
          .run("send", () => "sent")
          .catch((error: unknown) => {
            stepErrors.push(error);
            throw error;
          });
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger: createRecordingLogger().logger,
      tasks: [sendNewsletterTask],
    });

    try {
      await nextasking.startWorker();
      const { runId } = await nextasking.dispatch("send-newsletter", { issueId: "issue-7" });
      await vi.waitFor(
        () => {
          expect(stepErrors).toHaveLength(1);
        },
        { timeout: 10_000 },
      );
      const steps = await savedSteps(redisUrl, keyPrefix, "send-newsletter", runId);

      expect(stepErrors[0]).toMatchObject({
        name: "LostRunLockError",
        message: `Step "send" of run "${runId}" was not read: this attempt no longer holds its lock, so another attempt owns it now.`,
      });
      expect(steps.serializedResultsByStepId).toEqual({ "pick-recipients": '[["ada@example.com"]]' });
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);
