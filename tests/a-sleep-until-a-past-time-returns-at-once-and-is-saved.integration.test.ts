import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";
import { savedSteps } from "./saved-steps.js";
import { storedRun } from "./stored-run.js";

integrationTest(
  "a sleepUntil time in the past returns at once, without suspending, and is saved with its wake time",
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const trialEndedAt = new Date(Date.now() - 60_000);
    const closeTrial = defineTask("close-trial", {
      schema: z.object({ accountId: z.string() }),
      retries: 0,
      handler: async ({ step }) => {
        await step.sleepUntil("wait-for-trial-end", trialEndedAt);
        throw new Error("fail for good, keeping the saved steps to read them");
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [closeTrial],
    });

    try {
      await nextasking.startWorker();
      const { runId } = await nextasking.dispatch("close-trial", { accountId: "acc-1" });
      await vi.waitFor(
        async () => {
          expect(await storedRun(redisUrl, keyPrefix, "close-trial", runId)).toMatchObject({
            reason: "retries exhausted",
            errors: [{ attempt: 1, message: "fail for good, keeping the saved steps to read them" }],
          });
        },
        { timeout: 10_000 },
      );
      const { serializedResultsByStepId } = await savedSteps(redisUrl, keyPrefix, "close-trial", runId);

      expect(lines.map((line) => line.message)).not.toContain("run suspended");
      expect(serializedResultsByStepId).toEqual({
        "wait-for-trial-end": JSON.stringify({ kind: "sleep", wakeAt: trialEndedAt.getTime(), attempt: 1 }),
      });
    } finally {
      await closeNextasking(nextasking);
    }
  },
);
