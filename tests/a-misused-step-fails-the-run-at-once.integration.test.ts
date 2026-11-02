import { expect } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";
import { savedSteps } from "./saved-steps.js";
import { storedRun } from "./stored-run.js";

integrationTest(
  "a repeated step id the handler catches still fails the run at once, with its retries left unused",
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const notifyFollowers = defineTask("notify-followers", {
      schema: z.object({ postId: z.string() }),
      backoff: "0s",
      handler: async ({ step }) => {
        for (const followerId of ["follower-1", "follower-2"]) {
          await step.run("notify", () => followerId).catch(() => undefined);
        }
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [notifyFollowers],
    });

    try {
      await nextasking.startWorker();
      const { runId } = await nextasking.dispatch("notify-followers", { postId: "post-7" });
      await waitForLogMessage(lines, "run failed");
      const run = await storedRun(redisUrl, keyPrefix, "notify-followers", runId);

      expect(lines.filter((line) => line.message === "attempt started")).toHaveLength(1);
      expect(run).toMatchObject({
        reason: "non-retriable",
        errors: [
          {
            attempt: 1,
            name: "StepMisuseError",
            message:
              'Step id "notify" is used twice in one run of task "notify-followers". Give each step.run call its own id.',
          },
        ],
      });
    } finally {
      await closeNextasking(nextasking);
    }
  },
);

integrationTest(
  "a step result over 1 MiB fails the run with the misuse, not the handler's later error, and is not saved",
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const buildExport = defineTask("build-export", {
      schema: z.object({ exportId: z.string() }),
      backoff: "0s",
      handler: async ({ step }) => {
        await step.run("count-rows", () => 3);
        await step.run("load-rows", () => "x".repeat(2 * 1024 * 1024)).catch(() => undefined);
        throw new Error("export is empty");
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [buildExport],
    });

    try {
      await nextasking.startWorker();
      const { runId } = await nextasking.dispatch("build-export", { exportId: "export-7" });
      await waitForLogMessage(lines, "run failed");
      const run = await storedRun(redisUrl, keyPrefix, "build-export", runId);
      const steps = await savedSteps(redisUrl, keyPrefix, "build-export", runId);

      expect(run).toMatchObject({
        reason: "non-retriable",
        errors: [{ attempt: 1, name: "StepMisuseError", message: expect.stringContaining('"load-rows"') }],
      });
      expect(steps.serializedResultsByStepId).toEqual({ "count-rows": "[3]" });
    } finally {
      await closeNextasking(nextasking);
    }
  },
);
