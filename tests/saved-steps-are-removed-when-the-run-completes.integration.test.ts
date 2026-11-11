import { expect } from "vitest";
import { z } from "zod";

import { engineKeysOf } from "../src/engine/engine-keys.js";
import { createNextasking, defineTask } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";
import { savedSteps } from "./saved-steps.js";
import type { SavedSteps } from "./saved-steps.js";
import { withRedis } from "./with-redis.js";

integrationTest(
  "a run's saved steps live in a hash next to its job while it runs, and the hash is removed when it completes",
  { timeout: 15_000 },
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const savedStepsSeenByHandler: SavedSteps[] = [];
    const exportReportTask = defineTask("export-report", {
      schema: z.object({ reportId: z.string() }),
      handler: async ({ step, runId }) => {
        await step.run("render", () => ({ pages: 3 }));
        await step.run("upload", async () => {});
        savedStepsSeenByHandler.push(await savedSteps(redisUrl, keyPrefix, "export-report", runId));
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [exportReportTask],
    });

    try {
      await nextasking.startWorker();
      const { runId } = await nextasking.dispatch("export-report", { reportId: "report-7" });
      await waitForLogMessage(lines, "run completed");
      const stepsHashCountOnceCompleted = await withRedis(redisUrl, (redis) =>
        redis.exists(engineKeysOf(keyPrefix).runSteps("export-report", runId)),
      );

      expect(stepsHashCountOnceCompleted).toBe(0);
      expect(savedStepsSeenByHandler).toHaveLength(1);
      expect(savedStepsSeenByHandler[0]?.serializedResultsByStepId).toEqual({
        render: '[{"pages":3}]',
        upload: "[]",
      });
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);
