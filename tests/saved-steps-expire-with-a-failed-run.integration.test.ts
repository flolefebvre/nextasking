import { expect } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask, NonRetriableError } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";
import { savedSteps } from "./saved-steps.js";

const FOURTEEN_DAYS_IN_MILLISECONDS = 14 * 24 * 60 * 60 * 1000;

integrationTest(
  "a run that fails for good keeps its saved steps until they expire with the failed-run retention of 14 days",
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const importContacts = defineTask("import-contacts", {
      schema: z.object({ fileId: z.string() }),
      handler: async ({ step }) => {
        await step.run("parse-file", () => ({ contactCount: 2 }));
        throw new NonRetriableError("file holds no email column");
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [importContacts],
    });

    try {
      await nextasking.startWorker();
      const { runId } = await nextasking.dispatch("import-contacts", { fileId: "file-7" });
      await waitForLogMessage(lines, "run failed");
      const steps = await savedSteps(redisUrl, keyPrefix, "import-contacts", runId);

      expect(steps.serializedResultsByStepId).toEqual({ "parse-file": '[{"contactCount":2}]' });
      expect(steps.timeToLiveInMilliseconds).toBeGreaterThan(FOURTEEN_DAYS_IN_MILLISECONDS - 60_000);
      expect(steps.timeToLiveInMilliseconds).toBeLessThanOrEqual(FOURTEEN_DAYS_IN_MILLISECONDS);
    } finally {
      await closeNextasking(nextasking);
    }
  },
);
