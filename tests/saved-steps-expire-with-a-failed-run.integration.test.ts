import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask, NonRetriableError } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { backdateEndedRun, engineTestContextOf } from "./engine-test.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";
import { savedSteps } from "./saved-steps.js";
import { storedRun } from "./stored-run.js";
import { withRedis } from "./with-redis.js";

const FIFTEEN_DAYS_IN_MILLISECONDS = 15 * 24 * 60 * 60 * 1000;

integrationTest(
  "a run that fails for good keeps its saved steps until the failed-run retention of 14 days removes it with them",
  { timeout: 25_000 },
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const importContactsTask = defineTask("import-contacts", {
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
      tasks: [importContactsTask],
    });

    try {
      await nextasking.startWorker();
      const { runId } = await nextasking.dispatch("import-contacts", { fileId: "file-7" });
      await waitForLogMessage(lines, "run failed");
      const stepsOfTheFailedRun = await savedSteps(redisUrl, keyPrefix, "import-contacts", runId);
      await withRedis(redisUrl, (redis, client) =>
        backdateEndedRun(
          engineTestContextOf(redis, client, keyPrefix),
          "import-contacts",
          runId,
          FIFTEEN_DAYS_IN_MILLISECONDS,
        ),
      );
      await vi.waitFor(
        async () => {
          expect(await storedRun(redisUrl, keyPrefix, "import-contacts", runId)).toBeUndefined();
        },
        { timeout: 10_000 },
      );

      expect(stepsOfTheFailedRun.serializedResultsByStepId).toEqual({ "parse-file": '[{"contactCount":2}]' });
      expect((await savedSteps(redisUrl, keyPrefix, "import-contacts", runId)).serializedResultsByStepId).toEqual({});
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);
