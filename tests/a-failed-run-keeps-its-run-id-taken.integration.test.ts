import { expect } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";
import { storedRunIdentities } from "./stored-run-identities.js";

integrationTest(
  "a repeat of a failed run's runId is dropped: a final failure doesn't reopen the dedup window",
  { timeout: 15_000 },
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    let attemptCount = 0;
    const importContacts = defineTask("import-contacts", {
      schema: z.object({ fileId: z.string() }),
      retries: 0,
      handler: async () => {
        attemptCount += 1;
        throw new Error("file unreadable");
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [importContacts],
    });
    const runId = "import-contacts-file-7";

    try {
      await nextasking.startWorker();
      await nextasking.dispatch("import-contacts", { fileId: "file-7" }, { runId });
      await waitForLogMessage(lines, "run failed");
      const repeat = await nextasking.dispatch("import-contacts", { fileId: "file-7" }, { runId });

      expect(repeat).toEqual({ runId });
      expect(lines.filter((line) => line.message.includes("dispatch"))).toEqual([
        { level: "debug", fields: { task: "import-contacts", runId }, message: "run dispatched" },
        { level: "debug", fields: { task: "import-contacts", runId }, message: "duplicate dispatch dropped" },
      ]);
      expect(await storedRunIdentities(redisUrl, keyPrefix)).toEqual([{ task: "import-contacts", runId }]);
      expect(attemptCount).toBe(1);
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);
