import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";
import { storedJobNames } from "./stored-job-names.js";

integrationTest(
  "a repeated runId returns the first id and starts one run, while the same runId on another task starts its own",
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const recordedEmails: string[] = [];
    const sendWelcomeEmail = defineTask("send-welcome-email", {
      schema: z.object({ email: z.string() }),
      handler: async ({ payload }) => {
        recordedEmails.push(payload.email);
      },
    });
    const purgeSessions = defineTask("purge-sessions", { schema: z.object({}), handler: async () => undefined });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [sendWelcomeEmail, purgeSessions],
    });
    const runId = "welcome-user-7";

    try {
      const firstDispatch = await nextasking.dispatch("send-welcome-email", { email: "first@example.com" }, { runId });
      const pendingRepeat = await nextasking.dispatch("send-welcome-email", { email: "second@example.com" }, { runId });
      await nextasking.startWorker();
      await vi.waitFor(
        () => {
          expect(recordedEmails).toEqual(["first@example.com"]);
        },
        { timeout: 10_000 },
      );
      const completedRepeat = await nextasking.dispatch(
        "send-welcome-email",
        { email: "third@example.com" },
        { runId },
      );
      await nextasking.dispatch("purge-sessions", {}, { runId });

      expect([firstDispatch, pendingRepeat, completedRepeat]).toEqual([{ runId }, { runId }, { runId }]);
      expect(await storedJobNames(redisUrl, keyPrefix)).toEqual(["purge-sessions", "send-welcome-email"]);
      const dispatchLines = lines.filter((line) => line.message.includes("dispatch"));
      expect(dispatchLines).toEqual([
        { level: "debug", fields: { task: "send-welcome-email", runId }, message: "run dispatched" },
        { level: "debug", fields: { task: "send-welcome-email", runId }, message: "duplicate dispatch dropped" },
        { level: "debug", fields: { task: "send-welcome-email", runId }, message: "duplicate dispatch dropped" },
        { level: "debug", fields: { task: "purge-sessions", runId }, message: "run dispatched" },
      ]);
      expect(recordedEmails).toEqual(["first@example.com"]);
    } finally {
      await closeNextasking(nextasking);
    }
  },
);
