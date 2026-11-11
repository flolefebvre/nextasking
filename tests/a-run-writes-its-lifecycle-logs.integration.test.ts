import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";

integrationTest(
  "a run writes its lifecycle logs, and the handler's own lines carry the run",
  { timeout: 15_000 },
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const sendWelcomeEmailTask = defineTask("send-welcome-email", {
      schema: z.object({ email: z.email() }),
      handler: async (context) => {
        context.logger.info({ template: "welcome", task: "spoofed-task" }, "welcome email sent");
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [sendWelcomeEmailTask],
    });

    try {
      await nextasking.startWorker();
      const { runId } = await nextasking.dispatch("send-welcome-email", { email: "ada@example.com" });
      await vi.waitFor(
        () => {
          expect(lines.map((line) => line.message)).toContain("run completed");
        },
        { timeout: 10_000 },
      );
      await closeNextaskingAndCheckItsDuration(nextasking);

      const runFields = { task: "send-welcome-email", runId };
      const attemptFields = { ...runFields, attempt: 1 };
      expect(lines.at(0)).toEqual({ level: "info", fields: {}, message: "worker started" });
      expect(lines.at(1)).toEqual({
        level: "warn",
        fields: {},
        message:
          "NEXT_MANUAL_SIG_HANDLE is not set: on SIGTERM Next exits at once and cuts the attempts running in this worker",
      });
      expect(lines.at(-1)).toEqual({ level: "info", fields: {}, message: "worker stopped" });
      expect(lines).toContainEqual({ level: "debug", fields: runFields, message: "run dispatched" });
      expect(lines.filter((line) => line.fields.attempt !== undefined)).toEqual([
        { level: "debug", fields: attemptFields, message: "attempt started" },
        { level: "info", fields: { template: "welcome", ...attemptFields }, message: "welcome email sent" },
        {
          level: "info",
          fields: {
            ...attemptFields,
            runDurationInMilliseconds: expect.any(Number),
            executionDurationInMilliseconds: expect.any(Number),
          },
          message: "run completed",
        },
      ]);
      const runCompletedFields = lines.find((line) => line.message === "run completed")?.fields;
      expect(runCompletedFields?.executionDurationInMilliseconds).toBeGreaterThanOrEqual(0);
      expect(runCompletedFields?.runDurationInMilliseconds).toBeGreaterThanOrEqual(
        runCompletedFields?.executionDurationInMilliseconds as number,
      );
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);
