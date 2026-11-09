import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";

const SLEEP_IN_MILLISECONDS = 1500;

integrationTest(
  "a sleeping run holds no worker, resumes at its wake time with the same attempt number, and logs both ends",
  { timeout: 35_000 },
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const attemptsSeenAfterTheSleep: number[] = [];
    let resumedAt: number | undefined;
    const waitForTrialEnd = defineTask("wait-for-trial-end", {
      schema: z.object({ trialEndsAt: z.iso.datetime() }),
      handler: async ({ payload, step, attempt }) => {
        await step.sleepUntil("wait-for-trial-end", payload.trialEndsAt);
        resumedAt = Date.now();
        attemptsSeenAfterTheSleep.push(attempt);
      },
    });
    const sendWelcomeEmail = defineTask("send-welcome-email", {
      schema: z.object({ email: z.email() }),
      handler: async ({ logger: runLogger }) => {
        runLogger.info({}, "welcome email sent");
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [waitForTrialEnd, sendWelcomeEmail],
    });

    try {
      await nextasking.startWorker();
      const trialEndsAt = new Date(Date.now() + SLEEP_IN_MILLISECONDS).toISOString();
      const { runId } = await nextasking.dispatch("wait-for-trial-end", { trialEndsAt });
      await waitForLogMessage(lines, "run suspended");
      await nextasking.dispatch("send-welcome-email", { email: "ada@example.com" });
      await waitForLogMessage(lines, "welcome email sent");
      const isSleepingRunStillSuspended = resumedAt === undefined;
      await vi.waitFor(
        () => {
          expect(lines.filter((line) => line.message === "run completed")).toHaveLength(2);
        },
        { timeout: 10_000 },
      );

      const sleepingRunFields = { task: "wait-for-trial-end", runId, attempt: 1 };
      expect(isSleepingRunStillSuspended).toBe(true);
      expect(attemptsSeenAfterTheSleep).toEqual([1]);
      expect(resumedAt).toBeGreaterThanOrEqual(Date.parse(trialEndsAt));
      expect(
        lines.filter((line) => line.fields.task === "wait-for-trial-end" && line.fields.attempt !== undefined),
      ).toEqual([
        { level: "debug", fields: sleepingRunFields, message: "attempt started" },
        { level: "debug", fields: { ...sleepingRunFields, resumesAt: trialEndsAt }, message: "run suspended" },
        { level: "debug", fields: sleepingRunFields, message: "run resumed" },
        {
          level: "info",
          fields: {
            ...sleepingRunFields,
            runDurationInMilliseconds: expect.any(Number),
            executionDurationInMilliseconds: expect.any(Number),
          },
          message: "run completed",
        },
      ]);
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);

integrationTest(
  "a task's timeout covers each stretch of a sleeping run, never the time spent sleeping",
  { timeout: 15_000 },
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const remindTrialEnd = defineTask("remind-trial-end", {
      schema: z.object({ trialEndsAt: z.iso.datetime() }),
      timeout: "1s",
      retries: 0,
      handler: async ({ payload, step }) => {
        await step.sleepUntil("wait-for-trial-end", payload.trialEndsAt);
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [remindTrialEnd],
    });

    try {
      await nextasking.startWorker();
      const trialEndsAt = new Date(Date.now() + SLEEP_IN_MILLISECONDS).toISOString();
      await nextasking.dispatch("remind-trial-end", { trialEndsAt });
      await waitForLogMessage(lines, "run completed");

      expect(lines.map((line) => line.message)).not.toContain("run failed");
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);
