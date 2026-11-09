import { expect } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";
import { storedRun } from "./stored-run.js";

const ISO_TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

integrationTest(
  "a failing run waits its backoff, retries, then fails for good and keeps one error record per attempt",
  { timeout: 15_000 },
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const syncAccount = defineTask("sync-account", {
      schema: z.object({ accountId: z.string() }),
      retries: 1,
      backoff: "1s",
      handler: async ({ attempt }) => {
        if (attempt === 1) {
          throw "upstream unavailable";
        }
        throw new Error("upstream still unavailable");
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [syncAccount],
    });

    try {
      await nextasking.startWorker();
      const { runId } = await nextasking.dispatch("sync-account", { accountId: "account-7" });
      await waitForLogMessage(lines, "run failed");
      const run = await storedRun(redisUrl, keyPrefix, "sync-account", runId);

      const runFields = { task: "sync-account", runId };
      expect(lines.filter((line) => line.fields.attempt !== undefined)).toEqual([
        { level: "debug", fields: { ...runFields, attempt: 1 }, message: "attempt started" },
        {
          level: "warn",
          fields: {
            ...runFields,
            attempt: 1,
            err: expect.objectContaining({ name: "Error", message: "upstream unavailable" }),
            retriesLeft: 1,
            backoffInMilliseconds: expect.any(Number),
          },
          message: "attempt failed",
        },
        { level: "debug", fields: { ...runFields, attempt: 2 }, message: "attempt started" },
        {
          level: "error",
          fields: {
            ...runFields,
            attempt: 2,
            err: expect.objectContaining({ name: "Error", message: "upstream still unavailable" }),
            reason: "retries exhausted",
            runDurationInMilliseconds: expect.any(Number),
            executionDurationInMilliseconds: expect.any(Number),
          },
          message: "run failed",
        },
      ]);
      const attemptFailedFields = lines.find((line) => line.message === "attempt failed")?.fields;
      const runFailedFields = lines.find((line) => line.message === "run failed")?.fields;
      expect(attemptFailedFields?.err).toBeInstanceOf(Error);
      expect(attemptFailedFields?.backoffInMilliseconds).toBeGreaterThanOrEqual(800);
      expect(attemptFailedFields?.backoffInMilliseconds).toBeLessThanOrEqual(1000);
      expect(runFailedFields?.runDurationInMilliseconds).toBeGreaterThanOrEqual(
        attemptFailedFields?.backoffInMilliseconds as number,
      );
      expect(run).toMatchObject({
        payload: { accountId: "account-7" },
        reason: "retries exhausted",
        errors: [
          {
            attempt: 1,
            name: "Error",
            message: "upstream unavailable",
            stack: expect.stringContaining("upstream unavailable"),
            failedAt: expect.stringMatching(ISO_TIMESTAMP),
          },
          {
            attempt: 2,
            name: "Error",
            message: "upstream still unavailable",
            stack: expect.stringContaining("upstream still unavailable"),
            failedAt: expect.stringMatching(ISO_TIMESTAMP),
          },
        ],
      });
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);

integrationTest(
  "a task that leaves retries unset makes six attempts: the first and five retries",
  { timeout: 20_000 },
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const syncAccount = defineTask("sync-account", {
      schema: z.object({ accountId: z.string() }),
      backoff: "0s",
      handler: async () => {
        throw new Error("upstream unavailable");
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [syncAccount],
    });

    try {
      await nextasking.startWorker();
      await nextasking.dispatch("sync-account", { accountId: "account-7" });
      await waitForLogMessage(lines, "run failed", { timeout: 15_000 });

      const failureLines = lines.filter((line) => line.message === "attempt failed" || line.message === "run failed");
      expect(failureLines.map((line) => [line.message, line.fields.attempt, line.fields.retriesLeft])).toEqual([
        ["attempt failed", 1, 5],
        ["attempt failed", 2, 4],
        ["attempt failed", 3, 3],
        ["attempt failed", 4, 2],
        ["attempt failed", 5, 1],
        ["run failed", 6, undefined],
      ]);
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);

integrationTest(
  "a timeout fails an attempt that runs too long, and the attempt retries like any other failure",
  { timeout: 15_000 },
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const renderReport = defineTask("render-report", {
      schema: z.object({ reportId: z.string() }),
      retries: 1,
      backoff: "0s",
      timeout: "1s",
      handler: () => new Promise<void>(() => undefined),
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [renderReport],
    });

    try {
      await nextasking.startWorker();
      const { runId } = await nextasking.dispatch("render-report", { reportId: "report-7" });
      await waitForLogMessage(lines, "run failed");
      const run = await storedRun(redisUrl, keyPrefix, "render-report", runId);

      const timeoutError = { name: "AttemptTimeoutError", message: 'Task "render-report" timed out after 1s' };
      expect(lines.filter((line) => line.fields.err !== undefined)).toEqual([
        expect.objectContaining({
          message: "attempt failed",
          fields: expect.objectContaining({ attempt: 1, err: expect.objectContaining(timeoutError) }),
        }),
        expect.objectContaining({
          message: "run failed",
          fields: expect.objectContaining({
            attempt: 2,
            reason: "retries exhausted",
            err: expect.objectContaining(timeoutError),
          }),
        }),
      ]);
      expect(run).toMatchObject({
        errors: [
          { attempt: 1, ...timeoutError },
          { attempt: 2, ...timeoutError },
        ],
      });
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);
