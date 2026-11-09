import { expect } from "vitest";
import { z } from "zod";

import { closeNextaskingAndCheckItsDuration } from "../tests/close-nextasking-and-check-its-duration.js";
import { integrationTest } from "../tests/integration-test.js";
import { createRecordingLogger } from "../tests/recording-logger.js";
import { defineTask } from "./definitions.js";
import { createNextasking } from "./registry.js";

const purgeSessions = defineTask("purge-sessions", {
  schema: z.object({}),
  handler: async () => undefined,
});

integrationTest("startWorker refuses a second worker in the same process", async ({ redisUrl, keyPrefix }) => {
  const nextasking = createNextasking({
    connection: { url: redisUrl },
    prefix: keyPrefix,
    logger: createRecordingLogger().logger,
    tasks: [purgeSessions],
  });
  const otherNextasking = createNextasking({
    connection: { url: redisUrl },
    prefix: keyPrefix,
    logger: createRecordingLogger().logger,
    tasks: [purgeSessions],
  });

  try {
    await nextasking.startWorker();

    await expect(otherNextasking.startWorker()).rejects.toThrow(
      "The nextasking worker is already started in this process",
    );
  } finally {
    await closeNextaskingAndCheckItsDuration(otherNextasking);
    await closeNextaskingAndCheckItsDuration(nextasking);
  }
});

integrationTest("a closed worker lets the process start another one", async ({ redisUrl, keyPrefix }) => {
  const nextasking = createNextasking({
    connection: { url: redisUrl },
    prefix: keyPrefix,
    logger: createRecordingLogger().logger,
    tasks: [purgeSessions],
  });
  await nextasking.startWorker();
  await closeNextaskingAndCheckItsDuration(nextasking);
  const nextaskingAfterClose = createNextasking({
    connection: { url: redisUrl },
    prefix: keyPrefix,
    logger: createRecordingLogger().logger,
    tasks: [purgeSessions],
  });

  try {
    await expect(nextaskingAfterClose.startWorker()).resolves.toBeUndefined();
  } finally {
    await closeNextaskingAndCheckItsDuration(nextaskingAfterClose);
  }
});
