import { setTimeout as bodyStillRunningFor } from "node:timers/promises";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";
import { testFailingWith } from "./test-failing-with.js";

const TIMED_OUT_TEST_TIMEOUT_IN_MILLISECONDS = 3_000;

const TIMED_OUT_BODY_DURATION_IN_MILLISECONDS = 2 * TIMED_OUT_TEST_TIMEOUT_IN_MILLISECONDS;

const TIMED_OUT_MESSAGE = `Test timed out in ${String(TIMED_OUT_TEST_TIMEOUT_IN_MILLISECONDS)}ms.\nIf this is a long-running test, pass a timeout value as the last argument or configure it globally with "testTimeout".`;

function createIdleWorkerNextasking(redisUrl: string, keyPrefix: string) {
  return createNextasking({
    connection: { url: redisUrl },
    prefix: keyPrefix,
    logger: createRecordingLogger().logger,
    tasks: [defineTask("never-dispatched", { schema: z.object({}), handler: async () => undefined })],
  });
}

testFailingWith(
  integrationTest,
  "a test whose body outlasts its timeout with a worker started fails with the timeout alone",
  () => [TIMED_OUT_MESSAGE],
  async ({ redisUrl, keyPrefix }) => {
    const nextasking = createIdleWorkerNextasking(redisUrl, keyPrefix);

    try {
      await nextasking.startWorker();
      await bodyStillRunningFor(TIMED_OUT_BODY_DURATION_IN_MILLISECONDS);
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
  { timeout: TIMED_OUT_TEST_TIMEOUT_IN_MILLISECONDS },
);

integrationTest("the next test of the file starts and closes a worker of its own", async ({ redisUrl, keyPrefix }) => {
  const nextasking = createIdleWorkerNextasking(redisUrl, keyPrefix);

  try {
    await nextasking.startWorker();
  } finally {
    await closeNextaskingAndCheckItsDuration(nextasking);
  }
});
