import { setTimeout as sleep } from "node:timers/promises";
import { expect } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import type { StartWorkerOptions } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";

const BACKLOG_SIZE = 5;

async function runABacklogOfResizes({
  redisUrl,
  keyPrefix,
  handler,
  startWorkerOptions,
}: {
  redisUrl: string;
  keyPrefix: string;
  handler: () => Promise<void>;
  startWorkerOptions?: StartWorkerOptions;
}): Promise<void> {
  const { logger, lines } = createRecordingLogger();
  const resizeImageTask = defineTask("resize-image", { schema: z.object({ imageId: z.string() }), handler });
  const nextasking = createNextasking({
    connection: { url: redisUrl },
    prefix: keyPrefix,
    logger,
    tasks: [resizeImageTask],
  });
  try {
    for (let imageNumber = 0; imageNumber < BACKLOG_SIZE; imageNumber += 1) {
      await nextasking.dispatch("resize-image", { imageId: `image-${String(imageNumber)}` });
    }
    await nextasking.startWorker(startWorkerOptions);
    await expect
      .poll(() => lines.filter((line) => line.message === "run completed").length, { timeout: 10_000 })
      .toBe(BACKLOG_SIZE);
  } finally {
    await closeNextaskingAndCheckItsDuration(nextasking);
  }
}

integrationTest("a worker started with concurrency 1 runs one attempt at a time", async ({ redisUrl, keyPrefix }) => {
  let attemptsInFlight = 0;
  let mostAttemptsInFlight = 0;
  const resizeOneImage = async () => {
    attemptsInFlight += 1;
    mostAttemptsInFlight = Math.max(mostAttemptsInFlight, attemptsInFlight);
    await sleep(100);
    attemptsInFlight -= 1;
  };

  await runABacklogOfResizes({
    redisUrl,
    keyPrefix,
    handler: resizeOneImage,
    startWorkerOptions: { concurrency: 1 },
  });

  expect(mostAttemptsInFlight).toBe(1);
});

integrationTest(
  "a worker started without concurrency starts a whole backlog at once",
  async ({ redisUrl, keyPrefix }) => {
    let attemptsStarted = 0;
    let markWholeBacklogStarted = () => undefined as void;
    const wholeBacklogStarted = new Promise<void>((resolve) => {
      markWholeBacklogStarted = resolve;
    });
    const waitForTheWholeBacklog = async () => {
      attemptsStarted += 1;
      if (attemptsStarted === BACKLOG_SIZE) {
        markWholeBacklogStarted();
      }
      await Promise.race([wholeBacklogStarted, sleep(5_000)]);
    };

    await runABacklogOfResizes({ redisUrl, keyPrefix, handler: waitForTheWholeBacklog });

    expect(attemptsStarted).toBe(BACKLOG_SIZE);
  },
);
