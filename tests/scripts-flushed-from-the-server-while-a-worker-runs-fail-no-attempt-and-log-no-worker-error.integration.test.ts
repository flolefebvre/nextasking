import { setTimeout as sleep } from "node:timers/promises";

import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";
import { withRedis } from "./with-redis.js";

const RUN_COUNT = 60;

const FLUSH_INTERVAL_IN_MILLISECONDS = 5;

integrationTest(
  "scripts flushed from the server while a worker runs fail no attempt and log no worker error",
  { timeout: 30_000 },
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [
        defineTask("resize-image", {
          schema: z.object({ imageId: z.string() }),
          retries: 0,
          handler: async ({ step }) => {
            await step.run("resize", async () => {
              await sleep(5);
              return "resized";
            });
          },
        }),
      ],
    });
    try {
      await withRedis(redisUrl, async (admin) => {
        await nextasking.startWorker();
        let isFlushing = true;
        const flushes = (async () => {
          let flushCount = 0;
          while (isFlushing) {
            await admin.script("FLUSH");
            flushCount += 1;
            await sleep(FLUSH_INTERVAL_IN_MILLISECONDS);
          }
          return flushCount;
        })();

        try {
          for (let position = 0; position < RUN_COUNT; position += 1) {
            await nextasking.dispatch("resize-image", { imageId: `image-${String(position)}` });
          }
          await vi.waitFor(
            async () => {
              expect((await nextasking.countRuns())["resize-image"]).toMatchObject({ completed: RUN_COUNT });
            },
            { timeout: 20_000, interval: 100 },
          );
        } finally {
          isFlushing = false;
        }
        const flushCount = await flushes;

        expect(flushCount).toBeGreaterThan(5);
        expect((await nextasking.countRuns())["resize-image"]).toMatchObject({ failed: 0 });
        expect(lines.map(({ message }) => message)).not.toContain("attempt failed");
      });
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);
