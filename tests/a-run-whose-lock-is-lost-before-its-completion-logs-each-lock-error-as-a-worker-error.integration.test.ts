import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";
import { runJobKey } from "./run-job-key.js";
import { withRedis } from "./with-redis.js";

integrationTest(
  "a run whose lock is gone when its handler returns, while the server still holds it active, logs each lock error as a worker error and no run completed",
  async ({ redisUrl, keyPrefix }) => {
    await withRedis(redisUrl, async (redis) => {
      const { logger, lines } = createRecordingLogger();
      const nextasking = createNextasking({
        connection: { url: redisUrl },
        prefix: keyPrefix,
        logger,
        tasks: [
          defineTask("archive-invoice", {
            schema: z.object({ invoiceId: z.string() }),
            handler: async ({ runId }) => {
              await redis.del(`${runJobKey(keyPrefix, "archive-invoice", runId)}:lock`);
            },
          }),
        ],
      });

      try {
        await nextasking.startWorker();
        const { runId } = await nextasking.dispatch("archive-invoice", { invoiceId: "invoice-1" });
        const workerErrorLines = await vi.waitUntil(
          () => {
            const workerErrors = lines.filter((line) => line.message === "worker error");
            return workerErrors.length >= 2 ? workerErrors : undefined;
          },
          { timeout: 10_000 },
        );

        const workerErrorLine = {
          level: "error",
          fields: {
            err: expect.objectContaining({ message: expect.stringMatching(/^Missing lock for job .+\. \w+$/) }),
          },
          message: "worker error",
        };
        expect(workerErrorLines).toEqual([workerErrorLine, workerErrorLine]);
        expect(
          lines.filter(
            (line) =>
              (line.message === "run completed" || line.message === "run completion unconfirmed") &&
              line.fields.runId === runId,
          ),
        ).toEqual([]);
      } finally {
        await closeNextasking(nextasking);
      }
    });
  },
);
