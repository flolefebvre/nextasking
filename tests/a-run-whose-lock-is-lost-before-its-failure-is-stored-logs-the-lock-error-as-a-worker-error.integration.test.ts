import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import type { Logger } from "../src/index.js";
import { closeWhenTestFinishes } from "./close-when-test-finishes.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";
import { runJobKey } from "./run-job-key.js";
import { startTcpProxy } from "./tcp-proxy.js";
import { withRedis } from "./with-redis.js";

integrationTest(
  "a run whose lock is lost after its failed attempt is recorded, before its failure is stored, logs the lock error as a worker error",
  { timeout: 20_000 },
  async ({ redisUrl, keyPrefix }) => {
    await withRedis(redisUrl, async (redis) => {
      const proxy = await startTcpProxy(redisUrl);
      const { logger: recordingLogger, lines } = createRecordingLogger();
      const logger: Logger = {
        ...recordingLogger,
        warn: (fields, message) => {
          recordingLogger.warn(fields, message);
          if (message === "attempt failed") {
            void proxy.cut();
          }
        },
      };
      const chargeInvoice = defineTask("charge-invoice", {
        schema: z.object({ invoiceId: z.string() }),
        retries: 1,
        backoff: "1s",
        handler: async ({ attempt }) => {
          if (attempt === 1) {
            throw new Error("card declined");
          }
        },
      });
      const workerNextasking = createNextasking({
        connection: { url: proxy.url, giveUpAfter: "1s" },
        prefix: keyPrefix,
        logger,
        tasks: [chargeInvoice],
      });
      const directNextasking = createNextasking({
        connection: { url: redisUrl },
        prefix: keyPrefix,
        tasks: [chargeInvoice],
      });
      closeWhenTestFinishes({ proxy, nextaskings: [workerNextasking, directNextasking], lines });

      await workerNextasking.startWorker();
      const { runId } = await directNextasking.dispatch("charge-invoice", { invoiceId: "invoice-7" });
      await waitForLogMessage(lines, "connection error");
      await redis.del(`${runJobKey(keyPrefix, "charge-invoice", runId)}:lock`);
      await proxy.restore();
      const workerErrorLines = await vi.waitUntil(
        () => {
          const workerErrors = lines.filter((line) => line.message === "worker error");
          return workerErrors.length >= 1 ? workerErrors : undefined;
        },
        { timeout: 15_000 },
      );

      expect(workerErrorLines).toEqual([
        {
          level: "error",
          fields: {
            err: expect.objectContaining({
              message: expect.stringMatching(/^Missing lock for job .+\. moveToDelayed$/),
            }),
          },
          message: "worker error",
        },
      ]);
      expect(lines.filter((line) => line.message === "run failure unconfirmed")).toEqual([]);
    });
  },
);
