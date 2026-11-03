import { expect } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";
import {
  createRecordingLogger,
  expectOneConnectionErrorAndOneConnectionRestored,
  waitForLogMessage,
} from "./recording-logger.js";
import { runJobKey } from "./run-job-key.js";
import { startTcpProxy } from "./tcp-proxy.js";

const MOVE_TO_COMPLETED_ARGUMENT = "returnvalue";

integrationTest(
  "a connection cut after the completion of a run is stored, before its reply arrives, logs run completed once after the reconnect and no worker error",
  { timeout: 20_000 },
  async ({ redisUrl, keyPrefix }) => {
    const proxy = await startTcpProxy(redisUrl);
    const { logger, lines } = createRecordingLogger();
    let completionReplyCut: Promise<void> | undefined;
    const nextasking = createNextasking({
      connection: { url: proxy.url, giveUpAfter: "1s" },
      prefix: keyPrefix,
      logger,
      tasks: [
        defineTask("send-welcome-email", {
          schema: z.object({ email: z.string() }),
          handler: async ({ runId }) => {
            const jobKey = runJobKey(keyPrefix, "send-welcome-email", runId);
            completionReplyCut = proxy.cutBeforeReplyTo(
              (request) => request.includes(jobKey) && request.includes(MOVE_TO_COMPLETED_ARGUMENT),
            );
          },
        }),
      ],
    });

    try {
      await nextasking.startWorker();
      const { runId } = await nextasking.dispatch("send-welcome-email", { email: "ada@example.com" });
      await waitForLogMessage(lines, "connection error");
      await completionReplyCut;
      await proxy.restore();
      await waitForLogMessage(lines, "run completed", { timeout: 15_000 });
      await waitForLogMessage(lines, "connection restored", { timeout: 15_000 });

      expect(lines.filter((line) => line.message === "run completed")).toEqual([
        {
          level: "info",
          fields: {
            task: "send-welcome-email",
            runId,
            attempt: 1,
            runDurationInMilliseconds: expect.any(Number),
            executionDurationInMilliseconds: expect.any(Number),
          },
          message: "run completed",
        },
      ]);
      expectOneConnectionErrorAndOneConnectionRestored(lines);
      expect(
        lines.filter((line) => line.message === "worker error" || line.message === "run completion unconfirmed"),
      ).toEqual([]);
    } finally {
      await closeNextasking(nextasking);
      await proxy.stop();
    }
  },
);
