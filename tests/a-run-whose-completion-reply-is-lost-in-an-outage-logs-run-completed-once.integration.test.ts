import { expect } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeWhenTestFinishes } from "./close-when-test-finishes.js";
import { integrationTest } from "./integration-test.js";
import {
  createRecordingLogger,
  expectOneConnectionErrorAndOneConnectionRestored,
  waitForLogMessage,
} from "./recording-logger.js";
import { isSettleRequestOf } from "./engine-requests.js";
import { startTcpProxy } from "./tcp-proxy.js";

integrationTest(
  "a connection cut after the completion of a run is stored, before its reply arrives, logs run completed once after the reconnect and no worker error",
  { timeout: 20_000 },
  async ({ redisUrl, keyPrefix }) => {
    const proxy = await startTcpProxy(redisUrl);
    const { logger, lines } = createRecordingLogger();
    let completionReplyCut: Promise<void> | undefined;
    const sendWelcomeEmail = defineTask("send-welcome-email", {
      schema: z.object({ email: z.string() }),
      handler: async ({ runId }) => {
        completionReplyCut = proxy.cutBeforeReplyTo((request) =>
          isSettleRequestOf(request, keyPrefix, { task: "send-welcome-email", runId, settleKind: "complete" }),
        );
      },
    });
    const workerNextasking = createNextasking({
      connection: { url: proxy.url, giveUpAfter: "1s" },
      prefix: keyPrefix,
      logger,
      tasks: [sendWelcomeEmail],
    });
    const directNextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      tasks: [sendWelcomeEmail],
    });
    closeWhenTestFinishes({ proxy, nextaskings: [workerNextasking, directNextasking], lines });

    await workerNextasking.startWorker();
    const { runId } = await directNextasking.dispatch("send-welcome-email", { email: "ada@example.com" });
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
    expect(lines.filter((line) => line.message === "worker error")).toEqual([]);
  },
);
