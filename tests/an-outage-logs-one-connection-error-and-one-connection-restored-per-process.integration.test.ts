import { expect, vi } from "vitest";

import { closeNextasking } from "../src/registry.js";
import { createGiveUpAfterOneSecondNextasking } from "./give-up-after-one-second-nextasking.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";
import { startTcpProxy } from "./tcp-proxy.js";

const OUTAGE_DURATION_IN_MILLISECONDS = 3_000;

integrationTest(
  "an outage cutting the instance's and the worker's connections logs one connection error and one connection restored",
  async ({ redisUrl, keyPrefix }) => {
    const proxy = await startTcpProxy(redisUrl);
    const { logger, lines } = createRecordingLogger();
    const completedEmails: string[] = [];
    const nextasking = createGiveUpAfterOneSecondNextasking(proxy.url, keyPrefix, {
      logger,
      onWelcomeEmail: (email) => completedEmails.push(email),
    });

    try {
      await nextasking.startWorker();
      await nextasking.dispatch("send-welcome-email", { email: "ada@example.com" });
      await vi.waitFor(() => expect(completedEmails).toEqual(["ada@example.com"]), { timeout: 10_000 });

      const outage = proxy.cutFor(OUTAGE_DURATION_IN_MILLISECONDS);
      await waitForLogMessage(lines, "connection error");
      await outage;
      await waitForLogMessage(lines, "connection restored", { timeout: 15_000 });
      await nextasking.dispatch("send-welcome-email", { email: "grace@example.com" });
      await vi.waitFor(() => expect(completedEmails).toContain("grace@example.com"), { timeout: 10_000 });

      const connectionLines = lines.filter((line) => line.message.startsWith("connection "));
      expect(connectionLines).toEqual([
        { level: "error", fields: { err: expect.any(Error) }, message: "connection error" },
        { level: "info", fields: {}, message: "connection restored" },
      ]);
    } finally {
      await closeNextasking(nextasking);
      await proxy.stop();
    }
  },
);
