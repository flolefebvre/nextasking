import { expect, vi } from "vitest";

import { createGiveUpAfterOneSecondNextasking } from "./give-up-after-one-second-nextasking.js";
import { closeWhenTestFinishes } from "./close-when-test-finishes.js";
import { integrationTest } from "./integration-test.js";
import {
  createRecordingLogger,
  expectOneConnectionErrorAndOneConnectionRestored,
  waitForLogMessage,
} from "./recording-logger.js";
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

    closeWhenTestFinishes({ proxy, nextaskings: [nextasking], lines });

    await nextasking.startWorker();
    await nextasking.dispatch("send-welcome-email", { email: "ada@example.com" });
    await waitForLogMessage(lines, "run completed");

    const outage = proxy.cutFor(OUTAGE_DURATION_IN_MILLISECONDS);
    await waitForLogMessage(lines, "connection error");
    await outage;
    await waitForLogMessage(lines, "connection restored", { timeout: 15_000 });
    await nextasking.dispatch("send-welcome-email", { email: "grace@example.com" });
    await vi.waitFor(() => expect(completedEmails).toContain("grace@example.com"), { timeout: 10_000 });

    expectOneConnectionErrorAndOneConnectionRestored(lines);
    expect(lines.filter((line) => line.message === "queue error" || line.message === "worker error")).toEqual([]);
  },
);
