import { expect, vi } from "vitest";

import { RedisUnavailableError } from "../src/index.js";
import { closeWhenTestFinishes } from "./close-when-test-finishes.js";
import { createGiveUpAfterOneSecondNextasking } from "./give-up-after-one-second-nextasking.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";
import { startTcpProxy } from "./tcp-proxy.js";

const GIVE_UP_AFTER_IN_MILLISECONDS = 1_000;

const TIMER_SLACK_IN_MILLISECONDS = 50;

integrationTest(
  "a call in flight on a connection that stops answering without closing throws RedisUnavailableError within giveUpAfter, and a later call succeeds once it answers again",
  async ({ redisUrl, keyPrefix }) => {
    const proxy = await startTcpProxy(redisUrl);
    const { logger, lines } = createRecordingLogger();
    const nextasking = createGiveUpAfterOneSecondNextasking(proxy.url, keyPrefix, { logger });
    closeWhenTestFinishes({ proxy, nextaskings: [nextasking], lines });
    await nextasking.dispatch("send-welcome-email", { email: "ada@example.com" }, { runId: "welcome-ada" });
    proxy.stopAnswering();

    const startedAt = performance.now();
    const rejection = await nextasking
      .dispatch("send-welcome-email", { email: "grace@example.com" }, { runId: "welcome-grace" })
      .then(
        () => new Error("the call resolved"),
        (error: unknown) => error,
      );
    const elapsed = performance.now() - startedAt;
    proxy.answerAgain();
    const laterRun = await vi.waitFor(() => nextasking.getRun("send-welcome-email", "welcome-ada"), {
      timeout: 10_000,
    });

    expect(rejection).toBeInstanceOf(RedisUnavailableError);
    expect(rejection).toMatchObject({
      message: "Redis didn't answer within 1s",
      runId: "welcome-grace",
      cause: expect.any(Error),
    });
    expect(elapsed).toBeGreaterThanOrEqual(GIVE_UP_AFTER_IN_MILLISECONDS - TIMER_SLACK_IN_MILLISECONDS);
    expect(elapsed).toBeLessThan(GIVE_UP_AFTER_IN_MILLISECONDS + 500);
    expect(laterRun).toMatchObject({ runId: "welcome-ada" });
    expect(lines.filter(({ level }) => level !== "debug").map(({ message }) => message)).toEqual([
      "connection error",
      "connection restored",
    ]);
  },
  30_000,
);
