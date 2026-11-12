import { expect } from "vitest";

import { closeWhenTestFinishes } from "./close-when-test-finishes.js";
import { createGiveUpAfterOneSecondNextasking } from "./give-up-after-one-second-nextasking.js";
import { integrationTest, TEST_TIMEOUT_PAST_ITS_WAITS_IN_MILLISECONDS } from "./integration-test.js";
import { measuredDurationOf } from "./measured-duration.js";
import {
  createRecordingLogger,
  expectOneConnectionErrorAndOneConnectionRestored,
  waitForLogMessage,
} from "./recording-logger.js";
import { startTcpProxy } from "./tcp-proxy.js";

const QUIET_CHECK_INTERVAL_IN_MILLISECONDS = 10_000;

const GIVE_UP_AFTER_IN_MILLISECONDS = 1_000;

const EARLINESS_ALLOWED_IN_MILLISECONDS = 500;

const LATENESS_ALLOWED_IN_MILLISECONDS = 1_000;

const CONNECTION_ERROR_TIMEOUT_IN_MILLISECONDS = 20_000;

const CONNECTION_RESTORED_TIMEOUT_IN_MILLISECONDS = 10_000;

const NEXT_DISPATCH_BOUND_IN_MILLISECONDS = 2 * GIVE_UP_AFTER_IN_MILLISECONDS;

integrationTest(
  "a quiet connection whose server stops answering without closing it is reconnected within 10 s plus giveUpAfter, with one connection error, and one connection restored once the server answers again",
  {
    timeout:
      2 * NEXT_DISPATCH_BOUND_IN_MILLISECONDS +
      CONNECTION_ERROR_TIMEOUT_IN_MILLISECONDS +
      CONNECTION_RESTORED_TIMEOUT_IN_MILLISECONDS +
      TEST_TIMEOUT_PAST_ITS_WAITS_IN_MILLISECONDS,
  },
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const proxy = await startTcpProxy(redisUrl);
    const nextasking = createGiveUpAfterOneSecondNextasking(proxy.url, keyPrefix, { logger });
    closeWhenTestFinishes({ proxy, nextaskings: [nextasking], lines });
    await nextasking.dispatch("send-welcome-email", { email: "linus@example.com" }, { runId: "welcome-linus" });

    const { elapsedInMilliseconds, elapsedNetOfStallInMilliseconds } = await measuredDurationOf(() => {
      proxy.stopAnswering();
      return waitForLogMessage(lines, "connection error", { timeout: CONNECTION_ERROR_TIMEOUT_IN_MILLISECONDS });
    });
    proxy.answerAgain();
    await waitForLogMessage(lines, "connection restored", { timeout: CONNECTION_RESTORED_TIMEOUT_IN_MILLISECONDS });
    const nextRun = await nextasking.getRun("send-welcome-email", "welcome-linus");

    expect(elapsedInMilliseconds).toBeGreaterThanOrEqual(
      QUIET_CHECK_INTERVAL_IN_MILLISECONDS + GIVE_UP_AFTER_IN_MILLISECONDS - EARLINESS_ALLOWED_IN_MILLISECONDS,
    );
    expect(elapsedNetOfStallInMilliseconds).toBeLessThan(
      QUIET_CHECK_INTERVAL_IN_MILLISECONDS + GIVE_UP_AFTER_IN_MILLISECONDS + LATENESS_ALLOWED_IN_MILLISECONDS,
    );
    expect(nextRun).toMatchObject({ runId: "welcome-linus", state: "pending" });
    expectOneConnectionErrorAndOneConnectionRestored(lines);
  },
);
