import { expect, vi } from "vitest";

import { RedisUnavailableError } from "../src/index.js";
import { giveUpAfterOneSecondNextaskingBehindACutProxyOf } from "./give-up-after-one-second-nextasking.js";
import { integrationTest } from "./integration-test.js";

const GIVE_UP_AFTER_IN_MILLISECONDS = 1_000;

const TIMER_SLACK_IN_MILLISECONDS = 50;

const SLOWEST_GIVE_UP_IN_MILLISECONDS = 1_500;

const BURST_CALL_COUNT = 20;

async function rejectionAndElapsedOf(call: () => Promise<unknown>): Promise<{ rejection: unknown; elapsed: number }> {
  const startedAt = performance.now();
  const rejection = await call().then(
    () => new Error("the call resolved"),
    (error: unknown) => error,
  );
  return { rejection, elapsed: performance.now() - startedAt };
}

integrationTest(
  "a burst of calls made together before the first ready each throw RedisUnavailableError at giveUpAfter from its own start, then a call answers once Redis is back",
  async ({ redisUrl, keyPrefix }) => {
    const { proxy, nextasking } = await giveUpAfterOneSecondNextaskingBehindACutProxyOf(redisUrl, keyPrefix);

    const outcomes = await Promise.all(
      Array.from({ length: BURST_CALL_COUNT }, (_, position) =>
        rejectionAndElapsedOf(() =>
          position % 2 === 0
            ? nextasking.dispatch(
                "send-welcome-email",
                { email: "ada@example.com" },
                { runId: `welcome-${String(position)}` },
              )
            : nextasking.getRun("send-welcome-email", `welcome-${String(position)}`),
        ),
      ),
    );
    await proxy.restore();
    const answer = await vi.waitFor(() => nextasking.getRun("send-welcome-email", "welcome-0"), { timeout: 10_000 });

    for (const { rejection, elapsed } of outcomes) {
      expect(rejection).toBeInstanceOf(RedisUnavailableError);
      expect(elapsed).toBeGreaterThanOrEqual(GIVE_UP_AFTER_IN_MILLISECONDS - TIMER_SLACK_IN_MILLISECONDS);
      expect(elapsed).toBeLessThan(SLOWEST_GIVE_UP_IN_MILLISECONDS);
    }
    expect(answer).toBeUndefined();
  },
  30_000,
);
