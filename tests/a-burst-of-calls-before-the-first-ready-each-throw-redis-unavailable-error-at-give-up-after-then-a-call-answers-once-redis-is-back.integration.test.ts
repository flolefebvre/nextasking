import { expect, vi } from "vitest";

import { RedisUnavailableError } from "../src/index.js";
import { giveUpAfterOneSecondNextaskingBehindACutProxyOf } from "./give-up-after-one-second-nextasking.js";
import { integrationTest } from "./integration-test.js";
import { measuredRejectionOf } from "./measured-duration.js";

const GIVE_UP_AFTER_IN_MILLISECONDS = 1_000;

const TIMER_SLACK_IN_MILLISECONDS = 50;

const SLOWEST_GIVE_UP_IN_MILLISECONDS = 1_500;

const BURST_CALL_COUNT = 20;

integrationTest(
  "a burst of calls made together before the first ready each throw RedisUnavailableError at giveUpAfter from its own start, then a call answers once Redis is back",
  async ({ redisUrl, keyPrefix }) => {
    const { proxy, nextasking } = await giveUpAfterOneSecondNextaskingBehindACutProxyOf(redisUrl, keyPrefix);

    const outcomes = await Promise.all(
      Array.from({ length: BURST_CALL_COUNT }, (_, position) =>
        measuredRejectionOf(() =>
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

    for (const { result: rejection, elapsedInMilliseconds, elapsedNetOfStallInMilliseconds } of outcomes) {
      expect(rejection).toBeInstanceOf(RedisUnavailableError);
      expect(elapsedInMilliseconds).toBeGreaterThanOrEqual(GIVE_UP_AFTER_IN_MILLISECONDS - TIMER_SLACK_IN_MILLISECONDS);
      expect(elapsedNetOfStallInMilliseconds).toBeLessThan(SLOWEST_GIVE_UP_IN_MILLISECONDS);
    }
    expect(answer).toBeUndefined();
  },
  30_000,
);
