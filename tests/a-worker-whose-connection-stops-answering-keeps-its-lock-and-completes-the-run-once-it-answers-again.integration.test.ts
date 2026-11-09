import { expect, vi } from "vitest";

import { integrationTest } from "./integration-test.js";
import { connectionLinesOf, logLinesOfARunCompletedInItsFirstAttemptAcrossASilence } from "./run-across-a-silence.js";

integrationTest(
  "a worker whose connection stops answering without closing, for longer than giveUpAfter, renews its lock once it answers again and completes the run in its first attempt",
  { timeout: 45_000 },
  async ({ redisUrl, keyPrefix }) => {
    const workerLogLines = await logLinesOfARunCompletedInItsFirstAttemptAcrossASilence(
      { redisUrl, keyPrefix },
      async ({ proxy }) => {
        proxy.stopAnswering();
        await vi.waitFor(() => expect(proxy.silencedConnectionCount()).toBe(0), { timeout: 10_000 });
        proxy.answerAgain();
      },
    );

    expect(connectionLinesOf(workerLogLines)).toEqual([
      expect.objectContaining({ message: "connection error" }),
      expect.objectContaining({ message: "connection restored" }),
    ]);
  },
);
