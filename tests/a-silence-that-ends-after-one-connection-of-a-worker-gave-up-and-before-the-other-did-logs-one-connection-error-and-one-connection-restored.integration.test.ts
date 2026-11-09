import { expect, vi } from "vitest";

import { integrationTest } from "./integration-test.js";
import { connectionLinesOf, logLinesOfARunCompletedInItsFirstAttemptAcrossASilence } from "./run-across-a-silence.js";

integrationTest(
  "a server that stops answering a worker and answers again after one of its connections gave up and before the other did logs one connection error and one connection restored, once both answer again, and the run completes in its first attempt",
  { timeout: 80_000 },
  async ({ redisUrl, keyPrefix }) => {
    let silencedConnectionCountAtTheFirstRestored: number | undefined;

    const workerLogLines = await logLinesOfARunCompletedInItsFirstAttemptAcrossASilence(
      { redisUrl, keyPrefix, startWorkerOptions: { concurrency: 1 } },
      async ({ proxy, workerProcess }) => {
        proxy.stopAnswering();
        await vi.waitFor(() => expect(proxy.silencedConnectionCount()).toBe(1), { timeout: 10_000, interval: 5 });
        proxy.answerAgain();
        await vi.waitUntil(
          () => {
            const isRestored = connectionLinesOf(workerProcess.logLines).some(
              ({ message }) => message === "connection restored",
            );
            if (isRestored) {
              silencedConnectionCountAtTheFirstRestored = proxy.silencedConnectionCount();
            }
            return isRestored;
          },
          { timeout: 15_000, interval: 5 },
        );
        await vi.waitFor(
          () => {
            expect(proxy.silencedConnectionCount()).toBe(0);
            expect(connectionLinesOf(workerProcess.logLines).at(-1)?.message).toBe("connection restored");
          },
          { timeout: 20_000 },
        );
      },
    );

    expect(connectionLinesOf(workerLogLines)).toEqual([
      expect.objectContaining({ message: "connection error", fields: { err: expect.anything() } }),
      expect.objectContaining({ message: "connection restored" }),
    ]);
    expect(silencedConnectionCountAtTheFirstRestored).toBe(0);
  },
);
