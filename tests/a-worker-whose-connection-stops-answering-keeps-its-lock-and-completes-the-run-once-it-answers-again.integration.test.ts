import { setTimeout as sleep } from "node:timers/promises";

import { expect, vi } from "vitest";

import { fixtureRecordsOf } from "./fixture-records.js";
import { integrationTest } from "./integration-test.js";
import { waitForLogMessage } from "./recording-logger.js";
import { startTcpProxy } from "./tcp-proxy.js";
import { startWorkerProcess } from "./worker-process.js";
import { createWorkerProcessNextasking } from "./worker-process-nextasking.js";

const LOCK_DURATION_IN_MILLISECONDS = 6_000;

const SILENCE_LONGER_THAN_GIVE_UP_AFTER_IN_MILLISECONDS = 2_500;

integrationTest(
  "a worker whose connection stops answering without closing, for longer than giveUpAfter, renews its lock once it answers again and completes the run in its first attempt",
  { timeout: 45_000 },
  async ({ redisUrl, keyPrefix }) => {
    const proxy = await startTcpProxy(redisUrl);
    try {
      const workerProcess = await startWorkerProcess({
        redisUrl,
        keyPrefix,
        connection: { url: proxy.url, giveUpAfter: "1s" },
        timing: {
          lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS,
          lockTimeLeftThatTriggersRenewalInMilliseconds: LOCK_DURATION_IN_MILLISECONDS - 1_000,
        },
      });
      const nextasking = createWorkerProcessNextasking(redisUrl, keyPrefix);
      try {
        const { runId } = await nextasking.dispatch("wait-then-record", {
          waitInMilliseconds: LOCK_DURATION_IN_MILLISECONDS + 2_000,
        });
        await vi.waitFor(
          async () => {
            expect(await fixtureRecordsOf(redisUrl, keyPrefix, "wait-then-record")).toHaveLength(1);
          },
          { timeout: 10_000 },
        );

        proxy.stopAnswering();
        await sleep(SILENCE_LONGER_THAN_GIVE_UP_AFTER_IN_MILLISECONDS);
        proxy.answerAgain();
        await waitForLogMessage(workerProcess.logLines, "run completed");

        expect(await fixtureRecordsOf(redisUrl, keyPrefix, "wait-then-record")).toEqual([
          { step: "wait", attempt: 1 },
          { step: "record", attempt: 1 },
        ]);
        expect(workerProcess.logLines.map(({ message }) => message)).not.toContain("attempt aborted: lock lost");
        expect(workerProcess.logLines).toContainEqual(
          expect.objectContaining({ message: "run completed", fields: expect.objectContaining({ runId }) }),
        );
        expect(workerProcess.logLines.filter(({ message }) => message.startsWith("connection "))).toEqual([
          expect.objectContaining({ message: "connection error" }),
          expect.objectContaining({ message: "connection restored" }),
        ]);
      } finally {
        await nextasking.close();
        await workerProcess.stop();
      }
    } finally {
      await proxy.stop();
    }
  },
);
