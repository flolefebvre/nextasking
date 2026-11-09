import { expect, vi } from "vitest";

import { fixtureRecordsOf } from "./fixture-records.js";
import { integrationTest } from "./integration-test.js";
import { waitForLogMessage } from "./recording-logger.js";
import { storedRun } from "./stored-run.js";
import { startTcpProxy } from "./tcp-proxy.js";
import { startWorkerProcess } from "./worker-process.js";
import { createWorkerProcessNextasking } from "./worker-process-nextasking.js";

const TASK = "wait-for-abort-once";

const LOCK_DURATION_IN_MILLISECONDS = 5_000;

const LOCK_TIME_LEFT_THAT_TRIGGERS_RENEWAL_IN_MILLISECONDS = 500;

const TICK_INTERVAL_IN_MILLISECONDS = 500;

const PICKUP_BEFORE_THE_HANDLER_STARTS_BOUND_IN_MILLISECONDS = 1_000;

const CHILD_STALL_MARGIN_IN_MILLISECONDS = 1_000;

type AttemptRecord = { event: "started" | "aborted"; abortReason?: string; at: number };

integrationTest(
  "an attempt whose worker lost Redis is aborted with lock lost at its lock duration by the worker's own clock, before Redis answers, and the run completes once Redis is back, with one stall",
  { timeout: 20_000 + 10_000 + LOCK_DURATION_IN_MILLISECONDS + 10_000 + 20_000 + 5_000 },
  async ({ redisUrl, keyPrefix }) => {
    const proxy = await startTcpProxy(redisUrl);
    const nextasking = createWorkerProcessNextasking(redisUrl, keyPrefix);
    try {
      const workerProcess = await startWorkerProcess({
        redisUrl,
        keyPrefix,
        connection: { url: proxy.url, giveUpAfter: "1s" },
        timing: {
          lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS,
          lockTimeLeftThatTriggersRenewalInMilliseconds: LOCK_TIME_LEFT_THAT_TRIGGERS_RENEWAL_IN_MILLISECONDS,
          tickIntervalInMilliseconds: TICK_INTERVAL_IN_MILLISECONDS,
        },
      });
      try {
        const attemptRecords = async () => (await fixtureRecordsOf(redisUrl, keyPrefix, TASK)) as AttemptRecord[];
        const { runId } = await nextasking.dispatch(TASK, {});
        await vi.waitFor(async () => expect(await attemptRecords()).toHaveLength(1), { timeout: 10_000 });
        await proxy.cut();
        const runStoredDuringTheCut = await storedRun(redisUrl, keyPrefix, TASK, runId);
        await waitForLogMessage(workerProcess.logLines, "attempt aborted: lock lost", {
          timeout: LOCK_DURATION_IN_MILLISECONDS + 10_000,
        });
        await vi.waitFor(async () => expect(await attemptRecords()).toHaveLength(2), { timeout: 10_000 });
        const recordsBeforeRedisAnswers = await attemptRecords();
        await proxy.restore();
        await waitForLogMessage(workerProcess.logLines, "run completed", { timeout: 20_000 });

        const [started, aborted] = recordsBeforeRedisAnswers;
        const abortedAfterTheStartInMilliseconds = aborted!.at - started!.at;
        expect(Number(runStoredDuringTheCut?.lockedUntil) - Number(runStoredDuringTheCut?.startedAt)).toBe(
          LOCK_DURATION_IN_MILLISECONDS,
        );
        expect(recordsBeforeRedisAnswers).toEqual([
          { event: "started", at: expect.any(Number) },
          {
            event: "aborted",
            abortReason: `The attempt of run "${runId}" was aborted: it lost its lock, so another attempt may own the run now.`,
            at: expect.any(Number),
          },
        ]);
        expect(abortedAfterTheStartInMilliseconds).toBeGreaterThanOrEqual(
          LOCK_DURATION_IN_MILLISECONDS - PICKUP_BEFORE_THE_HANDLER_STARTS_BOUND_IN_MILLISECONDS,
        );
        expect(abortedAfterTheStartInMilliseconds).toBeLessThanOrEqual(
          LOCK_DURATION_IN_MILLISECONDS + TICK_INTERVAL_IN_MILLISECONDS + CHILD_STALL_MARGIN_IN_MILLISECONDS,
        );
        expect(await storedRun(redisUrl, keyPrefix, TASK, runId)).toMatchObject({ state: "completed", stalls: "1" });
      } finally {
        await workerProcess.stop();
      }
    } finally {
      await nextasking.close();
      await proxy.stop();
    }
  },
);
