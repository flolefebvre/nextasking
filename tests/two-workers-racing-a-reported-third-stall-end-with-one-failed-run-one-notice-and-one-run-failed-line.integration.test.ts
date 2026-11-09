import { expect, vi } from "vitest";

import { deriveFailureNoticeEventId, deriveListenerRunId } from "../src/ids.js";
import { isSettleRequestOf } from "./engine-requests.js";
import { engineTestContextOf, expireLock, fetchedLockedRunsOf } from "./engine-test.js";
import { fixtureRecordsOf } from "./fixture-records.js";
import { integrationTest } from "./integration-test.js";
import { storedRunIdentities } from "./stored-run-identities.js";
import { storedRun } from "./stored-run.js";
import { startTcpProxy } from "./tcp-proxy.js";
import { withRedis } from "./with-redis.js";
import { FAILURE_NOTICE_RECORDING_LISTENER_NAME } from "./worker-process-config.js";
import { startWorkerProcess } from "./worker-process.js";
import type { WorkerProcess } from "./worker-process.js";
import { createWorkerProcessNextasking } from "./worker-process-nextasking.js";

const TASK = "wait-for-abort";

const WORKER_READY_BOUND_IN_MILLISECONDS = 20_000;

const SETTLE_HELD_BOUND_IN_MILLISECONDS = 10_000;

const RUN_FAILED_BOUND_IN_MILLISECONDS = 10_000;

const NOTICE_RUN_COMPLETED_BOUND_IN_MILLISECONDS = 10_000;

const WORKER_EXIT_BOUND_IN_MILLISECONDS = 5_000;

function linesOfBoth(workerProcesses: readonly WorkerProcess[], message: string) {
  return workerProcesses.flatMap((workerProcess) => workerProcess.logLines.filter((line) => line.message === message));
}

integrationTest(
  "two workers racing a reported third stall end with one failed run, one notice run and one run failed line",
  {
    timeout:
      WORKER_READY_BOUND_IN_MILLISECONDS +
      SETTLE_HELD_BOUND_IN_MILLISECONDS +
      RUN_FAILED_BOUND_IN_MILLISECONDS +
      NOTICE_RUN_COMPLETED_BOUND_IN_MILLISECONDS +
      WORKER_EXIT_BOUND_IN_MILLISECONDS +
      5_000,
  },
  async ({ redisUrl, keyPrefix }) => {
    const nextasking = createWorkerProcessNextasking(redisUrl, keyPrefix);
    const proxy = await startTcpProxy(redisUrl);
    const workerProcesses: WorkerProcess[] = [];

    try {
      const { runId } = await nextasking.dispatch(TASK, {});
      await withRedis(redisUrl, async (redis, client) => {
        const context = engineTestContextOf(redis, client, keyPrefix);
        await fetchedLockedRunsOf(context, 1);
        await redis.hSet(context.keys.runRecord(TASK, runId), "stalls", 2);
      });
      workerProcesses.push(
        ...(await Promise.all(
          [1, 2].map(() =>
            startWorkerProcess({
              redisUrl,
              keyPrefix,
              connection: { url: proxy.url, giveUpAfter: "30s" },
              timing: { tickIntervalInMilliseconds: 500 },
              isFailureNoticeRecorded: true,
              startWorkerOptions: { shutdownTimeout: "1s" },
            }),
          ),
        )),
      );
      const heldSettle = proxy.holdRequest((request) =>
        isSettleRequestOf(request, keyPrefix, { task: TASK, runId, settleKind: "fail" }),
      );
      await withRedis(redisUrl, (redis, client) =>
        expireLock(engineTestContextOf(redis, client, keyPrefix), TASK, runId),
      );
      const { release } = await vi.waitUntil(() => heldSettle, { timeout: SETTLE_HELD_BOUND_IN_MILLISECONDS });
      await vi.waitFor(
        () => {
          expect(linesOfBoth(workerProcesses, "run failed")).toHaveLength(1);
        },
        { timeout: RUN_FAILED_BOUND_IN_MILLISECONDS },
      );
      release();
      const noticeRunId = deriveListenerRunId(
        deriveFailureNoticeEventId(TASK, runId, 0),
        FAILURE_NOTICE_RECORDING_LISTENER_NAME,
      );
      await vi.waitFor(
        async () => {
          expect(
            (await storedRun(redisUrl, keyPrefix, FAILURE_NOTICE_RECORDING_LISTENER_NAME, noticeRunId))?.state,
          ).toBe("completed");
        },
        { timeout: NOTICE_RUN_COMPLETED_BOUND_IN_MILLISECONDS, interval: 200 },
      );
      for (const workerProcess of workerProcesses) {
        workerProcess.kill("SIGTERM");
      }
      const exits = await Promise.all(workerProcesses.map((workerProcess) => workerProcess.exited));

      expect(exits).toEqual([
        { code: 0, signal: null },
        { code: 0, signal: null },
      ]);
      expect(linesOfBoth(workerProcesses, "run failed")).toEqual([
        {
          level: "error",
          fields: {
            task: TASK,
            runId,
            attempt: 1,
            err: { name: "StalledRunError", message: expect.stringContaining("stalled 3 times") },
            reason: "stalled",
            runDurationInMilliseconds: expect.any(Number),
            executionDurationInMilliseconds: expect.any(Number),
          },
          message: "run failed",
        },
      ]);
      expect(await storedRun(redisUrl, keyPrefix, TASK, runId)).toMatchObject({
        state: "failed",
        reason: "stalled",
        errors: [{ attempt: 1, name: "StalledRunError" }],
      });
      expect(await fixtureRecordsOf(redisUrl, keyPrefix, FAILURE_NOTICE_RECORDING_LISTENER_NAME)).toEqual([
        { task: TASK, runId, reason: "stalled" },
      ]);
      expect(
        (await storedRunIdentities(redisUrl, keyPrefix)).filter(
          ({ task }) => task === FAILURE_NOTICE_RECORDING_LISTENER_NAME,
        ),
      ).toEqual([{ task: FAILURE_NOTICE_RECORDING_LISTENER_NAME, runId: noticeRunId }]);
    } finally {
      await Promise.all(workerProcesses.map((workerProcess) => workerProcess.stop()));
      await proxy.stop();
      await nextasking.close();
    }
  },
);
