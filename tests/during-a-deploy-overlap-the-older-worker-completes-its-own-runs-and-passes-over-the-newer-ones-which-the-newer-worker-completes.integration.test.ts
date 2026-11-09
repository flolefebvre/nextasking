import { setTimeout as sleep } from "node:timers/promises";
import { expect, vi } from "vitest";

import { createEngine, RECORD_VERSION, serializePayload } from "../src/engine/engine.js";
import { engineKeysOf, storedRunIdOf } from "../src/engine/engine-keys.js";
import { deriveFiringRunId } from "../src/ids.js";
import type { RunId } from "../src/ids.js";
import { isFetchRequest } from "./engine-requests.js";
import { fixtureRecordsOf } from "./fixture-records.js";
import { integrationTest } from "./integration-test.js";
import { limitedExecutionsOf } from "./limited-executions.js";
import { waitForLogMessage } from "./recording-logger.js";
import { storedRunIdentities } from "./stored-run-identities.js";
import { storedRun } from "./stored-run.js";
import { moveNextFiringTo, storedSchedules } from "./stored-schedules.js";
import { startTcpProxy } from "./tcp-proxy.js";
import { withRedis } from "./with-redis.js";
import { startWorkerProcess } from "./worker-process.js";
import type { WorkerProcess } from "./worker-process.js";
import { createWorkerProcessNextasking } from "./worker-process-nextasking.js";

const TASK = "three-in-all";

const PAYLOAD = { concurrencyKey: "", workInMilliseconds: 10 };

const OLDER_RECORD_VERSION = RECORD_VERSION - 1;

const NEWER_RUN_COUNT_AHEAD = 5;

const OLDER_RUN_COUNT = 3;

const NEWER_RUN_COUNT_FILLING_THE_HEAD = 10;

const PASS_OVER_WAIT_IN_MILLISECONDS = 2000;

const PAUSE_WINDOW_IN_MILLISECONDS = 6000;

const MOST_FETCHES_IN_THE_PAUSE_WINDOW = 1 + Math.ceil(PAUSE_WINDOW_IN_MILLISECONDS / PASS_OVER_WAIT_IN_MILLISECONDS);

const OLDER_WORKER_TIMING = {
  supportedRecordVersion: OLDER_RECORD_VERSION,
  passOverWaitInMilliseconds: PASS_OVER_WAIT_IN_MILLISECONDS,
  tickIntervalInMilliseconds: 200,
};

const WORKER_READY_BOUND_IN_MILLISECONDS = 20_000;

const RUNS_COMPLETED_BOUND_IN_MILLISECONDS = 15_000;

function isFetchScriptCall(requestChunk: string, keyPrefix: string): boolean {
  return requestChunk.includes("EVALSHA") && isFetchRequest(requestChunk, keyPrefix);
}

async function dispatchedRunIdsOf(
  nextasking: ReturnType<typeof createWorkerProcessNextasking>,
  runCount: number,
): Promise<RunId[]> {
  const runIds: RunId[] = [];
  for (let position = 0; position < runCount; position += 1) {
    runIds.push((await nextasking.dispatch(TASK, PAYLOAD)).runId);
  }
  return runIds;
}

async function enqueuedAtTheOlderVersionRunIdsOf(redisUrl: string, keyPrefix: string): Promise<RunId[]> {
  const runIds = Array.from({ length: OLDER_RUN_COUNT }, (_, position) => `older-run-${String(position + 1)}`);
  await withRedis(redisUrl, (_redis, client) =>
    createEngine(client, keyPrefix, OLDER_RECORD_VERSION).enqueue({
      runs: runIds.map((runId) => ({ runId, task: TASK, payload: serializePayload(PAYLOAD), retries: 0 })),
    }),
  );
  return runIds;
}

async function pendingRunTracesOf(redisUrl: string, keyPrefix: string, runIds: readonly RunId[]) {
  const keys = engineKeysOf(keyPrefix);
  return withRedis(redisUrl, (redis) =>
    Promise.all(
      runIds.map(async (runId) => ({
        runId,
        record: await redis.hGetAll(keys.runRecord(TASK, runId)),
        pendingScore: await redis.zScore(keys.pending, storedRunIdOf(TASK, runId)),
        pendingIndexScore: await redis.zScore(keys.runIndex(TASK, "pending"), storedRunIdOf(TASK, runId)),
      })),
    ),
  );
}

async function waitUntilCompletedBy(
  redisUrl: string,
  keyPrefix: string,
  runIds: readonly RunId[],
): Promise<Map<RunId, number[]>> {
  const pidsByRunId = new Map<RunId, number[]>();
  await vi.waitFor(
    async () => {
      const executions = await limitedExecutionsOf(redisUrl, keyPrefix, [TASK], new Map());
      pidsByRunId.clear();
      for (const execution of executions.filter(({ endedAt }) => endedAt !== undefined)) {
        pidsByRunId.set(execution.runId, [...(pidsByRunId.get(execution.runId) ?? []), execution.pid]);
      }
      expect(runIds.filter((runId) => !pidsByRunId.has(runId))).toEqual([]);
    },
    { timeout: RUNS_COMPLETED_BOUND_IN_MILLISECONDS, interval: 200 },
  );
  return pidsByRunId;
}

function startedRunIdsOf(workerProcess: WorkerProcess): unknown[] {
  return workerProcess.logLines
    .filter((line) => line.message === "attempt started")
    .map((line) => line.fields.runId)
    .sort();
}

integrationTest(
  "during a deploy overlap the older worker completes the runs of its own version behind newer ones, passes over the newer ones without touching or locking them, pauses while the head of pending is all newer and logs that once, and the newer worker completes them",
  {
    timeout:
      2 * WORKER_READY_BOUND_IN_MILLISECONDS +
      2 * RUNS_COMPLETED_BOUND_IN_MILLISECONDS +
      PAUSE_WINDOW_IN_MILLISECONDS +
      10_000 +
      5_000,
  },
  async ({ redisUrl, keyPrefix }) => {
    const nextasking = createWorkerProcessNextasking(redisUrl, keyPrefix);
    const proxy = await startTcpProxy(redisUrl);
    const workerProcesses: WorkerProcess[] = [];

    try {
      const newerRunIdsAhead = await dispatchedRunIdsOf(nextasking, NEWER_RUN_COUNT_AHEAD);
      const olderRunIds = await enqueuedAtTheOlderVersionRunIdsOf(redisUrl, keyPrefix);
      const tracesOfTheNewerRunsAhead = await pendingRunTracesOf(redisUrl, keyPrefix, newerRunIdsAhead);
      const olderWorker = await startWorkerProcess({
        redisUrl,
        keyPrefix,
        connection: { url: proxy.url },
        timing: OLDER_WORKER_TIMING,
      });
      workerProcesses.push(olderWorker);
      const pidsByOlderRunId = await waitUntilCompletedBy(redisUrl, keyPrefix, olderRunIds);
      await waitForLogMessage(olderWorker.logLines, "runs passed over: newer record version");
      const newerRunIdsFillingTheHead = await dispatchedRunIdsOf(nextasking, NEWER_RUN_COUNT_FILLING_THE_HEAD);
      const tracesOfTheNewerRunsFillingTheHead = await pendingRunTracesOf(
        redisUrl,
        keyPrefix,
        newerRunIdsFillingTheHead,
      );
      const fetchRequestChunksOf = proxy.recordRequestChunks();
      await sleep(PAUSE_WINDOW_IN_MILLISECONDS);
      const fetchCountInThePauseWindow = fetchRequestChunksOf().filter((requestChunk) =>
        isFetchScriptCall(requestChunk, keyPrefix),
      ).length;
      const newerRunIds = [...newerRunIdsAhead, ...newerRunIdsFillingTheHead];
      const tracesOfTheNewerRunsAfterTheOlderWorker = await pendingRunTracesOf(redisUrl, keyPrefix, newerRunIds);
      const lockCountBeforeTheNewerWorker = await withRedis(redisUrl, (redis) =>
        redis.zCard(engineKeysOf(keyPrefix).locks),
      );
      const newerWorker = await startWorkerProcess({ redisUrl, keyPrefix });
      workerProcesses.push(newerWorker);
      const pidsByRunId = await waitUntilCompletedBy(redisUrl, keyPrefix, newerRunIds);

      expect(olderRunIds.map((runId) => pidsByOlderRunId.get(runId))).toEqual(olderRunIds.map(() => [olderWorker.pid]));
      expect(
        [...tracesOfTheNewerRunsAhead, ...tracesOfTheNewerRunsFillingTheHead].map(({ record }) => record.state),
      ).toEqual(newerRunIds.map(() => "pending"));
      expect(tracesOfTheNewerRunsAfterTheOlderWorker).toEqual([
        ...tracesOfTheNewerRunsAhead,
        ...tracesOfTheNewerRunsFillingTheHead,
      ]);
      expect(lockCountBeforeTheNewerWorker).toBe(0);
      expect(fetchCountInThePauseWindow).toBeGreaterThanOrEqual(2);
      expect(fetchCountInThePauseWindow).toBeLessThanOrEqual(MOST_FETCHES_IN_THE_PAUSE_WINDOW);
      expect(olderWorker.logLines.filter((line) => line.message === "runs passed over: newer record version")).toEqual([
        {
          level: "debug",
          fields: { recordVersion: RECORD_VERSION, supportedVersion: OLDER_RECORD_VERSION },
          message: "runs passed over: newer record version",
        },
      ]);
      expect(startedRunIdsOf(olderWorker)).toEqual([...olderRunIds].sort());
      expect(newerRunIds.map((runId) => pidsByRunId.get(runId))).toEqual(newerRunIds.map(() => [newerWorker.pid]));
    } finally {
      await Promise.all(workerProcesses.map((workerProcess) => workerProcess.stop()));
      await proxy.stop();
      await nextasking.close();
    }
  },
);

const SCHEDULED_TASK = "block-event-loop-on-schedule";

const PLANNED_INSTANT = Date.parse("2026-01-01T00:00:00.000Z");

const FIRED_RUN_COMPLETED_BOUND_IN_MILLISECONDS = 15_000;

integrationTest(
  "a schedule record the older release wrote is fired once by the newer worker after the older one is gone, and keeps the older version",
  {
    timeout: 2 * WORKER_READY_BOUND_IN_MILLISECONDS + FIRED_RUN_COMPLETED_BOUND_IN_MILLISECONDS + 10_000 + 5_000,
  },
  async ({ redisUrl, keyPrefix }) => {
    const workerProcesses: WorkerProcess[] = [];

    try {
      const olderWorker = await startWorkerProcess({ redisUrl, keyPrefix, timing: OLDER_WORKER_TIMING });
      workerProcesses.push(olderWorker);
      await olderWorker.stop();
      const newerWorker = await startWorkerProcess({ redisUrl, keyPrefix });
      workerProcesses.push(newerWorker);
      await moveNextFiringTo(redisUrl, keyPrefix, SCHEDULED_TASK, PLANNED_INSTANT);
      const firingRunId = deriveFiringRunId(SCHEDULED_TASK, PLANNED_INSTANT);
      await vi.waitFor(
        async () => {
          expect((await storedRun(redisUrl, keyPrefix, SCHEDULED_TASK, firingRunId))?.state).toBe("completed");
        },
        { timeout: FIRED_RUN_COMPLETED_BOUND_IN_MILLISECONDS, interval: 200 },
      );

      expect(await storedRunIdentities(redisUrl, keyPrefix)).toEqual([{ task: SCHEDULED_TASK, runId: firingRunId }]);
      expect(await fixtureRecordsOf(redisUrl, keyPrefix, SCHEDULED_TASK)).toEqual([
        { runId: firingRunId, scheduledAt: new Date(PLANNED_INSTANT).toISOString(), event: "started" },
      ]);
      expect(await storedSchedules(redisUrl, keyPrefix)).toEqual([
        expect.objectContaining({
          task: SCHEDULED_TASK,
          version: String(OLDER_RECORD_VERSION),
          previousFiringAt: String(PLANNED_INSTANT),
        }),
      ]);
      expect(newerWorker.logLines.filter((line) => line.message.startsWith("schedule "))).toEqual([]);
    } finally {
      await Promise.all(workerProcesses.map((workerProcess) => workerProcess.stop()));
    }
  },
);
