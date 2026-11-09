import { setTimeout as sleep } from "node:timers/promises";

import { expect, vi } from "vitest";

import { engineKeysOf, runIdentityOf } from "../src/engine/engine-keys.js";
import { MOST_RUNS_PRUNED_PER_PRUNE } from "../src/engine/engine.js";
import { DEFAULT_WORKER_TIMING } from "../src/worker-timing.js";
import { runIdsOf } from "./ended-runs.js";
import { integrationTest } from "./integration-test.js";
import { serverInstantInMillisecondsOf } from "./server-instant.js";
import { withRedis } from "./with-redis.js";
import type { TestClient } from "./with-redis.js";
import { startWorkerProcess } from "./worker-process.js";
import { createWorkerProcessNextasking } from "./worker-process-nextasking.js";

const TASK = "complete-at-once";

const RETAINED_RUN_COUNT = DEFAULT_WORKER_TIMING.completedRunRetention.maxRunCount;

const RUN_COUNT = RETAINED_RUN_COUNT + MOST_RUNS_PRUNED_PER_PRUNE;

const DISPATCHES_PER_BATCH = 100;

const SAMPLING_INTERVAL_IN_MILLISECONDS = 50;

const TICK_INTERVAL_IN_MILLISECONDS = DEFAULT_WORKER_TIMING.tickIntervalInMilliseconds;

const PRUNE_MARGIN_IN_MILLISECONDS = 1_000;

const ALL_COMPLETED_BOUND_IN_MILLISECONDS = 60_000;

const PRUNED_TO_THE_CAP_BOUND_IN_MILLISECONDS = 10_000;

async function finishedAtByRunIdIn(redis: TestClient, completedIndexKey: string): Promise<Map<string, number>> {
  const entries = await redis.zRangeWithScores(completedIndexKey, 0, -1);
  return new Map(entries.map(({ value, score }) => [runIdentityOf(value)!.runId, score]));
}

integrationTest(
  "a worker that completed 1,200 runs of one task holds exactly the 1,000 newest within two ticks of the last completion",
  {
    timeout: 20_000 + ALL_COMPLETED_BOUND_IN_MILLISECONDS + PRUNED_TO_THE_CAP_BOUND_IN_MILLISECONDS + 10_000 + 5_000,
  },
  async ({ redisUrl, keyPrefix }) => {
    const keys = engineKeysOf(keyPrefix);
    const nextasking = createWorkerProcessNextasking(redisUrl, keyPrefix);
    const workerProcess = await startWorkerProcess({ redisUrl, keyPrefix });
    try {
      await withRedis(redisUrl, async (redis) => {
        const sampledFinishedAtByRunId = new Map<string, number>();
        let isSampling = true;
        const sampling = (async () => {
          while (isSampling) {
            for (const [runId, finishedAt] of await finishedAtByRunIdIn(redis, keys.runIndex(TASK, "completed"))) {
              sampledFinishedAtByRunId.set(runId, finishedAt);
            }
            await sleep(SAMPLING_INTERVAL_IN_MILLISECONDS);
          }
        })();
        const runIds = runIdsOf("run", RUN_COUNT);
        try {
          for (let offset = 0; offset < RUN_COUNT; offset += DISPATCHES_PER_BATCH) {
            await Promise.all(
              runIds
                .slice(offset, offset + DISPATCHES_PER_BATCH)
                .map((runId) => nextasking.dispatch(TASK, {}, { runId })),
            );
          }
          await vi.waitFor(
            async () => {
              expect(await redis.zCard(keys.runIndex(TASK, "pending"))).toBe(0);
              expect(await redis.zCard(keys.runIndex(TASK, "executing"))).toBe(0);
            },
            { timeout: ALL_COMPLETED_BOUND_IN_MILLISECONDS },
          );
          const [newest] = await redis.zRangeWithScores(keys.runIndex(TASK, "completed"), -1, -1);
          await vi.waitFor(
            async () => expect(await redis.zCard(keys.runIndex(TASK, "completed"))).toBe(RETAINED_RUN_COUNT),
            { timeout: PRUNED_TO_THE_CAP_BOUND_IN_MILLISECONDS, interval: SAMPLING_INTERVAL_IN_MILLISECONDS },
          );
          const prunedToTheCapAt = await serverInstantInMillisecondsOf(redis);
          await sleep(2 * TICK_INTERVAL_IN_MILLISECONDS);
          const retainedFinishedAtByRunId = await finishedAtByRunIdIn(redis, keys.runIndex(TASK, "completed"));
          const runCounts = await nextasking.countRuns();

          const prunedRunIds = runIds.filter((runId) => !retainedFinishedAtByRunId.has(runId));
          const oldestRetainedFinishedAt = Math.min(...retainedFinishedAtByRunId.values());
          expect(prunedToTheCapAt - newest!.score).toBeLessThanOrEqual(
            2 * TICK_INTERVAL_IN_MILLISECONDS + PRUNE_MARGIN_IN_MILLISECONDS,
          );
          expect(retainedFinishedAtByRunId.size).toBe(RETAINED_RUN_COUNT);
          expect(runCounts[TASK].completed).toBe(RETAINED_RUN_COUNT);
          expect(prunedRunIds).toHaveLength(MOST_RUNS_PRUNED_PER_PRUNE);
          expect(prunedRunIds.filter((runId) => !sampledFinishedAtByRunId.has(runId))).toEqual([]);
          expect(
            prunedRunIds.filter((runId) => sampledFinishedAtByRunId.get(runId)! > oldestRetainedFinishedAt),
          ).toEqual([]);
        } finally {
          isSampling = false;
          await sampling;
        }
      });
    } finally {
      await nextasking.close();
      await workerProcess.stop();
    }
  },
);
