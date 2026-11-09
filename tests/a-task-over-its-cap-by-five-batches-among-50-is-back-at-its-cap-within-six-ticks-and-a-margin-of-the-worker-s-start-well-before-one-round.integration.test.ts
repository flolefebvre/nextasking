import { expect, vi } from "vitest";

import { createEngine, MOST_RUNS_PRUNED_PER_PRUNE } from "../src/engine/engine.js";
import { engineKeysOf } from "../src/engine/engine-keys.js";
import { DEFAULT_WORKER_TIMING } from "../src/worker-timing.js";
import { endRuns, runIdsOf } from "./ended-runs.js";
import { integrationTest } from "./integration-test.js";
import { serverInstantInMillisecondsOf } from "./server-instant.js";
import { withRedis } from "./with-redis.js";
import { startWorkerProcess } from "./worker-process.js";

const QUIET_TASK_NAMES = Array.from(
  { length: 49 },
  (_, position) => `quiet-task-${String(position + 1).padStart(2, "0")}`,
);

const BUSY_TASK = "busy-task";

const KEPT_RUN_COUNT = 10;

const BATCHES_OVER_THE_CAP = 5;

const TICK_DURATION_IN_MILLISECONDS = 200;

const TICKS_BEFORE_THE_FIRST_PRUNE = 1;

const MEASURED_MARGIN_IN_MILLISECONDS = 2_000;

const BACK_AT_THE_CAP_BOUND_IN_MILLISECONDS =
  (TICKS_BEFORE_THE_FIRST_PRUNE + BATCHES_OVER_THE_CAP) * TICK_DURATION_IN_MILLISECONDS +
  MEASURED_MARGIN_IN_MILLISECONDS;

const ONE_ROUND_OF_THE_50_IN_MILLISECONDS = (QUIET_TASK_NAMES.length + 1) * TICK_DURATION_IN_MILLISECONDS;

integrationTest(
  "a task over its cap by five batches among 50 tasks is back at its cap within six ticks and a margin of the worker's start, well before one round of the 50",
  { timeout: 20_000 + ONE_ROUND_OF_THE_50_IN_MILLISECONDS + 10_000 + 5_000 },
  async ({ redisUrl, keyPrefix }) => {
    const busyTaskCompletedIndexKey = engineKeysOf(keyPrefix).runIndex(BUSY_TASK, "completed");
    await withRedis(redisUrl, async (redis, client) => {
      const engine = createEngine(client, keyPrefix);
      for (const quietTaskName of QUIET_TASK_NAMES) {
        await endRuns(engine, quietTaskName, [`${quietTaskName}-run`], "complete");
      }
      await endRuns(
        engine,
        BUSY_TASK,
        runIdsOf("busy-run", KEPT_RUN_COUNT + BATCHES_OVER_THE_CAP * MOST_RUNS_PRUNED_PER_PRUNE),
        "complete",
      );

      const workerProcess = await startWorkerProcess({
        redisUrl,
        keyPrefix,
        timing: {
          tickIntervalInMilliseconds: TICK_DURATION_IN_MILLISECONDS,
          completedRunRetention: { ...DEFAULT_WORKER_TIMING.completedRunRetention, maxRunCount: KEPT_RUN_COUNT },
        },
      });
      try {
        const workerStartedAt = await serverInstantInMillisecondsOf(redis);
        await vi.waitFor(async () => expect(await redis.zCard(busyTaskCompletedIndexKey)).toBe(KEPT_RUN_COUNT), {
          timeout: ONE_ROUND_OF_THE_50_IN_MILLISECONDS,
          interval: TICK_DURATION_IN_MILLISECONDS / 4,
        });
        const backAtTheCapAt = await serverInstantInMillisecondsOf(redis);

        expect(backAtTheCapAt - workerStartedAt).toBeLessThanOrEqual(BACK_AT_THE_CAP_BOUND_IN_MILLISECONDS);
      } finally {
        await workerProcess.stop();
      }
    });
  },
);
