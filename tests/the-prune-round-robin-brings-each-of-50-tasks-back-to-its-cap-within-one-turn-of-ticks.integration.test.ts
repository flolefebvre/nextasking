import { expect, vi } from "vitest";

import { createEngine } from "../src/engine/engine.js";
import { engineKeysOf } from "../src/engine/engine-keys.js";
import { DEFAULT_WORKER_TIMING } from "../src/worker-timing.js";
import { endRuns, runIdsOf } from "./ended-runs.js";
import { integrationTest } from "./integration-test.js";
import { serverInstantInMillisecondsOf } from "./server-instant.js";
import { withRedis } from "./with-redis.js";
import { startWorkerProcess } from "./worker-process.js";

const TASK_COUNT = 50;

const TASK_NAMES = Array.from({ length: TASK_COUNT }, (_, position) => `task-${String(position + 1).padStart(2, "0")}`);

const MAX_RUN_COUNT = 10;

const COMPLETED_RUNS_PER_TASK = MAX_RUN_COUNT + 2;

const TICK_INTERVAL_IN_MILLISECONDS = 200;

const ONE_TURN_IN_MILLISECONDS = TASK_COUNT * TICK_INTERVAL_IN_MILLISECONDS;

const CHILD_MARGIN_IN_MILLISECONDS = 2_000;

const ALL_CAPPED_BOUND_IN_MILLISECONDS =
  ONE_TURN_IN_MILLISECONDS + TICK_INTERVAL_IN_MILLISECONDS + CHILD_MARGIN_IN_MILLISECONDS;

integrationTest(
  "the prune round robin brings each of 50 tasks back to its cap within one turn of ticks",
  { timeout: 20_000 + 20_000 + ALL_CAPPED_BOUND_IN_MILLISECONDS + 10_000 + 5_000 },
  async ({ redisUrl, keyPrefix }) => {
    const keys = engineKeysOf(keyPrefix);
    await withRedis(redisUrl, async (redis, client) => {
      const engine = createEngine(client, keyPrefix);
      for (const taskName of TASK_NAMES) {
        await endRuns(engine, taskName, runIdsOf(`${taskName}-run`, COMPLETED_RUNS_PER_TASK), "complete");
      }
      const completedRunCounts = () =>
        Promise.all(TASK_NAMES.map((taskName) => redis.zCard(keys.runIndex(taskName, "completed"))));

      const workerProcess = await startWorkerProcess({
        redisUrl,
        keyPrefix,
        timing: {
          tickIntervalInMilliseconds: TICK_INTERVAL_IN_MILLISECONDS,
          completedRunRetention: {
            maxAgeInMilliseconds: DEFAULT_WORKER_TIMING.completedRunRetention.maxAgeInMilliseconds,
            maxRunCount: MAX_RUN_COUNT,
          },
        },
      });
      try {
        const workerReadyAt = await serverInstantInMillisecondsOf(redis);
        await vi.waitFor(async () => expect(await completedRunCounts()).toEqual(TASK_NAMES.map(() => MAX_RUN_COUNT)), {
          timeout: 2 * ALL_CAPPED_BOUND_IN_MILLISECONDS,
          interval: TICK_INTERVAL_IN_MILLISECONDS / 2,
        });
        const allCappedAt = await serverInstantInMillisecondsOf(redis);

        expect(allCappedAt - workerReadyAt).toBeLessThanOrEqual(ALL_CAPPED_BOUND_IN_MILLISECONDS);
      } finally {
        await workerProcess.stop();
      }
    });
  },
);
