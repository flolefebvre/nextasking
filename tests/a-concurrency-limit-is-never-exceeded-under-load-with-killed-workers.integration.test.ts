import { setTimeout as sleep } from "node:timers/promises";
import { expect, vi } from "vitest";

import type { EpochMilliseconds } from "../src/durations.js";
import { integrationTest } from "./integration-test.js";
import { concurrencyViolationsIn, limitedExecutionsOf } from "./limited-executions.js";
import { seededRandomnessOfThisRun } from "./seeded-random.js";
import { startWorkerProcess } from "./worker-process.js";
import type { WorkerProcess } from "./worker-process.js";
import { createWorkerProcessNextasking } from "./worker-process-nextasking.js";

const LIMIT_BY_TASK_NAME = new Map<"one-per-key" | "two-per-key" | "three-in-all", number>([
  ["one-per-key", 1],
  ["two-per-key", 2],
  ["three-in-all", 3],
]);

const TASK_NAMES = [...LIMIT_BY_TASK_NAME.keys()];

const CONCURRENCY_KEYS = ["account-1", "account-2", "account-3"];

const RUN_COUNT = 60;

const WORKER_COUNT = 3;

const KILL_COUNT = 2;

const FAST_TIMING = {
  lockDurationInMilliseconds: 1000,
  stallIntervalInMilliseconds: 1000,
  concurrencySlotSafetyDelayInMilliseconds: 3000,
};

integrationTest(
  "a concurrency limit is never exceeded under randomized load with SIGKILLed workers, and every run completes",
  { timeout: 60_000 },
  async ({ redisUrl, keyPrefix }) => {
    const { random, pick, seedMessage } = seededRandomnessOfThisRun();
    const nextasking = createWorkerProcessNextasking(redisUrl, keyPrefix);
    const startWorker = () =>
      startWorkerProcess({ redisUrl, keyPrefix, timing: FAST_TIMING, startWorkerOptions: { concurrency: 5 } });
    const workerProcesses: WorkerProcess[] = [];
    const killTimeByPid = new Map<number, EpochMilliseconds>();

    try {
      for (let workerIndex = 0; workerIndex < WORKER_COUNT; workerIndex += 1) {
        workerProcesses.push(await startWorker());
      }
      const dispatches = Array.from({ length: RUN_COUNT }, () => ({
        taskName: pick(TASK_NAMES),
        concurrencyKey: pick(CONCURRENCY_KEYS),
        workInMilliseconds: 50 + Math.floor(random() * 250),
      }));
      const killDelaysInMilliseconds = Array.from({ length: KILL_COUNT }, () => 300 + Math.floor(random() * 1500));
      const runIds = await Promise.all(
        dispatches.map(async ({ taskName, concurrencyKey, workInMilliseconds }) => {
          const { runId } = await nextasking.dispatch(taskName, {
            concurrencyKey,
            workInMilliseconds,
          });
          return runId;
        }),
      );
      for (const killDelayInMilliseconds of killDelaysInMilliseconds) {
        await sleep(killDelayInMilliseconds);
        const killedProcess = pick(workerProcesses);
        killTimeByPid.set(killedProcess.pid ?? 0, Date.now());
        killedProcess.kill("SIGKILL");
        await killedProcess.exited;
        workerProcesses.splice(workerProcesses.indexOf(killedProcess), 1, await startWorker());
      }
      await vi.waitFor(
        async () => {
          const executions = await limitedExecutionsOf(redisUrl, keyPrefix, TASK_NAMES, killTimeByPid);
          const completedRunIds = new Set(
            executions.filter((execution) => execution.endedAt !== undefined).map((execution) => execution.runId),
          );
          expect(
            runIds.filter((runId) => !completedRunIds.has(runId)),
            seedMessage,
          ).toEqual([]);
        },
        { timeout: 40_000, interval: 500 },
      );
      const executions = await limitedExecutionsOf(redisUrl, keyPrefix, TASK_NAMES, killTimeByPid);

      expect(concurrencyViolationsIn(executions, LIMIT_BY_TASK_NAME), seedMessage).toEqual([]);
      expect(
        executions.filter((execution) => execution.endedAt === undefined && execution.killedAt === undefined),
        seedMessage,
      ).toEqual([]);
      expect(
        executions.filter((execution) => execution.attempt !== 1),
        seedMessage,
      ).toEqual([]);
    } finally {
      await Promise.all(workerProcesses.map((workerProcess) => workerProcess.stop()));
      await nextasking.close();
    }
  },
);
