import { expect, vi } from "vitest";

import type { EpochMilliseconds } from "../src/durations.js";
import type { RunId } from "../src/ids.js";
import { fixtureRecordsOf } from "./fixture-records.js";
import { integrationTest } from "./integration-test.js";
import { storedRun } from "./stored-run.js";
import type { StoredRun } from "./stored-run.js";
import { startWorkerProcess } from "./worker-process.js";
import type { WorkerProcess } from "./worker-process.js";
import { createWorkerProcessNextasking } from "./worker-process-nextasking.js";

const LOCK_DURATION_IN_MILLISECONDS = 4000;

const KILLED_WORKER_TIMING = {
  lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS,
  lockTimeLeftThatTriggersRenewalInMilliseconds: 3000,
};

const PICKUP_AFTER_THE_LOCK_EXPIRED_BOUND_IN_MILLISECONDS = 2000;

const WORKERS_READY_BOUND_IN_MILLISECONDS = 20_000;

const ATTEMPT_STARTED_BOUND_IN_MILLISECONDS = 10_000;

const RUNS_COMPLETED_BOUND_IN_MILLISECONDS = 20_000;

const TEST_TIMEOUT_IN_MILLISECONDS =
  WORKERS_READY_BOUND_IN_MILLISECONDS +
  2 * ATTEMPT_STARTED_BOUND_IN_MILLISECONDS +
  RUNS_COMPLETED_BOUND_IN_MILLISECONDS +
  5_000;

type TwoWorkers = {
  nextasking: ReturnType<typeof createWorkerProcessNextasking>;
  workerProcesses: readonly WorkerProcess[];
};

type KilledHolder = { survivingProcess: WorkerProcess; killedHoldersLockedUntil: EpochMilliseconds };

async function withTwoWorkers(
  redisUrl: string,
  keyPrefix: string,
  use: (twoWorkers: TwoWorkers) => Promise<void>,
): Promise<void> {
  const nextasking = createWorkerProcessNextasking(redisUrl, keyPrefix);
  const workerProcesses: WorkerProcess[] = [];
  try {
    workerProcesses.push(
      ...(await Promise.all(
        [1, 2].map(() => startWorkerProcess({ redisUrl, keyPrefix, timing: KILLED_WORKER_TIMING })),
      )),
    );
    await use({ nextasking, workerProcesses });
  } finally {
    await Promise.all(workerProcesses.map((workerProcess) => workerProcess.stop()));
    await nextasking.close();
  }
}

function startedAttemptsOf(workerProcess: WorkerProcess, runId: RunId) {
  return workerProcess.logLines.filter((line) => line.message === "attempt started" && line.fields.runId === runId);
}

async function killedHolderOf(
  { redisUrl, keyPrefix, task, runId }: { redisUrl: string; keyPrefix: string; task: string; runId: RunId },
  workerProcesses: readonly WorkerProcess[],
): Promise<KilledHolder> {
  const holdingProcess = await vi.waitUntil(
    () => workerProcesses.find((workerProcess) => startedAttemptsOf(workerProcess, runId).length > 0),
    { timeout: ATTEMPT_STARTED_BOUND_IN_MILLISECONDS },
  );
  holdingProcess.kill("SIGKILL");
  await holdingProcess.exited;
  const runOfTheKilledHolder = await storedRun(redisUrl, keyPrefix, task, runId);
  expect(runOfTheKilledHolder?.state, "the run must still be locked by the killed worker").toBe("executing");
  return {
    survivingProcess: workerProcesses.find((workerProcess) => workerProcess !== holdingProcess)!,
    killedHoldersLockedUntil: Number(runOfTheKilledHolder?.lockedUntil),
  };
}

async function completedRunsOf(
  { redisUrl, keyPrefix, task }: { redisUrl: string; keyPrefix: string; task: string },
  runIds: readonly RunId[],
): Promise<StoredRun[]> {
  return vi.waitUntil(
    async () => {
      const runs = await Promise.all(runIds.map((runId) => storedRun(redisUrl, keyPrefix, task, runId)));
      return runs.every((run) => run?.state === "completed") ? (runs as StoredRun[]) : undefined;
    },
    { timeout: RUNS_COMPLETED_BOUND_IN_MILLISECONDS, interval: 200 },
  );
}

function expectPickedUpWithinTheBoundAfter(lockedUntil: EpochMilliseconds, pickedUpRun: StoredRun): void {
  const pickupAfterTheLockExpiredInMilliseconds = Number(pickedUpRun.startedAt) - lockedUntil;
  expect(pickupAfterTheLockExpiredInMilliseconds).toBeGreaterThanOrEqual(0);
  expect(pickupAfterTheLockExpiredInMilliseconds).toBeLessThanOrEqual(
    PICKUP_AFTER_THE_LOCK_EXPIRED_BOUND_IN_MILLISECONDS,
  );
}

integrationTest(
  "a SIGKILLed worker's run is found stalled and picked up again by another worker within its lock plus about a second, never before",
  { timeout: TEST_TIMEOUT_IN_MILLISECONDS },
  async ({ redisUrl, keyPrefix }) => {
    await withTwoWorkers(redisUrl, keyPrefix, async ({ nextasking, workerProcesses }) => {
      const task = "wait-then-record";
      const { runId } = await nextasking.dispatch(task, { waitInMilliseconds: 2000 });
      const { survivingProcess, killedHoldersLockedUntil } = await killedHolderOf(
        { redisUrl, keyPrefix, task, runId },
        workerProcesses,
      );
      const [pickedUpRun] = await completedRunsOf({ redisUrl, keyPrefix, task }, [runId]);

      expectPickedUpWithinTheBoundAfter(killedHoldersLockedUntil, pickedUpRun!);
      expect(survivingProcess.logLines.filter((line) => line.fields.runId === runId)).toEqual([
        { level: "warn", fields: { task, runId, attempt: 1, stalls: 1 }, message: "attempt stalled" },
        { level: "debug", fields: { task, runId, attempt: 1 }, message: "attempt started" },
        expect.objectContaining({ message: "run completed", fields: expect.objectContaining({ runId, attempt: 1 }) }),
      ]);
      expect(await fixtureRecordsOf(redisUrl, keyPrefix, task)).toEqual([
        { step: "wait", attempt: 1 },
        { step: "wait", attempt: 1 },
        { step: "record", attempt: 1 },
      ]);
    });
  },
);

integrationTest(
  "a key at its limit stays blocked while a SIGKILLed worker's lock lasts, and no longer: no run of the key starts before the lock expires, and the next starts within about a second after",
  { timeout: TEST_TIMEOUT_IN_MILLISECONDS },
  async ({ redisUrl, keyPrefix }) => {
    await withTwoWorkers(redisUrl, keyPrefix, async ({ nextasking, workerProcesses }) => {
      const task = "one-per-key";
      const { runId: killedRunId } = await nextasking.dispatch(task, {
        concurrencyKey: "account-7",
        workInMilliseconds: 3000,
      });
      const holdingProcess = await vi.waitUntil(
        () => workerProcesses.find((workerProcess) => startedAttemptsOf(workerProcess, killedRunId).length > 0),
        { timeout: ATTEMPT_STARTED_BOUND_IN_MILLISECONDS },
      );
      const { runId: parkedRunId } = await nextasking.dispatch(task, {
        concurrencyKey: "account-7",
        workInMilliseconds: 10,
      });
      await vi.waitUntil(
        () =>
          workerProcesses.some((workerProcess) =>
            workerProcess.logLines.some(
              (line) => line.message === "run waiting for a concurrency slot" && line.fields.runId === parkedRunId,
            ),
          ),
        { timeout: ATTEMPT_STARTED_BOUND_IN_MILLISECONDS },
      );
      expect(startedAttemptsOf(holdingProcess, killedRunId)).toHaveLength(1);
      const { killedHoldersLockedUntil } = await killedHolderOf(
        { redisUrl, keyPrefix, task, runId: killedRunId },
        workerProcesses,
      );
      const [killedRun, parkedRun] = await completedRunsOf({ redisUrl, keyPrefix, task }, [killedRunId, parkedRunId]);

      expectPickedUpWithinTheBoundAfter(killedHoldersLockedUntil, killedRun!);
      expect(Number(parkedRun!.startedAt)).toBeGreaterThanOrEqual(Number(killedRun!.finishedAt));
      expect(
        ((await fixtureRecordsOf(redisUrl, keyPrefix, task)) as { runId: RunId; event: string }[])
          .filter(({ event }) => event === "started")
          .map(({ runId }) => runId),
      ).toEqual([killedRunId, killedRunId, parkedRunId]);
    });
  },
);
