import { expect, vi } from "vitest";

import { createEngine } from "../src/engine/engine.js";
import { deriveFiringRunId } from "../src/ids.js";
import { isFireRequestOf } from "./engine-requests.js";
import { fixtureRecordsOf } from "./fixture-records.js";
import { integrationTest } from "./integration-test.js";
import { storedRunIdentities } from "./stored-run-identities.js";
import { storedRun } from "./stored-run.js";
import { moveNextFiringTo, storedSchedules } from "./stored-schedules.js";
import { startTcpProxy } from "./tcp-proxy.js";
import type { TcpProxy } from "./tcp-proxy.js";
import { withRedis } from "./with-redis.js";
import { startWorkerProcess } from "./worker-process.js";
import type { WorkerProcess } from "./worker-process.js";

const SCHEDULED_TASK = "block-event-loop-on-schedule";

const PLANNED_INSTANT = Date.parse("2026-01-01T00:00:00.000Z");

const FIRING_AFTER_THE_PLANNED_INSTANT = Date.parse("2027-01-01T00:00:00.000Z");

const FIRING_RUN_ID = deriveFiringRunId(SCHEDULED_TASK, PLANNED_INSTANT);

const WORKER_COUNT = 3;

const WORKERS_READY_BOUND_IN_MILLISECONDS = 20_000;

const FIRE_REQUEST_ARRIVED_BOUND_IN_MILLISECONDS = 10_000;

const FIRING_RUN_COMPLETED_BOUND_IN_MILLISECONDS = 20_000;

const WORKER_EXIT_BOUND_IN_MILLISECONDS = 5_000;

const TEST_TIMEOUT_IN_MILLISECONDS =
  WORKERS_READY_BOUND_IN_MILLISECONDS +
  FIRE_REQUEST_ARRIVED_BOUND_IN_MILLISECONDS +
  FIRING_RUN_COMPLETED_BOUND_IN_MILLISECONDS +
  WORKER_EXIT_BOUND_IN_MILLISECONDS +
  5_000;

type WorkersBehindOneProxy = { proxy: TcpProxy; redisUrl: string; keyPrefix: string };

async function withWorkersBehindOneProxy(
  redisUrl: string,
  keyPrefix: string,
  use: (workersBehindOneProxy: WorkersBehindOneProxy) => Promise<void>,
): Promise<void> {
  const proxy = await startTcpProxy(redisUrl);
  const workerProcesses: WorkerProcess[] = [];
  try {
    workerProcesses.push(
      ...(await Promise.all(
        Array.from({ length: WORKER_COUNT }, () =>
          startWorkerProcess({
            redisUrl,
            keyPrefix,
            connection: { url: proxy.url, giveUpAfter: "30s" },
            startWorkerOptions: { shutdownTimeout: "1s" },
          }),
        ),
      )),
    );
    await use({ proxy, redisUrl, keyPrefix });
    for (const workerProcess of workerProcesses) {
      workerProcess.kill("SIGTERM");
    }
    const exits = await Promise.all(workerProcesses.map((workerProcess) => workerProcess.exited));

    expect(exits).toEqual(workerProcesses.map(() => ({ code: 0, signal: null })));
    expect(await storedRunIdentities(redisUrl, keyPrefix)).toEqual([{ task: SCHEDULED_TASK, runId: FIRING_RUN_ID }]);
    expect(await fixtureRecordsOf(redisUrl, keyPrefix, SCHEDULED_TASK)).toEqual([
      { runId: FIRING_RUN_ID, scheduledAt: new Date(PLANNED_INSTANT).toISOString(), event: "started" },
    ]);
    expect(await storedSchedules(redisUrl, keyPrefix)).toEqual([
      expect.objectContaining({
        task: SCHEDULED_TASK,
        previousFiringAt: String(PLANNED_INSTANT),
        nextFiringAt: String(FIRING_AFTER_THE_PLANNED_INSTANT),
      }),
    ]);
  } finally {
    await Promise.all(workerProcesses.map((workerProcess) => workerProcess.stop()));
    await proxy.stop();
  }
}

async function waitUntilTheFiringRunCompleted({ redisUrl, keyPrefix }: WorkersBehindOneProxy): Promise<void> {
  await vi.waitFor(
    async () => {
      expect((await storedRun(redisUrl, keyPrefix, SCHEDULED_TASK, FIRING_RUN_ID))?.state).toBe("completed");
    },
    { timeout: FIRING_RUN_COMPLETED_BOUND_IN_MILLISECONDS, interval: 200 },
  );
}

integrationTest(
  "workers firing the same planned instant start one run: the fire of the first is held until another's has fired, then hears it already fired",
  { timeout: TEST_TIMEOUT_IN_MILLISECONDS },
  async ({ redisUrl, keyPrefix }) => {
    await withWorkersBehindOneProxy(redisUrl, keyPrefix, async (workersBehindOneProxy) => {
      const { proxy } = workersBehindOneProxy;
      const heldFire = proxy.holdRequest((request) => isFireRequestOf(request, keyPrefix, SCHEDULED_TASK));

      await moveNextFiringTo(redisUrl, keyPrefix, SCHEDULED_TASK, PLANNED_INSTANT);
      const { release } = await vi.waitUntil(() => heldFire, { timeout: FIRE_REQUEST_ARRIVED_BOUND_IN_MILLISECONDS });
      await waitUntilTheFiringRunCompleted(workersBehindOneProxy);
      release();
    });
  },
);

integrationTest(
  "a fire whose reply is lost after it ran is sent again and fires nothing twice",
  { timeout: TEST_TIMEOUT_IN_MILLISECONDS },
  async ({ redisUrl, keyPrefix }) => {
    await withWorkersBehindOneProxy(redisUrl, keyPrefix, async (workersBehindOneProxy) => {
      const { proxy } = workersBehindOneProxy;
      await withRedis(redisUrl, (_redis, client) =>
        createEngine(client, keyPrefix).fire({
          task: "never-scheduled",
          instant: PLANNED_INSTANT,
          instantAfter: FIRING_AFTER_THE_PLANNED_INSTANT,
          runId: "never-fired",
          retries: 0,
        }),
      );
      const fireCut = proxy.cutBeforeReplyTo((request) => isFireRequestOf(request, keyPrefix, SCHEDULED_TASK));

      await moveNextFiringTo(redisUrl, keyPrefix, SCHEDULED_TASK, PLANNED_INSTANT);
      await vi.waitUntil(() => fireCut.then(() => true), { timeout: FIRE_REQUEST_ARRIVED_BOUND_IN_MILLISECONDS });
      const runStoredBeforeTheReplyWasLost = await storedRun(redisUrl, keyPrefix, SCHEDULED_TASK, FIRING_RUN_ID);
      await proxy.restore();
      await waitUntilTheFiringRunCompleted(workersBehindOneProxy);

      expect(runStoredBeforeTheReplyWasLost).toMatchObject({ scheduledAt: String(PLANNED_INSTANT) });
    });
  },
);
