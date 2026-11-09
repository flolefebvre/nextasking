import { setTimeout as sleep } from "node:timers/promises";
import { expect, vi } from "vitest";

import { createEngine } from "../../src/engine/engine.js";
import { createNextasking } from "../../src/index.js";
import type { EventId, RunId } from "../../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "../close-nextasking-and-check-its-duration.js";
import { endRuns, runIdsOf } from "../ended-runs.js";
import { fixtureRecordsOf } from "../fixture-records.js";
import { readTrafficOf } from "../read-traffic.js";
import { driversCreatedFor } from "../recorded-redis-drivers.js";
import { createRecordingLogger } from "../recording-logger.js";
import { seededRandomnessOfThisRun } from "../seeded-random.js";
import { startTcpProxy } from "../tcp-proxy.js";
import { withRedis } from "../with-redis.js";
import { startWorkerProcess } from "../worker-process.js";
import { benchmarkTest } from "./benchmark-test.js";
import {
  envelopeDefinitionsOf,
  HANDLED_RUNS_RECORD_NAME,
  LOAD_EVENT_NAME,
  SCHEDULED_TASK_NAME,
} from "./envelope-definitions.js";
import type { HandledRunRecord } from "./envelope-definitions.js";
import {
  ARRIVAL_TICK_COUNT,
  ARRIVAL_TICK_IN_MILLISECONDS,
  LOAD_DURATION_IN_MILLISECONDS,
  arrivalCountOfTick,
} from "./envelope-load.js";
import { throughputLineOf } from "./throughput.js";
import { unfinishedRunCountIn } from "./unfinished-run-count.js";

vi.mock("../../src/node-redis-driver.js", async (importOriginal) =>
  (await import("../recorded-redis-drivers.js")).nodeRedisDriverModuleRecordingDrivers(await importOriginal()),
);

const WORKER_PROCESS_COUNT = 3;

const KEPT_RUNS_PER_TASK_AND_ENDED_STATE = 1000;

const EMITTED_SHARE_OF_ARRIVALS = 0.1;

const ARRIVAL_COUNT = 1950;

const READ_CYCLE_PAUSE_IN_MILLISECONDS = 250;

const LEAST_ROUND_TRIPS_PER_READ = 1;

const MOST_ROUND_TRIPS_PER_READ = 2;

const LEAST_READ_CYCLE_COUNT = 100;

const LOAD_LATENESS_ALLOWED_IN_MILLISECONDS = 2_000;

const DRAIN_TIMEOUT_IN_MILLISECONDS = 5_000;

const DRAIN_POLL_INTERVAL_IN_MILLISECONDS = 100;

const SEEDING_BUDGET_IN_MILLISECONDS = 60_000;

const WORKER_PROCESS_STARTS_BUDGET_IN_MILLISECONDS = 20_000;

const CLOSES_BUDGET_IN_MILLISECONDS = 30_000;

type RunIdentity = { task: string; runId: RunId };

type MeasuredRead = { read: string; roundTripCount: number };

function handledRunKeyOf({ task, runId }: RunIdentity): string {
  return `${task} ${runId}`;
}

function definitionsOfTheTestProcess() {
  return envelopeDefinitionsOf(async () => {
    throw new Error("The test process only dispatches, emits and reads: the worker processes run the tasks");
  });
}

benchmarkTest(
  "under 10 runs per second with bursts of 100 over 50 tasks with 100,000 runs kept, no read takes over two round trips and three workers keep up",
  {
    timeout:
      SEEDING_BUDGET_IN_MILLISECONDS +
      WORKER_PROCESS_STARTS_BUDGET_IN_MILLISECONDS +
      LOAD_DURATION_IN_MILLISECONDS +
      LOAD_LATENESS_ALLOWED_IN_MILLISECONDS +
      DRAIN_TIMEOUT_IN_MILLISECONDS +
      CLOSES_BUDGET_IN_MILLISECONDS +
      5_000,
  },
  async ({ redisUrl, keyPrefix }) => {
    const { random, pick, seedMessage } = seededRandomnessOfThisRun();
    const { plainTaskNames, tasks, events } = definitionsOfTheTestProcess();
    const dispatchedTaskNames = [...plainTaskNames, SCHEDULED_TASK_NAME];
    await withRedis(redisUrl, async (_redis, client) => {
      const engine = createEngine(client, keyPrefix);
      for (const { name } of tasks) {
        await endRuns(engine, name, runIdsOf(`completed-${name}`, KEPT_RUNS_PER_TASK_AND_ENDED_STATE), "complete");
        await endRuns(engine, name, runIdsOf(`failed-${name}`, KEPT_RUNS_PER_TASK_AND_ENDED_STATE), "fail");
      }
    });
    const createAppInstance = (url: string) =>
      createNextasking({
        connection: { url },
        prefix: keyPrefix,
        logger: createRecordingLogger().logger,
        tasks,
        events,
      });
    const app = createAppInstance(redisUrl);
    const readerProxy = await startTcpProxy(redisUrl);
    const reader = createAppInstance(readerProxy.url);
    const workerProcesses = await Promise.all(
      Array.from({ length: WORKER_PROCESS_COUNT }, () =>
        startWorkerProcess({ redisUrl, keyPrefix, definitions: "envelope load" }),
      ),
    );
    try {
      await reader.listSchedules();
      const readerDrivers = driversCreatedFor(readerProxy.url);
      expect(readerDrivers).toHaveLength(1);
      const [readerDriver] = readerDrivers;
      let latestRun: RunIdentity = { task: "task-01", runId: runIdsOf("completed-task-01", 1)[0]! };
      const { eventId: eventIdEmittedBeforeTheLoad } = await app.emit(LOAD_EVENT_NAME, {});
      let latestEventId: EventId = eventIdEmittedBeforeTheLoad;
      const dispatchedRuns: RunIdentity[] = [];
      const emittedEventIds: EventId[] = [];
      const measuredReads: MeasuredRead[] = [];
      let readCycleCount = 0;
      let completedListingWithoutASecondPageCount = 0;
      let isLoadOver = false;
      const measureRead = async (read: string, readCall: () => Promise<unknown>) => {
        const { roundTripCount } = await readTrafficOf(readerDriver!, readCall);
        measuredReads.push({ read, roundTripCount });
      };
      const readCycles = (async () => {
        while (!isLoadOver) {
          const { task, runId } = latestRun;
          await measureRead("getRun", () => reader.getRun(task, runId));
          let nextCursor: string | undefined;
          await measureRead("listRuns completed", async () => {
            ({ nextCursor } = await reader.listRuns({ state: "completed" }));
          });
          if (nextCursor === undefined) {
            completedListingWithoutASecondPageCount += 1;
          } else {
            const cursor = nextCursor;
            await measureRead("listRuns completed, second page", () => reader.listRuns({ state: "completed", cursor }));
          }
          await measureRead("listRuns failed", () => reader.listRuns({ state: "failed" }));
          await measureRead("listRuns pending of one task", () => reader.listRuns({ state: "pending", task }));
          await measureRead("countRuns", () => reader.countRuns());
          await measureRead("getEvent", () => reader.getEvent(LOAD_EVENT_NAME, latestEventId));
          await measureRead("listSchedules", () => reader.listSchedules());
          readCycleCount += 1;
          await sleep(READ_CYCLE_PAUSE_IN_MILLISECONDS);
        }
      })();
      const arrive = async () => {
        if (random() < EMITTED_SHARE_OF_ARRIVALS) {
          const { eventId } = await app.emit(LOAD_EVENT_NAME, {});
          emittedEventIds.push(eventId);
          latestEventId = eventId;
          return;
        }
        const task = pick(dispatchedTaskNames);
        const { runId } =
          task === SCHEDULED_TASK_NAME ? await app.dispatch(SCHEDULED_TASK_NAME) : await app.dispatch(task, {});
        dispatchedRuns.push({ task, runId });
        latestRun = { task, runId };
      };
      const handledRunRecords = async () =>
        (await fixtureRecordsOf(redisUrl, keyPrefix, HANDLED_RUNS_RECORD_NAME)) as HandledRunRecord[];

      const loadStartedAt = performance.now();
      for (let tick = 0; tick < ARRIVAL_TICK_COUNT; tick += 1) {
        await Promise.all(Array.from({ length: arrivalCountOfTick(tick) }, arrive));
        await sleep(Math.max(0, loadStartedAt + (tick + 1) * ARRIVAL_TICK_IN_MILLISECONDS - performance.now()));
      }
      const loadEndedAt = performance.now();
      isLoadOver = true;
      await readCycles;
      const arrivalCount = dispatchedRuns.length + emittedEventIds.length;
      const runCountToHandle = arrivalCount + 1;
      const isEveryRunCompleted = await vi
        .waitUntil(
          async () =>
            (await handledRunRecords()).length >= runCountToHandle && unfinishedRunCountIn(await app.countRuns()) === 0,
          { timeout: DRAIN_TIMEOUT_IN_MILLISECONDS, interval: DRAIN_POLL_INTERVAL_IN_MILLISECONDS },
        )
        .then(
          () => true,
          () => false,
        );
      const drainedAfterInMilliseconds = performance.now() - loadEndedAt;
      const handledRunKeys = (await handledRunRecords()).map(handledRunKeyOf);
      const roundTripCounts = measuredReads.map(({ roundTripCount }) => roundTripCount);
      console.log(
        [
          throughputLineOf(
            `${String(WORKER_PROCESS_COUNT)} workers under 10 runs/s with bursts of 100/s over 50 tasks, 100,000 runs kept`,
            { runCount: arrivalCount, elapsedInMilliseconds: loadEndedAt - loadStartedAt },
          ),
          `benchmark: reads under that load: ${String(measuredReads.length)} reads in ${String(readCycleCount)} cycles, ${String(Math.min(...roundTripCounts))} to ${String(Math.max(...roundTripCounts))} round trips each; the queue drained ${(drainedAfterInMilliseconds / 1000).toFixed(2)} s after the last arrival`,
        ].join("\n"),
      );

      expect(
        measuredReads.filter(
          ({ roundTripCount }) =>
            roundTripCount < LEAST_ROUND_TRIPS_PER_READ || roundTripCount > MOST_ROUND_TRIPS_PER_READ,
        ),
        seedMessage,
      ).toEqual([]);
      expect(
        {
          hasEnoughReadCycles: readCycleCount >= LEAST_READ_CYCLE_COUNT,
          completedListingWithoutASecondPageCount,
        },
        seedMessage,
      ).toEqual({ hasEnoughReadCycles: true, completedListingWithoutASecondPageCount: 0 });
      expect(
        {
          arrivalCount,
          isLoadOnTime:
            loadEndedAt - loadStartedAt <= LOAD_DURATION_IN_MILLISECONDS + LOAD_LATENESS_ALLOWED_IN_MILLISECONDS,
        },
        seedMessage,
      ).toEqual({ arrivalCount: ARRIVAL_COUNT, isLoadOnTime: true });
      expect(
        {
          isEveryRunCompleted,
          handledRunCount: handledRunKeys.length,
          runsHandledMoreThanOnce: handledRunKeys.filter(
            (handledRunKey, position) => handledRunKeys.indexOf(handledRunKey) !== position,
          ),
          dispatchedRunsNeverHandled: dispatchedRuns
            .map(handledRunKeyOf)
            .filter((dispatchedRunKey) => !handledRunKeys.includes(dispatchedRunKey)),
        },
        seedMessage,
      ).toEqual({
        isEveryRunCompleted: true,
        handledRunCount: runCountToHandle,
        runsHandledMoreThanOnce: [],
        dispatchedRunsNeverHandled: [],
      });
    } finally {
      for (const workerProcess of workerProcesses) {
        workerProcess.kill("SIGTERM");
      }
      await Promise.all(workerProcesses.map(({ exited }) => exited));
      await closeNextaskingAndCheckItsDuration(app);
      await closeNextaskingAndCheckItsDuration(reader);
      await readerProxy.stop();
    }
  },
);
