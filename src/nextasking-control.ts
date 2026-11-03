import { Job } from "bullmq";
import type { Redis } from "ioredis";
import { setTimeout as sleep } from "node:timers/promises";

import { createRedisConnections } from "./connection.js";
import type { ConnectionConfig } from "./connection.js";
import { parseDuration } from "./durations.js";
import type { Duration, DurationInMilliseconds } from "./durations.js";
import { deriveFiringRunId } from "./ids.js";
import type { RunId } from "./ids.js";
import { silentLogger } from "./logger.js";
import { forEachBatchOfJobIds, readJobFields } from "./queue-reader.js";
import { wakeSavedSleepsNow } from "./redis-step-store.js";
import { DEFAULT_PREFIX } from "./registry.js";
import { FIRING_JOB_NAME, createRunQueue } from "./run-queue.js";
import type { RunQueue, StoredRun } from "./run-queue.js";
import { BULLMQ_JOB_STATE_BY_SET, runStateOf } from "./run-states.js";
import type { BullmqJobSet, RunState } from "./run-states.js";
import { timezoneOf } from "./schedules.js";

export type NextaskingControlOptions = {
  connection: ConnectionConfig;
  prefix?: string;
};

export type NextaskingControl = {
  readonly waitForIdle: (options?: { timeout?: Duration }) => Promise<void>;
  readonly makeRunsDueNow: (options?: { task?: string }) => Promise<{ task: string; runId: RunId }[]>;
  readonly fireSchedule: (taskName: string) => Promise<{ runId: RunId }>;
  readonly reset: (options?: { timeout?: Duration }) => Promise<void>;
  readonly close: () => Promise<void>;
  readonly [Symbol.asyncDispose]: () => Promise<void>;
};

type StoredJob =
  | { kind: "run"; jobId: string; jobKey: string; task: string; runId: RunId; state: RunState }
  | { kind: "fan-out" | "firing"; jobId: string; jobKey: string; bullmqSet: BullmqJobSet };

type BusyCountsByKind = {
  pending: number;
  executing: number;
  waitingForConcurrencySlot: number;
  fanOutsLeft: number;
  firingsStarting: number;
};

const JOB_FIELD_NAMES = ["name", "data"];

const IDLE_BULLMQ_SETS: readonly BullmqJobSet[] = ["active", "wait", "prioritized", "waiting-children", "delayed"];

const BUSY_RUN_STATES: ReadonlySet<RunState> = new Set(["pending", "executing", "waitingForConcurrencySlot"]);

const RUN_STATES_MADE_DUE: ReadonlySet<RunState> = new Set(["delayed", "suspended", "backingOff"]);

const DEFAULT_TIMEOUT: Duration = "30s";

const POLL_INTERVAL_IN_MILLISECONDS: DurationInMilliseconds = 50;

const IDLE_SNAPSHOTS_IN_A_ROW = 2;

export function createNextaskingControl(options: NextaskingControlOptions): NextaskingControl {
  const connections = createRedisConnections(options.connection, silentLogger);
  const prefix = options.prefix ?? DEFAULT_PREFIX;
  let runQueue: RunQueue | undefined;

  const callOnQueue = <Result>(call: (openRunQueue: RunQueue) => Promise<Result>): Promise<Result> =>
    connections.callOnRedis({}, async () => {
      runQueue ??= createRunQueue(connections, "instance", prefix, silentLogger);
      await connections.waitUntilReadyWithinGiveUpAfter(runQueue.client);
      return call(runQueue);
    });

  const storedJobsIn = async (openRunQueue: RunQueue, bullmqSets: readonly BullmqJobSet[]): Promise<StoredJob[]> => {
    const storedJobs: StoredJob[] = [];
    for (const bullmqSet of bullmqSets) {
      await forEachBatchOfJobIds(openRunQueue, bullmqSet, 0, async (jobIds) => {
        const fieldsByJob = await readJobFields(openRunQueue, jobIds, JOB_FIELD_NAMES);
        for (const [index, { jobKey, fieldValues }] of fieldsByJob.entries()) {
          const storedJob = storedJobOf(jobIds[index] ?? "", jobKey, bullmqSet, fieldValues);
          if (storedJob !== undefined) {
            storedJobs.push(storedJob);
          }
        }
        return "next batch";
      });
    }
    return storedJobs;
  };

  const busyCountsByKind = (): Promise<BusyCountsByKind> =>
    callOnQueue(async (openRunQueue) => busyCountsOf(await storedJobsIn(openRunQueue, IDLE_BULLMQ_SETS)));

  const close = async () => {
    await runQueue?.close();
    runQueue = undefined;
  };

  return {
    waitForIdle: async ({ timeout = DEFAULT_TIMEOUT } = {}) => {
      const timeoutInMilliseconds = timeoutInMillisecondsOf("waitForIdle", timeout);
      const deadline = Date.now() + timeoutInMilliseconds;
      let idleSnapshotsInARow = 0;
      for (;;) {
        const busyCounts = await busyCountsByKind();
        idleSnapshotsInARow = isIdle(busyCounts) ? idleSnapshotsInARow + 1 : 0;
        if (idleSnapshotsInARow === IDLE_SNAPSHOTS_IN_A_ROW) {
          return;
        }
        if (Date.now() >= deadline) {
          throw new Error(`The queue is not idle after ${timeout}: ${shownBusyCountsOf(busyCounts)}`);
        }
        await sleep(POLL_INTERVAL_IN_MILLISECONDS);
      }
    },

    makeRunsDueNow: async ({ task } = {}) =>
      callOnQueue(async (openRunQueue) => {
        const runsMadeDue: { task: string; runId: RunId }[] = [];
        for (const storedJob of await storedJobsIn(openRunQueue, ["delayed"])) {
          if (
            storedJob.kind !== "run" ||
            !RUN_STATES_MADE_DUE.has(storedJob.state) ||
            (task !== undefined && storedJob.task !== task)
          ) {
            continue;
          }
          if (storedJob.state === "suspended") {
            await wakeSavedSleepsNow(openRunQueue.client, storedJob.jobKey);
          }
          const job = await Job.fromId(openRunQueue.bullmqQueue, storedJob.jobId);
          const isPromoted = await job?.promote().then(
            () => true,
            () => false,
          );
          if (isPromoted === true) {
            runsMadeDue.push({ task: storedJob.task, runId: storedJob.runId });
          }
        }
        return runsMadeDue;
      }),

    fireSchedule: async (taskName) =>
      callOnQueue(async (openRunQueue) => {
        const storedScheduler = await openRunQueue.bullmqQueue.getJobScheduler(taskName);
        if (storedScheduler === undefined || storedScheduler === null) {
          throw new Error(
            `No schedule is stored for task "${taskName}": fire a scheduled task whose worker has started once`,
          );
        }
        const plannedInstant = Date.now();
        await openRunQueue.enqueueFiring(taskName, plannedInstant);
        return { runId: deriveFiringRunId(taskName, plannedInstant) };
      }),

    reset: async ({ timeout = DEFAULT_TIMEOUT } = {}) => {
      if (process.env.NODE_ENV === "production") {
        throw new Error(
          "reset() removes every key under the prefix, so it refuses to run in a process whose NODE_ENV is production",
        );
      }
      const deadline = Date.now() + timeoutInMillisecondsOf("reset", timeout);
      await callOnQueue(async (openRunQueue) => {
        for (;;) {
          const executingCount = await openRunQueue.bullmqQueue.getActiveCount();
          if (executingCount === 0) {
            break;
          }
          if (Date.now() >= deadline) {
            throw new Error(`reset() found attempts still executing after ${timeout}: ${executingCount} executing`);
          }
          await sleep(POLL_INTERVAL_IN_MILLISECONDS);
        }
        const storedSchedulers = await openRunQueue.bullmqQueue.getJobSchedulers(0, -1);
        await unlinkKeysUnder(openRunQueue.client, prefix);
        await openRunQueue.syncSchedules(
          storedSchedulers.flatMap((storedScheduler) =>
            storedScheduler.pattern === undefined || storedScheduler.pattern === null
              ? []
              : [
                  {
                    taskName: storedScheduler.key,
                    cron: storedScheduler.pattern,
                    timezone: timezoneOf({ cron: storedScheduler.pattern, timezone: storedScheduler.tz ?? undefined }),
                  },
                ],
          ),
        );
      });
    },

    close,
    [Symbol.asyncDispose]: close,
  };
}

const KEYS_SCANNED_PER_CALL = 1000;

const GLOB_SPECIAL_CHARACTERS = /[*?[\]\\]/g;

async function unlinkKeysUnder(client: Redis, prefix: string): Promise<void> {
  const keyPattern = `${prefix.replace(GLOB_SPECIAL_CHARACTERS, (character) => `\\${character}`)}:*`;
  let cursor = "0";
  do {
    const [nextCursor, keys] = await client.scan(cursor, "MATCH", keyPattern, "COUNT", KEYS_SCANNED_PER_CALL);
    if (keys.length > 0) {
      await client.unlink(...keys);
    }
    cursor = nextCursor;
  } while (cursor !== "0");
}

function storedJobOf(
  jobId: string,
  jobKey: string,
  bullmqSet: BullmqJobSet,
  [name, data]: (string | null)[],
): StoredJob | undefined {
  if (name === null || name === undefined || data === null || data === undefined) {
    return undefined;
  }
  if (name === FIRING_JOB_NAME) {
    return { kind: "firing", jobId, jobKey, bullmqSet };
  }
  const storedData = JSON.parse(data) as Partial<StoredRun> & { listenerNames?: unknown };
  if (storedData.listenerNames !== undefined) {
    return { kind: "fan-out", jobId, jobKey, bullmqSet };
  }
  const state = runStateOf(BULLMQ_JOB_STATE_BY_SET[bullmqSet], storedData.delayedRunState);
  if (state === undefined || storedData.runId === undefined) {
    return undefined;
  }
  return { kind: "run", jobId, jobKey, task: name, runId: storedData.runId, state };
}

function busyCountsOf(storedJobs: readonly StoredJob[]): BusyCountsByKind {
  const busyCounts: BusyCountsByKind = {
    pending: 0,
    executing: 0,
    waitingForConcurrencySlot: 0,
    fanOutsLeft: 0,
    firingsStarting: 0,
  };
  for (const storedJob of storedJobs) {
    if (storedJob.kind === "run") {
      if (BUSY_RUN_STATES.has(storedJob.state)) {
        busyCounts[storedJob.state as "pending" | "executing" | "waitingForConcurrencySlot"] += 1;
      }
    } else if (storedJob.kind === "fan-out") {
      busyCounts.fanOutsLeft += 1;
    } else if (storedJob.bullmqSet !== "delayed") {
      busyCounts.firingsStarting += 1;
    }
  }
  return busyCounts;
}

function isIdle(busyCounts: BusyCountsByKind): boolean {
  return Object.values(busyCounts).every((count) => count === 0);
}

function shownBusyCountsOf(busyCounts: BusyCountsByKind): string {
  return [
    `${busyCounts.pending} runs pending`,
    `${busyCounts.executing} executing`,
    `${busyCounts.waitingForConcurrencySlot} waiting for a concurrency slot`,
    `${busyCounts.fanOutsLeft} fan-outs left`,
    `${busyCounts.firingsStarting} firings starting a run`,
  ].join(", ");
}

function timeoutInMillisecondsOf(callName: string, timeout: Duration): DurationInMilliseconds {
  try {
    return parseDuration(timeout);
  } catch (error) {
    throw new Error(`Invalid ${callName} timeout: ${(error as Error).message}`, { cause: error });
  }
}
