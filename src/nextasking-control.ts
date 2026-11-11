import { setTimeout as sleep } from "node:timers/promises";

import { createRedisConnections } from "./connection.js";
import type { ConnectionConfig, RedisConnection } from "./connection.js";
import { retriesOf } from "./definitions.js";
import { parseDuration } from "./durations.js";
import type { Duration, DurationInMilliseconds } from "./durations.js";
import { engineKeysOf, hashTaggedPrefixOf } from "./engine/engine-keys.js";
import type { EngineKeys } from "./engine/engine-keys.js";
import { createEngine } from "./engine/engine.js";
import { workerTiming } from "./worker-timing.js";
import type { BusyRunCounts, Engine, MadeDueRun } from "./engine/engine.js";
import { deriveFiringRunId } from "./ids.js";
import type { RunId } from "./ids.js";
import { silentLogger } from "./logger.js";
import { builtInCommandOf } from "./redis-client.js";
import type { RedisClient } from "./redis-client.js";
import { prefixOf } from "./registry.js";
import type { ControlledScheduledTaskNameIn, ControlledTaskNameIn, Register } from "./registry.js";

export type NextaskingControlOptions = {
  connection: ConnectionConfig;
  prefix?: string;
};

type CheckedTaskNameOf<Name extends string, ListedName extends string> = Name extends ListedName
  ? Name
  : string extends Name
    ? Name
    : ListedName;

type ControlledTaskName = ControlledTaskNameIn<Register>;

type ControlledScheduledTaskName = ControlledScheduledTaskNameIn<Register>;

export type NextaskingControl = {
  readonly waitForIdle: (options?: { timeout?: Duration }) => Promise<void>;
  readonly makeRunsDueNow: <const Name extends string = ControlledTaskName>(options?: {
    task?: CheckedTaskNameOf<Name, ControlledTaskName>;
  }) => Promise<{ task: string; runId: RunId }[]>;
  readonly fireSchedule: <const Name extends string>(
    taskName: CheckedTaskNameOf<Name, ControlledScheduledTaskName>,
  ) => Promise<{ runId: RunId }>;
  readonly reset: (options?: { timeout?: Duration }) => Promise<void>;
  readonly close: () => Promise<void>;
  readonly [Symbol.asyncDispose]: () => Promise<void>;
};

type OpenEngine = { connection: RedisConnection; engine: Engine };

const CLOSE_BOUND_IN_MILLISECONDS: DurationInMilliseconds = 1000;

function timeoutInMillisecondsOf(callName: string, timeout: Duration): DurationInMilliseconds {
  try {
    return parseDuration(timeout);
  } catch (error) {
    throw new Error(`Invalid ${callName} timeout: ${(error as Error).message}`, { cause: error });
  }
}

const DEFAULT_TIMEOUT: Duration = "30s";

const POLL_INTERVAL_IN_MILLISECONDS: DurationInMilliseconds = 50;

const KEYS_SCANNED_PER_CALL = 1000;

const GLOB_SPECIAL_CHARACTERS = /[*?[\]\\]/g;

export function createNextaskingControl(options: NextaskingControlOptions): NextaskingControl {
  const prefix = prefixOf(options.prefix);
  const connections = createRedisConnections(options.connection, silentLogger);
  const keys = engineKeysOf(prefix);
  let openEngine: OpenEngine | undefined;

  const callOnEngine = <Result>(call: (opened: OpenEngine) => Promise<Result>): Promise<Result> =>
    connections.callOnRedis({}, async () => {
      if (openEngine === undefined) {
        const connection = connections.createCommandConnection();
        openEngine = {
          connection,
          engine: createEngine(connection.instanceCalls, prefix, workerTiming().supportedRecordVersion),
        };
      }
      return call(openEngine);
    });

  const busyRunCountsOnceTrue = async (
    isAwaitedCounts: (counts: BusyRunCounts) => boolean,
    timeoutInMilliseconds: DurationInMilliseconds,
    timeoutMessageOf: (counts: BusyRunCounts) => string,
  ): Promise<void> => {
    const deadline = Date.now() + timeoutInMilliseconds;
    for (;;) {
      const counts = await callOnEngine(({ engine }) => engine.busyRunCounts());
      if (isAwaitedCounts(counts)) {
        return;
      }
      if (Date.now() >= deadline) {
        throw new Error(timeoutMessageOf(counts));
      }
      await sleep(POLL_INTERVAL_IN_MILLISECONDS);
    }
  };

  const close = async () => {
    const closedConnection = openEngine?.connection;
    openEngine = undefined;
    if (closedConnection !== undefined) {
      await connections.closeConnection(closedConnection, CLOSE_BOUND_IN_MILLISECONDS);
    }
  };

  return {
    waitForIdle: async ({ timeout = DEFAULT_TIMEOUT } = {}) => {
      await busyRunCountsOnceTrue(
        isIdle,
        timeoutInMillisecondsOf("waitForIdle", timeout),
        (counts) => `The queue is not idle after ${timeout}, runs left: ${shownBusyRunCountsOf(counts)}`,
      );
    },

    makeRunsDueNow: async ({ task } = {}) =>
      callOnEngine(async ({ engine }) => {
        const madeDueRuns: MadeDueRun[] = [];
        for (;;) {
          const reply = await engine.makeDue(task === undefined ? {} : { task });
          if (reply.madeDueRuns.length === 0) {
            return madeDueRuns;
          }
          madeDueRuns.push(...reply.madeDueRuns);
        }
      }),

    fireSchedule: async (taskName) =>
      callOnEngine(async ({ engine }) => {
        const instant = await engine.serverInstant();
        const runId = deriveFiringRunId(taskName, instant);
        const reply = await engine.fireNow({ task: taskName, instant, runId, defaultRetries: retriesOf(undefined) });
        if (reply.kind === "noSchedule") {
          throw new Error(
            `No schedule is stored for task "${taskName}": fire a scheduled task whose worker has started once`,
          );
        }
        if (reply.kind === "unknownVersion") {
          throw new Error(
            `The firing of task "${taskName}" was refused: a newer release wrote a schedule record of version ${String(reply.recordVersion)} this release cannot read, so no run was started.`,
          );
        }
        return { runId };
      }),

    reset: async ({ timeout = DEFAULT_TIMEOUT } = {}) => {
      rejectResetInProduction();
      await busyRunCountsOnceTrue(
        ({ executingRunCount }) => executingRunCount === 0,
        timeoutInMillisecondsOf("reset", timeout),
        ({ executingRunCount }) =>
          `reset() found attempts still executing after ${timeout}: ${String(executingRunCount)} executing`,
      );
      await callOnEngine(({ connection }) => unlinkKeysOtherThanTheSchedules(connection.instanceCalls, keys, prefix));
    },

    close,
    [Symbol.asyncDispose]: close,
  };
}

type ScanPage = [nextCursor: string, scannedKeys: string[]];

function scanPageOf(reply: unknown): ScanPage {
  if (
    Array.isArray(reply) &&
    reply.length === 2 &&
    typeof reply[0] === "string" &&
    Array.isArray(reply[1]) &&
    reply[1].every((scannedKey) => typeof scannedKey === "string")
  ) {
    return reply as ScanPage;
  }
  throw new Error(`The SCAN of the keys under the prefix sent a reply reset() cannot read: ${JSON.stringify(reply)}`);
}

async function unlinkKeysOtherThanTheSchedules(client: RedisClient, keys: EngineKeys, prefix: string): Promise<void> {
  const escapedPrefix = hashTaggedPrefixOf(prefix).replace(GLOB_SPECIAL_CHARACTERS, (character) => `\\${character}`);
  let cursor = "0";
  do {
    const [nextCursor, scannedKeys] = scanPageOf(
      await client.send(
        builtInCommandOf("SCAN", cursor, "MATCH", `${escapedPrefix}:*`, "COUNT", KEYS_SCANNED_PER_CALL),
      ),
    );
    const unlinkedKeys = scannedKeys.filter((scannedKey) => !keys.isScheduleKey(scannedKey));
    if (unlinkedKeys.length > 0) {
      await client.send(builtInCommandOf("UNLINK", ...unlinkedKeys));
    }
    cursor = nextCursor;
  } while (cursor !== "0");
}

function rejectResetInProduction(): void {
  if (process.env.NODE_ENV === "production") {
    throw new Error(
      "reset() removes every key under the prefix, so it refuses to run in a process whose NODE_ENV is production",
    );
  }
}

function isIdle(counts: BusyRunCounts): boolean {
  return Object.values(counts).every((count) => count === 0);
}

function shownBusyRunCountsOf(counts: BusyRunCounts): string {
  return [
    `${String(counts.pendingRunCount)} pending`,
    `${String(counts.executingRunCount)} executing`,
    `${String(counts.waitingForConcurrencySlotRunCount)} waiting for a concurrency slot`,
    `${String(counts.dueRunCount)} due and not yet promoted`,
  ].join(", ");
}
