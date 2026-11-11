import type { AnyTaskDefinition, EventDefinition, Payload, PayloadSchema } from "./definitions.js";
import type { EpochMilliseconds } from "./durations.js";
import { compareInSortedSetMemberOrder, engineKeysOf, runIdentityOf, storedRunIdOf } from "./engine/engine-keys.js";
import type { RunIdentity } from "./engine/engine-keys.js";
import { deserializePayload, isFailureReason, isRecordVersionReadable, storedIntegerOf } from "./engine/engine.js";
import type { ErrorRecord, ScheduleSummary } from "./engine/engine.js";
import { indexPagesCommandOf, indexPagesOf } from "./engine/index-page-script.js";
import type { IndexPlace } from "./engine/index-page-script.js";
import { listedRunReadCommandOf, readPartsOf, runReadCommandOf } from "./engine/run-read-scripts.js";
import { UnknownEventError, UnknownTaskError } from "./errors.js";
import { deriveFiringRunId, deriveListenerRunId } from "./ids.js";
import type { EventId, RunId } from "./ids.js";
import { decodeRunListCursor, encodeRunListCursor } from "./run-list-cursor.js";
import type { ListedIndexes, RunListCursor } from "./run-list-cursor.js";
import { RUN_STATES, isRunState } from "./run-states.js";
import type { RunState, RunStateFields } from "./run-states.js";
import type { CronExpression, IanaTimeZone } from "./schedules.js";
import { builtInCommandOf } from "./redis-client.js";
import type { CommandResult, RedisClient, RedisCommand } from "./redis-client.js";
import { savedStepResultsOf } from "./step-engine.js";
import type { SavedStepResult } from "./step-engine.js";

export type RunError = {
  attempt: number;
  name: string;
  message: string;
  stack: string;
  failedAt: Date;
};

type PayloadOfTask<Task> = Task extends { schema: infer Schema extends PayloadSchema } ? Payload<Schema> : undefined;

type RunSummaryFields = {
  runId: RunId;
  attempt: number;
  dispatchedAt: Date;
  finishedAt?: Date;
};

export type RunOfTask<Task extends AnyTaskDefinition> = Task extends AnyTaskDefinition
  ? { task: Task["name"] } & RunSummaryFields &
      RunStateFields & {
        payload: PayloadOfTask<Task>;
        errors: RunError[];
        steps: SavedStepResult[];
      }
  : never;

export type ListedRunOfTask<Task extends AnyTaskDefinition> = Task extends AnyTaskDefinition
  ? { task: Task["name"] } & RunSummaryFields & RunStateFields & { lastErrorMessage?: string }
  : never;

export type ListRunsOptionsOf<TaskName extends string, State extends RunState = RunState> = {
  state: State;
  task?: TaskName;
  limit?: number;
  cursor?: RunListCursor;
};

export type RunPageOf<ListedRunOfAnyTask> = {
  runs: ListedRunOfAnyTask[];
  totalRunCount: number;
  firstRunPosition?: number;
  nextCursor?: RunListCursor;
};

export type RunCountsOf<TaskName extends string> = { [Name in TaskName]: Record<RunState, number> };

type ListenerOf<Task, EventName extends string> = Extract<Task, { on: { name: EventName } }>;

export type EventWithRunsOf<
  Task extends AnyTaskDefinition,
  Event extends EventDefinition,
> = Event extends EventDefinition
  ? {
      event: Event["name"];
      eventId: EventId;
      runsByListener: {
        [ListenerName in ListenerOf<Task, Event["name"]>["name"]]:
          RunOfTask<Extract<Task, { name: ListenerName }>> | undefined;
      };
    }
  : never;

export type PreviousFiring = {
  scheduledAt: Date;
  runId: RunId;
};

export type ListedScheduleOf<TaskName extends string> = {
  task: TaskName;
  cron: CronExpression;
  timezone: IanaTimeZone;
  nextFiringAt?: Date;
  previousFiring?: PreviousFiring;
};

export type AnyRun = RunOfTask<AnyTaskDefinition>;

export type AnyListedRun = ListedRunOfTask<AnyTaskDefinition>;

export const DEFAULT_LIST_LIMIT = 50;

export function rejectInvalidState(state: unknown): asserts state is RunState {
  if (!RUN_STATES.includes(state as RunState)) {
    const shownState = typeof state === "string" ? JSON.stringify(state) : String(state);
    const shownStates = RUN_STATES.map((runState) => `"${runState}"`).join(", ");
    throw new Error(`Invalid listRuns state: ${shownState} is not one of ${shownStates}`);
  }
}

export function listLimitOf(limit: unknown): number {
  if (limit === undefined) {
    return DEFAULT_LIST_LIMIT;
  }
  if (typeof limit !== "number" || !Number.isInteger(limit) || limit <= 0) {
    const shownLimit = typeof limit === "string" ? JSON.stringify(limit) : String(limit);
    throw new Error(`Invalid listRuns limit: ${shownLimit} is not a positive integer`);
  }
  return limit;
}

export function rejectUnknownTask(tasksByName: ReadonlyMap<string, AnyTaskDefinition>, taskName: string): void {
  if (!tasksByName.has(taskName)) {
    throw new UnknownTaskError(taskName, [...tasksByName.keys()]);
  }
}

export function zeroRunCountsOf(taskNames: Iterable<string>): RunCountsOf<string> {
  return Object.fromEntries(
    [...taskNames].map((taskName) => [
      taskName,
      Object.fromEntries(RUN_STATES.map((runState) => [runState, 0])) as Record<RunState, number>,
    ]),
  );
}

export type ReadApiConfig = {
  client: RedisClient;
  prefix: string;
  tasksByName: ReadonlyMap<string, AnyTaskDefinition>;
  listenerNamesByEventName: ReadonlyMap<string, readonly string[]>;
  declaredSchedules: readonly ScheduleSummary[];
  supportedRecordVersion: number;
};

export type ReadApi = {
  getRun: (taskName: string, runId: RunId) => Promise<AnyRun | undefined>;
  listRuns: (options: ListRunsOptionsOf<string>) => Promise<RunPageOf<AnyListedRun>>;
  countRuns: () => Promise<RunCountsOf<string>>;
  getEvent: (eventName: string, eventId: EventId) => Promise<EventWithRunsOf<AnyTaskDefinition, EventDefinition>>;
  listSchedules: () => Promise<ListedScheduleOf<string>[]>;
};

const RUN_RECORD_FIELDS = [
  "version",
  "task",
  "state",
  "attempt",
  "dispatchedAt",
  "finishedAt",
  "startsAt",
  "resumesAt",
  "nextAttemptAt",
  "reason",
] as const;

const SCHEDULE_RECORD_FIELDS = ["version", "nextFiringAt", "previousFiringAt"] as const;

type StoredValue = string | null;

type AnyRunSummary = Omit<AnyListedRun, "lastErrorMessage">;

type IndexEntry = { indexPlace: IndexPlace; runIdentity: RunIdentity };

function storedDateOf(storedValue: unknown): Date | undefined {
  const instant: EpochMilliseconds | undefined = storedIntegerOf(storedValue);
  return instant === undefined ? undefined : new Date(instant);
}

function storedValuesOf(reply: unknown, fieldCount: number): StoredValue[] | undefined {
  return Array.isArray(reply) &&
    reply.length === fieldCount &&
    reply.every((value) => value === null || typeof value === "string")
    ? (reply as StoredValue[])
    : undefined;
}

function storedFieldsOf<Field extends string>(
  reply: unknown,
  fields: readonly Field[],
): Record<Field, StoredValue> | undefined {
  const storedValues = storedValuesOf(reply, fields.length);
  return storedValues === undefined
    ? undefined
    : (Object.fromEntries(fields.map((field, position) => [field, storedValues[position]])) as Record<
        Field,
        StoredValue
      >);
}

type StoredRunRecord = Record<(typeof RUN_RECORD_FIELDS)[number], StoredValue>;

function dueStateFieldsOf(state: RunState, storedRecord: StoredRunRecord): RunStateFields | undefined {
  const startsAt = storedDateOf(storedRecord.startsAt);
  const resumesAt = storedDateOf(storedRecord.resumesAt);
  const nextAttemptAt = storedDateOf(storedRecord.nextAttemptAt);
  if (state === "delayed") {
    return startsAt === undefined ? undefined : { state, startsAt };
  }
  if (state === "suspended") {
    return resumesAt === undefined ? undefined : { state, resumesAt };
  }
  return nextAttemptAt === undefined ? undefined : { state: "backingOff", nextAttemptAt };
}

function runStateFieldsOf(state: RunState, storedRecord: StoredRunRecord): RunStateFields | undefined {
  switch (state) {
    case "delayed":
    case "suspended":
    case "backingOff":
      return dueStateFieldsOf(state, storedRecord);
    case "failed":
      return isFailureReason(storedRecord.reason) ? { state, reason: storedRecord.reason } : undefined;
    default:
      return { state };
  }
}

function runSummaryOf(runId: RunId, recordReply: unknown, supportedRecordVersion: number): AnyRunSummary | undefined {
  const storedRecord = storedFieldsOf(recordReply, RUN_RECORD_FIELDS);
  if (
    storedRecord === undefined ||
    !isRecordVersionReadable(storedRecord.version, supportedRecordVersion) ||
    storedRecord.task === null
  ) {
    return undefined;
  }
  const { state } = storedRecord;
  const attempt = storedIntegerOf(storedRecord.attempt);
  const dispatchedAt = storedDateOf(storedRecord.dispatchedAt);
  const stateFields = isRunState(state) ? runStateFieldsOf(state, storedRecord) : undefined;
  if (attempt === undefined || dispatchedAt === undefined || stateFields === undefined) {
    return undefined;
  }
  const finishedAt = storedDateOf(storedRecord.finishedAt);
  return {
    task: storedRecord.task,
    runId,
    ...stateFields,
    attempt,
    dispatchedAt,
    ...(finishedAt === undefined ? {} : { finishedAt }),
  } as AnyRunSummary;
}

function errorRecordOf(storedError: unknown): ErrorRecord | undefined {
  if (typeof storedError !== "string") {
    return undefined;
  }
  try {
    const errorRecord = JSON.parse(storedError) as Partial<ErrorRecord> | null;
    return typeof errorRecord?.message === "string" ? (errorRecord as ErrorRecord) : undefined;
  } catch {
    return undefined;
  }
}

function runErrorsOf(errorsReply: unknown): RunError[] | undefined {
  if (!Array.isArray(errorsReply)) {
    return undefined;
  }
  const runErrors: RunError[] = [];
  for (const storedError of errorsReply) {
    const errorRecord = errorRecordOf(storedError);
    if (errorRecord === undefined) {
      return undefined;
    }
    runErrors.push({ ...errorRecord, failedAt: new Date(errorRecord.failedAt) });
  }
  return runErrors;
}

function readOrUndefined<Value>(read: () => Value): Value | undefined {
  try {
    return read();
  } catch {
    return undefined;
  }
}

function storedValuesByFieldOf(hashReply: unknown): Record<string, string> {
  if (
    !Array.isArray(hashReply) ||
    hashReply.length % 2 !== 0 ||
    !hashReply.every((element) => typeof element === "string")
  ) {
    throw new Error(`The HGETALL of a hash sent a reply the reader cannot read: ${JSON.stringify(hashReply)}`);
  }
  return Object.fromEntries(
    Array.from({ length: hashReply.length / 2 }, (_, position) => [
      hashReply[position * 2],
      hashReply[position * 2 + 1],
    ]),
  ) as Record<string, string>;
}

function runOf(
  taskName: string,
  runId: RunId,
  [recordReply, payloadReply, errorsReply, stepsReply]: readonly unknown[],
  supportedRecordVersion: number,
): AnyRun | undefined {
  const runSummary = runSummaryOf(runId, recordReply, supportedRecordVersion);
  if (runSummary?.task !== taskName || typeof payloadReply !== "string") {
    return undefined;
  }
  const errors = runErrorsOf(errorsReply);
  const payload = readOrUndefined(() => ({ value: deserializePayload(payloadReply) }));
  const steps = readOrUndefined(() => savedStepResultsOf(storedValuesByFieldOf(stepsReply)));
  if (errors === undefined || payload === undefined || steps === undefined) {
    return undefined;
  }
  return { ...runSummary, payload: payload.value, errors, steps } as AnyRun;
}

function listedRunOf(
  entry: IndexEntry,
  state: RunState,
  [recordReply, lastErrorReply]: readonly unknown[],
  supportedRecordVersion: number,
) {
  const runSummary = runSummaryOf(entry.runIdentity.runId, recordReply, supportedRecordVersion);
  if (runSummary?.task !== entry.runIdentity.task || runSummary.state !== state) {
    return undefined;
  }
  if (lastErrorReply === null) {
    return runSummary as AnyListedRun;
  }
  const lastError = errorRecordOf(lastErrorReply);
  return lastError === undefined ? undefined : ({ ...runSummary, lastErrorMessage: lastError.message } as AnyListedRun);
}

function compareInIndexOrder(left: IndexEntry, right: IndexEntry): number {
  return (
    right.indexPlace.score - left.indexPlace.score ||
    compareInSortedSetMemberOrder(right.indexPlace.storedRunId, left.indexPlace.storedRunId)
  );
}

function indexPlaceAfter(cursor: unknown, listedIndexes: ListedIndexes): IndexPlace | undefined {
  const { place } = decodeRunListCursor(cursor, listedIndexes);
  return place === undefined ? undefined : { score: place.score, storedRunId: storedRunIdOf(place.task, place.runId) };
}

function indexEntryOf(indexKey: string, indexPlace: IndexPlace): IndexEntry {
  const runIdentity = runIdentityOf(indexPlace.storedRunId);
  if (runIdentity === undefined) {
    throw new Error(
      `The run index ${indexKey} holds a member that is not a stored run id: ${JSON.stringify(indexPlace.storedRunId)}`,
    );
  }
  return { indexPlace, runIdentity };
}

function sumOf(counts: readonly number[]): number {
  return counts.reduce((sum, count) => sum + count, 0);
}

function repliesOf(commandResults: readonly CommandResult[]): unknown[] {
  return commandResults.map((commandResult) => {
    if (commandResult.kind === "failed") {
      throw commandResult.error;
    }
    return commandResult.reply;
  });
}

export function createReadApi(config: ReadApiConfig): ReadApi {
  const { client, tasksByName, listenerNamesByEventName, declaredSchedules, supportedRecordVersion } = config;
  const keys = engineKeysOf(config.prefix);
  const repliesToBatchOf = async (commands: readonly RedisCommand[]): Promise<unknown[]> =>
    repliesOf(await client.sendBatch(commands));

  const readRuns = async (runReads: readonly RunIdentity[]): Promise<(AnyRun | undefined)[]> => {
    const replies = await repliesToBatchOf(
      runReads.map((runIdentity) => runReadCommandOf(keys, runIdentity, RUN_RECORD_FIELDS)),
    );
    return runReads.map(({ task, runId }, position) =>
      runOf(task, runId, readPartsOf(replies[position]), supportedRecordVersion),
    );
  };

  const readIndexPages = async (
    state: RunState,
    taskNames: readonly string[],
    limit: number,
    after: IndexPlace | undefined,
  ): Promise<{ indexEntries: IndexEntry[]; totalRunCount: number; entryCountBeforeBoundary: number }> => {
    if (taskNames.length === 0) {
      return { indexEntries: [], totalRunCount: 0, entryCountBeforeBoundary: 0 };
    }
    const indexKeys = taskNames.map((taskName) => keys.runIndex(taskName, state));
    const indexPages = indexPagesOf(
      await client.send(indexPagesCommandOf({ indexKeys, limit, after })),
      indexKeys.length,
    );
    return {
      indexEntries: indexPages.flatMap(({ places }, indexNumber) =>
        places.map((indexPlace) => indexEntryOf(indexKeys[indexNumber] ?? "", indexPlace)),
      ),
      totalRunCount: sumOf(indexPages.map(({ entryCount }) => entryCount)),
      entryCountBeforeBoundary: sumOf(indexPages.map((indexPage) => indexPage.entryCountBeforeBoundary)),
    };
  };

  const readListedRuns = async (state: RunState, indexEntries: readonly IndexEntry[]): Promise<AnyListedRun[]> => {
    if (indexEntries.length === 0) {
      return [];
    }
    const replies = await repliesToBatchOf(
      indexEntries.map(({ runIdentity }) => listedRunReadCommandOf(keys, runIdentity, RUN_RECORD_FIELDS)),
    );
    return indexEntries.flatMap((entry, position) => {
      const listedRun = listedRunOf(entry, state, readPartsOf(replies[position]), supportedRecordVersion);
      return listedRun === undefined ? [] : [listedRun];
    });
  };

  return {
    getRun: async (taskName, runId) => {
      rejectUnknownTask(tasksByName, taskName);
      if (runId === "") {
        return undefined;
      }
      const [run] = await readRuns([{ task: taskName, runId }]);
      return run;
    },

    listRuns: async (options) => {
      const { state, task } = options;
      rejectInvalidState(state);
      if (task !== undefined) {
        rejectUnknownTask(tasksByName, task);
      }
      const limit = listLimitOf(options.limit);
      const listedIndexes = { state, listedTask: task ?? null };
      const after = options.cursor === undefined ? undefined : indexPlaceAfter(options.cursor, listedIndexes);
      const taskNames = task === undefined ? [...tasksByName.keys()] : [task];
      const { indexEntries, totalRunCount, entryCountBeforeBoundary } = await readIndexPages(
        state,
        taskNames,
        limit,
        after,
      );
      const takenEntries = indexEntries.sort(compareInIndexOrder).slice(0, limit);
      const lastTakenEntry = takenEntries.at(-1);
      const runs = await readListedRuns(state, takenEntries);
      if (lastTakenEntry === undefined) {
        return { runs, totalRunCount };
      }
      const firstRunPosition = entryCountBeforeBoundary + 1;
      const isAnyEntryAfterPage = entryCountBeforeBoundary + takenEntries.length < totalRunCount;
      if (!isAnyEntryAfterPage) {
        return { runs, totalRunCount, firstRunPosition };
      }
      const place = { score: lastTakenEntry.indexPlace.score, ...lastTakenEntry.runIdentity };
      return {
        runs,
        totalRunCount,
        firstRunPosition,
        nextCursor: encodeRunListCursor({ ...listedIndexes, direction: "forward", place }),
      };
    },

    countRuns: async () => {
      const taskNames = [...tasksByName.keys()];
      const replies = await repliesToBatchOf(
        taskNames.flatMap((taskName) =>
          RUN_STATES.map((state) => builtInCommandOf("ZCARD", keys.runIndex(taskName, state))),
        ),
      );
      const runCounts = zeroRunCountsOf(taskNames);
      taskNames.forEach((taskName, taskPosition) => {
        RUN_STATES.forEach((state, statePosition) => {
          runCounts[taskName]![state] = Number(replies[taskPosition * RUN_STATES.length + statePosition]);
        });
      });
      return runCounts;
    },

    getEvent: async (eventName, eventId) => {
      const listenerNames = listenerNamesByEventName.get(eventName);
      if (listenerNames === undefined) {
        throw new UnknownEventError(eventName, [...listenerNamesByEventName.keys()]);
      }
      const listenerRuns = await readRuns(
        listenerNames.map((listenerName) => ({
          task: listenerName,
          runId: deriveListenerRunId(eventId, listenerName),
        })),
      );
      return {
        event: eventName,
        eventId,
        runsByListener: Object.fromEntries(
          listenerNames.map((listenerName, position) => [listenerName, listenerRuns[position]]),
        ),
      };
    },

    listSchedules: async () => {
      const replies = await repliesToBatchOf(
        declaredSchedules.map(({ task }) => builtInCommandOf("HMGET", keys.schedule(task), ...SCHEDULE_RECORD_FIELDS)),
      );
      return declaredSchedules.map(({ task, cron, timezone }, position) => {
        const storedSchedule = storedFieldsOf(replies[position], SCHEDULE_RECORD_FIELDS);
        const isScheduleRecordKnown =
          storedSchedule !== undefined && isRecordVersionReadable(storedSchedule.version, supportedRecordVersion);
        const nextFiringAt = isScheduleRecordKnown ? storedIntegerOf(storedSchedule.nextFiringAt) : undefined;
        const previousFiringAt = isScheduleRecordKnown ? storedIntegerOf(storedSchedule.previousFiringAt) : undefined;
        return {
          task,
          cron,
          timezone,
          ...(nextFiringAt === undefined ? {} : { nextFiringAt: new Date(nextFiringAt) }),
          ...(previousFiringAt === undefined
            ? {}
            : {
                previousFiring: {
                  scheduledAt: new Date(previousFiringAt),
                  runId: deriveFiringRunId(task, previousFiringAt),
                },
              }),
        };
      });
    },
  };
}
