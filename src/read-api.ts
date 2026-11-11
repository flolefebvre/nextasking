import type { AnyTaskDefinition, EventDefinition, Payload, PayloadSchema } from "./definitions.js";
import type { EpochMilliseconds } from "./durations.js";
import { compareInSortedSetMemberOrder, engineKeysOf, runIdentityOf, storedRunIdOf } from "./engine/engine-keys.js";
import type { RunIdentity } from "./engine/engine-keys.js";
import { deserializePayload, isFailureReason, isRecordVersionReadable, storedIntegerOf } from "./engine/engine.js";
import type { ErrorRecord, ScheduleSummary } from "./engine/engine.js";
import { indexPagesCommandOf, indexPagesOf } from "./engine/index-page-script.js";
import type {
  IndexInList,
  IndexPage,
  IndexPageBoundary,
  IndexPlace,
  OrderGroupNumber,
  ListOrder,
  PagingDirection,
} from "./engine/index-page-script.js";
import { listedRunReadCommandOf, readPartsOf, runReadCommandOf } from "./engine/run-read-scripts.js";
import { UnknownEventError, UnknownTaskError } from "./errors.js";
import { deriveFiringRunId, deriveListenerRunId } from "./ids.js";
import type { EventId, RunId } from "./ids.js";
import {
  EVERY_TIME,
  decodeRunListCursor,
  encodeRunListCursor,
  quotedListOf,
  sentenceListOf,
} from "./run-list-cursor.js";
import type { RunListCursor, RunListRequest, TimeRange } from "./run-list-cursor.js";
import { ORDER_GROUPS, RUN_STATES, isRunState, orderGroupOf } from "./run-states.js";
import type { OrderGroup, RunState, RunStateFields } from "./run-states.js";
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
  state?: State | readonly State[];
  task?: TaskName | readonly TaskName[];
  from?: Date;
  until?: Date;
  order?: ListOrder;
  limit?: number;
  cursor?: RunListCursor;
};

export type RunPageOf<ListedRunOfAnyTask> = {
  runs: ListedRunOfAnyTask[];
  totalRunCount: number;
  firstRunPosition?: number;
  previousCursor?: RunListCursor;
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

function listOrderOf(order: unknown): ListOrder {
  if (order === undefined) {
    return "descending";
  }
  if (order !== "descending" && order !== "ascending") {
    const shownOrder = typeof order === "string" ? JSON.stringify(order) : String(order);
    throw new Error(`Invalid listRuns order: ${shownOrder} is not "descending" or "ascending"`);
  }
  return order;
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
  listRuns: (options?: ListRunsOptionsOf<string>) => Promise<RunPageOf<AnyListedRun>>;
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
  "startedAt",
  "startsAt",
  "resumesAt",
  "nextAttemptAt",
  "reason",
] as const;

const SCHEDULE_RECORD_FIELDS = ["version", "nextFiringAt", "previousFiringAt"] as const;

type StoredValue = string | null;

type AnyRunSummary = Omit<AnyListedRun, "lastErrorMessage">;

type IndexEntry = {
  indexPlace: IndexPlace;
  runIdentity: RunIdentity;
  indexState: RunState;
  orderGroupNumber: OrderGroupNumber;
};

type ListedRunIndex = IndexInList & { indexState: RunState };

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
    case "executing": {
      const startedAt = storedDateOf(storedRecord.startedAt);
      return startedAt === undefined ? undefined : { state, startedAt };
    }
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
  [recordReply, lastErrorReply]: readonly unknown[],
  supportedRecordVersion: number,
) {
  const runSummary = runSummaryOf(entry.runIdentity.runId, recordReply, supportedRecordVersion);
  if (runSummary?.task !== entry.runIdentity.task || runSummary.state !== entry.indexState) {
    return undefined;
  }
  if (lastErrorReply === null) {
    return runSummary as AnyListedRun;
  }
  const lastError = errorRecordOf(lastErrorReply);
  return lastError === undefined ? undefined : ({ ...runSummary, lastErrorMessage: lastError.message } as AnyListedRun);
}

function compareInGroupOrder(left: IndexEntry, right: IndexEntry): number {
  return (
    left.indexPlace.score - right.indexPlace.score ||
    compareInSortedSetMemberOrder(left.indexPlace.storedRunId, right.indexPlace.storedRunId)
  );
}

function compareInListOrderOf(order: ListOrder): (left: IndexEntry, right: IndexEntry) => number {
  const orderSign = order === "ascending" ? 1 : -1;
  return (left, right) =>
    left.orderGroupNumber - right.orderGroupNumber || orderSign * compareInGroupOrder(left, right);
}

function orderGroupNumberOf(orderGroup: OrderGroup): OrderGroupNumber {
  return ORDER_GROUPS.findIndex(({ name }) => name === orderGroup) + 1;
}

function pageBoundaryOf(cursor: unknown, runListRequest: RunListRequest): IndexPageBoundary {
  if (cursor === undefined) {
    return { direction: "forward", place: undefined };
  }
  const { direction, place } = decodeRunListCursor(cursor, runListRequest);
  return {
    direction,
    place:
      place === undefined
        ? undefined
        : {
            orderGroupNumber: orderGroupNumberOf(place.orderGroup),
            score: place.score,
            storedRunId: storedRunIdOf(place.task, place.runId),
          },
  };
}

function listedStatesOf(state: unknown): RunState[] | null {
  if (state === undefined) {
    return null;
  }
  if (!Array.isArray(state)) {
    rejectInvalidState(state);
    return [state];
  }
  if (state.length === 0) {
    throw new Error("Invalid listRuns state: list at least one state, or leave state out to list every state");
  }
  state.forEach((listedState) => rejectInvalidState(listedState));
  return RUN_STATES.filter((runState) => state.includes(runState));
}

function listedTasksOf(task: unknown, tasksByName: ReadonlyMap<string, AnyTaskDefinition>): string[] | null {
  if (task === undefined) {
    return null;
  }
  if (!Array.isArray(task)) {
    rejectUnknownTask(tasksByName, String(task));
    return [String(task)];
  }
  if (task.length === 0) {
    throw new Error("Invalid listRuns task: list at least one task, or leave task out to list every task");
  }
  task.forEach((listedTask) => rejectUnknownTask(tasksByName, String(listedTask)));
  return [...tasksByName.keys()].filter((taskName) => task.includes(taskName));
}

const TIME_FIELD_BY_TIMED_STATE: Readonly<Partial<Record<RunState, string>>> = {
  executing: "startedAt",
  delayed: "startsAt",
  suspended: "resumesAt",
  backingOff: "nextAttemptAt",
  completed: "finishedAt",
  failed: "finishedAt",
};

const RANGED_ORDER_GROUPS_SENTENCE =
  "A range takes the states of one order group ordered by a time: executing, waiting for a time, or ended";

function orderingOf(orderGroup: OrderGroup, states: readonly RunState[]): string {
  const timeFields = [...new Set(states.flatMap((state) => TIME_FIELD_BY_TIMED_STATE[state] ?? []))];
  switch (orderGroup) {
    case "queued":
      return "by their place in the queue, not by a time";
    case "waitingForATime":
      return `by when they are due, ${sentenceListOf(timeFields)}`;
    default:
      return `by ${sentenceListOf(timeFields)}`;
  }
}

function timeBoundOf(bound: unknown, boundName: "from" | "until"): EpochMilliseconds | undefined {
  if (bound === undefined) {
    return undefined;
  }
  if (!(bound instanceof Date)) {
    const shownBound = typeof bound === "string" ? JSON.stringify(bound) : String(bound);
    throw new Error(`Invalid listRuns ${boundName}: pass a Date, got ${shownBound}`);
  }
  if (Number.isNaN(bound.getTime())) {
    throw new Error(`Invalid listRuns ${boundName}: pass a valid Date, got an Invalid Date`);
  }
  return bound.getTime();
}

function rejectRangeOverStatesWithoutOneTime(listedStates: readonly RunState[] | null): void {
  const listedOrderGroups = ORDER_GROUPS.flatMap(({ name, states }) => {
    const listedStatesOfGroup = states.filter((state) => listedStates === null || listedStates.includes(state));
    return listedStatesOfGroup.length === 0 ? [] : [{ name, states: listedStatesOfGroup }];
  });
  if (listedOrderGroups.length > 1) {
    const listedSubject =
      listedStates === null
        ? "state left out lists every state, and its order groups are"
        : "the states listed are of several order groups,";
    const shownOrderings = listedOrderGroups.map(
      ({ name, states }) => `${quotedListOf(states)} ${orderingOf(name, states)}`,
    );
    throw new Error(
      `Invalid listRuns range: ${listedSubject} each ordered its own way: ${shownOrderings.join("; ")}. ${RANGED_ORDER_GROUPS_SENTENCE}`,
    );
  }
  const { name, states } = listedOrderGroups[0]!;
  if (name === "queued") {
    throw new Error(
      `Invalid listRuns range: ${quotedListOf(states)} runs are ordered ${orderingOf(name, states)}. ${RANGED_ORDER_GROUPS_SENTENCE}`,
    );
  }
}

function listedTimeRangeOf(from: unknown, until: unknown, listedStates: readonly RunState[] | null): TimeRange {
  const timeRange = { from: timeBoundOf(from, "from"), until: timeBoundOf(until, "until") };
  if (timeRange.from === undefined && timeRange.until === undefined) {
    return EVERY_TIME;
  }
  if (timeRange.from !== undefined && timeRange.until !== undefined && timeRange.from > timeRange.until) {
    throw new Error(
      `Invalid listRuns range: from ${new Date(timeRange.from).toISOString()} is after until ${new Date(timeRange.until).toISOString()}; pass a from at or before until`,
    );
  }
  rejectRangeOverStatesWithoutOneTime(listedStates);
  return timeRange;
}

type IndexPageOfEntries = Omit<IndexPage, "places"> & { indexEntries: IndexEntry[] };

type TakenEntries = { takenEntries: IndexEntry[]; entryCountBeforePage: number };

function takenEntriesOf(
  indexPages: readonly IndexPageOfEntries[],
  direction: PagingDirection,
  limit: number,
  order: ListOrder,
): TakenEntries {
  const mergedInListOrder = (indexEntries: readonly IndexEntry[]) =>
    [...indexEntries].sort(compareInListOrderOf(order));
  const entryCountBeforeBoundary = sumOf(indexPages.map((indexPage) => indexPage.entryCountBeforeBoundary));
  const everyIndexEntry = indexPages.flatMap(({ indexEntries }) => indexEntries);
  if (direction === "forward") {
    return {
      takenEntries: mergedInListOrder(everyIndexEntry).slice(0, limit),
      entryCountBeforePage: entryCountBeforeBoundary,
    };
  }
  if (entryCountBeforeBoundary < limit) {
    return { takenEntries: mergedInListOrder(everyIndexEntry).slice(0, limit), entryCountBeforePage: 0 };
  }
  const entriesBeforeBoundary = indexPages.flatMap((indexPage) =>
    indexPage.indexEntries.slice(0, indexPage.entryCountBeforeBoundary - indexPage.firstEntryRank),
  );
  return {
    takenEntries: mergedInListOrder(entriesBeforeBoundary).slice(-limit),
    entryCountBeforePage: entryCountBeforeBoundary - limit,
  };
}

function indexEntryOf({ indexKey, indexState, orderGroupNumber }: ListedRunIndex, indexPlace: IndexPlace): IndexEntry {
  const runIdentity = runIdentityOf(indexPlace.storedRunId);
  if (runIdentity === undefined) {
    throw new Error(
      `The run index ${indexKey} holds a member that is not a stored run id: ${JSON.stringify(indexPlace.storedRunId)}`,
    );
  }
  return { indexPlace, runIdentity, indexState, orderGroupNumber };
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

  const listedRunIndexesOf = (
    listedStates: readonly RunState[] | null,
    taskNames: readonly string[],
  ): ListedRunIndex[] =>
    ORDER_GROUPS.flatMap(({ name, states }) =>
      states
        .filter((state) => listedStates === null || listedStates.includes(state))
        .flatMap((indexState) =>
          taskNames.map((taskName) => ({
            indexKey: keys.runIndex(taskName, indexState),
            orderGroupNumber: orderGroupNumberOf(name),
            indexState,
          })),
        ),
    );

  const readIndexPages = async (
    listedRunIndexes: readonly ListedRunIndex[],
    limit: number,
    boundary: IndexPageBoundary,
    { timeRange, order }: RunListRequest,
  ): Promise<IndexPageOfEntries[]> => {
    if (listedRunIndexes.length === 0) {
      return [];
    }
    const indexPages = indexPagesOf(
      await client.send(
        indexPagesCommandOf({
          indexesInList: listedRunIndexes,
          limit,
          boundary,
          order,
          scoreRange: { lowestScore: timeRange.from, highestScore: timeRange.until },
        }),
      ),
      listedRunIndexes.length,
    );
    return indexPages.map(({ places, ...entryCounts }, indexNumber) => ({
      ...entryCounts,
      indexEntries: places.map((indexPlace) => indexEntryOf(listedRunIndexes[indexNumber]!, indexPlace)),
    }));
  };

  const readListedRuns = async (indexEntries: readonly IndexEntry[]): Promise<AnyListedRun[]> => {
    if (indexEntries.length === 0) {
      return [];
    }
    const replies = await repliesToBatchOf(
      indexEntries.map(({ runIdentity }) => listedRunReadCommandOf(keys, runIdentity, RUN_RECORD_FIELDS)),
    );
    return indexEntries.flatMap((entry, entryNumber) => {
      const listedRun = listedRunOf(entry, readPartsOf(replies[entryNumber]), supportedRecordVersion);
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

    listRuns: async (options = {}) => {
      const listedStates = listedStatesOf(options.state);
      const listedTasks = listedTasksOf(options.task, tasksByName);
      const limit = listLimitOf(options.limit);
      const timeRange = listedTimeRangeOf(options.from, options.until, listedStates);
      const order = listOrderOf(options.order);
      const runListRequest = { listedStates, listedTasks, timeRange, order };
      const boundary = pageBoundaryOf(options.cursor, runListRequest);
      const taskNames = listedTasks ?? [...tasksByName.keys()];
      const indexPages = await readIndexPages(
        listedRunIndexesOf(listedStates, taskNames),
        limit,
        boundary,
        runListRequest,
      );
      const totalRunCount = sumOf(indexPages.map(({ entryCount }) => entryCount));
      const { takenEntries, entryCountBeforePage } = takenEntriesOf(indexPages, boundary.direction, limit, order);
      const runs = await readListedRuns(takenEntries);
      const cursorOf = (direction: PagingDirection, entry: IndexEntry | undefined) =>
        encodeRunListCursor({
          ...runListRequest,
          direction,
          place:
            entry === undefined
              ? undefined
              : { orderGroup: orderGroupOf(entry.indexState), score: entry.indexPlace.score, ...entry.runIdentity },
        });
      const [firstTakenEntry, lastTakenEntry] = [takenEntries[0], takenEntries.at(-1)];
      if (firstTakenEntry === undefined || lastTakenEntry === undefined) {
        return totalRunCount === 0
          ? { runs, totalRunCount }
          : { runs, totalRunCount, previousCursor: cursorOf("backward", undefined) };
      }
      const isAnyEntryAfterPage = entryCountBeforePage + takenEntries.length < totalRunCount;
      return {
        runs,
        totalRunCount,
        firstRunPosition: entryCountBeforePage + 1,
        ...(entryCountBeforePage > 0 ? { previousCursor: cursorOf("backward", firstTakenEntry) } : {}),
        ...(isAnyEntryAfterPage ? { nextCursor: cursorOf("forward", lastTakenEntry) } : {}),
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
