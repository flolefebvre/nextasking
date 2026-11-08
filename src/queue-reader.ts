import type { AnyTaskDefinition, EventDefinition, Payload, PayloadSchema } from "./definitions.js";
import type { DurationInMilliseconds, EpochMilliseconds } from "./durations.js";
import { UnknownEventError, UnknownTaskError } from "./errors.js";
import { deriveFiringRunId, deriveJobId, deriveListenerRunId } from "./ids.js";
import type { EventId, JobId, RunId } from "./ids.js";
import { loadSavedStepResults } from "./redis-step-store.js";
import { decodeRunListCursor, encodeRunListCursor } from "./run-list-cursor.js";
import type { RunListCursor, RunListPosition } from "./run-list-cursor.js";
import type { ErrorRecord, RunQueue, StoredRun } from "./run-queue.js";
import {
  BULLMQ_JOB_STATE_BY_SET,
  BULLMQ_SETS_BY_RUN_STATE,
  RUN_STATES,
  attemptOf,
  runStateFieldsOf,
  runStateOf,
} from "./run-states.js";
import type { BullmqJobSet, BullmqJobState, RunState, RunStateFields } from "./run-states.js";
import { previousFiringInstantBefore } from "./schedules.js";
import type { CronExpression, DeclaredSchedule, IanaTimeZone } from "./schedules.js";
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
  nextCursor?: RunListCursor;
};

export type RunCountsOf<TaskName extends string> = { [Name in TaskName]: Record<RunState, number> };

type AnyListedRun = ListedRunOfTask<AnyTaskDefinition>;

export type FanOutState = "pending" | "completed" | "failed";

type ListenerOf<Task, EventName extends string> = Extract<Task, { on: { name: EventName } }>;

export type EventWithRunsOf<
  Task extends AnyTaskDefinition,
  Event extends EventDefinition,
> = Event extends EventDefinition
  ? {
      event: Event["name"];
      eventId: EventId;
      fanOut: FanOutState | undefined;
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

export type QueueReaderConfig = {
  openRunQueue: () => Promise<RunQueue>;
  tasksByName: ReadonlyMap<string, AnyTaskDefinition>;
  listenerNamesByEventName: ReadonlyMap<string, readonly string[]>;
  declaredSchedules: readonly DeclaredSchedule[];
};

export type QueueReader = {
  getRun: (taskName: string, runId: RunId) => Promise<RunOfTask<AnyTaskDefinition> | undefined>;
  listRuns: (options: ListRunsOptionsOf<string>) => Promise<RunPageOf<AnyListedRun>>;
  countRuns: () => Promise<RunCountsOf<string>>;
  getEvent: (eventName: string, eventId: EventId) => Promise<EventWithRunsOf<AnyTaskDefinition, EventDefinition>>;
  listSchedules: () => Promise<ListedScheduleOf<string>[]>;
};

const FAN_OUT_STATE_BY_BULLMQ_STATE: Partial<Record<BullmqJobState, FanOutState>> = {
  waiting: "pending",
  prioritized: "pending",
  "waiting-children": "pending",
  delayed: "pending",
  active: "pending",
  completed: "completed",
  failed: "failed",
};

const FIRST_ITERATION = 1;

const DEFAULT_LIST_LIMIT = 50;

const JOBS_READ_PER_BATCH = 200;

const RUN_JOB_FIELD_NAMES = ["name", "data", "timestamp", "delay", "atm", "finishedOn"];

type RunJobRecord = {
  jobKey: string;
  name: string;
  data: StoredRun;
  timestamp: EpochMilliseconds;
  delay: DurationInMilliseconds;
  attemptsMade: number;
  finishedOn: EpochMilliseconds | undefined;
};

function runJobRecordOf(jobKey: string, [name, data, timestamp, delay, attemptsMade, finishedOn]: JobFieldValues) {
  if (name === null || name === undefined || data === null || data === undefined) {
    return undefined;
  }
  return {
    jobKey,
    name,
    data: JSON.parse(data) as StoredRun,
    timestamp: Number(timestamp ?? 0),
    delay: Number(delay ?? 0),
    attemptsMade: Number(attemptsMade ?? 0),
    finishedOn: finishedOn === null || finishedOn === undefined ? undefined : Number(finishedOn),
  } satisfies RunJobRecord;
}

function runErrorOf(errorRecord: ErrorRecord): RunError {
  return { ...errorRecord, failedAt: new Date(errorRecord.failedAt) };
}

type AnyRunSummary = Omit<AnyListedRun, "lastErrorMessage">;

function runSummaryOf(record: RunJobRecord, bullmqState: BullmqJobState): AnyRunSummary | undefined {
  const stateFields = runStateFieldsOf({
    bullmqState,
    storedRun: record.data,
    dispatchedAt: record.timestamp,
    delayInMilliseconds: record.delay,
  });
  if (stateFields === undefined) {
    return undefined;
  }
  return {
    task: record.name,
    runId: record.data.runId,
    ...stateFields,
    attempt: attemptOf(record, stateFields.state),
    dispatchedAt: new Date(record.timestamp),
    ...(record.finishedOn === undefined ? {} : { finishedAt: new Date(record.finishedOn) }),
  } as AnyRunSummary;
}

function listedRunOf(record: RunJobRecord, bullmqState: BullmqJobState): AnyListedRun | undefined {
  const runSummary = runSummaryOf(record, bullmqState);
  const lastErrorMessage = record.data.errors.at(-1)?.message;
  if (runSummary === undefined) {
    return undefined;
  }
  return (lastErrorMessage === undefined ? runSummary : { ...runSummary, lastErrorMessage }) as AnyListedRun;
}

function rejectInvalidState(state: unknown): asserts state is RunState {
  if (!RUN_STATES.includes(state as RunState)) {
    const shownState = typeof state === "string" ? JSON.stringify(state) : String(state);
    const shownStates = RUN_STATES.map((runState) => `"${runState}"`).join(", ");
    throw new Error(`Invalid listRuns state: ${shownState} is not one of ${shownStates}`);
  }
}

function listLimitOf(limit: unknown): number {
  if (limit === undefined) {
    return DEFAULT_LIST_LIMIT;
  }
  if (typeof limit !== "number" || !Number.isInteger(limit) || limit <= 0) {
    const shownLimit = typeof limit === "string" ? JSON.stringify(limit) : String(limit);
    throw new Error(`Invalid listRuns limit: ${shownLimit} is not a positive integer`);
  }
  return limit;
}

function zeroRunCountsOf(taskNames: Iterable<string>): RunCountsOf<string> {
  return Object.fromEntries(
    [...taskNames].map((taskName) => [
      taskName,
      Object.fromEntries(RUN_STATES.map((runState) => [runState, 0])) as Record<RunState, number>,
    ]),
  );
}

export type JobFieldValues = (string | null)[];

export type JobFields = { jobKey: string; fieldValues: JobFieldValues };

export type QueueAccess = Pick<RunQueue, "bullmqQueue" | "client">;

export async function readJobFields(
  { bullmqQueue, client }: QueueAccess,
  jobIds: readonly JobId[],
  fieldNames: readonly string[],
): Promise<JobFields[]> {
  const jobKeys = jobIds.map((jobId) => bullmqQueue.toKey(jobId));
  const pipeline = client.pipeline();
  for (const jobKey of jobKeys) {
    pipeline.hmget(jobKey, ...fieldNames);
  }
  const replies = (await pipeline.exec()) ?? [];
  return replies.map(([error, fieldValues], index) => {
    if (error !== null) {
      throw error;
    }
    return { jobKey: jobKeys[index] ?? "", fieldValues: fieldValues as JobFieldValues };
  });
}

export async function forEachBatchOfJobIds(
  { bullmqQueue }: QueueAccess,
  set: BullmqJobSet,
  firstOffset: number,
  useBatch: (jobIds: JobId[], offset: number) => Promise<"next batch" | "stop">,
): Promise<"set walked" | "stopped"> {
  for (let offset = firstOffset; ; offset += JOBS_READ_PER_BATCH) {
    const jobIds = await bullmqQueue.getRanges([set], offset, offset + JOBS_READ_PER_BATCH - 1, false);
    if ((await useBatch(jobIds, offset)) === "stop") {
      return "stopped";
    }
    if (jobIds.length < JOBS_READ_PER_BATCH) {
      return "set walked";
    }
  }
}

export function createQueueReader(config: QueueReaderConfig): QueueReader {
  const rejectUnknownTask = (taskName: string): void => {
    if (!config.tasksByName.has(taskName)) {
      throw new UnknownTaskError(taskName, [...config.tasksByName.keys()]);
    }
  };

  const readJobFieldsOfQueue = async (jobIds: readonly JobId[], fieldNames: readonly string[]) =>
    readJobFields(await config.openRunQueue(), jobIds, fieldNames);

  const readRunJobRecords = async (jobIds: readonly JobId[]): Promise<RunJobRecord[]> =>
    (await readJobFieldsOfQueue(jobIds, RUN_JOB_FIELD_NAMES)).flatMap(({ jobKey, fieldValues }) => {
      const record = runJobRecordOf(jobKey, fieldValues);
      return record !== undefined && config.tasksByName.has(record.name) ? [record] : [];
    });

  const forEachBatchOfJobIdsOfQueue = async (
    set: BullmqJobSet,
    firstOffset: number,
    useBatch: (jobIds: JobId[], offset: number) => Promise<"next batch" | "stop">,
  ) => forEachBatchOfJobIds(await config.openRunQueue(), set, firstOffset, useBatch);

  const getRun: QueueReader["getRun"] = async (taskName, runId) => {
    rejectUnknownTask(taskName);
    const { bullmqQueue, client } = await config.openRunQueue();
    const jobId = deriveJobId(taskName, runId);
    const [[record], bullmqState] = await Promise.all([readRunJobRecords([jobId]), bullmqQueue.getJobState(jobId)]);
    const runSummary = record === undefined ? undefined : runSummaryOf(record, bullmqState);
    if (record === undefined || runSummary === undefined) {
      return undefined;
    }
    return {
      ...runSummary,
      payload: record.data.payload,
      errors: record.data.errors.map(runErrorOf),
      steps: await loadSavedStepResults(client, record.jobKey),
    } as RunOfTask<AnyTaskDefinition>;
  };

  return {
    getRun,

    listRuns: async (options) => {
      const { state, task } = options;
      rejectInvalidState(state);
      if (task !== undefined) {
        rejectUnknownTask(task);
      }
      const limit = listLimitOf(options.limit);
      const sets = BULLMQ_SETS_BY_RUN_STATE[state];
      const startPosition: RunListPosition =
        options.cursor === undefined ? { state, setIndex: 0, offset: 0 } : decodeRunListCursor(options.cursor, state);
      const runs: AnyListedRun[] = [];
      let nextCursor: RunListCursor | undefined;
      for (let setIndex = startPosition.setIndex; setIndex < sets.length && nextCursor === undefined; setIndex += 1) {
        const set = sets[setIndex] ?? "wait";
        const firstOffset = setIndex === startPosition.setIndex ? startPosition.offset : 0;
        await forEachBatchOfJobIdsOfQueue(set, firstOffset, async (jobIds, offset) => {
          const fieldsByJob = await readJobFieldsOfQueue(jobIds, RUN_JOB_FIELD_NAMES);
          for (const [index, { jobKey, fieldValues }] of fieldsByJob.entries()) {
            const record = runJobRecordOf(jobKey, fieldValues);
            const listedRun =
              record === undefined || !config.tasksByName.has(record.name)
                ? undefined
                : listedRunOf(record, BULLMQ_JOB_STATE_BY_SET[set]);
            if (listedRun?.state === state && (task === undefined || listedRun.task === task)) {
              runs.push(listedRun);
            }
            if (runs.length === limit) {
              nextCursor = encodeRunListCursor({ state, setIndex, offset: offset + index + 1 });
              return "stop";
            }
          }
          return "next batch";
        });
      }
      return nextCursor === undefined ? { runs } : { runs, nextCursor };
    },

    countRuns: async () => {
      const runCounts = zeroRunCountsOf(config.tasksByName.keys());
      for (const set of Object.keys(BULLMQ_JOB_STATE_BY_SET) as BullmqJobSet[]) {
        const fieldNames = set === "delayed" ? ["name", "data"] : ["name"];
        await forEachBatchOfJobIdsOfQueue(set, 0, async (jobIds) => {
          for (const { fieldValues } of await readJobFieldsOfQueue(jobIds, fieldNames)) {
            const [name, data] = fieldValues;
            const taskRunCounts =
              name === null || name === undefined || !config.tasksByName.has(name) ? undefined : runCounts[name];
            const delayedRunState =
              data === null || data === undefined ? undefined : (JSON.parse(data) as StoredRun).delayedRunState;
            const state = runStateOf(BULLMQ_JOB_STATE_BY_SET[set], delayedRunState);
            if (taskRunCounts !== undefined && state !== undefined) {
              taskRunCounts[state] += 1;
            }
          }
          return "next batch";
        });
      }
      return runCounts;
    },

    getEvent: async (eventName, eventId) => {
      const listenerNames = config.listenerNamesByEventName.get(eventName);
      if (listenerNames === undefined) {
        throw new UnknownEventError(eventName, [...config.listenerNamesByEventName.keys()]);
      }
      const [fanOutBullmqState, ...listenerRuns] = await Promise.all([
        (await config.openRunQueue()).bullmqQueue.getJobState(deriveJobId(eventName, eventId)),
        ...listenerNames.map((listenerName) => getRun(listenerName, deriveListenerRunId(eventId, listenerName))),
      ]);
      return {
        event: eventName,
        eventId,
        fanOut: FAN_OUT_STATE_BY_BULLMQ_STATE[fanOutBullmqState],
        runsByListener: Object.fromEntries(
          listenerNames.map((listenerName, index) => [listenerName, listenerRuns[index]]),
        ),
      };
    },

    listSchedules: async () => {
      const storedSchedulers = await (await config.openRunQueue()).bullmqQueue.getJobSchedulers(0, -1);
      const storedSchedulersByTaskName = new Map(storedSchedulers.map((scheduler) => [scheduler.key, scheduler]));
      return config.declaredSchedules.map((declaredSchedule) => {
        const { taskName, cron, timezone } = declaredSchedule;
        const storedScheduler = storedSchedulersByTaskName.get(taskName);
        const nextFiringInstant = storedScheduler?.next ?? undefined;
        if (nextFiringInstant === undefined) {
          return { task: taskName, cron, timezone };
        }
        const hasFired = (storedScheduler?.iterationCount ?? FIRST_ITERATION) > FIRST_ITERATION;
        const previousFiringInstant = previousFiringInstantBefore(declaredSchedule, nextFiringInstant);
        return {
          task: taskName,
          cron,
          timezone,
          nextFiringAt: new Date(nextFiringInstant),
          ...(hasFired
            ? {
                previousFiring: {
                  scheduledAt: new Date(previousFiringInstant),
                  runId: deriveFiringRunId(taskName, previousFiringInstant),
                },
              }
            : {}),
        };
      });
    },
  };
}
