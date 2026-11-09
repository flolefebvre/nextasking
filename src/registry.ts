import { BUILT_IN_EVENTS, isBuiltInEventName } from "./built-in-events.js";
import type { BuiltInEvent, BuiltInEventName } from "./built-in-events.js";
import { createRedisConnections } from "./connection.js";
import type { ConnectionConfig } from "./connection.js";
import type { AnyTaskDefinition, EventDefinition, PayloadInput, PayloadSchema } from "./definitions.js";
import { parseDuration } from "./durations.js";
import { createEngine, wakeUpWaitOf } from "./engine/engine.js";
import { createReadApi } from "./read-api.js";
import type { ReadApi } from "./read-api.js";
import { RESERVED_NAME_PREFIX, createEnqueueRequests } from "./enqueue-requests.js";
import type { DispatchedRun, DispatchOptions, EmitOptions, EmittedEvent, ReplayedRun } from "./enqueue-requests.js";
import type { RedisCallIds } from "./errors.js";
import type { EventId, RunId } from "./ids.js";
import { createInstanceLifecycle } from "./instance-lifecycle.js";
import type { OpenedCommandClient } from "./instance-lifecycle.js";
import { silentLogger } from "./logger.js";
import type { Logger } from "./logger.js";
import type {
  EventWithRunsOf,
  ListRunsOptionsOf,
  ListedRunOfTask,
  ListedScheduleOf,
  RunCountsOf,
  RunOfTask,
  RunPageOf,
} from "./read-api.js";
import { eventDefinitionSummariesOf, taskDefinitionSummariesOf } from "./definition-summaries.js";
import type { EventDefinitionSummary, TaskDefinitionSummary } from "./definition-summaries.js";
import type { RunState } from "./run-states.js";
import { rejectInvalidSchedule } from "./schedules.js";
import { appSendersOn, engineSendTargetOf, handlerSenderOn } from "./send-target.js";
import type { AppSenders, SendTarget } from "./send-target.js";
import type { StartWorkerOptions } from "./worker-options.js";
import { declaredSchedulesOf } from "./worker-schedules.js";

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export interface Register {}

export type TaskNameIn<Registration> = Registration extends {
  nextasking: { isTaskName: (name: string) => name is infer Name extends string };
}
  ? Name
  : never;

export type EventNameIn<Registration> = Registration extends {
  nextasking: { eventDefinitions: readonly { readonly name: infer Name extends string }[] };
}
  ? Name
  : never;

export type TaskName = TaskNameIn<Register>;

export type EventName = EventNameIn<Register>;

type RegisteredDefinitions = Register extends { nextasking: Nextasking<infer Task, infer Event> }
  ? { task: Task; event: Event }
  : { task: never; event: never };

type RegisteredTask = RegisteredDefinitions["task"];

type RegisteredEvent = RegisteredDefinitions["event"];

export type Run = RunOfTask<RegisteredTask>;

export type ListedRun = ListedRunOfTask<RegisteredTask>;

export type ListRunsOptions = ListRunsOptionsOf<TaskName>;

export type RunPage = RunPageOf<ListedRun>;

export type RunCounts = RunCountsOf<TaskName>;

export type EventWithRuns = EventWithRunsOf<RegisteredTask, RegisteredEvent>;

export type ListedSchedule = ListedScheduleOf<ScheduledTaskNameOf<RegisteredTask>>;

type ScheduledTaskNameOf<Task> =
  Extract<Task, { schedule: object }> extends { name: infer Name extends string } ? Name : never;

export type DispatchArguments<Task> = Task extends { schema: infer Schema extends PayloadSchema }
  ? [payload: PayloadInput<Schema>, options?: DispatchOptions]
  : [payload?: undefined, options?: DispatchOptions];

export interface RegisteredDispatch {
  <Name extends TaskName>(
    name: Name,
    ...dispatchArguments: DispatchArguments<Extract<RegisteredTask, { name: Name }>>
  ): Promise<DispatchedRun>;
}

export type EmittableEventNameOf<Event extends EventDefinition> = Exclude<Event["name"], BuiltInEventName>;

export interface RegisteredEmit {
  <Name extends Exclude<EventName, BuiltInEventName>>(
    name: Name,
    payload: PayloadInput<Extract<RegisteredEvent, { name: Name }>["schema"]>,
    options?: EmitOptions,
  ): Promise<EmittedEvent>;
}

export type Nextasking<Task extends AnyTaskDefinition, Event extends EventDefinition = never> = {
  readonly dispatch: <Name extends Task["name"]>(
    name: Name,
    ...dispatchArguments: DispatchArguments<Extract<Task, { name: Name }>>
  ) => Promise<DispatchedRun>;
  readonly emit: <Name extends EmittableEventNameOf<Event>>(
    name: Name,
    payload: PayloadInput<Extract<Event, { name: Name }>["schema"]>,
    options?: EmitOptions,
  ) => Promise<EmittedEvent>;
  readonly replay: <Name extends Task["name"]>(name: Name, runId: RunId) => Promise<ReplayedRun>;
  readonly isTaskName: (name: string) => name is Task["name"];
  readonly isEventName: (name: string) => name is EmittableEventNameOf<Event>;
  readonly startWorker: (options?: StartWorkerOptions) => Promise<void>;
  readonly getRun: <Name extends Task["name"]>(
    name: Name,
    runId: RunId,
  ) => Promise<RunOfTask<Extract<Task, { name: Name }>> | undefined>;
  readonly listRuns: <State extends RunState, Name extends Task["name"] = Task["name"]>(
    options: ListRunsOptionsOf<Name, State>,
  ) => Promise<RunPageOf<Extract<ListedRunOfTask<Extract<Task, { name: Name }>>, { state: State }>>>;
  readonly countRuns: () => Promise<RunCountsOf<Task["name"]>>;
  readonly getEvent: <Name extends Event["name"]>(
    name: Name,
    eventId: EventId,
  ) => Promise<EventWithRunsOf<Task, Extract<Event, { name: Name }>>>;
  readonly listSchedules: () => Promise<ListedScheduleOf<ScheduledTaskNameOf<Task>>[]>;
  readonly taskDefinitions: readonly TaskDefinitionSummary<Task["name"], Event["name"]>[];
  readonly eventDefinitions: readonly EventDefinitionSummary<Event["name"], Task["name"]>[];
};

export type NextaskingConfig<
  Tasks extends readonly AnyTaskDefinition[],
  Events extends readonly EventDefinition[] = readonly [],
> = {
  connection: ConnectionConfig;
  prefix?: string;
  logger?: Logger;
  events?: Events;
  tasks: Tasks;
};

export const DEFAULT_PREFIX = "nextasking";

export type NextaskingControls = {
  close: () => Promise<void>;
  startRecordingSends: (recordingSendTarget: SendTarget) => void;
  stopRecordingSends: () => void;
  tasksByName: ReadonlyMap<string, AnyTaskDefinition>;
  appSendersTo: (sendTarget: SendTarget) => AppSenders;
};

const controlsByNextasking = new WeakMap<object, NextaskingControls>();

export function controlsOf(nextasking: object): NextaskingControls | undefined {
  return controlsByNextasking.get(nextasking);
}

function fakedInstanceError(callName: string): Error {
  return new Error(
    `This nextasking instance is faked: ${callName} needs Redis, which a faked instance never touches. Call restore() on the fake first`,
  );
}

export function createNextasking<
  const Tasks extends readonly AnyTaskDefinition[],
  const Events extends readonly EventDefinition[] = readonly [],
>(config: NextaskingConfig<Tasks, Events>): Nextasking<Tasks[number], Events[number] | BuiltInEvent> {
  const appEvents: readonly EventDefinition[] = config.events ?? [];
  rejectInvalidConfig(config.tasks, appEvents);
  const events: readonly EventDefinition[] = [...appEvents, ...BUILT_IN_EVENTS];
  const prefix = config.prefix ?? DEFAULT_PREFIX;
  const logger = config.logger ?? silentLogger;
  const connections = createRedisConnections(config.connection, logger);
  const tasksByName = new Map<string, AnyTaskDefinition>(config.tasks.map((task) => [task.name, task]));
  const eventsByName = new Map<string, EventDefinition>(events.map((event) => [event.name, event]));
  const listenerNamesByEventName = new Map<string, string[]>(events.map((event) => [event.name, []]));
  for (const task of config.tasks) {
    if (task.on !== undefined) {
      listenerNamesByEventName.get(task.on.name)?.push(task.name);
    }
  }
  const declaredSchedules = declaredSchedulesOf({ tasksByName, listenerNamesByEventName });
  const requests = createEnqueueRequests({ tasksByName, eventsByName, listenerNamesByEventName });
  const lifecycle = createInstanceLifecycle({
    connections,
    engineOn: (commandClient) => createEngine(commandClient, prefix),
    wakeUpWaitOn: (blockingClient) => wakeUpWaitOf(blockingClient, prefix),
    senderOn: (engine) => handlerSenderOn(engine, requests, logger),
    definitions: { tasksByName, listenerNamesByEventName },
    logger,
  });
  const callOnRedisWhileOpen = <Result>(
    callIds: RedisCallIds,
    call: (opened: OpenedCommandClient) => Promise<Result>,
  ): Promise<Result> => connections.callOnRedis(callIds, () => lifecycle.callWhileOpen(call));
  const engineSendTarget = engineSendTargetOf((callIds, call) =>
    callOnRedisWhileOpen(callIds, ({ engine }) => call(engine)),
  );
  let recordingSendTarget: SendTarget | undefined;
  let isWorkerStarted = false;
  const appSendersTo = (sendTarget: SendTarget): AppSenders =>
    appSendersOn({ requests, tasksByName, sendTarget: () => sendTarget, logger });
  const { dispatch, emit, replay } = appSendersOn({
    requests,
    tasksByName,
    sendTarget: () => recordingSendTarget ?? engineSendTarget,
    logger,
  });

  let readApi: ReadApi | undefined;
  const readOnRedis = <Result>(
    callName: string,
    callIds: RedisCallIds,
    read: (reader: ReadApi) => Promise<Result>,
  ): Promise<Result> =>
    recordingSendTarget === undefined
      ? callOnRedisWhileOpen(callIds, ({ commandClient }) => {
          readApi ??= createReadApi({
            client: commandClient,
            prefix,
            tasksByName,
            listenerNamesByEventName,
            declaredSchedules,
          });
          return read(readApi);
        })
      : Promise.reject(fakedInstanceError(callName));

  type Instance = Nextasking<Tasks[number], Events[number] | BuiltInEvent>;
  const nextasking: Instance = {
    dispatch: dispatch as Instance["dispatch"],
    emit: emit as Instance["emit"],
    replay,
    isTaskName: (name): name is Tasks[number]["name"] => tasksByName.has(name),
    isEventName: (name): name is EmittableEventNameOf<Events[number] | BuiltInEvent> =>
      eventsByName.has(name) && !isBuiltInEventName(name),
    getRun: ((taskName: string, runId: RunId) =>
      readOnRedis("getRun", { runId }, (reader) => reader.getRun(taskName, runId))) as Instance["getRun"],
    listRuns: ((options: Parameters<ReadApi["listRuns"]>[0]) =>
      readOnRedis("listRuns", {}, (reader) => reader.listRuns(options))) as Instance["listRuns"],
    countRuns: (() => readOnRedis("countRuns", {}, (reader) => reader.countRuns())) as Instance["countRuns"],
    getEvent: ((eventName: string, eventId: EventId) =>
      readOnRedis("getEvent", { eventId }, (reader) => reader.getEvent(eventName, eventId))) as Instance["getEvent"],
    listSchedules: (() =>
      readOnRedis("listSchedules", {}, (reader) => reader.listSchedules())) as Instance["listSchedules"],
    taskDefinitions: taskDefinitionSummariesOf(config.tasks) as Instance["taskDefinitions"],
    eventDefinitions: eventDefinitionSummariesOf(events, listenerNamesByEventName) as Instance["eventDefinitions"],
    startWorker: async (options) => {
      if (recordingSendTarget !== undefined) {
        throw fakedInstanceError("startWorker");
      }
      isWorkerStarted = true;
      try {
        await lifecycle.startWorker(options);
      } catch (error) {
        isWorkerStarted = false;
        throw error;
      }
    },
  };

  controlsByNextasking.set(nextasking, {
    close: lifecycle.close,
    startRecordingSends: (sendTarget) => {
      if (recordingSendTarget !== undefined) {
        throw new Error("This nextasking instance is already faked: restore() its fake before faking it again");
      }
      if (isWorkerStarted) {
        throw new Error(
          "This nextasking instance has started its worker, which would keep writing to Redis: fake an instance whose worker is not started",
        );
      }
      recordingSendTarget = sendTarget;
    },
    stopRecordingSends: () => {
      recordingSendTarget = undefined;
    },
    tasksByName,
    appSendersTo,
  });

  return nextasking;
}

function rejectInvalidConfig(tasks: readonly AnyTaskDefinition[], events: readonly EventDefinition[]): void {
  rejectDuplicateNames([...tasks.map((task) => task.name), ...events.map((event) => event.name)]);
  for (const event of events) {
    if (isBuiltInEventName(event.name)) {
      throw new Error(
        `Event "${event.name}" is built in and added automatically: remove it from events, and listen to it as it is`,
      );
    }
    if (event.name.startsWith(RESERVED_NAME_PREFIX)) {
      throw new Error(
        `Event "${event.name}" has a reserved name: names starting with "${RESERVED_NAME_PREFIX}" belong to the built-in events`,
      );
    }
  }
  const eventNames = new Set([...events, ...BUILT_IN_EVENTS].map((event) => event.name));
  for (const task of tasks) {
    if (task.name.startsWith(RESERVED_NAME_PREFIX)) {
      throw new Error(
        `Task "${task.name}" has a reserved name: names starting with "${RESERVED_NAME_PREFIX}" are reserved for the library`,
      );
    }
    if (task.on !== undefined && !eventNames.has(task.on.name)) {
      throw new Error(`Listener "${task.name}" listens to event "${task.on.name}", which is not listed in events`);
    }
    rejectInvalidTaskOptions(task);
  }
}

function rejectDuplicateNames(definitionNames: readonly string[]): void {
  const seenNames = new Set<string>();
  for (const name of definitionNames) {
    if (seenNames.has(name)) {
      throw new Error(`Duplicate name "${name}": every task and event needs its own name`);
    }
    seenNames.add(name);
  }
}

const DURATION_OPTIONS = ["backoff", "timeout"] as const;

function rejectInvalidTaskOptions(task: AnyTaskDefinition): void {
  if (task.schedule !== undefined) {
    rejectInvalidSchedule(task.name, task.schedule);
  }
  if (task.concurrency !== undefined) {
    const limit: unknown = task.concurrency.limit;
    if (typeof limit !== "number" || !Number.isInteger(limit) || limit <= 0) {
      throw new Error(
        `Task "${task.name}" has an invalid concurrency.limit: ${shownOptionValue(limit)} is not a positive integer`,
      );
    }
  }
  if (task.retries !== undefined) {
    const retries: unknown = task.retries;
    if (typeof retries !== "number" || !Number.isInteger(retries) || retries < 0) {
      throw new Error(
        `Task "${task.name}" has an invalid retries: ${shownOptionValue(retries)} is not a non-negative integer`,
      );
    }
  }
  for (const option of DURATION_OPTIONS) {
    const duration = task[option];
    if (duration === undefined) {
      continue;
    }
    try {
      parseDuration(duration);
    } catch (error) {
      throw new Error(`Task "${task.name}" has an invalid ${option}: ${(error as Error).message}`, { cause: error });
    }
  }
}

function shownOptionValue(optionValue: unknown): string {
  return typeof optionValue === "string" ? JSON.stringify(optionValue) : String(optionValue);
}

export async function closeNextasking(nextasking: object): Promise<void> {
  await controlsByNextasking.get(nextasking)?.close();
}
