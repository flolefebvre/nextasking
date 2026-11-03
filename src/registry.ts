import { BUILT_IN_EVENTS, isBuiltInEventName } from "./built-in-events.js";
import type { BuiltInEvent, BuiltInEventName } from "./built-in-events.js";
import type { ConnectionConfig } from "./connection.js";
import { concurrencyKeyOf, retriesOf, validatePayload } from "./definitions.js";
import type { AnyTaskDefinition, EventDefinition, PayloadInput, PayloadSchema } from "./definitions.js";
import { parseDuration } from "./durations.js";
import type { Duration, DurationInMilliseconds } from "./durations.js";
import { FanOutNotReplayableError, RunNotReplayableError, UnknownEventError, UnknownTaskError } from "./errors.js";
import type { DefinitionKind } from "./errors.js";
import { createRandomId } from "./ids.js";
import type { EventId, RunId } from "./ids.js";
import { silentLogger } from "./logger.js";
import type { Logger } from "./logger.js";
import { createQueueReader } from "./queue-reader.js";
import type {
  EventWithRunsOf,
  ListRunsOptionsOf,
  ListedRunOfTask,
  ListedScheduleOf,
  RunCountsOf,
  RunOfTask,
  RunPageOf,
} from "./queue-reader.js";
import { eventDefinitionSummariesOf, taskDefinitionSummariesOf } from "./definition-summaries.js";
import type { EventDefinitionSummary, TaskDefinitionSummary } from "./definition-summaries.js";
import { createRunQueue } from "./run-queue.js";
import type { RunState } from "./run-states.js";
import type { RunQueue } from "./run-queue.js";
import { rejectInvalidSchedule, timezoneOf } from "./schedules.js";
import {
  MISSING_MANUAL_SIGNAL_HANDLING_WARNING,
  exitOnShutdownSignals,
  isDevelopment,
  isSignalHandlingLeftToTheApp,
} from "./shutdown-signals.js";
import { startRunWorker, workerOptionsFrom } from "./worker-runtime.js";
import type { RunningWorker, StartWorkerOptions } from "./worker-runtime.js";

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export interface Register {}

export type TaskNameIn<Registration> = Registration extends {
  nextasking: { isTaskName: (name: string) => name is infer Name extends string };
}
  ? Name
  : never;

export type EventNameIn<Registration> = Registration extends {
  nextasking: { isEventName: (name: string) => name is infer Name extends string };
}
  ? Name
  : never;

export type TaskName = TaskNameIn<Register>;

export type EventName = EventNameIn<Register>;

export type DispatchedRun = {
  runId: RunId;
};

export type EmittedEvent = {
  eventId: EventId;
};

export type ReplayedRun = {
  runId: RunId;
};

export type ReplayedEvent = {
  eventId: EventId;
};

export type DispatchByName = (name: string, payload?: unknown, options?: DispatchOptions) => Promise<DispatchedRun>;

export type EmitByName = (name: string, payload: unknown, options?: EmitOptions) => Promise<EmittedEvent>;

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

export type DispatchOptions = {
  runId?: RunId;
  delay?: Duration;
};

export type EmitOptions = {
  eventId?: EventId;
};

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
  readonly replayEvent: <Name extends Event["name"]>(name: Name, eventId: EventId) => Promise<ReplayedEvent>;
  readonly isTaskName: (name: string) => name is Task["name"];
  readonly isEventName: (name: string) => name is Event["name"];
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

const DEFAULT_PREFIX = "nextasking";

const RESERVED_NAME_PREFIX = "nextasking/";

const closeByNextasking = new WeakMap<object, () => Promise<void>>();

export function createNextasking<
  const Tasks extends readonly AnyTaskDefinition[],
  const Events extends readonly EventDefinition[] = readonly [],
>(config: NextaskingConfig<Tasks, Events>): Nextasking<Tasks[number], Events[number] | BuiltInEvent> {
  const appEvents: readonly EventDefinition[] = config.events ?? [];
  rejectInvalidConfig(config.tasks, appEvents);
  const events: readonly EventDefinition[] = [...appEvents, ...BUILT_IN_EVENTS];
  const prefix = config.prefix ?? DEFAULT_PREFIX;
  const logger = config.logger ?? silentLogger;
  const tasksByName = new Map<string, AnyTaskDefinition>(config.tasks.map((task) => [task.name, task]));
  const eventsByName = new Map<string, EventDefinition>(events.map((event) => [event.name, event]));
  const listenerNamesByEventName = new Map<string, string[]>(events.map((event) => [event.name, []]));
  for (const task of config.tasks) {
    if (task.on !== undefined) {
      listenerNamesByEventName.get(task.on.name)?.push(task.name);
    }
  }
  const declaredSchedules = config.tasks.flatMap((task) =>
    task.schedule === undefined
      ? []
      : [{ taskName: task.name, cron: task.schedule.cron, timezone: timezoneOf(task.schedule) }],
  );
  let runQueue: RunQueue | undefined;
  let runningWorker: RunningWorker | undefined;
  const openRunQueue = (): RunQueue => {
    runQueue ??= createRunQueue(config.connection, prefix);
    return runQueue;
  };

  const queueReader = createQueueReader({ openRunQueue, tasksByName, listenerNamesByEventName, declaredSchedules });

  const dispatchByName: DispatchByName = async (name, payload, options = {}) => {
    const task = tasksByName.get(name);
    if (task === undefined) {
      throw new UnknownTaskError(name, [...tasksByName.keys()]);
    }
    const runId = consumerFacingIdOrRandomId("task", task.name, "runId", options.runId);
    const delayInMilliseconds = delayInMillisecondsOf(task.name, options.delay);
    if (task.schedule !== undefined && payload !== undefined) {
      throw new Error(`Scheduled task "${task.name}" takes no payload: dispatch it by its name alone to run it now`);
    }
    const validatedPayload = task.schedule === undefined ? await validatePayload("task", task, payload) : undefined;
    const concurrencyKey = concurrencyKeyOf(task, validatedPayload);
    const { isDuplicate } = await openRunQueue().enqueueRun(task.name, {
      runId,
      payload: validatedPayload,
      retries: retriesOf(task),
      ...(concurrencyKey === undefined ? {} : { concurrencyKey }),
      ...(delayInMilliseconds === undefined ? {} : { delayInMilliseconds }),
    });
    logger.debug({ task: task.name, runId }, isDuplicate ? "duplicate dispatch dropped" : "run dispatched");
    return { runId };
  };

  const emitByName: EmitByName = async (name, payload, options = {}) => {
    if (isBuiltInEventName(name)) {
      throw new Error(
        `Event "${name}" is built in: names starting with "${RESERVED_NAME_PREFIX}" belong to the built-in events, which only the library emits`,
      );
    }
    const event = eventsByName.get(name);
    if (event === undefined) {
      throw new UnknownEventError(
        name,
        appEvents.map((appEvent) => appEvent.name),
      );
    }
    const eventId = consumerFacingIdOrRandomId("event", event.name, "eventId", options.eventId);
    const validatedPayload = await validatePayload("event", event, payload);
    const listenerNames = listenerNamesByEventName.get(event.name) ?? [];
    if (listenerNames.length === 0) {
      logger.warn({ eventName: event.name, eventId }, "no listeners for event");
      return { eventId };
    }
    const { isDuplicate } = await openRunQueue().enqueueFanOut(event.name, {
      eventId,
      payload: validatedPayload,
      listenerNames,
    });
    if (isDuplicate) {
      logger.debug({ eventName: event.name, eventId }, "duplicate dispatch dropped");
    }
    return { eventId };
  };

  const replay = async (name: string, runId: RunId): Promise<ReplayedRun> => {
    if (!tasksByName.has(name)) {
      throw new UnknownTaskError(name, [...tasksByName.keys()]);
    }
    const replayOutcome = await openRunQueue().replayRun(name, runId);
    if (replayOutcome !== "replayed") {
      throw new RunNotReplayableError(name, runId, replayOutcome);
    }
    logger.info({ task: name, runId }, "run replayed");
    return { runId };
  };

  const replayEvent = async (name: string, eventId: EventId): Promise<ReplayedEvent> => {
    if (!eventsByName.has(name)) {
      throw new UnknownEventError(name, [...eventsByName.keys()]);
    }
    const replayOutcome = await openRunQueue().replayFanOut(name, eventId);
    if (replayOutcome !== "replayed") {
      throw new FanOutNotReplayableError(name, eventId, replayOutcome);
    }
    return { eventId };
  };

  type Instance = Nextasking<Tasks[number], Events[number] | BuiltInEvent>;
  const nextasking: Instance = {
    dispatch: dispatchByName,
    emit: emitByName,
    replay,
    replayEvent,
    isTaskName: (name): name is Tasks[number]["name"] => tasksByName.has(name),
    isEventName: (name): name is (Events[number] | BuiltInEvent)["name"] => eventsByName.has(name),
    getRun: queueReader.getRun as Instance["getRun"],
    listRuns: queueReader.listRuns as Instance["listRuns"],
    countRuns: queueReader.countRuns as Instance["countRuns"],
    getEvent: queueReader.getEvent as Instance["getEvent"],
    listSchedules: queueReader.listSchedules as Instance["listSchedules"],
    taskDefinitions: taskDefinitionSummariesOf(config.tasks) as Instance["taskDefinitions"],
    eventDefinitions: eventDefinitionSummariesOf(events, listenerNamesByEventName) as Instance["eventDefinitions"],
    startWorker: async (options = {}) => {
      const workerOptions = workerOptionsFrom(options);
      const scheduleChanges = await openRunQueue().syncSchedules(declaredSchedules);
      for (const { kind, taskName, cron, timezone } of scheduleChanges) {
        logger.info({ task: taskName, cron, timezone }, `schedule ${kind}`);
      }
      runningWorker = startRunWorker({
        options: workerOptions,
        connection: config.connection,
        prefix,
        logger,
        tasksByName,
        eventsByName,
        openRunQueue,
        dispatch: dispatchByName,
        emit: emitByName,
      });
      if (isSignalHandlingLeftToTheApp()) {
        stopExitingOnShutdownSignals = exitOnShutdownSignals(() => closeInstance({ isDraining: true }), logger);
      } else if (!isDevelopment()) {
        logger.warn({}, MISSING_MANUAL_SIGNAL_HANDLING_WARNING);
      }
    },
  };

  let stopExitingOnShutdownSignals: (() => void) | undefined;
  const closeInstance = async ({ isDraining }: { isDraining: boolean }) => {
    if (!isDraining) {
      stopExitingOnShutdownSignals?.();
      stopExitingOnShutdownSignals = undefined;
    }
    await (isDraining ? runningWorker?.shutDown() : runningWorker?.close());
    runningWorker = undefined;
    await runQueue?.close();
    runQueue = undefined;
  };

  closeByNextasking.set(nextasking, () => closeInstance({ isDraining: false }));

  return nextasking;
}

function consumerFacingIdOrRandomId(
  definitionKind: DefinitionKind,
  definitionName: string,
  idOption: "runId" | "eventId",
  consumerFacingId: unknown,
): string {
  if (consumerFacingId === undefined) {
    return createRandomId();
  }
  if (typeof consumerFacingId !== "string" || consumerFacingId === "") {
    const shownId = JSON.stringify(consumerFacingId) ?? String(consumerFacingId);
    throw new Error(
      `Invalid ${idOption} for ${definitionKind} "${definitionName}": ${shownId} is not a non-empty string`,
    );
  }
  return consumerFacingId;
}

function delayInMillisecondsOf(taskName: string, delay: Duration | undefined): DurationInMilliseconds | undefined {
  if (delay === undefined) {
    return undefined;
  }
  try {
    return parseDuration(delay);
  } catch (error) {
    throw new Error(`Invalid delay for task "${taskName}": ${(error as Error).message}`, { cause: error });
  }
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
  await closeByNextasking.get(nextasking)?.();
}
