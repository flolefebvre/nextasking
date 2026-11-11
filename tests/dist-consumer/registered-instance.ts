import {
  createNextasking,
  defineEvent,
  defineListener,
  defineScheduledTask,
  defineTask,
  InstanceClosedError,
  InvalidPayloadError,
  NonRetriableError,
  RedisUnavailableError,
  runFailedEvent,
  RunNotReplayableError,
  StepMisuseError,
  UnknownEventError,
  UnknownTaskError,
} from "@flefebvre/nextasking";
import type {
  ConnectionConfig,
  DispatchedRun,
  DispatchOptions,
  Duration,
  EmitOptions,
  EmittedEvent,
  EventDefinition,
  EventDefinitionSummary,
  EventId,
  EventName,
  EventOptions,
  EventWithRuns,
  FailureNoticeError,
  FailureReason,
  ListedRun,
  ListedSchedule,
  ListenerDefinition,
  ListenerOptions,
  ListRunsOptions,
  Logger,
  Nextasking,
  NextaskingConfig,
  PayloadIssue,
  PayloadSchema,
  PreviousFiring,
  Register,
  ReplayedRun,
  Run,
  RunFailedPayload,
  RunCounts,
  RunError,
  RunId,
  RunPage,
  RunState,
  SavedStepResult,
  Schedule,
  ScheduledTaskContext,
  ScheduledTaskDefinition,
  ScheduledTaskHandler,
  ScheduledTaskOptions,
  StandardSchemaV1,
  StartWorkerOptions,
  StepContext,
  StepTools,
  TaskContext,
  TaskDefinition,
  TaskDefinitionSummary,
  TaskHandler,
  TaskName,
  TaskOptions,
} from "@flefebvre/nextasking";
import { z } from "zod";

import type { Expect, IsEqual } from "./type-equality.js";

const welcomeEmailSchema = z.object({ email: z.email(), locale: z.string().default("en") });

const userSignedUpSchema = z.object({ userId: z.string() });

type WelcomeEmailPayload = { email: string; locale: string };

type WelcomeEmailInput = { email: string; locale?: string | undefined };

type RegisteredTaskName = "send-welcome-email" | "create-default-workspace" | "purge-expired-sessions";

type UserSignedUpPayload = { userId: string };

export const sendWelcomeEmailTask = defineTask("send-welcome-email", {
  schema: welcomeEmailSchema,
  retries: 3,
  backoff: "10s",
  timeout: "2m",
  handler: async ({ payload, step, logger }) => {
    const messageId = await step.run("send", ({ idempotencyKey }: StepContext) => `${idempotencyKey}:${payload.email}`);
    await step.sleep("cool-down", "1h");
    await step.sleepUntil("follow-up", new Date());
    // @ts-expect-error a duration needs a unit
    await step.sleep("bad-duration", "5");
    // @ts-expect-error a step result must be JSON
    await step.run("not-json", () => new Map<string, string>());
    logger.info({ messageId }, "welcome email sent");
  },
});

export const userSignedUpEvent = defineEvent("user.signed-up", { schema: userSignedUpSchema });

export const createDefaultWorkspaceTask = defineListener("create-default-workspace", {
  on: userSignedUpEvent,
  handler: async ({ payload, dispatch, emit }) => {
    await dispatch("send-welcome-email", { email: `${payload.userId}@example.com` });
    await emit("user.signed-up", { userId: payload.userId }, { eventId: "signed-up-again" });
    // @ts-expect-error the instance lists no such task
    await dispatch("send-goodbye-email", { email: "ada@example.com" });
    // @ts-expect-error the payload doesn't match the event's schema
    await emit("user.signed-up", { email: "ada@example.com" });
  },
});

export const purgeExpiredSessionsTask = defineScheduledTask("purge-expired-sessions", {
  schedule: { cron: "0 3 * * *", timezone: "Europe/Paris" },
  retries: 2,
  concurrency: { limit: 1 },
  handler: async ({ scheduledAt, dispatch }) => {
    await Promise.resolve(scheduledAt?.toISOString());
    await dispatch("purge-expired-sessions");
    // @ts-expect-error a scheduled task takes no payload
    await dispatch("purge-expired-sessions", { olderThanDays: 30 });
  },
});

export type ScheduledTaskHandlerGetsScheduledAt = Expect<
  IsEqual<ScheduledTaskContext["scheduledAt"], Date | undefined>
>;

export type ScheduledTaskIsAScheduledTaskDefinition = Expect<
  IsEqual<typeof purgeExpiredSessionsTask, ScheduledTaskDefinition<"purge-expired-sessions">>
>;

export const purgeExpiredSessionsOptions: ScheduledTaskOptions = {
  schedule: { cron: "0 3 * * *" } satisfies Schedule,
  handler: (async () => undefined) satisfies ScheduledTaskHandler,
};

type WelcomeEmailContext = Parameters<(typeof sendWelcomeEmailTask)["handler"]>[0];

type WorkspaceListenerContext = Parameters<(typeof createDefaultWorkspaceTask)["handler"]>[0];

export type HandlerPayloadIsTheSchemaOutput = Expect<IsEqual<WelcomeEmailContext["payload"], WelcomeEmailPayload>>;

export type ListenerPayloadIsTheEventOutput = Expect<IsEqual<WorkspaceListenerContext["payload"], UserSignedUpPayload>>;

export type HandlerStepIsStepTools = Expect<IsEqual<WelcomeEmailContext["step"], StepTools>>;

export type HandlerLoggerIsLogger = Expect<IsEqual<WelcomeEmailContext["logger"], Logger>>;

export type HandlerRunIdIsRunId = Expect<IsEqual<WelcomeEmailContext["runId"], RunId>>;

export type HandlerAttemptIsANumber = Expect<IsEqual<WelcomeEmailContext["attempt"], number>>;

export type HandlerDispatchReturnsTheRun = Expect<
  IsEqual<ReturnType<WelcomeEmailContext["dispatch"]>, Promise<DispatchedRun>>
>;

export type HandlerEmitReturnsTheEvent = Expect<
  IsEqual<ReturnType<WelcomeEmailContext["emit"]>, Promise<EmittedEvent>>
>;

declare const step: StepTools;

export const savedMessageId = step.run("send", () => "message-1" as const);

export type StepResultKeepsItsType = Expect<IsEqual<typeof savedMessageId, Promise<"message-1">>>;

const connection: ConnectionConfig = { url: "redis://nextasking.invalid:6379", giveUpAfter: "5s" };

export const nextasking = createNextasking({
  connection,
  prefix: "dist-consumer",
  events: [userSignedUpEvent],
  tasks: [sendWelcomeEmailTask, createDefaultWorkspaceTask, purgeExpiredSessionsTask],
});

declare module "@flefebvre/nextasking" {
  interface Register {
    nextasking: typeof nextasking;
  }
}

export type RegisterHoldsTheInstance = Expect<IsEqual<Register["nextasking"], typeof nextasking>>;

export type TaskNameIsTheRegisteredTasks = Expect<IsEqual<TaskName, RegisteredTaskName>>;

export type EventNameIsTheRegisteredEvents = Expect<IsEqual<EventName, "user.signed-up" | "nextasking/run.failed">>;

export type DispatchTakesARegisteredTaskName = Expect<
  IsEqual<Parameters<typeof nextasking.dispatch>[0], RegisteredTaskName>
>;

export type EmitTakesARegisteredEventName = Expect<IsEqual<Parameters<typeof nextasking.emit>[0], "user.signed-up">>;

export type DispatchTakesTheSchemaInput = Expect<
  IsEqual<Parameters<typeof nextasking.dispatch<"send-welcome-email">>[1], WelcomeEmailInput>
>;

export type EmitTakesTheSchemaInput = Expect<
  IsEqual<Parameters<typeof nextasking.emit<"user.signed-up">>[1], UserSignedUpPayload>
>;

export type HandlerDispatchTakesARegisteredTaskName = Expect<
  IsEqual<Parameters<TaskContext<unknown>["dispatch"]>[0], TaskName>
>;

export type HandlerEmitTakesARegisteredEventName = Expect<
  IsEqual<Parameters<TaskContext<unknown>["emit"]>[0], "user.signed-up">
>;

export type DispatchOptionsHoldRunIdAndDelay = Expect<IsEqual<DispatchOptions, { runId?: RunId; delay?: Duration }>>;

export type EmitOptionsHoldEventId = Expect<IsEqual<EmitOptions, { eventId?: EventId }>>;

export type StartWorkerOptionsHoldConcurrencyShutdownTimeoutAndOnWorkerNotStarted = Expect<
  IsEqual<
    StartWorkerOptions,
    {
      concurrency?: number;
      shutdownTimeout?: Duration;
      onWorkerNotStarted?: (error: unknown) => void | Promise<void>;
    }
  >
>;

export type StartWorkerTakesItsOptions = Expect<
  IsEqual<Parameters<typeof nextasking.startWorker>, [options?: StartWorkerOptions]>
>;

export type InstanceIsANextasking = Expect<
  IsEqual<
    typeof nextasking,
    Nextasking<
      typeof sendWelcomeEmailTask | typeof createDefaultWorkspaceTask | typeof purgeExpiredSessionsTask,
      typeof userSignedUpEvent | typeof runFailedEvent
    >
  >
>;

export type TaskIsATaskDefinition = Expect<
  IsEqual<typeof sendWelcomeEmailTask, TaskDefinition<"send-welcome-email", typeof welcomeEmailSchema>>
>;

export type EventIsAnEventDefinition = Expect<
  IsEqual<typeof userSignedUpEvent, EventDefinition<"user.signed-up", typeof userSignedUpSchema>>
>;

export type ListenerIsAListenerDefinition = Expect<
  IsEqual<typeof createDefaultWorkspaceTask, ListenerDefinition<"create-default-workspace", typeof userSignedUpEvent>>
>;

export const welcomeEmailOptions: TaskOptions<typeof welcomeEmailSchema> = {
  schema: welcomeEmailSchema,
  handler: async () => undefined,
};

export const userSignedUpOptions: EventOptions<typeof userSignedUpSchema> = { schema: userSignedUpSchema };

export const workspaceListenerOptions: ListenerOptions<typeof userSignedUpEvent> = {
  on: userSignedUpEvent,
  handler: async () => undefined,
};

export const welcomeEmailHandler: TaskHandler<WelcomeEmailPayload> = async ({ payload }) => {
  await Promise.resolve(payload.locale);
};

export const consumerConfig: NextaskingConfig<readonly [typeof sendWelcomeEmailTask]> = {
  connection,
  tasks: [sendWelcomeEmailTask],
};

export const anyStandardSchema: StandardSchemaV1 = welcomeEmailSchema;

export const anyPayloadSchema: PayloadSchema = userSignedUpSchema;

export const payloadIssues: PayloadIssue[] = [];

export const errorClasses: (new (...args: never[]) => Error)[] = [
  InstanceClosedError,
  InvalidPayloadError,
  NonRetriableError,
  RedisUnavailableError,
  RunNotReplayableError,
  StepMisuseError,
  UnknownEventError,
  UnknownTaskError,
];

export const thrownToSkipRetries = new NonRetriableError("the card was declined");

export const dispatchedFromServerCode = nextasking.dispatch(
  "send-welcome-email",
  { email: "ada@example.com" },
  { runId: "welcome-ada", delay: "5m" },
);

export const emittedFromServerCode = nextasking.emit("user.signed-up", { userId: "user-1" });

// @ts-expect-error the instance lists no such task
export const dispatchedUnknownTask = nextasking.dispatch("send-goodbye-email", { email: "ada@example.com" });

// @ts-expect-error the task's schema refuses the payload
export const dispatchedWrongPayload = nextasking.dispatch("send-welcome-email", { userId: "user-1" });

// @ts-expect-error the instance lists no such event
export const emittedUnknownEvent = nextasking.emit("user.deleted", { userId: "user-1" });

// @ts-expect-error a delay needs a unit
export const dispatchedBadDelay = nextasking.dispatch("send-welcome-email", { email: "a@b.c" }, { delay: "5" });

export const scheduledTaskRunNow = nextasking.dispatch("purge-expired-sessions");

export const scheduledTaskRunNowWithItsOwnId = nextasking.dispatch("purge-expired-sessions", undefined, {
  runId: "purge-now",
});

// @ts-expect-error a scheduled task takes no payload
export const scheduledTaskGivenAPayload = nextasking.dispatch("purge-expired-sessions", { olderThanDays: 30 });

export const workerStarted: Promise<void> = nextasking.startWorker();

export const replayedRun: Promise<ReplayedRun> = nextasking.replay("send-welcome-email", "welcome-ada");

// @ts-expect-error the instance lists no such task
export const replayedUnknownTask = nextasking.replay("send-goodbye-email", "goodbye-ada");

// @ts-expect-error only the library emits a built-in event
export const emittedBuiltInEvent = nextasking.emit("nextasking/run.failed", {});

function eventNameFor(webhookType: string) {
  const eventName = `user.${webhookType}`;
  return nextasking.isEventName(eventName) ? eventName : undefined;
}

export async function emitWebhookEvent(webhookType: string, data: { userId: string }): Promise<void> {
  const eventName = eventNameFor(webhookType);
  if (eventName !== undefined) {
    await nextasking.emit(eventName, data);
  }
}

export type IsEventNameNarrowsToTheEmittableEvents = Expect<
  IsEqual<NonNullable<ReturnType<typeof eventNameFor>>, "user.signed-up">
>;

export const alertOnRunFailedTask = defineListener("alert-on-run-failed", {
  on: runFailedEvent,
  handler: async ({ payload }) => {
    await Promise.resolve(`${payload.task} ${payload.runId} ${payload.reason} ${payload.error.message}`);
  },
});

type RunFailedListenerContext = Parameters<(typeof alertOnRunFailedTask)["handler"]>[0];

export type RunFailedListenerGetsTheNotice = Expect<IsEqual<RunFailedListenerContext["payload"], RunFailedPayload>>;

export type NoticeErrorHoldsNameMessageAndStack = Expect<
  IsEqual<FailureNoticeError, { name: string; message: string; stack: string }>
>;

export const runReadBack = nextasking.getRun("send-welcome-email", "welcome-ada");

export type GetRunNarrowsThePayloadByTask = Expect<
  IsEqual<NonNullable<Awaited<typeof runReadBack>>["payload"], WelcomeEmailPayload>
>;

export function dueTimeOf(run: Run): Date | FailureReason | undefined {
  switch (run.state) {
    case "delayed":
      return run.startsAt;
    case "suspended":
      return run.resumesAt;
    case "backingOff":
      return run.nextAttemptAt;
    case "failed":
      return run.reason;
    default:
      // @ts-expect-error only a delayed run has a startsAt
      return run.startsAt;
  }
}

export type RunStateIsTheEightStates = Expect<
  IsEqual<
    RunState,
    | "delayed"
    | "pending"
    | "waitingForConcurrencySlot"
    | "executing"
    | "suspended"
    | "backingOff"
    | "completed"
    | "failed"
  >
>;

export type AnExecutingRunShowsWhenItStarted = Expect<IsEqual<Extract<Run, { state: "executing" }>["startedAt"], Date>>;

export type AnExecutingListedRunShowsWhenItStarted = Expect<
  IsEqual<Extract<ListedRun, { state: "executing" }>["startedAt"], Date>
>;

export type RunErrorsAreRunErrors = Expect<IsEqual<Run["errors"], RunError[]>>;

export type RunStepsAreSavedStepResults = Expect<IsEqual<Run["steps"], SavedStepResult[]>>;

export const failedRunsPage: Promise<RunPage> = nextasking.listRuns({ state: "failed", limit: 20 });

export const listRunsOptions: ListRunsOptions = { state: "pending", task: "send-welcome-email" };

export const listRunsOptionsOfSeveralStates: ListRunsOptions = { state: ["pending", "executing"] };

export const listRunsOptionsOfEveryState: ListRunsOptions = { limit: 20 };

export const listRunsOptionsOfATimeRange: ListRunsOptions = {
  state: "failed",
  from: new Date("2026-11-11T08:00:00.000Z"),
  until: new Date("2026-11-11T10:00:00.000Z"),
  order: "ascending",
};

export type ListRunsTakesTwoOrders = Expect<IsEqual<ListRunsOptions["order"], "descending" | "ascending" | undefined>>;

export const listFailedRuns = () => nextasking.listRuns({ state: "failed" });

export const listEndedRuns = () => nextasking.listRuns({ state: ["completed", "failed"] });

export const listEveryRun = () => nextasking.listRuns({});

export const listEveryRunWithNoArgument = () => nextasking.listRuns();

export const listEveryRunOfUndefined = () => nextasking.listRuns(undefined);

type ListedRunOf<List extends () => Promise<RunPage>> = Awaited<ReturnType<List>>["runs"][number];

export type NoArgumentGivesEveryStateOfEveryTask = Expect<
  IsEqual<ListedRunOf<typeof listEveryRunWithNoArgument>, ListedRun>
>;

export type UndefinedGivesEveryStateOfEveryTask = Expect<
  IsEqual<ListedRunOf<typeof listEveryRunOfUndefined>, ListedRun>
>;

export type OneListedStateKeepsItsOwnType = Expect<
  IsEqual<ListedRunOf<typeof listFailedRuns>, Extract<ListedRun, { state: "failed" }>>
>;

export type SeveralListedStatesGiveTheirUnion = Expect<
  IsEqual<ListedRunOf<typeof listEndedRuns>["state"], "completed" | "failed">
>;

export type NoListedStateGivesEveryState = Expect<IsEqual<ListedRunOf<typeof listEveryRun>, ListedRun>>;

export const listRunsOptionsOfSeveralTasks: ListRunsOptions = {
  task: ["send-welcome-email", "create-default-workspace"],
};

export const listWelcomeEmailRuns = () => nextasking.listRuns({ state: "failed", task: "send-welcome-email" });

export const listRunsOfTwoTasks = () =>
  nextasking.listRuns({ task: ["send-welcome-email", "create-default-workspace"] });

export type OneListedTaskKeepsItsOwnType = Expect<
  IsEqual<ListedRunOf<typeof listWelcomeEmailRuns>, Extract<ListedRun, { state: "failed"; task: "send-welcome-email" }>>
>;

export type SeveralListedTasksGiveTheirUnion = Expect<
  IsEqual<ListedRunOf<typeof listRunsOfTwoTasks>["task"], "send-welcome-email" | "create-default-workspace">
>;

export type RunPageHasItsTotal = Expect<IsEqual<RunPage["totalRunCount"], number>>;

export type RunPageHasThePositionOfItsFirstRun = Expect<IsEqual<RunPage["firstRunPosition"], number | undefined>>;

export type RunPageHasACursorToThePageBefore = Expect<IsEqual<RunPage["previousCursor"], string | undefined>>;

export type ListedRunHasNoPayload = Expect<IsEqual<"payload" extends keyof ListedRun ? true : false, false>>;

export const runCounts: Promise<RunCounts> = nextasking.countRuns();

export const followedEvent: Promise<EventWithRuns> = nextasking.getEvent("user.signed-up", "event-1");

export type EventWithRunsHasNoFanOut = Expect<IsEqual<"fanOut" extends keyof EventWithRuns ? true : false, false>>;

export type RunsByListenerHoldEachListener = Expect<
  IsEqual<keyof Extract<EventWithRuns, { event: "user.signed-up" }>["runsByListener"], "create-default-workspace">
>;

export const listedSchedules: Promise<ListedSchedule[]> = nextasking.listSchedules();

export type ListedScheduleIsAScheduledTask = Expect<IsEqual<ListedSchedule["task"], "purge-expired-sessions">>;

export type PreviousFiringHoldsItsRunId = Expect<IsEqual<PreviousFiring, { scheduledAt: Date; runId: RunId }>>;

export const taskDefinitions: readonly TaskDefinitionSummary<TaskName, EventName>[] = nextasking.taskDefinitions;

export const eventDefinitions: readonly EventDefinitionSummary<EventName, TaskName>[] = nextasking.eventDefinitions;

export type RunFailedIsAnEventDefinition = Expect<
  IsEqual<
    typeof runFailedEvent,
    EventDefinition<"nextasking/run.failed", StandardSchemaV1<RunFailedPayload, RunFailedPayload>>
  >
>;

// @ts-expect-error a task is not an event
export const taskAsEvent: EventDefinition = sendWelcomeEmailTask;

// @ts-expect-error a listener is not an event
export const listenerAsEvent: EventDefinition = createDefaultWorkspaceTask;

export const eventAsEvent: EventDefinition = userSignedUpEvent;

export const listenerOnATask = defineListener("send-welcome-sms", {
  // @ts-expect-error a task is not an event
  on: sendWelcomeEmailTask,
  handler: async () => undefined,
});

export const instanceListingATaskAsAnEvent = createNextasking({
  connection,
  // @ts-expect-error a task is not an event
  events: [userSignedUpEvent, sendWelcomeEmailTask],
  tasks: [],
});

export const instanceListingAListenerAsAnEvent = createNextasking({
  connection,
  // @ts-expect-error a listener is not an event
  events: [createDefaultWorkspaceTask],
  tasks: [],
});

const welcomeEmailTaskOf = <const Name extends string>(name: Name) =>
  defineTask(name, { schema: welcomeEmailSchema, handler: async () => undefined });

export const sendWelcomeEmailAgain = welcomeEmailTaskOf("send-welcome-email-again");

export type AGenericFactoryKeepsTheLiteralName = Expect<
  IsEqual<typeof sendWelcomeEmailAgain, TaskDefinition<"send-welcome-email-again", typeof welcomeEmailSchema>>
>;

export type TaskDefinitionsAreNamedByTaskName = Expect<
  IsEqual<(typeof nextasking.taskDefinitions)[number]["name"], TaskName>
>;
