import {
  createNextasking,
  defineEvent,
  defineListener,
  defineTask,
  InvalidPayloadError,
  NonRetriableError,
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
  EventId,
  EventName,
  EventOptions,
  ListenerDefinition,
  ListenerOptions,
  Logger,
  Nextasking,
  NextaskingConfig,
  PayloadIssue,
  PayloadSchema,
  Register,
  RunId,
  StandardSchemaV1,
  StartWorkerOptions,
  StepContext,
  StepTools,
  TaskContext,
  TaskDefinition,
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

type UserSignedUpPayload = { userId: string };

export const sendWelcomeEmail = defineTask("send-welcome-email", {
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

export const userSignedUp = defineEvent("user.signed-up", { schema: userSignedUpSchema });

export const createDefaultWorkspace = defineListener("create-default-workspace", {
  on: userSignedUp,
  handler: async ({ payload, dispatch, emit }) => {
    await dispatch("send-welcome-email", { email: `${payload.userId}@example.com` });
    await emit("user.signed-up", { userId: payload.userId }, { eventId: "signed-up-again" });
    // @ts-expect-error the instance lists no such task
    await dispatch("send-goodbye-email", { email: "ada@example.com" });
    // @ts-expect-error the payload doesn't match the event's schema
    await emit("user.signed-up", { email: "ada@example.com" });
  },
});

type WelcomeEmailContext = Parameters<(typeof sendWelcomeEmail)["handler"]>[0];

type WorkspaceListenerContext = Parameters<(typeof createDefaultWorkspace)["handler"]>[0];

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

const connection: ConnectionConfig = { url: "redis://nextasking.invalid:6379" };

export const nextasking = createNextasking({
  connection,
  prefix: "dist-consumer",
  events: [userSignedUp],
  tasks: [sendWelcomeEmail, createDefaultWorkspace],
});

declare module "@flefebvre/nextasking" {
  interface Register {
    nextasking: typeof nextasking;
  }
}

export type RegisterHoldsTheInstance = Expect<IsEqual<Register["nextasking"], typeof nextasking>>;

export type TaskNameIsTheRegisteredTasks = Expect<IsEqual<TaskName, "send-welcome-email" | "create-default-workspace">>;

export type EventNameIsTheRegisteredEvents = Expect<IsEqual<EventName, "user.signed-up">>;

export type DispatchTakesARegisteredTaskName = Expect<
  IsEqual<Parameters<typeof nextasking.dispatch>[0], "send-welcome-email" | "create-default-workspace">
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
  IsEqual<Parameters<TaskContext<unknown>["emit"]>[0], EventName>
>;

export type DispatchOptionsHoldRunIdAndDelay = Expect<IsEqual<DispatchOptions, { runId?: RunId; delay?: Duration }>>;

export type EmitOptionsHoldEventId = Expect<IsEqual<EmitOptions, { eventId?: EventId }>>;

export type StartWorkerOptionsHoldConcurrencyAndShutdownTimeout = Expect<
  IsEqual<StartWorkerOptions, { concurrency?: number; shutdownTimeout?: Duration }>
>;

export type StartWorkerTakesItsOptions = Expect<
  IsEqual<Parameters<typeof nextasking.startWorker>, [options?: StartWorkerOptions]>
>;

export type InstanceIsANextasking = Expect<
  IsEqual<typeof nextasking, Nextasking<typeof sendWelcomeEmail | typeof createDefaultWorkspace, typeof userSignedUp>>
>;

export type TaskIsATaskDefinition = Expect<
  IsEqual<typeof sendWelcomeEmail, TaskDefinition<"send-welcome-email", typeof welcomeEmailSchema>>
>;

export type EventIsAnEventDefinition = Expect<
  IsEqual<typeof userSignedUp, EventDefinition<"user.signed-up", typeof userSignedUpSchema>>
>;

export type ListenerIsAListenerDefinition = Expect<
  IsEqual<typeof createDefaultWorkspace, ListenerDefinition<"create-default-workspace", typeof userSignedUp>>
>;

export const welcomeEmailOptions: TaskOptions<typeof welcomeEmailSchema> = {
  schema: welcomeEmailSchema,
  handler: async () => undefined,
};

export const userSignedUpOptions: EventOptions<typeof userSignedUpSchema> = { schema: userSignedUpSchema };

export const workspaceListenerOptions: ListenerOptions<typeof userSignedUp> = {
  on: userSignedUp,
  handler: async () => undefined,
};

export const welcomeEmailHandler: TaskHandler<WelcomeEmailPayload> = async ({ payload }) => {
  await Promise.resolve(payload.locale);
};

export const consumerConfig: NextaskingConfig<readonly [typeof sendWelcomeEmail]> = {
  connection,
  tasks: [sendWelcomeEmail],
};

export const anyStandardSchema: StandardSchemaV1 = welcomeEmailSchema;

export const anyPayloadSchema: PayloadSchema = userSignedUpSchema;

export const payloadIssues: PayloadIssue[] = [];

export const errorClasses: (new (...args: never[]) => Error)[] = [
  InvalidPayloadError,
  NonRetriableError,
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

export const workerStarted: Promise<void> = nextasking.startWorker();
