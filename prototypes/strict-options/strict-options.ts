import { z } from "zod";

import type { BuiltInEvent } from "../../src/built-in-events.js";
import type { ConnectionConfig } from "../../src/connection.js";
import type {
  AnyTaskDefinition,
  EventDefinition,
  EventOptions,
  ListenerDefinition,
  ListenerOptions,
  PayloadInput,
  PayloadSchema,
  ScheduledTaskDefinition,
  ScheduledTaskOptions,
  TaskDefinition,
  TaskOptions,
} from "../../src/definitions.js";
import type { DispatchedRun, DispatchOptions, EmitOptions, EmittedEvent } from "../../src/enqueue-requests.js";
import { createNextasking, defineEvent, defineListener, defineScheduledTask, defineTask } from "../../src/index.js";
import type { ListRunsOptions, Logger, RunState, StartWorkerOptions } from "../../src/index.js";
import type { ListedRunOfTask, ListRunsOptionsOf, RunPageOf } from "../../src/read-api.js";
import type { EmittableEventNameOf, Nextasking } from "../../src/registry.js";

type IsEqual<Left, Right> =
  (<Probe>() => Probe extends Left ? 1 : 2) extends <Probe>() => Probe extends Right ? 1 : 2 ? true : false;
type Expect<Condition extends true> = Condition;

type UnknownOption<Key> = Key extends string ? `"${Key}" is not an option of this call` : "not an option of this call";

type UnknownKeysRefused<Given, Known> = Given extends object
  ? { [Key in Exclude<keyof Given, keyof Known>]: UnknownOption<Key> }
  : never;

type KeysRead<Given> = { [Key in keyof Given]?: unknown };

type OnlyKnown<Given, Known> = Known & UnknownKeysRefused<Given, Known> & KeysRead<Given>;

type StrictEvent = EventDefinition & { handler?: never };

type DefinitionName<Name extends string> = Name extends "" | `nextasking/${string}` ? never : Name;

type StrictDispatchArguments<Task, Given> = Task extends { schema: infer Schema extends PayloadSchema }
  ? [payload: PayloadInput<Schema>, options?: OnlyKnown<Given, DispatchOptions>]
  : [payload?: undefined, options?: OnlyKnown<Given, DispatchOptions>];

type StrictNextasking<Task extends AnyTaskDefinition, Event extends EventDefinition = never> = Omit<
  Nextasking<Task, Event>,
  "dispatch" | "emit" | "startWorker" | "listRuns"
> & {
  readonly dispatch: <Name extends Task["name"], Given = DispatchOptions>(
    name: Name,
    ...dispatchArguments: StrictDispatchArguments<Extract<Task, { name: Name }>, Given>
  ) => Promise<DispatchedRun>;
  readonly emit: <Name extends EmittableEventNameOf<Event>, Given = EmitOptions>(
    name: Name,
    payload: PayloadInput<Extract<Event, { name: Name }>["schema"]>,
    options?: OnlyKnown<Given, EmitOptions>,
  ) => Promise<EmittedEvent>;
  readonly startWorker: <Given = StartWorkerOptions>(options?: OnlyKnown<Given, StartWorkerOptions>) => Promise<void>;
  readonly listRuns: <
    State extends RunState = RunState,
    Name extends Task["name"] = Task["name"],
    Given = ListRunsOptionsOf<Name, State>,
  >(
    options?: ListRunsOptionsOf<Name, State> & UnknownKeysRefused<Given, ListRunsOptionsOf<string>> & KeysRead<Given>,
  ) => Promise<RunPageOf<Extract<ListedRunOfTask<Extract<Task, { name: Name }>>, { state: State }>>>;
};

type StrictConfig<Tasks, Events, Connection = ConnectionConfig> = {
  connection: Connection;
  prefix?: string;
  logger?: Logger;
  events?: Events;
  tasks: Tasks;
};

declare function strictCreateNextasking<
  const Tasks extends readonly AnyTaskDefinition[],
  const Events extends readonly StrictEvent[] = readonly [],
  Given = StrictConfig<Tasks, Events>,
  GivenConnection = ConnectionConfig,
>(
  config: OnlyKnown<Given, StrictConfig<Tasks, Events, OnlyKnown<GivenConnection, ConnectionConfig>>>,
): StrictNextasking<Tasks[number], Events[number] | BuiltInEvent>;

declare function strictDefineTask<const Name extends string, Schema extends PayloadSchema, Given = TaskOptions<Schema>>(
  name: DefinitionName<Name>,
  options: OnlyKnown<Given, TaskOptions<Schema>>,
): TaskDefinition<Name, Schema>;

declare function strictDefineScheduledTask<const Name extends string, Given = ScheduledTaskOptions>(
  name: DefinitionName<Name>,
  options: OnlyKnown<Given, ScheduledTaskOptions>,
): ScheduledTaskDefinition<Name>;

declare function strictDefineEvent<
  const Name extends string,
  Schema extends PayloadSchema,
  Given = EventOptions<Schema>,
>(name: DefinitionName<Name>, options: OnlyKnown<Given, EventOptions<Schema>>): EventDefinition<Name, Schema>;

declare function strictDefineListener<
  const Name extends string,
  Event extends StrictEvent,
  Given = ListenerOptions<Event>,
>(name: DefinitionName<Name>, options: OnlyKnown<Given, ListenerOptions<Event>>): ListenerDefinition<Name, Event>;

const schema = z.object({ userId: z.string() });
const connection = { url: "redis://localhost:6379" };
const payload = { userId: "u1" };

const realUserSignedUp = defineEvent("user-signed-up", { schema });
const realSendEmail = defineTask("send-email", {
  schema,
  retries: 3,
  concurrency: { limit: 2, key: ({ userId }) => userId },
  handler: async ({ payload: { userId }, step }) => {
    await step.run("send", () => userId.length);
  },
});
const realPurge = defineScheduledTask("purge", {
  schedule: { cron: "0 * * * *" },
  handler: async ({ scheduledAt }) => void scheduledAt,
});
const realOnSignUp = defineListener("on-sign-up", {
  on: realUserSignedUp,
  handler: async ({ payload: { userId } }) => void userId.length,
});
const real = createNextasking({
  connection,
  events: [realUserSignedUp],
  tasks: [realSendEmail, realPurge, realOnSignUp],
});

const userSignedUp = strictDefineEvent("user-signed-up", { schema });
const sendEmail = strictDefineTask("send-email", {
  schema,
  retries: 3,
  concurrency: { limit: 2, key: ({ userId }) => userId },
  handler: async ({ payload: { userId }, step }) => {
    await step.run("send", () => userId.length);
  },
});
const purge = strictDefineScheduledTask("purge", {
  schedule: { cron: "0 * * * *" },
  handler: async ({ scheduledAt }) => void scheduledAt,
});
const onSignUp = strictDefineListener("on-sign-up", {
  on: userSignedUp,
  handler: async ({ payload: { userId } }) => void userId.length,
});
const strict = strictCreateNextasking({
  connection,
  events: [userSignedUp],
  tasks: [sendEmail, purge, onSignUp],
});

export type DefinitionsKeepTheirType = [
  Expect<IsEqual<typeof userSignedUp, typeof realUserSignedUp>>,
  Expect<IsEqual<typeof sendEmail, typeof realSendEmail>>,
  Expect<IsEqual<typeof purge, typeof realPurge>>,
  Expect<IsEqual<typeof onSignUp, typeof realOnSignUp>>,
];

export type InstanceKeepsItsNames = [
  Expect<IsEqual<(typeof strict)["taskDefinitions"], (typeof real)["taskDefinitions"]>>,
  Expect<IsEqual<(typeof strict)["eventDefinitions"], (typeof real)["eventDefinitions"]>>,
  Expect<IsEqual<(typeof strict)["getRun"], (typeof real)["getRun"]>>,
];

const sharedTaskOptions = { retries: 2, timeout: "5m" } as const;
export const taskFromSpreadOfKnownOptions = strictDefineTask("from-spread", {
  schema,
  ...sharedTaskOptions,
  handler: async ({ payload: { userId } }) => void userId.length,
});
const wholeTaskOptions: TaskOptions<typeof schema> = { schema, handler: async () => undefined };
export const taskFromTypedVariable = strictDefineTask("from-variable", wholeTaskOptions);

const realEveryRun = real.listRuns({});
const everyRun = strict.listRuns();
const everyRunWithBraces = strict.listRuns({});
const realFailed = real.listRuns({ state: "failed", limit: 20 });
const failed = strict.listRuns({ state: "failed", limit: 20 });
const realEnded = real.listRuns({ state: ["completed", "failed"], task: "send-email" });
const ended = strict.listRuns({ state: ["completed", "failed"], task: "send-email" });
const realExplicit = real.listRuns<"failed", "purge">({ state: "failed", task: "purge" });
const explicit = strict.listRuns<"failed", "purge">({ state: "failed", task: "purge" });
const states: RunState[] = ["failed"];
const realFromVariable = real.listRuns({ state: states, limit: 10 });
const fromVariable = strict.listRuns({ state: states, limit: 10 });
const typedOptions: ListRunsOptionsOf<"send-email" | "purge" | "on-sign-up"> = { limit: 5 };
const realFromTypedOptions = real.listRuns(typedOptions);
const fromTypedOptions = strict.listRuns(typedOptions);
const knownListOptions = { limit: 20, order: "ascending" as const };
const realFromSpread = real.listRuns({ ...knownListOptions, state: "failed" });
const fromSpread = strict.listRuns({ ...knownListOptions, state: "failed" });
declare const cursor: string | undefined;
declare const maybeOptions: ListRunsOptionsOf<"send-email"> | undefined;
void strict.listRuns({ state: "failed", cursor });
void strict.listRuns(maybeOptions);
void strict.listRuns({ state: "failed", from: new Date(), until: new Date() });

export type ListRunsKeepsItsTypes = [
  Expect<IsEqual<typeof everyRun, typeof realEveryRun>>,
  Expect<IsEqual<typeof everyRunWithBraces, typeof realEveryRun>>,
  Expect<IsEqual<typeof failed, typeof realFailed>>,
  Expect<IsEqual<typeof ended, typeof realEnded>>,
  Expect<IsEqual<typeof explicit, typeof realExplicit>>,
  Expect<IsEqual<typeof fromVariable, typeof realFromVariable>>,
  Expect<IsEqual<typeof fromTypedOptions, typeof realFromTypedOptions>>,
  Expect<IsEqual<typeof fromSpread, typeof realFromSpread>>,
  Expect<
    IsEqual<
      Awaited<ReturnType<typeof strict.listRuns<"failed", "send-email">>>,
      Awaited<ReturnType<typeof real.listRuns<"failed", "send-email">>>
    >
  >,
];

void strict.dispatch("send-email", payload);
void strict.dispatch("send-email", payload, { delay: "5m" });
void strict.dispatch("send-email", payload, { runId: "r1", delay: "5m" });
void strict.dispatch("send-email", payload, {});
void strict.dispatch("send-email", payload, undefined);
void strict.dispatch("purge");
void strict.dispatch("purge", undefined, { delay: "1h" });
const typedDispatchOptions: DispatchOptions = { delay: "5m" };
void strict.dispatch("send-email", payload, typedDispatchOptions);
declare const maybeDispatchOptions: DispatchOptions | undefined;
void strict.dispatch("send-email", payload, maybeDispatchOptions);
const knownDispatchOptions = { delay: "5m" } as const;
void strict.dispatch("send-email", payload, { runId: "r1", ...knownDispatchOptions });
declare const runId: string | undefined;
void strict.dispatch("send-email", payload, { runId });
void strict.emit("user-signed-up", payload);
void strict.emit("user-signed-up", payload, { eventId: "e1" });
const typedEmitOptions: EmitOptions = { eventId: "e1" };
void strict.emit("user-signed-up", payload, typedEmitOptions);
void strict.startWorker();
void strict.startWorker({});
void strict.startWorker({ concurrency: 5, shutdownTimeout: "30s", onWorkerNotStarted: (error) => void error });
const typedWorkerOptions: StartWorkerOptions = { concurrency: 5 };
void strict.startWorker(typedWorkerOptions);
const knownWorkerOptions = { shutdownTimeout: "10s" } as const;
void strict.startWorker({ concurrency: 1, ...knownWorkerOptions });

declare const isDelayed: boolean;
declare const partialDispatchOptions: Partial<DispatchOptions>;
declare const readonlyDispatchOptions: Readonly<DispatchOptions>;
declare const pickedListOptions: Pick<ListRunsOptions, "limit" | "cursor">;
void strict.dispatch("send-email", payload, isDelayed ? { delay: "5m" } : {});
void strict.dispatch("send-email", payload, isDelayed ? { delay: "5m" } : undefined);
void strict.dispatch("send-email", payload, { ...(isDelayed ? { delay: "5m" as const } : {}) });
void strict.dispatch("send-email", payload, maybeDispatchOptions ?? {});
void strict.dispatch("send-email", payload, partialDispatchOptions);
void strict.dispatch("send-email", payload, readonlyDispatchOptions);
void strict.dispatch("send-email", payload, { delay: undefined, runId: undefined });
void strict.listRuns(pickedListOptions);
void strict.listRuns({ ...pickedListOptions, state: isDelayed ? "delayed" : ["completed", "failed"] });
export const listFailedRuns = (options: ListRunsOptionsOf<"send-email" | "purge" | "on-sign-up">) =>
  strict.listRuns({ ...options, state: "failed" });
export const dispatchLater = (options: DispatchOptions) =>
  strict.dispatch("send-email", payload, { ...options, delay: "5m" });
export const startWith = (options?: StartWorkerOptions) => strict.startWorker(options);

// @ts-expect-error
strictDefineTask("", { schema, handler: async () => undefined });
// @ts-expect-error
strictDefineTask("nextasking/sync", { schema, handler: async () => undefined });
// @ts-expect-error
strictDefineTask("t", { schema, handler: async () => undefined, retry: 3 });
const unknownTaskOptions = { retry: 3, timeOut: "5m" };
// @ts-expect-error
strictDefineTask("t", { schema, handler: async () => undefined, ...unknownTaskOptions });
const wholeUnknownTaskOptions = { schema, handler: async () => undefined, retry: 3 };
// @ts-expect-error
strictDefineTask("t", wholeUnknownTaskOptions);
// @ts-expect-error
strictDefineTask("t", { schema });
// @ts-expect-error
strictDefineTask("t", { handler: async () => undefined });
// @ts-expect-error
strictDefineTask("t", { schema, handler: async ({ payload: { missing } }) => void missing });
// @ts-expect-error
strictDefineTask("t", { schema, handler: async () => undefined, retries: "3" });
// @ts-expect-error
strictDefineScheduledTask("s", {
  schedule: { cron: "0 * * * *" },
  handler: async () => undefined,
  ...unknownTaskOptions,
});
// @ts-expect-error
strictDefineEvent("e", { schema, ...unknownTaskOptions });
// @ts-expect-error
strictDefineListener("l", { on: sendEmail, handler: async () => undefined });
// @ts-expect-error
strictDefineListener("l", { on: realSendEmail, handler: async () => undefined });
// @ts-expect-error
strictDefineListener("l", { on: userSignedUp, handler: async () => undefined, ...unknownTaskOptions });
// @ts-expect-error
strictDefineListener("l", { on: "user-signed-up", handler: async () => undefined });

// @ts-expect-error
strictCreateNextasking({ connection, tasks: [sendEmail], events: [sendEmail] });
// @ts-expect-error
strictCreateNextasking({ connection, tasks: [sendEmail], events: [onSignUp] });
// @ts-expect-error
strictCreateNextasking({ connection, tasks: [userSignedUp] });
// @ts-expect-error
strictCreateNextasking({ connection, tasks: [sendEmail], task: [sendEmail] });
const unknownConfig = { prefixx: "app" };
// @ts-expect-error
strictCreateNextasking({ connection, tasks: [sendEmail], ...unknownConfig });
const wholeUnknownConfig = { connection, tasks: [sendEmail] as const, prefixx: "app" };
// @ts-expect-error
strictCreateNextasking(wholeUnknownConfig);
const unknownConnection = { url: "redis://localhost:6379", giveUpAfterr: "5s" };
// @ts-expect-error
strictCreateNextasking({ connection: unknownConnection, tasks: [sendEmail] });
// @ts-expect-error
strictCreateNextasking({ connection: { ...connection, ...{ giveUpAfterr: "5s" } }, tasks: [sendEmail] });
// @ts-expect-error
strictCreateNextasking({ tasks: [sendEmail] });
// @ts-expect-error
strictCreateNextasking({ connection, tasks: [sendEmail], prefix: 123 });

// @ts-expect-error
void strict.listRuns({ states: ["failed"] });
const unknownListOptions = { states: ["failed"], search: "abc" };
// @ts-expect-error
void strict.listRuns({ limit: 20, ...unknownListOptions });
const listOptionsWithOneKnown = { limit: 20, states: ["failed"] };
// @ts-expect-error
void strict.listRuns(listOptionsWithOneKnown);
const listOptionsWithNoneKnown = { states: ["failed"] };
// @ts-expect-error
void strict.listRuns(listOptionsWithNoneKnown);
const widerThanOptions: ListRunsOptions & { states: string[] } = { states: ["failed"] };
// @ts-expect-error
void strict.listRuns(widerThanOptions);
// @ts-expect-error
void strict.listRuns("failed");
// @ts-expect-error
void strict.listRuns(null);
// @ts-expect-error
void strict.listRuns(["failed"]);
// @ts-expect-error
void strict.listRuns({ state: "done" });
// @ts-expect-error
void strict.listRuns({ task: "unknown" });
// @ts-expect-error
void strict.listRuns({ limit: "20" });
// @ts-expect-error
void strict.listRuns<"failed">({ state: "completed" });

// @ts-expect-error
void strict.dispatch("unknown", payload);
// @ts-expect-error
void strict.dispatch("send-email");
// @ts-expect-error
void strict.dispatch("send-email", { userId: 1 });
// @ts-expect-error
void strict.dispatch("purge", payload);
// @ts-expect-error
void strict.dispatch("send-email", payload, "5m");
// @ts-expect-error
void strict.dispatch("send-email", payload, null);
// @ts-expect-error
void strict.dispatch("send-email", payload, { delay: 300 });
// @ts-expect-error
void strict.dispatch("send-email", payload, { delayy: "5m" });
const unknownDispatchOptions = { delayy: "5m" };
// @ts-expect-error
void strict.dispatch("send-email", payload, { runId: "r", ...unknownDispatchOptions });
const dispatchOptionsWithOneKnown = { runID: "r", delay: "5m" as const };
// @ts-expect-error
void strict.dispatch("send-email", payload, dispatchOptionsWithOneKnown);
// @ts-expect-error
void strict.dispatch("send-email", payload, unknownDispatchOptions);
// @ts-expect-error
void strict.dispatch("purge", undefined, { delay: "1h", ...unknownDispatchOptions });

// @ts-expect-error
void strict.emit("user-signed-up", payload, "my-id");
// @ts-expect-error
void strict.emit("user-signed-up", payload, { eventID: "x" });
const unknownEmitOptions = { eventID: "x" };
// @ts-expect-error
void strict.emit("user-signed-up", payload, { eventId: "e", ...unknownEmitOptions });
const emitOptionsWithOneKnown = { eventId: "e", eventID: "x" };
// @ts-expect-error
void strict.emit("user-signed-up", payload, emitOptionsWithOneKnown);
// @ts-expect-error
void strict.emit("nextasking/run.failed", payload);
// @ts-expect-error
void strict.emit("user-signed-up", { userId: 1 });

// @ts-expect-error
void strict.startWorker("x");
// @ts-expect-error
void strict.startWorker(null);
// @ts-expect-error
void strict.startWorker({ concurency: 1 });
const unknownWorkerOptions = { concurency: 1 };
// @ts-expect-error
void strict.startWorker(unknownWorkerOptions);
const workerOptionsWithOneKnown = { concurency: 1, shutdownTimeout: "10s" as const };
// @ts-expect-error
void strict.startWorker(workerOptionsWithOneKnown);
// @ts-expect-error
void strict.startWorker({ shutdownTimeout: "10s", ...unknownWorkerOptions });
// @ts-expect-error
void strict.startWorker({ concurrency: "5" });

export const dispatchWithGenericOptions = <Options extends DispatchOptions>(options: Options) =>
  strict.dispatch("send-email", payload, options);
interface DispatchOptionsWithMore extends DispatchOptions {
  tenant: string;
}
declare const dispatchOptionsWithMore: DispatchOptionsWithMore;
// @ts-expect-error
void strict.dispatch("send-email", payload, dispatchOptionsWithMore);
