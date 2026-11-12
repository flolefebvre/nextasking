import type {
  DispatchedRun,
  DispatchOptions,
  EmitOptions,
  EmittedEvent,
  ListedRun,
  ListRunsOptions,
  RunPage,
  RunState,
  StartWorkerOptions,
  TaskContext,
} from "@flefebvre/nextasking";

import { nextasking } from "./registered-instance.js";
import type { Expect, IsEqual } from "./type-equality.js";

declare const isDraining: boolean;

const welcomeEmail = { email: "ada@example.com" };

const signUp = { userId: "user-1" };

interface DispatchOptionsWithATenant extends DispatchOptions {
  tenant: string;
}

const unknownDispatchOptions = { delayy: "5m" };

const dispatchOptionsWithOneKnown = { runID: "welcome-ada", delay: "5m" as const };

declare const dispatchOptionsWiderThanTheirType: DispatchOptionsWithATenant;

const unknownEmitOptions = { eventID: "signed-up-1" };

const emitOptionsWithOneKnown = { eventId: "signed-up-1", eventID: "signed-up-1" };

export async function sendWithUnknownOptions(): Promise<void> {
  // @ts-expect-error delayy is not an option of dispatch
  await nextasking.dispatch("send-welcome-email", welcomeEmail, { delayy: "5m" });
  // @ts-expect-error delayy is not an option of dispatch
  await nextasking.dispatch("send-welcome-email", welcomeEmail, unknownDispatchOptions);
  // @ts-expect-error runID is not an option of dispatch
  await nextasking.dispatch("send-welcome-email", welcomeEmail, dispatchOptionsWithOneKnown);
  // @ts-expect-error delayy is not an option of dispatch
  await nextasking.dispatch("purge-expired-sessions", undefined, { runId: "purge-now", ...unknownDispatchOptions });
  // @ts-expect-error tenant is not an option of dispatch
  await nextasking.dispatch("send-welcome-email", welcomeEmail, dispatchOptionsWiderThanTheirType);
  // @ts-expect-error a string is not the options
  await nextasking.dispatch("send-welcome-email", welcomeEmail, "5m");
  // @ts-expect-error a list is not the options
  await nextasking.dispatch("send-welcome-email", welcomeEmail, ["5m"]);
  // @ts-expect-error null is not the options
  await nextasking.dispatch("send-welcome-email", welcomeEmail, null);
  // @ts-expect-error a delay is a Duration
  await nextasking.dispatch("send-welcome-email", welcomeEmail, { delay: 300 });
  // @ts-expect-error eventID is not an option of emit
  await nextasking.emit("user.signed-up", signUp, { eventID: "signed-up-1" });
  // @ts-expect-error eventID is not an option of emit
  await nextasking.emit("user.signed-up", signUp, unknownEmitOptions);
  // @ts-expect-error eventID is not an option of emit
  await nextasking.emit("user.signed-up", signUp, emitOptionsWithOneKnown);
  // @ts-expect-error eventID is not an option of emit
  await nextasking.emit("user.signed-up", signUp, { eventId: "signed-up-1", ...unknownEmitOptions });
  // @ts-expect-error a string is not the options
  await nextasking.emit("user.signed-up", signUp, "signed-up-1");
  // @ts-expect-error null is not the options
  await nextasking.emit("user.signed-up", signUp, null);
}

export async function sendFromAHandlerWithUnknownOptions({ dispatch, emit }: TaskContext<unknown>): Promise<void> {
  // @ts-expect-error delayy is not an option of dispatch
  await dispatch("send-welcome-email", welcomeEmail, unknownDispatchOptions);
  // @ts-expect-error delayy is not an option of dispatch
  await dispatch("purge-expired-sessions", undefined, { runId: "purge-now", ...unknownDispatchOptions });
  // @ts-expect-error a list is not the options
  await dispatch("purge-expired-sessions", undefined, ["5m"]);
  // @ts-expect-error eventID is not an option of emit
  await emit("user.signed-up", signUp, emitOptionsWithOneKnown);
  // @ts-expect-error eventID is not an option of emit
  await emit("user.signed-up", signUp, { eventId: "signed-up-1", ...unknownEmitOptions });
}

declare const typedDispatchOptions: DispatchOptions;

declare const partialDispatchOptions: Partial<DispatchOptions>;

declare const readonlyDispatchOptions: Readonly<DispatchOptions>;

declare const typedEmitOptions: EmitOptions;

const knownDispatchOptions = { delay: "5m" } as const;

export async function sendWithKnownOptions(): Promise<void> {
  await nextasking.dispatch("send-welcome-email", welcomeEmail, typedDispatchOptions);
  await nextasking.dispatch("send-welcome-email", welcomeEmail, { runId: "welcome-ada", ...knownDispatchOptions });
  await nextasking.dispatch("send-welcome-email", welcomeEmail, { ...typedDispatchOptions, runId: "welcome-ada" });
  await nextasking.dispatch("send-welcome-email", welcomeEmail, isDraining ? { delay: "5m" } : {});
  await nextasking.dispatch("send-welcome-email", welcomeEmail, isDraining ? { delay: "5m" } : undefined);
  await nextasking.dispatch("send-welcome-email", welcomeEmail, partialDispatchOptions);
  await nextasking.dispatch("send-welcome-email", welcomeEmail, readonlyDispatchOptions);
  await nextasking.dispatch("send-welcome-email", welcomeEmail, { delay: undefined, runId: undefined });
  await nextasking.dispatch("purge-expired-sessions", undefined, { delay: "1h" });
  await nextasking.emit("user.signed-up", signUp, typedEmitOptions);
  await nextasking.emit("user.signed-up", signUp, { ...typedEmitOptions });
  await nextasking.emit("user.signed-up", signUp, { eventId: undefined });
}

export async function sendFromAHandlerWithKnownOptions({ dispatch, emit }: TaskContext<unknown>): Promise<void> {
  const dispatchAScheduledTask: (
    name: "purge-expired-sessions",
    payload?: undefined,
    options?: DispatchOptions,
  ) => Promise<DispatchedRun> = dispatch;
  const emitASignUp: (name: "user.signed-up", payload: typeof signUp, options?: EmitOptions) => Promise<EmittedEvent> =
    emit;
  await dispatchAScheduledTask("purge-expired-sessions", undefined, typedDispatchOptions);
  await emitASignUp("user.signed-up", signUp, typedEmitOptions);
  await dispatch("send-welcome-email", welcomeEmail, { ...typedDispatchOptions, runId: "welcome-ada" });
  await emit("user.signed-up", signUp, isDraining ? { eventId: "signed-up-1" } : {});
}

export const dispatchAsAFunctionOfItsOptions: (
  name: "send-welcome-email",
  payload: typeof welcomeEmail,
  options?: DispatchOptions,
) => Promise<DispatchedRun> = nextasking.dispatch;

export const emitAsAFunctionOfItsOptions: (
  name: "user.signed-up",
  payload: typeof signUp,
  options?: EmitOptions,
) => Promise<EmittedEvent> = nextasking.emit;

export type DispatchAtItsDefaultTakesItsOptions = Expect<
  IsEqual<Parameters<typeof nextasking.dispatch<"purge-expired-sessions">>[2], DispatchOptions | undefined>
>;

export type EmitTakesItsOptions = Expect<IsEqual<Parameters<typeof nextasking.emit>[2], EmitOptions | undefined>>;

export type EmitAtItsDefaultTakesItsOptions = Expect<
  IsEqual<Parameters<typeof nextasking.emit<"user.signed-up">>[2], EmitOptions | undefined>
>;

export type HandlerEmitTakesItsOptions = Expect<
  IsEqual<Parameters<TaskContext<unknown>["emit"]>[2], EmitOptions | undefined>
>;

export type DispatchGivenTheMembersOfANumberTakesItsOptions = Expect<
  IsEqual<
    Parameters<typeof nextasking.dispatch<"purge-expired-sessions", { [Member in keyof number]: unknown }>>[2],
    DispatchOptions | undefined
  >
>;

interface StartWorkerOptionsWithATenant extends StartWorkerOptions {
  tenant: string;
}

const unknownWorkerOptions = { concurency: 1 };

const workerOptionsWithOneKnown = { concurency: 1, shutdownTimeout: "10s" as const };

declare const workerOptionsWiderThanTheirType: StartWorkerOptionsWithATenant;

export async function startWorkerWithUnknownOptions(): Promise<void> {
  // @ts-expect-error concurency is not an option of startWorker
  await nextasking.startWorker({ concurency: 1 });
  // @ts-expect-error concurency is not an option of startWorker
  await nextasking.startWorker(unknownWorkerOptions);
  // @ts-expect-error concurency is not an option of startWorker
  await nextasking.startWorker(workerOptionsWithOneKnown);
  // @ts-expect-error concurency is not an option of startWorker
  await nextasking.startWorker({ shutdownTimeout: "10s", ...unknownWorkerOptions });
  // @ts-expect-error tenant is not an option of startWorker
  await nextasking.startWorker(workerOptionsWiderThanTheirType);
  // @ts-expect-error a string is not the options
  await nextasking.startWorker("10s");
  // @ts-expect-error a list is not the options
  await nextasking.startWorker(["10s"]);
  // @ts-expect-error null is not the options
  await nextasking.startWorker(null);
  // @ts-expect-error concurrency is a number
  await nextasking.startWorker({ concurrency: "5" });
  // @ts-expect-error onWorkerStopped takes no argument
  await nextasking.startWorker({ onWorkerStopped: (reason: string) => void reason });
  // @ts-expect-error onWorkerStoppd is not an option of startWorker
  await nextasking.startWorker({ onWorkerStoppd: async () => undefined });
}

declare const typedWorkerOptions: StartWorkerOptions;

declare const partialWorkerOptions: Partial<StartWorkerOptions>;

declare const readonlyWorkerOptions: Readonly<StartWorkerOptions>;

const knownWorkerOptions = { shutdownTimeout: "10s" } as const;

export async function startWorkerWithKnownOptions(): Promise<void> {
  await nextasking.startWorker(typedWorkerOptions);
  await nextasking.startWorker({ concurrency: 1, ...knownWorkerOptions });
  await nextasking.startWorker({ ...typedWorkerOptions, concurrency: 2 });
  await nextasking.startWorker(isDraining ? { shutdownTimeout: "1m" } : {});
  await nextasking.startWorker(partialWorkerOptions);
  await nextasking.startWorker(readonlyWorkerOptions);
  await nextasking.startWorker({ concurrency: undefined, onWorkerNotStarted: (error) => void error });
  await nextasking.startWorker({ onWorkerStopped: async () => undefined });
  await nextasking.startWorker(undefined);
}

export const startWorkerAsAFunctionOfItsOptions: (options?: StartWorkerOptions) => Promise<void> =
  nextasking.startWorker;

export type StartWorkerAtItsDefaultTakesItsOptions = Expect<
  IsEqual<Parameters<typeof nextasking.startWorker<StartWorkerOptions>>, [options?: StartWorkerOptions]>
>;

export type StartWorkerGivenTheMembersOfAStringTakesItsOptions = Expect<
  IsEqual<
    Parameters<typeof nextasking.startWorker<{ [Member in keyof string]: unknown }>>,
    [options?: StartWorkerOptions]
  >
>;

export type StartWorkerGivenAListTakesItsOptions = Expect<
  IsEqual<Parameters<typeof nextasking.startWorker<string[]>>, [options?: StartWorkerOptions]>
>;

interface ListRunsOptionsWithASearch extends ListRunsOptions {
  search: string;
}

const unknownListOptions = { states: ["failed"], search: "abc" };

const listOptionsWithOneKnown = { limit: 20, states: ["failed"] };

declare const listOptionsWiderThanTheirType: ListRunsOptionsWithASearch;

export async function listRunsWithUnknownOptions(): Promise<void> {
  // @ts-expect-error states is not an option of listRuns
  await nextasking.listRuns({ states: ["failed"] });
  // @ts-expect-error states is not an option of listRuns
  await nextasking.listRuns(unknownListOptions);
  // @ts-expect-error states is not an option of listRuns
  await nextasking.listRuns(listOptionsWithOneKnown);
  // @ts-expect-error states is not an option of listRuns
  await nextasking.listRuns({ limit: 20, ...unknownListOptions });
  // @ts-expect-error search is not an option of listRuns
  await nextasking.listRuns(listOptionsWiderThanTheirType);
  // @ts-expect-error a string is not the options
  await nextasking.listRuns("failed");
  // @ts-expect-error a list is not the options
  await nextasking.listRuns(["failed"]);
  // @ts-expect-error null is not the options
  await nextasking.listRuns(null);
  // @ts-expect-error a limit is a number
  await nextasking.listRuns({ limit: "20" });
  // @ts-expect-error the state named by the type argument is not the state listed
  await nextasking.listRuns<"failed">({ state: "completed" });
}

declare const typedListOptions: ListRunsOptions;

declare const pickedListOptions: Pick<ListRunsOptions, "limit" | "cursor">;

declare const readonlyListOptions: Readonly<ListRunsOptions>;

declare const listedStates: RunState[];

declare const listCursor: string | undefined;

export async function listRunsWithKnownOptions(): Promise<void> {
  await nextasking.listRuns(typedListOptions);
  await nextasking.listRuns(pickedListOptions);
  await nextasking.listRuns(readonlyListOptions);
  await nextasking.listRuns({ state: listedStates, limit: 10 });
  await nextasking.listRuns({ state: "failed", cursor: listCursor });
  await nextasking.listRuns({ ...pickedListOptions, state: isDraining ? ["completed", "failed"] : "pending" });
  await nextasking.listRuns(isDraining ? { state: "failed" } : {});
  await nextasking.listRuns({ state: "failed", limit: undefined });
}

export const listFailedRunsOfTypedOptions = () => nextasking.listRuns({ ...typedListOptions, state: "failed" });

export const listWelcomeEmailRunsOfAVariableCursor = () =>
  nextasking.listRuns({ task: "send-welcome-email", cursor: listCursor });

export const listFailedWelcomeEmailRunsByTypeArguments = () =>
  nextasking.listRuns<"failed", "send-welcome-email">({ state: "failed", task: "send-welcome-email" });

type ListedRunOf<List extends () => Promise<RunPage>> = Awaited<ReturnType<List>>["runs"][number];

export type SpreadOptionsKeepTheirStateType = Expect<
  IsEqual<ListedRunOf<typeof listFailedRunsOfTypedOptions>, Extract<ListedRun, { state: "failed" }>>
>;

export type AVariableCursorKeepsTheTaskType = Expect<
  IsEqual<ListedRunOf<typeof listWelcomeEmailRunsOfAVariableCursor>, Extract<ListedRun, { task: "send-welcome-email" }>>
>;

export type TypeArgumentsKeepTheirType = Expect<
  IsEqual<
    ListedRunOf<typeof listFailedWelcomeEmailRunsByTypeArguments>,
    Extract<ListedRun, { state: "failed"; task: "send-welcome-email" }>
  >
>;

export const listRunsAsAFunctionOfItsOptions: (options?: ListRunsOptions) => Promise<RunPage> = nextasking.listRuns;

export type ListRunsTakesItsOptions = Expect<
  IsEqual<Parameters<typeof nextasking.listRuns>, [options?: ListRunsOptions]>
>;

export type ListRunsGivenTheMembersOfABooleanTakesItsOptions = Expect<
  IsEqual<
    Parameters<
      typeof nextasking.listRuns<
        RunState,
        "send-welcome-email" | "create-default-workspace" | "purge-expired-sessions",
        { [Member in keyof boolean]: unknown }
      >
    >,
    [options?: ListRunsOptions]
  >
>;
