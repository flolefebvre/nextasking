// PROTOTYPE: what `@flefebvre/nextasking` would export. Types only; the runtime throws.

type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue | undefined };
type JsonObject = { [key: string]: JsonValue | undefined };

interface StandardSchemaV1<Input = unknown, Output = Input> {
  readonly "~standard": {
    readonly version: 1;
    readonly vendor: string;
    readonly validate: (value: unknown) => unknown;
    readonly types?: { readonly input: Input; readonly output: Output } | undefined;
  };
}

type PayloadSchema = StandardSchemaV1<JsonObject, JsonObject>;
type PayloadInput<Schema extends PayloadSchema> = NonNullable<Schema["~standard"]["types"]>["input"];
type Payload<Schema extends PayloadSchema> = NonNullable<Schema["~standard"]["types"]>["output"];

type DurationUnit = "s" | "m" | "h" | "d";
type Digit = "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9";
export type Duration = `${bigint}${DurationUnit}` & `${Digit}${string}`;

export type Logger = {
  info: (message: string, details?: Record<string, unknown>) => void;
  error: (message: string, details?: Record<string, unknown>) => void;
};

export type Step = {
  run: <Result extends JsonValue | void>(id: string, work: () => Promise<Result>) => Promise<Result>;
  sleep: (id: string, duration: Duration) => Promise<void>;
};

export type Schedule = { cron: string; timezone?: string };

export type DispatchOptions = { runId?: string; delay?: Duration };
export type DispatchedRun = { runId: string };

export type EventDefinition<Name extends string = string, Schema extends PayloadSchema = PayloadSchema> = {
  readonly kind: "event";
  readonly name: Name;
  readonly schema: Schema;
};

export type TaskDefinition<Name extends string = string, Input = unknown> = {
  readonly kind: "task";
  readonly name: Name;
  readonly "~payloadInput"?: Input;
};

type PayloadInputOfTask<Task> = Task extends TaskDefinition<string, infer Input> ? Input : never;

type DispatchArguments<Input> = [Input] extends [undefined]
  ? [options?: DispatchOptions]
  : [payload: Input, options?: DispatchOptions];

export type Dispatch<Task extends TaskDefinition> = <Name extends Task["name"]>(
  name: Name,
  ...dispatchArguments: DispatchArguments<PayloadInputOfTask<Extract<Task, { name: Name }>>>
) => Promise<DispatchedRun>;

export type Emit<Event extends EventDefinition> = <Name extends Event["name"]>(
  name: Name,
  payload: PayloadInput<Extract<Event, { name: Name }>["schema"]>,
) => Promise<void>;

export interface Register {}

type RegisteredNextasking = Register extends { nextasking: infer Registered } ? Registered : never;
type RegisteredDispatch = RegisteredNextasking extends { dispatch: infer Registered } ? Registered : never;
type RegisteredEmit = RegisteredNextasking extends { emit: infer Registered } ? Registered : never;

export type RunContext = {
  step: Step;
  dispatch: RegisteredDispatch;
  emit: RegisteredEmit;
  logger: Logger;
  runId: string;
  attempt: number;
  abortSignal: AbortSignal;
};

export type TaskContext<TaskPayload> = RunContext & { payload: TaskPayload };

type RunOptions = {
  retries?: number;
  timeout?: Duration;
};

export function defineEvent<const Name extends string, Schema extends PayloadSchema>(
  name: Name,
  options: { schema: Schema },
): EventDefinition<Name, Schema> {
  return { kind: "event", name, schema: options.schema };
}

export function defineTask<const Name extends string, Schema extends PayloadSchema>(
  name: Name,
  options: RunOptions & {
    schema: Schema;
    concurrency?: { key: (payload: Payload<Schema>) => string; limit: number };
    handler: (context: TaskContext<Payload<Schema>>) => Promise<void>;
  },
): TaskDefinition<Name, PayloadInput<Schema>> {
  throw new Error(`prototype: ${name} ${typeof options}`);
}

export function defineListener<const Name extends string, Schema extends PayloadSchema>(
  name: Name,
  options: RunOptions & {
    on: EventDefinition<string, Schema>;
    concurrency?: { key: (payload: Payload<Schema>) => string; limit: number };
    handler: (context: TaskContext<Payload<Schema>>) => Promise<void>;
  },
): TaskDefinition<Name, PayloadInput<Schema>> {
  throw new Error(`prototype: ${name} ${typeof options}`);
}

export function defineScheduledTask<const Name extends string>(
  name: Name,
  options: RunOptions & {
    schedule: Schedule;
    handler: (context: RunContext) => Promise<void>;
  },
): TaskDefinition<Name, undefined> {
  throw new Error(`prototype: ${name} ${typeof options}`);
}

export type Nextasking<Task extends TaskDefinition, Event extends EventDefinition> = {
  dispatch: Dispatch<Task>;
  emit: Emit<Event>;
};

export function createNextasking<
  const Tasks extends readonly TaskDefinition[],
  const Events extends readonly EventDefinition[],
>(config: {
  connection: { url: string };
  events: Events;
  tasks: Tasks;
  logger?: Logger;
}): Nextasking<Tasks[number], Events[number]> {
  throw new Error(`prototype: ${config.tasks.length} tasks`);
}

export function startWorker(nextasking: RegisteredNextasking): Promise<void> {
  throw new Error(`prototype: ${typeof nextasking}`);
}
