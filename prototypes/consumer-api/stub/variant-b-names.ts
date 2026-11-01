// PROTOTYPE: what `@flefebvre/nextasking` exports in variant B (names). Types only; the runtime throws.
import type {
  ConnectionOptions,
  DispatchArguments,
  DispatchedRun,
  Duration,
  EventDefinition,
  Logger,
  Payload,
  PayloadInput,
  PayloadInputOfTask,
  PayloadSchema,
  Schedule,
  Step,
  TaskDefinition,
} from "./schema";

export { defineEvent } from "./schema";
export type { EventDefinition, TaskDefinition, Logger } from "./schema";

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

export type TaskContext<TaskPayload> = {
  payload: TaskPayload;
  step: Step;
  dispatch: RegisteredDispatch;
  emit: RegisteredEmit;
  logger: Logger;
  run: { id: string; attempt: number };
  signal: AbortSignal;
};

type TaskOptions<TaskPayload> = {
  retries?: number;
  timeout?: Duration;
  concurrency?: { key: (payload: TaskPayload) => string; limit: number };
  handler: (context: TaskContext<TaskPayload>) => Promise<void>;
};

export function defineTask<const Name extends string, Schema extends PayloadSchema>(
  name: Name,
  options: TaskOptions<Payload<Schema>> & { schema: Schema },
): TaskDefinition<Name, PayloadInput<Schema>>;
export function defineTask<const Name extends string, Schema extends PayloadSchema>(
  name: Name,
  options: TaskOptions<Payload<Schema>> & { on: EventDefinition<string, Schema> },
): TaskDefinition<Name, PayloadInput<Schema>>;
export function defineTask<const Name extends string>(
  name: Name,
  options: TaskOptions<undefined> & { schedule: Schedule },
): TaskDefinition<Name, undefined>;
export function defineTask(): never {
  throw new Error("prototype");
}

export type Nextasking<Task extends TaskDefinition, Event extends EventDefinition> = {
  dispatch: Dispatch<Task>;
  emit: Emit<Event>;
};

export function createNextasking<
  const Tasks extends readonly TaskDefinition[],
  const Events extends readonly EventDefinition[],
>(config: {
  connection: ConnectionOptions;
  events: Events;
  tasks: Tasks;
  logger?: Logger;
}): Nextasking<Tasks[number], Events[number]> {
  throw new Error(`prototype: ${config.tasks.length} tasks`);
}

export function startWorker(nextasking: RegisteredNextasking): Promise<void> {
  throw new Error(`prototype: ${typeof nextasking}`);
}
