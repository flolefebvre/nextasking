// PROTOTYPE: what `@flefebvre/nextasking` exports in variant A (handles). Types only; the runtime throws.
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

export type Dispatch<Task extends TaskDefinition> = <Chosen extends Task>(
  task: Chosen,
  ...dispatchArguments: DispatchArguments<PayloadInputOfTask<Chosen>>
) => Promise<DispatchedRun>;

export type Emit<Event extends EventDefinition> = <Chosen extends Event>(
  event: Chosen,
  payload: PayloadInput<Chosen["schema"]>,
) => Promise<void>;

export type TaskContext<TaskPayload> = {
  payload: TaskPayload;
  step: Step;
  dispatch: Dispatch<TaskDefinition>;
  emit: Emit<EventDefinition>;
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

export function startWorker(nextasking: Nextasking<TaskDefinition, EventDefinition>): Promise<void> {
  throw new Error(`prototype: ${typeof nextasking}`);
}
