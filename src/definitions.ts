import type { Duration } from "./durations.js";
import { InvalidPayloadError } from "./errors.js";
import type { DefinitionKind } from "./errors.js";
import type { RunId } from "./ids.js";
import type { JsonValue } from "./json.js";
import type { Logger } from "./logger.js";
import type { StandardSchemaV1, StandardSchemaV1Input, StandardSchemaV1Output } from "./standard-schema.js";

export type PayloadSchema = StandardSchemaV1<JsonValue, JsonValue>;

export type PayloadInput<Schema extends PayloadSchema> = StandardSchemaV1Input<Schema>;

export type Payload<Schema extends PayloadSchema> = StandardSchemaV1Output<Schema>;

export type TaskContext<TaskPayload> = {
  payload: TaskPayload;
  runId: RunId;
  attempt: number;
  logger: Logger;
};

export type TaskHandler<TaskPayload> = (context: TaskContext<TaskPayload>) => Promise<void>;

export type TaskConcurrency<TaskPayload> = {
  key?: (payload: TaskPayload) => string;
  limit: number;
};

type SharedTaskOptions<TaskPayload> = {
  retries?: number;
  backoff?: Duration;
  timeout?: Duration;
  concurrency?: TaskConcurrency<TaskPayload>;
  handler: TaskHandler<TaskPayload>;
};

export type TaskOptions<Schema extends PayloadSchema> = { schema: Schema } & SharedTaskOptions<Payload<Schema>>;

export type TaskDefinition<Name extends string = string, Schema extends PayloadSchema = PayloadSchema> = Readonly<
  { name: Name; schema: Schema } & SharedTaskOptions<Payload<Schema>>
>;

export type EventOptions<Schema extends PayloadSchema> = {
  schema: Schema;
};

export type EventDefinition<Name extends string = string, Schema extends PayloadSchema = PayloadSchema> = {
  readonly name: Name;
  readonly schema: Schema;
};

export type ListenerOptions<Event extends EventDefinition> = { on: Event } & SharedTaskOptions<
  Payload<Event["schema"]>
>;

export type ListenerDefinition<
  Name extends string = string,
  Event extends EventDefinition = EventDefinition,
> = Readonly<{ name: Name; schema: Event["schema"]; on: Event } & SharedTaskOptions<Payload<Event["schema"]>>>;

export type AnyTaskDefinition = Readonly<
  { name: string; schema: PayloadSchema; on?: EventDefinition } & SharedTaskOptions<never>
>;

const DEFAULT_RETRIES = 5;

const DEFAULT_BACKOFF: Duration = "30s";

export function retriesOf(task: AnyTaskDefinition | undefined): number {
  return task?.retries ?? DEFAULT_RETRIES;
}

export function backoffOf(task: AnyTaskDefinition): Duration {
  return task.backoff ?? DEFAULT_BACKOFF;
}

export async function validatePayload(
  definitionKind: DefinitionKind,
  definition: { readonly name: string; readonly schema: PayloadSchema },
  payload: unknown,
): Promise<unknown> {
  const result = await definition.schema["~standard"].validate(payload);
  if (result.issues !== undefined) {
    throw new InvalidPayloadError(definitionKind, definition.name, result.issues, payload);
  }
  return result.value;
}

export function defineTask<const Name extends string, Schema extends PayloadSchema>(
  name: Name,
  options: TaskOptions<Schema>,
): TaskDefinition<Name, Schema> {
  return { ...options, name };
}

export function defineEvent<const Name extends string, Schema extends PayloadSchema>(
  name: Name,
  options: EventOptions<Schema>,
): EventDefinition<Name, Schema> {
  return { schema: options.schema, name };
}

export function defineListener<const Name extends string, Event extends EventDefinition>(
  name: Name,
  options: ListenerOptions<Event>,
): ListenerDefinition<Name, Event> {
  return { ...options, schema: options.on.schema, name };
}
