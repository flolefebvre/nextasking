import type { Duration } from "./durations.js";
import { InvalidPayloadError } from "./errors.js";
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

export type TaskOptions<Schema extends PayloadSchema> = {
  schema: Schema;
  retries?: number;
  backoff?: Duration;
  timeout?: Duration;
  concurrency?: TaskConcurrency<Payload<Schema>>;
  handler: TaskHandler<Payload<Schema>>;
};

export type TaskDefinition<Name extends string = string, Schema extends PayloadSchema = PayloadSchema> = {
  readonly name: Name;
  readonly schema: Schema;
  readonly retries?: number;
  readonly backoff?: Duration;
  readonly timeout?: Duration;
  readonly concurrency?: TaskConcurrency<Payload<Schema>>;
  readonly handler: TaskHandler<Payload<Schema>>;
};

export type AnyTaskDefinition = {
  readonly name: string;
  readonly schema: PayloadSchema;
  readonly retries?: number;
  readonly backoff?: Duration;
  readonly timeout?: Duration;
  readonly concurrency?: TaskConcurrency<never>;
  readonly handler: (context: never) => Promise<void>;
};

export async function validatePayload(task: AnyTaskDefinition, payload: unknown): Promise<unknown> {
  const result = await task.schema["~standard"].validate(payload);
  if (result.issues !== undefined) {
    throw new InvalidPayloadError(task.name, result.issues, payload);
  }
  return result.value;
}

export function defineTask<const Name extends string, Schema extends PayloadSchema>(
  name: Name,
  options: TaskOptions<Schema>,
): TaskDefinition<Name, Schema> {
  return { ...options, name };
}
