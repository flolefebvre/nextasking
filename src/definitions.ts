import type { RunId } from "./ids.js";
import type { StandardSchemaV1, StandardSchemaV1Input, StandardSchemaV1Output } from "./standard-schema.js";

export type PayloadSchema = StandardSchemaV1;

export type PayloadInput<Schema extends PayloadSchema> = StandardSchemaV1Input<Schema>;

export type Payload<Schema extends PayloadSchema> = StandardSchemaV1Output<Schema>;

export type TaskContext<TaskPayload> = {
  payload: TaskPayload;
  runId: RunId;
  attempt: number;
};

export type TaskHandler<TaskPayload> = (context: TaskContext<TaskPayload>) => Promise<void>;

export type TaskOptions<Schema extends PayloadSchema> = {
  schema: Schema;
  handler: TaskHandler<Payload<Schema>>;
};

export type TaskDefinition<Name extends string = string, Schema extends PayloadSchema = PayloadSchema> = {
  readonly name: Name;
  readonly schema: Schema;
  readonly handler: TaskHandler<Payload<Schema>>;
};

export type AnyTaskDefinition = {
  readonly name: string;
  readonly schema: PayloadSchema;
  readonly handler: (context: never) => Promise<void>;
};

export function defineTask<const Name extends string, Schema extends PayloadSchema>(
  name: Name,
  options: TaskOptions<Schema>,
): TaskDefinition<Name, Schema> {
  return { name, schema: options.schema, handler: options.handler };
}
