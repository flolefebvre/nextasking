// PROTOTYPE: type-level stand-in for the library's payload schema types, shared by both variants.

export type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue | undefined };
export type JsonObject = { [key: string]: JsonValue | undefined };

export interface StandardSchemaV1<Input = unknown, Output = Input> {
  readonly "~standard": {
    readonly version: 1;
    readonly vendor: string;
    readonly validate: (value: unknown) => unknown;
    readonly types?: { readonly input: Input; readonly output: Output } | undefined;
  };
}

export type PayloadSchema = StandardSchemaV1<JsonObject, JsonObject>;
export type PayloadInput<Schema extends PayloadSchema> = NonNullable<Schema["~standard"]["types"]>["input"];
export type Payload<Schema extends PayloadSchema> = NonNullable<Schema["~standard"]["types"]>["output"];

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

export type DispatchOptions = { id?: string; delay?: Duration };
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

export type PayloadInputOfTask<Task> = Task extends TaskDefinition<string, infer Input> ? Input : never;

export type DispatchArguments<Input> = [Input] extends [undefined]
  ? [options?: DispatchOptions]
  : [payload: Input, options?: DispatchOptions];

export type ConnectionOptions = { url: string };

export function defineEvent<const Name extends string, Schema extends PayloadSchema>(
  name: Name,
  schema: Schema,
): EventDefinition<Name, Schema> {
  return { kind: "event", name, schema };
}
