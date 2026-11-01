# Payload schemas and typing

Research for issue #7 (payload schemas and typing). It reports options and trade-offs and does not pick a design.

## Summary

- **Standard Schema** lets one definition give both runtime validation (`~standard.validate`) and static types (`~standard.types.input` and `.output`) for Zod, Valibot, ArkType and about 30 other libraries, with no runtime dependency. The spec covers only the decode direction: it has no `encode` and no introspection. So a library that accepts any Standard Schema cannot turn a `Date` back into a string, cannot inspect a schema to reject `z.date()`, and has to handle a `validate` that may return a Promise. Depending on Zod alone gives `z.encode` codecs and `z.toJSONSchema`, at the cost of a peer dependency on one major version of Zod.
- **JSON round trip**: BullMQ stores `JSON.stringify(data)` and reads back `JSON.parse(...)`, and it has no pluggable serializer. A type constraint such as `Input extends JsonPayload` on the schema's _input_ type rejects `z.date()`, `z.map()`, `z.bigint()`, `z.unknown()` and `z.coerce.date()` at compile time, but still accepts transforms whose _output_ is a `Date`. A design that stores the input and re-validates it in the worker keeps a transformed output type intact, but it runs defaults and transforms twice. Another option is to type the handler with `Jsonify<T>` and keep any payload.
- **Misuse that fails to compile**: typed handles (`dispatch(userSignedUp, payload)`) and a name-keyed registry (`dispatch("user.signed-up", payload)`, filled by module augmentation) both reject a wrong payload and a listener that doesn't match its event. Only the registry also rejects an unknown string name. Only a per-key augmented registry catches the same name registered twice, and only when the schemas differ. With handles, an unknown name can't be written in the first place, but a duplicate name compiles. BullMQ's own generics don't tie a job name to its data type.
- **Cost**: at 100 definitions every option costs 0.3 to 0.5 s of check time, close to the baseline of the Zod schemas alone. At 1,000 definitions, handles took about 2.0 s, a single augmented registry about 5.1 to 5.5 s, and `Jsonify` about 2.8 s. Error messages are short for a wrong payload or name, and long (about 10 lines) for the JSON constraint.

## Sources and how claims were checked

| Source                                                                                                            | Version read                                                                                       |
| ----------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| Standard Schema spec, [`standard-schema/standard-schema`](https://github.com/standard-schema/standard-schema)     | `@standard-schema/spec` 1.1.0 (`packages/spec/src/index.ts`, `schema.md`)                          |
| BullMQ, [`taskforcesh/bullmq`](https://github.com/taskforcesh/bullmq)                                             | 6.5.3 (`src/classes/job.ts`, `src/classes/queue.ts`; also the installed `dist/esm/classes/job.js`) |
| Inngest JS SDK, [`inngest/inngest-js`](https://github.com/inngest/inngest-js)                                     | `inngest` 4.26.2 (`src/components/triggers/triggers.ts`, `src/helpers/jsonify.ts`, `CHANGELOG.md`) |
| Trigger.dev, [`triggerdotdev/trigger.dev`](https://github.com/triggerdotdev/trigger.dev)                          | `@trigger.dev/core` 4.8.3 (`src/v3/types/schemas.ts`, `src/v3/utils/ioSerialization.ts`), `docs/`  |
| Zod docs, [codecs](https://github.com/colinhacks/zod/blob/v4.6.5/packages/docs/content/codecs.mdx), via ctx7      | v4.6.5 docs; zod 4.6.7 installed                                                                   |
| [TypeScript handbook: declaration merging](https://www.typescriptlang.org/docs/handbook/declaration-merging.html) | current                                                                                            |
| [TSConfig `strictFunctionTypes`](https://www.typescriptlang.org/tsconfig/strictFunctionTypes.html)                | current                                                                                            |
| [TanStack Router `Register`](https://github.com/tanstack/router/blob/main/docs/router/guide/creating-a-router.md) | main, via ctx7                                                                                     |
| [Inngest v3 to v4 migration](https://www.inngest.com/docs/reference/typescript/v4/migrations/v3-to-v4)            | current                                                                                            |

**Verified** marks a claim checked in a throwaway project compiled with this repo's TypeScript 6.0.3, using the repo's `strict` and `noUncheckedIndexedAccess` settings, against zod 4.6.7, valibot 1.5.0, arktype 2.2.9, type-fest 5.10.0 and bullmq 6.5.3. **Run** marks a claim checked by executing code on Node 24.16.

## 1. Standard Schema

### What the spec is

One interface under a single `~standard` key: `version: 1`, `vendor`, `validate(value: unknown) => Result | Promise<Result>`, and an optional `types?: { input; output }` that exists only at the type level. A failure is `{ issues: ReadonlyArray<{ message; path? }> }`, and a success is `{ value }` ([spec source](https://github.com/standard-schema/standard-schema/blob/main/packages/spec/src/index.ts)). Version 1.1.0 also defines `StandardJSONSchemaV1`, an optional `~standard.jsonSchema.input()/output()` converter.

Implementers include Zod 3.24+, Valibot 1.0+, ArkType 2.0+, Effect Schema (through an adapter), yup 1.7+, joi 18+ and typia 9.2+. Consumers include tRPC, TanStack Form and Router, Hono and T3 Env ([`schema.md`](https://github.com/standard-schema/standard-schema/blob/main/packages/spec/schema.md)).

### Accepting any Standard Schema vs depending on Zod alone

|                              | Any Standard Schema                                                                                                                                                                                                                                                                                               | Zod alone                                                                                                                         |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| Runtime dependency           | None. Copy the types, or add `@standard-schema/spec` (types only) as a regular dependency, not a dev dependency, "because the interface becomes part of your library's public API" ([FAQ](https://github.com/standard-schema/standard-schema/blob/main/packages/spec/schema.md#can-i-add-it-as-a-dev-dependency)) | Peer dependency on one Zod major version                                                                                          |
| Consumer's choice of library | Any implementer                                                                                                                                                                                                                                                                                                   | Zod only                                                                                                                          |
| Validate and infer types     | Yes                                                                                                                                                                                                                                                                                                               | Yes                                                                                                                               |
| Encode (output back to JSON) | No: the spec only decodes. Bidirectional encoding is an open suggestion ([spec issue #14](https://github.com/standard-schema/standard-schema/issues/14))                                                                                                                                                          | `z.encode` and `z.decode` with `z.codec` ([docs](https://github.com/colinhacks/zod/blob/v4.6.5/packages/docs/content/codecs.mdx)) |
| Inspecting the schema        | None in the spec. The optional `~standard.jsonSchema` is present on Zod and ArkType schemas but not on Valibot's (Run)                                                                                                                                                                                            | `z.toJSONSchema` throws `Date cannot be represented in JSON Schema` (Run)                                                         |
| Error format                 | `issues[].message` and `path` only                                                                                                                                                                                                                                                                                | `ZodError`, `z.prettifyError`                                                                                                     |
| Sync vs async                | `validate` may return a Promise                                                                                                                                                                                                                                                                                   | `parse` vs `parseAsync`, chosen by the caller                                                                                     |

How the prior art handles it:

- **Inngest v4** dropped its central `EventSchemas` (which offered `fromRecord`, `fromZod` and `fromSchema`) for `eventType(name, { schema })` typed by `StandardSchemaV1` ([triggers.ts](https://github.com/inngest/inngest-js/blob/main/packages/inngest/src/components/triggers/triggers.ts), [migration guide](https://www.inngest.com/docs/reference/typescript/v4/migrations/v3-to-v4)). It adds `staticSchema<T>()`, a passthrough `validate`, for typing with no runtime check.
- **Trigger.dev** `schemaTask` predates the spec. It duck-types each library (`parseAsync` for Zod, `_types` for Valibot, `assert` for ArkType, yup, superstruct and others) and keeps one adapter per shape ([schemas.ts](https://github.com/triggerdotdev/trigger.dev/blob/main/packages/core/src/v3/types/schemas.ts)). Standard Schema replaces that adapter table.

### Inference of input and output

`StandardSchemaV1.InferInput<S>` and `InferOutput<S>` read `NonNullable<S["~standard"]["types"]>`. A schema with no `types` infers as `unknown`.

```ts
const zodSchema = z.object({
  at: z.iso.datetime().transform((s) => new Date(s)),
  count: z.number().default(1),
});
type In = StandardSchemaV1.InferInput<typeof zodSchema>; // { at: string; count?: number | undefined }
type Out = StandardSchemaV1.InferOutput<typeof zodSchema>; // { at: Date; count: number }
```

Verified for all three libraries: the equivalent Valibot schema gives the same pair, and ArkType's `{ at: "string.date.iso.parse", "count?": "number" }` gives `{ at: string; count?: number }` in and `{ at: Date; count?: number }` out. ArkType writes `count?: number` without `| undefined`. All three are assignable to `S extends StandardSchemaV1`.

So a dispatch site can take the **input** type and a handler can receive the **output** type, as long as the worker re-runs `validate` (see section 2).

Keeping the per-schema types needs one generic parameter on the defining function, either `<Schema extends StandardSchemaV1>` or `<Input, Output>(schema: StandardSchemaV1<Input, Output>)`. A non-generic `StandardSchemaV1` parameter erases the payload type.

### Async validation

- The spec allows `validate` to return a Promise. Zod's `.refine(async ...)` and Valibot's `pipeAsync` do return one, while a plain object schema returns synchronously (Run).
- The spec advises implementers to "avoid returning `Promise` ... unless absolutely necessary". For consumers that need sync, its FAQ gives the pattern of throwing when `result instanceof Promise` ([FAQ](https://github.com/standard-schema/standard-schema/blob/main/packages/spec/schema.md#how-to-only-allow-synchronous-validation)).
- Enqueueing is already async (a Redis round trip), so awaiting `validate` at dispatch and in the worker adds no new API shape. Inngest splits `create()` from `validate()` "because validation may be async" ([triggers.ts](https://github.com/inngest/inngest-js/blob/main/packages/inngest/src/components/triggers/triggers.ts)).
- Async refinements, such as a database lookup, run on every attempt and after every deploy, in the worker as well as the dispatcher, if both sides validate.

## 2. The JSON round trip

### What BullMQ does

- `Job.asJSON()` stores `data: JSON.stringify(typeof this.data === 'undefined' ? {} : this.data)`, and `Job.fromJSON` reads `JSON.parse(json.data || '{}')`. Return values are stringified the same way ([job.ts](https://github.com/taskforcesh/bullmq/blob/master/src/classes/job.ts), 6.5.3, the same in the installed dist).
- The 6.5.3 source has no serializer option.
- `Queue<DataType, ResultType, NameType>` types the data per queue. There is no runtime check.

What comes back (Run, Node 24.16):

| Sent                         | Received                                       |
| ---------------------------- | ---------------------------------------------- |
| `Date`                       | ISO string                                     |
| `Map`, `Set`                 | `{}`                                           |
| `undefined` property         | key dropped                                    |
| `undefined` in an array      | `null`                                         |
| `NaN`, `Infinity`            | `null`                                         |
| `bigint`                     | throws `Do not know how to serialize a BigInt` |
| cyclic object                | throws `Converting circular structure to JSON` |
| class instance               | plain object; methods and prototype lost       |
| `undefined` as whole payload | `{}` (BullMQ substitution)                     |

Prior art:

- **Trigger.dev** serializes payloads with superjson (`application/super+json`), so a `Date` survives, and its docs tell users to pass Dates directly ([ioSerialization.ts](https://github.com/triggerdotdev/trigger.dev/blob/main/packages/core/src/v3/utils/ioSerialization.ts), [triggering.mdx](https://github.com/triggerdotdev/trigger.dev/blob/main/docs/triggering.mdx)).
- **Inngest** types step results with a fork of type-fest's `Jsonify` "to represent to users how a value will be serialized and deserialized" ([jsonify.ts](https://github.com/inngest/inngest-js/blob/main/packages/inngest/src/helpers/jsonify.ts)). Its `eventType` rejects schemas whose input and output differ (`AssertNoTransform`, "Transforms not supported: schema input/output types must match").

### Option A: constrain the schema's input to JSON

```ts
type JsonPayload = string | number | boolean | null | JsonPayload[] | { [key: string]: JsonPayload | undefined };

function defineTask<Input extends JsonPayload, Output>(name: string, schema: StandardSchemaV1<Input, Output>) {
  /* ... */
}

defineTask("ok", z.object({ id: z.string(), note: z.string().optional() })); // compiles
defineTask("ok", z.object({ at: z.iso.datetime().transform((s) => new Date(s)) })); // compiles: input is string
defineTask("ok", z.object({ extra: z.json() })); // compiles: free-form JSON
defineTask("bad", z.object({ at: z.date() })); // error
```

Verified:

- Rejected: `z.date()`, `z.map()`, `z.bigint()`, `z.coerce.date()` (its input is `unknown`) and `z.unknown()`.
- A value typed by an `interface`, or a class instance, is rejected by `extends JsonPayload` and by type-fest's `JsonValue` ("index signature is missing"). Inngest documents the same limitation for `staticSchema`: it "expects a type, not an interface". Schema-inferred types are object literal types, so this bites only hand-written payload types.
- type-fest 5's `JsonValue` accepts optional properties.
- The error for `z.date()` is about 10 lines and ends in `Type 'Date' is not assignable to type 'JsonPayload | undefined'` (section 4 shows it).
- Inngest's `StaticTypeError<"...">` trick, a conditional type that resolves to a message string, gives a one-line message, at the price of a conditional generic.

### Option B: also constrain the output

The same idea applied to `Output extends JsonPayload` forbids transforms that yield non-JSON values. This is Inngest's position, in its stronger form `[Input] extends [Output]`. Whatever the worker receives is then exactly what was sent.

### Where validation runs, and which type the handler sees

| Flow                                                                 | Handler type      | Consequence                                                                                                                                                                                                                                                                                                |
| -------------------------------------------------------------------- | ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Validate at dispatch, store the **input**, re-validate in the worker | output            | Transforms like string to `Date` work. Defaults and transforms run twice, and a non-pure default differs between the two runs: `z.uuid().default(() => crypto.randomUUID())` gave two different ids (Run). The worker validates with the schema of the deployed release (payload versioning, see the map). |
| Validate at dispatch, store the **output**                           | `Jsonify<output>` | Needs the output to be JSON-safe (Option B), or the handler type must reflect the loss. Re-validating the output in the worker is not always possible: a transformed output (`Date`) failed `safeParse` against the same schema (Run), while its JSON form (an ISO string) happened to pass.               |
| Zod-only: store `z.encode(schema, output)`, decode in the worker     | output            | A real round trip for codecs (`Date` to ISO string to `Date`, Run). Not available through Standard Schema.                                                                                                                                                                                                 |
| No schema constraint; type the handler as `Jsonify<T>`               | `Jsonify<T>`      | Any payload compiles. The handler sees `{ at: string; tags: {}; byId: {} }` for `{ at: Date; tags: Set; byId: Map }` (Verified with type-fest `Jsonify`). This is the costliest type in section 5.                                                                                                         |
| Change the serializer (superjson)                                    | output            | BullMQ has no hook, so the library would wrap `data` itself (Trigger.dev's approach). Payloads in Redis would no longer be plain JSON for other BullMQ tools.                                                                                                                                              |

## 3. Making misuse fail to compile

The cases are an unknown name, a wrong payload, and a listener whose payload type doesn't match its event.

### BullMQ alone

`Queue<UserPayload | OrderPayload, void, "user.signed-up" | "order.paid">` accepts `queue.add("user.signed-up", { orderId: "o1" })` (Verified). Name and data are independent type parameters, so the correlation has to come from nextasking's layer.

### Option 1: typed handles

```ts
const userSignedUp = defineEvent("user.signed-up", z.object({ userId: z.string(), plan: z.enum(["free", "pro"]) }));

dispatch(userSignedUp, { userId: "u1", plan: "pro" });
listen(userSignedUp, async (payload) => {
  /* payload: { userId: string; plan: "free" | "pro" } */
});
```

Signatures: `defineEvent<const Name extends string, Schema extends StandardSchemaV1>(name, schema)` and `dispatch<Schema>(event: EventDefinition<string, Schema>, payload: StandardSchemaV1.InferInput<Schema>)`.

Verified:

- A wrong payload, the payload of another event, and a listener typed for another event's payload all fail.
- An unknown name can't be written, since `dispatch` takes the handle.
- Two handles with the same name string compile.
- Dispatching by a string from a webhook needs a separate runtime lookup, which the map lists under "Not yet specified".
- Every dispatch site imports the definition, and with it the schema library.

This is Inngest v4's shape (`eventType()` handles shared by triggers, `send` and `waitForEvent`).

### Option 2: name-keyed registry by module augmentation

The library exports an empty interface. The app fills it once, as TanStack Router does with `interface Register { router: typeof router }` ([docs](https://github.com/tanstack/router/blob/main/docs/router/guide/creating-a-router.md)).

```ts
// library
export interface Register {}
type SchemaByEventName = Register extends {
  schemaByEventName: infer Registered extends Record<string, StandardSchemaV1>;
}
  ? Registered
  : {};
export type EventName = keyof SchemaByEventName & string;
export function dispatch<Name extends EventName>(
  name: Name,
  payload: StandardSchemaV1.InferInput<SchemaByEventName[Name]>,
): void;

// app
export const schemaByEventName = {
  "user.signed-up": z.object({ userId: z.string(), plan: z.enum(["free", "pro"]) }),
};
declare module "@flefebvre/nextasking" {
  interface Register {
    schemaByEventName: typeof schemaByEventName;
  }
}

dispatch("user.signed-up", { userId: "u1", plan: "pro" });
dispatch("user.signedup", { userId: "u1", plan: "pro" }); // error: not assignable to 'EventName'
```

Verified:

- An unknown name, a wrong payload and a mismatched listener all fail.
- With nothing registered, the `{}` fallback makes every name fail (`... not assignable to parameter of type 'never'`). Falling back to `never` instead would make `keyof` resolve to `string | number | symbol`.

A **per-key** variant lets each definition file add its own key (`interface SchemaByEventName { "invoice.sent": typeof invoiceSentSchema }`). Registering the same key twice with a different schema type is then a compile error (Verified). The handbook says non-function members "must be of the same type" ([declaration merging](https://www.typescriptlang.org/docs/handbook/declaration-merging.html)).

Limits:

- An augmentation can only patch existing declarations: it can add no new top-level declarations and can't augment default exports (handbook).
- It applies to every file in the TypeScript program, imported or not. So the types can say a name exists while the running process never imported the module that registers it at runtime, and a runtime "unknown name" check is still needed.
- One global registry per program: two registries in one app (tests, a second app in a monorepo package) share it.

### Option 3: registry carried by an instance, no augmentation

`const app = createX({ events: { "user.signed-up": schema } })`, then `app.dispatch("user.signed-up", ...)`. The type lookup is the same as in Option 2, but the types come from the instance's generic. There is no global augmentation, and callers import `app`. A worker module that both imports `app` and is imported by it risks an import cycle. This option was not measured separately; its lookup has the same shape as Option 2.

### Listener mismatch: method syntax is bivariant

Under `strictFunctionTypes`, parameter checks are contravariant only for function-typed members, "not to those in method syntax" ([TSConfig](https://www.typescriptlang.org/tsconfig/strictFunctionTypes.html)).

```ts
interface MethodListener<P> {
  handle(payload: P): Promise<void>;
} // a narrower listener is accepted: unsound
interface PropertyListener<P> {
  handle: (payload: P) => Promise<void>;
} // rejected: sound
```

Verified: a `MethodListener<{ userId; plan }>` assigns to `MethodListener<{ userId }>`, and the property form fails. Any listener or handler type in the public API needs the property form for the "doesn't compile" guarantee.

## 4. The TypeScript cost

Error messages captured from `tsc --pretty false` (Verified):

```
wrong payload (handle or registry):
  Type '"gold"' is not assignable to type '"free" | "pro"'.
unknown name (registry):
  Argument of type '"user.signedup"' is not assignable to parameter of type 'EventName'.
listener for the wrong event (handle):
  Argument of type '(payload: { orderId: string; }) => Promise<undefined>' is not assignable to parameter of type
  '(payload: { userId: string; plan: "free" | "pro"; }) => Promise<void>'. ... Property 'orderId' is missing ...
non-JSON schema (Option A): 10 lines, starting from
  Argument of type 'ZodObject<{ at: ZodDate; ... }>' is not assignable to parameter of type 'StandardSchemaV1<JsonPayload, ...>'
  ... Type 'Date' is not assignable to type 'JsonPayload | undefined'. Index signature for type 'string' is missing in type 'Date'.
```

The wrong-payload error points at the offending property. The unknown-name error names the alias `EventName` rather than listing the members, so the list shows on hover.

Check time was measured with `tsc --extendedDiagnostics` on generated projects. Each definition is a Zod object with 6 fields (enum, optional, array, nested object, a string-to-`Date` transform), plus one dispatch and one listener per definition. The 100-definition times are the median of three runs. At 1,000, the handles and registry rows are the median of three runs, and the other rows are a single run. Instantiation counts are deterministic.

| Style                                            | 100 defs: instantiations | 100 defs: check time | 1,000 defs: instantiations | 1,000 defs: check time |
| ------------------------------------------------ | ------------------------ | -------------------- | -------------------------- | ---------------------- |
| Baseline (schemas and `z.input`/`z.output` only) | 69,666                   | 0.28 s               | 688,866                    | 1.46 s                 |
| Typed handles                                    | 77,479                   | 0.33 s               | 761,479                    | 2.04 s                 |
| Typed handles plus `Input extends JsonPayload`   | 82,890                   | 0.34 s               | 814,590                    | 2.24 s                 |
| Handles with `Jsonify<T>` handler type           | 170,774                  | 0.53 s               | 1,682,774                  | 2.83 s                 |
| Single augmented registry (Option 2)             | 89,506                   | 0.38 s               | 885,106                    | 5.46 s                 |

Observations:

- At the target of about 100 definitions, every style adds 0.05 to 0.25 s over the schemas themselves.
- The registry scales worst on check time at 1,000 (about 2.7 times the handles' time with only 16% more instantiations). The likely cause is resolving `Name` against a 1,000-member union, plus one large object type, at every call. This is an inference, not profiled.
- `Jsonify` roughly doubles instantiations, because it recurses over every payload.
- Editor latency (tsserver) was not measured. The `tsc` figures stand in for it, and tsserver re-checks per open file, not per project.

Inference depth:

- Handles infer from one argument, one level deep: `Schema`, then `InferInput`.
- The registry adds a conditional (`Register extends { ... } ? ... : {}`) and an indexed access per call.
- None of the shapes above hit an instantiation-depth error.

## 5. Open questions for the map

- Validate at dispatch, in the worker, or both? And store the input or the output? This decides whether transforms are allowed (section 2).
- Is a `Date` in a payload a supported case (codec or superjson) or a compile error (JSON constraint)?
- Do handles and a string registry coexist, since "dispatch by string name" is already on the map? If so, which one is the source of truth for duplicate-name detection?
- Should listener and handler types in the public API be function-typed properties, so that method bivariance doesn't reopen the mismatch hole?
