# Tasks and events

A task is a named piece of work with a payload schema and a handler. An event is a named fact with a payload schema; a listener is a task subscribed to one event. Define them, list them in `createNextasking`, dispatch a task or emit an event by name, and start a worker to run them.

## Define a task

Put each task in its own file under `src/nextasking/tasks/`. Name tasks in kebab-case.

```ts
// src/nextasking/tasks/send-welcome-email.ts
import { defineTask } from "@flefebvre/nextasking";
import { z } from "zod";

export const sendWelcomeEmail = defineTask("send-welcome-email", {
  schema: z.object({ userId: z.string(), email: z.email() }),
  handler: async ({ payload }) => {
    await email.send({ to: payload.email, template: "welcome" });
  },
});
```

- `schema` takes any [Standard Schema](https://standardschema.dev) library: Zod, Valibot, ArkType and others.
- The handler receives one object:
  - `payload`: the schema's output, already validated
  - `runId`: the id `dispatch` returned for this run
  - `attempt`: the attempt number, starting at 1
  - `logger`: a logger that tags every line with the run; see [Logging](logging.md)
- The schema's input and output must be JSON: `z.date()`, or a transform into a `Date`, doesn't compile. Pass a date as an ISO string.
- Keep the payload small: ids and short values, not whole records. Load the rest in the handler.
- A misspelt option doesn't compile, and neither does a handler typed for another payload.

### Durations

An option that takes a time takes a duration: a whole number followed by `s`, `m`, `h` or `d`.

```ts
"30s"; // 30 seconds
"5m"; // 5 minutes
"2h"; // 2 hours
"3d"; // 3 days
```

- `"5 s"`, `"0.5h"` and `"2w"` don't compile; write `"30m"` for half an hour.
- A bad value that slips past the types, through a cast or plain JavaScript, makes `createNextasking` throw.
- Import the `Duration` type to type your own duration values.

## Define an event

Put each event in its own file under `src/nextasking/events/`. Name events dotted: `user.signed-up`, `invoice.paid`.

```ts
// src/nextasking/events/invoice-paid.ts
import { defineEvent } from "@flefebvre/nextasking";
import { z } from "zod";

export const invoicePaid = defineEvent("invoice.paid", {
  schema: z.object({ invoiceId: z.string(), amountInCents: z.int().positive() }),
});
```

- An event has a name and a schema, and nothing else: the work goes in its listeners.
- The schema follows the same rules as a task's: Standard Schema, JSON input and output, a small payload.

## Define a listener

A listener is a task subscribed to one event. Put it under `src/nextasking/tasks/` with the other tasks, and pass the event definition as `on`.

```ts
// src/nextasking/tasks/record-invoice-payment.ts
import { defineListener } from "@flefebvre/nextasking";
import { invoicePaid } from "@/nextasking/events/invoice-paid";

export const recordInvoicePayment = defineListener("record-invoice-payment", {
  on: invoicePaid,
  retries: 3,
  handler: async ({ payload, runId }) => {
    await ledger.record({ invoiceId: payload.invoiceId, amountInCents: payload.amountInCents, runId });
  },
});
```

- The payload is the event's schema output: a listener has no `schema` of its own.
- It takes every task option (`retries`, `backoff`, `timeout`, `concurrency`) and the same handler context.
- `on` takes the event definition, not its name.
- Give each reaction its own listener: one event can have any number of them, and each gets its own run.

## Register the tasks and events

Create the instance once, in `src/nextasking/index.ts`, and augment `Register` with its type. List events in `events` and every task, listeners included, in `tasks`.

```ts
// src/nextasking/index.ts
import { createNextasking } from "@flefebvre/nextasking";
import { invoicePaid } from "@/nextasking/events/invoice-paid";
import { recordInvoicePayment } from "@/nextasking/tasks/record-invoice-payment";
import { sendWelcomeEmail } from "@/nextasking/tasks/send-welcome-email";

export const nextasking = createNextasking({
  connection: { url: process.env.REDIS_URL ?? "redis://localhost:6379" },
  events: [invoicePaid],
  tasks: [sendWelcomeEmail, recordInvoicePayment],
});

declare module "@flefebvre/nextasking" {
  interface Register {
    nextasking: typeof nextasking;
  }
}
```

- `events` is optional: an app with tasks alone leaves it out.
- `createNextasking` opens no connection. Importing the instance is safe anywhere, `next build` included; the first `dispatch` or `emit` connects.
- `prefix` (default `"nextasking"`) puts every key of the instance under that prefix. Give two apps sharing one server different prefixes.
- `logger` takes your app's logger; without it the library is silent. See [Logging](logging.md).
- Without the `Register` augmentation, `TaskName` and `EventName` are `never`: add it once, next to the instance.

### Startup checks

`createNextasking` checks the definitions before anything else, with no connection, and throws an `Error` whose one-line message names the definition and what is wrong:

- two definitions with the same name: tasks and events share one namespace
- a listener whose `on` event isn't listed in `events`
- an event name starting with `nextasking/`, which is reserved for the library's built-in events
- a `concurrency.limit` that isn't a positive integer
- a duration option that isn't a valid duration, such as `"-5s"` or `"0.5h"` passed through a cast or plain JavaScript

```text
Duplicate name "invoice.paid": every task and event needs its own name
Listener "record-invoice-payment" listens to event "invoice.paid", which is not listed in events
Event "nextasking/run.failed" has a reserved name: names starting with "nextasking/" belong to the built-in events
Task "rebuild-index" has an invalid timeout: "0.5h" is not a duration: a whole number followed by s, m, h or d, such as "30s"
```

The error is a startup crash: fix the definition, don't catch it.

## Dispatch a task

```ts
// src/app/sign-up/actions.ts
"use server";
import { nextasking } from "@/nextasking";

export async function signUp(formData: FormData) {
  const user = await createUser(formData);
  await nextasking.dispatch("send-welcome-email", { userId: user.id, email: user.email });
}
```

- `dispatch(name, payload)` starts one run and resolves with `{ runId }`, a random UUID.
- The name is checked at compile time: a name the instance doesn't list doesn't compile, and the error lists the valid names.
- The payload is typed from the schema's input: a wrong payload doesn't compile.
- At runtime, `dispatch` validates the payload and stores the schema's output; the handler receives that output.
- A listener is a task: `dispatch("record-invoice-payment", payload)` takes its event's payload and starts one run of that listener alone. That run's log lines have no `eventName` or `eventId`.

## Emit an event

```ts
// src/app/api/billing/webhook/route.ts
import { nextasking } from "@/nextasking";

export async function POST(request: Request) {
  const invoice = await readPaidInvoice(request);
  await nextasking.emit("invoice.paid", { invoiceId: invoice.id, amountInCents: invoice.amountInCents });
  return new Response(null, { status: 204 });
}
```

- `emit(name, payload)` resolves with `{ eventId }`, a random UUID, once the event is stored.
- Each listener of the event then gets exactly one run, started by a worker, not by `emit`. Its handler receives the event's schema output.
- The name and the payload are checked at compile time, like `dispatch`'s; at runtime, `emit` validates the payload and stores the schema's output.
- The listeners are those of the release that emitted: a worker running another release still starts exactly those.
- A listener's lines carry `eventName` and `eventId` on top of `task`, `runId` and `attempt`. See [Logging](logging.md).

### An event with no listeners

Emitting an event no listener subscribes to validates the payload, logs `no listeners for event` at warn with `eventName` and `eventId`, and does nothing else: nothing is stored and no connection opens. Emit the event before writing its first listener, then add listeners later.

## Narrow a name known only at runtime

`isTaskName` and `isEventName` turn a runtime string into a name `dispatch` or `emit` takes, without a cast.

```ts
// src/app/api/webhooks/route.ts
import type { EventName } from "@flefebvre/nextasking";
import { nextasking } from "@/nextasking";

function eventNameFor(webhookType: string): EventName | undefined {
  const eventName = `billing.${webhookType}`;
  return nextasking.isEventName(eventName) ? eventName : undefined;
}

export async function POST(request: Request) {
  const { type, data } = await request.json();
  const eventName = eventNameFor(type);
  if (eventName === undefined) return new Response(null, { status: 204 });
  await nextasking.emit(eventName, data);
  return new Response(null, { status: 204 });
}
```

- `isTaskName(name)` is true for every name in `tasks`, listeners included. `isEventName(name)` is true for every name in `events`.
- `TaskName` and `EventName` are the union of the names the registered instance lists. They come from the `Register` augmentation, and are `never` without it.
- A narrowed name still takes a typed payload: validate untrusted data with the schema first, or let `emit` throw `InvalidPayloadError`.

## Dispatch and emit errors

```ts
import { InvalidPayloadError, UnknownEventError, UnknownTaskError } from "@flefebvre/nextasking";

try {
  await nextasking.dispatch(name, payload);
} catch (error) {
  if (error instanceof InvalidPayloadError) {
    return { status: 400, issues: error.issues };
  }
  throw error;
}
```

- `UnknownTaskError`: the name isn't in `tasks`. Its message lists the valid task names. Reached only through a cast or plain JavaScript.
- `UnknownEventError`: `emit` got a name that isn't in `events`. Its message lists the valid event names. Reached only through a cast or plain JavaScript.
- `InvalidPayloadError`: the schema refused the payload. `issues` lists each issue as `{ path, message }`, with `path` dotted (`address.zipCode`) or `(root)`. Messages are redacted of payload values (`[redacted]`), so the error is safe to log. Its message names the task or the event.
- All three are thrown before anything is stored.

## Start the worker

Start the worker from `instrumentation.ts`, behind an environment variable you own, so you choose which processes run tasks.

```ts
// src/instrumentation.ts
export async function register() {
  if (process.env.NEXT_RUNTIME !== "nodejs" || process.env.NEXTASKING_WORKER !== "embedded") return;
  const { nextasking } = await import("@/nextasking");
  await nextasking.startWorker();
}
```

- `startWorker()` runs one worker in this process, executing every task the instance lists and fanning out every event it lists.
- A process that never calls `startWorker()` only dispatches and emits.
- Calling `startWorker()` a second time in the same process throws.
- Completed runs and completed fan-outs are kept 24 hours, and at most 1,000.
- A fan-out that fails is retried 5 times, waiting 30 seconds and doubling, and kept 14 days once it has failed. A retry skips the listeners that already have a run.
