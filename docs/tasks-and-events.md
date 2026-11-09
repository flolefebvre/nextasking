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
  - `step`: durable steps, whose results a retry reuses, and sleeps; see [Steps](steps.md)
  - `emit` and `dispatch`: the instance's calls, sent once per run; see [Steps](steps.md#emit-and-dispatch-from-a-handler)
  - `runId`: the id `dispatch` returned for this run
  - `attempt`: the attempt number, starting at 1
  - `logger`: a logger that tags every line with the run; see [Logging](logging.md)
  - `abortSignal`: aborts when the attempt must stop; see [Worker](worker.md#stop-a-handler-cleanly-with-abortsignal)
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
- It takes every task option (`retries`, `backoff`, `timeout`, `concurrency`) and the same handler context. See [Retries and failures](retries-and-failures.md) for the first three and [Concurrency](concurrency.md) for `concurrency`.
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
- `connection.giveUpAfter` (default `"5s"`) bounds how long a call waits for Redis before it throws. See [When Redis is down](#when-redis-is-down).
- `prefix` (default `"nextasking"`) puts every key of the instance under that prefix. Give two apps sharing one server different prefixes.
- `logger` takes your app's logger; without it the library is silent. See [Logging](logging.md).
- Without the `Register` augmentation, `TaskName` and `EventName` are `never`, and a handler's `emit` and `dispatch` take no name: add it once, next to the instance.

### Startup checks

`createNextasking` checks the definitions before anything else, with no connection, and throws an `Error` whose one-line message names the definition and what is wrong:

- two definitions with the same name: tasks and events share one namespace
- a listener whose `on` event isn't listed in `events` and isn't a built-in event
- a task or event name starting with `nextasking/`, which is reserved for the library and its built-in events, `runFailed` (`nextasking/run.failed`) and `fanOutFailed` (`nextasking/fan-out.failed`); see [Retries and failures](retries-and-failures.md#react-to-a-failure)
- a built-in event listed in `events`: it is added automatically
- a `concurrency.limit` that isn't a positive integer
- `retries` that isn't a whole number, 0 or more
- a duration option that isn't a valid duration, such as `"-5s"` or `"0.5h"` passed through a cast or plain JavaScript, `connection.giveUpAfter` included
- a scheduled task's cron or time zone that isn't valid; see [Scheduled tasks](scheduled-tasks.md#startup-checks)

```text
Duplicate name "invoice.paid": every task and event needs its own name
Listener "record-invoice-payment" listens to event "invoice.paid", which is not listed in events
Event "nextasking/user.deleted" has a reserved name: names starting with "nextasking/" belong to the built-in events
Event "nextasking/run.failed" is built in and added automatically: remove it from events, and listen to it as it is
Task "nextasking/sync" has a reserved name: names starting with "nextasking/" are reserved for the library
Task "rebuild-index" has an invalid timeout: "0.5h" is not a duration: a whole number followed by s, m, h or d, such as "30s"
Task "sync-account" has an invalid retries: -1 is not a non-negative integer
Invalid connection.giveUpAfter: "0.5h" is not a duration: a whole number followed by s, m, h or d, such as "30s"
```

Fix the definition; don't catch the error.

- Thrown from `register()`, it fails loudly but doesn't stop the process: `next start` keeps running and answers 500 to every request, and logs the error on boot and on each request as `Error: An error occurred while loading instrumentation hook:` followed by the message above.

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
- `dispatch(name, payload, { runId, delay })` takes your own run id (see [Run once per business fact](#run-once-per-business-fact)) and a delay.

### Start a run later

```ts
await nextasking.dispatch("send-trial-reminder", { userId: user.id }, { delay: "3d" });
```

- `delay` is a [duration](#durations): the run starts once it has passed, not before. Use it instead of a sleep in the handler.
- An invalid `delay` passed through a cast or plain JavaScript makes `dispatch` throw an `Error` naming the task, before anything is stored.

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
- The listeners are those of the release that emitted: a worker running another release still starts exactly those. See [Deploys](deploys.md#which-release-decides-what).
- A listener's lines carry `eventName` and `eventId` on top of `task`, `runId` and `attempt`. See [Logging](logging.md).
- `emit(name, payload, { eventId })` takes your own event id: see [Run once per business fact](#run-once-per-business-fact).

### An event with no listeners

Emitting an event no listener subscribes to validates the payload, logs `no listeners for event` at warn with `eventName` and `eventId`, and does nothing else: nothing is stored and no connection opens. Emit the event before writing its first listener, then add listeners later.

## Run once per business fact

A webhook retried by its sender, or a form submitted twice, must not start the work twice. Pass your own `runId` to `dispatch`, or `eventId` to `emit`, built from the business fact itself.

```ts
// src/app/api/billing/webhook/route.ts
export async function POST(request: Request) {
  const stripeEvent = await readStripeEvent(request);
  await nextasking.emit(
    "invoice.paid",
    { invoiceId: stripeEvent.data.invoiceId, amountInCents: stripeEvent.data.amountInCents },
    { eventId: `stripe-${stripeEvent.id}` },
  );
  return new Response(null, { status: 204 });
}

// src/app/orders/actions.ts
await nextasking.dispatch("ship-order", { orderId: order.id }, { runId: `ship-order-${order.id}` });
```

- Build the id from what makes the fact unique (the provider's event id, the order id, the user id and the day), never from the time of the call or a random value.
- A repeat with the same id returns that same id, throws nothing, starts nothing, and logs `duplicate dispatch dropped` at debug. Its payload and its `delay` are ignored: the first call's payload is the one that runs.
- A `runId` is scoped to its task: the same `runId` on two tasks starts two runs. An `eventId` is scoped to its event.
- Each listener run of an emit with your `eventId` has a run id derived from that `eventId` and the listener name, so it is the same on every repeat.
- The id must be a non-empty string; anything else makes the call throw an `Error` naming the task or event, before anything is stored.
- Without your own id, every call gets a random one, and a repeat starts a second run.

### The dedup window

A repeat is dropped as long as the first call's stored run or fan-out exists:

- while it waits, is delayed or executes;
- once it has completed, for 24 hours;
- once it has failed, for as long as the failure is kept: 14 days. See [Retries and failures](retries-and-failures.md#what-a-failed-run-keeps).

So the window lasts at least 24 hours after the dispatch, and longer when the run takes longer. Two limits to plan around:

- The queue keeps at most 1,000 completed runs and fan-outs together, and 10,000 failed ones. Under load, the oldest are removed early, and a repeat of their id starts again.
- An emit of an event with no listeners stores nothing, so it has no window: a repeat warns again.

## Narrow a name known only at runtime

`isTaskName` and `isEventName` turn a runtime string into a name `dispatch` or `emit` takes, without a cast.

```ts
// src/app/api/webhooks/route.ts
import { nextasking } from "@/nextasking";

function eventNameFor(webhookType: string) {
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

- `isTaskName(name)` is true for every name in `tasks`, listeners included. `isEventName(name)` is true for every name in `events`, and false for the built-in events, which `emit` doesn't take.
- `TaskName` and `EventName` are the union of the names the registered instance lists, the built-in events included in `EventName`. They come from the `Register` augmentation, and are `never` without it.
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
- `RedisUnavailableError`: Redis didn't answer in time. See [When Redis is down](#when-redis-is-down).
- `InstanceClosedError`: the instance has closed, as it does when its worker shuts down; a call made then, or still waiting for Redis, throws it at once.

## When Redis is down

Answer 503 when a call throws `RedisUnavailableError`, and let the client retry.

```ts
// src/app/api/welcome-emails/route.ts
import { RedisUnavailableError } from "@flefebvre/nextasking";
import { nextasking } from "@/nextasking";

export async function POST(request: Request) {
  const { email } = (await request.json()) as { email: string };
  try {
    const { runId } = await nextasking.dispatch("send-welcome-email", { email });
    return Response.json({ runId }, { status: 202 });
  } catch (error) {
    if (error instanceof RedisUnavailableError) {
      return Response.json({ runId: error.runId }, { status: 503, headers: { "retry-after": "5" } });
    }
    throw error;
  }
}
```

- Every call on the instance throws `RedisUnavailableError` when Redis doesn't answer, or keeps answering `READONLY`, within `giveUpAfter`: `dispatch`, `emit`, `replay`, `replayEvent`, `getRun`, `listRuns`, `countRuns`, `getEvent` and `listSchedules`. It never hangs.
- `giveUpAfter` bounds each round trip, not the whole call, the wait for the connection included. Set it on `connection`: `connection: { url, giveUpAfter: "2s" }`. The default is `"5s"`.
- `error.cause` is the original error, such as `connect ECONNREFUSED` while Redis is unreachable, or a `TimeoutError` when it holds the connection open but doesn't answer, or the server's last `READONLY` reply, an `Error` whose message starts with `READONLY`, when it still refuses the write at `giveUpAfter`, as a replica does. `error.runId` or `error.eventId` is the id the call used, yours or the random one; reads carry the id they were given.
- A `dispatch` or `emit` that threw may still be stored once Redis is back. Retry it with the id on the error, so the retry is deduplicated: `dispatch(name, payload, { runId: error.runId })`, `emit(name, payload, { eventId: error.eventId })`. See [Run once per business fact](#run-once-per-business-fact).
- A failover shorter than `giveUpAfter` is a slow call, not an error: on a `READONLY` reply the library reconnects to the new primary and resends the command. A longer one throws `RedisUnavailableError` at `giveUpAfter`.
- Validation errors come first: an unknown name or an invalid payload throws its own error, Redis down or not.
- In a handler, `emit` and `dispatch` never throw `RedisUnavailableError`: like every command of the worker, they wait for Redis to come back, and the attempt goes on without spending a retry.
- An outage writes one `connection error` line, then one `connection restored` line, per process. See [Logging](logging.md).

## Start the worker

Start the worker from `instrumentation.ts`, behind an environment variable you own, so you choose which processes run tasks.

```ts
// src/instrumentation.ts
export async function register() {
  if (process.env.NEXT_RUNTIME !== "nodejs" || process.env.NEXTASKING_WORKER !== "on") return;
  const { nextasking } = await import("@/nextasking");
  await nextasking.startWorker();
}
```

- `startWorker()` runs one worker in this process, executing every task the instance lists and fanning out every event it lists.
- A process that never calls `startWorker()` only dispatches and emits.
- Calling `startWorker()` a second time in the same process throws.
- With Redis down, `startWorker()` resolves anyway and the worker starts once Redis is back, so pages keep rendering. See [Worker](worker.md#when-redis-is-down).
- Set `NEXT_MANUAL_SIG_HANDLE=1` in the environment of `next start`, so a deploy lets running attempts finish instead of cutting them. See [Worker](worker.md) for this, `concurrency` and the other options.
- Completed runs and completed fan-outs are kept 24 hours, and at most 1,000.
- A fan-out that fails is retried 5 times, waiting 30 seconds and doubling, and kept 14 days once it has failed, like a failed run. A retry skips the listeners that already have a run.
