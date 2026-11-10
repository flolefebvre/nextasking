# Core concepts

The words the rest of this manual uses, each in one or two sentences, with the page that details it. Read this page once before writing your first task.

## Definitions

```ts
export const invoicePaid = defineEvent("invoice.paid", { schema: z.object({ invoiceId: z.string() }) });

export const sendReceipt = defineListener("send-receipt", { on: invoicePaid, handler: async () => {} });

export const syncAccount = defineTask("sync-account", {
  schema: z.object({ accountId: z.string() }),
  handler: async () => {},
});

export const purgeSessions = defineScheduledTask("purge-sessions", {
  schedule: { cron: "0 3 * * *" },
  handler: async () => {},
});
```

- **Task**: a named piece of work with a payload schema and a handler. See [Tasks and events](tasks-and-events.md#define-a-task).
- **Event**: a named fact with a payload schema, and no handler: the work goes in its listeners. See [Tasks and events](tasks-and-events.md#define-an-event).
- **Listener**: a task subscribed to exactly one event, taking that event's payload. An event can have any number of them. See [Tasks and events](tasks-and-events.md#define-a-listener).
- **Scheduled task**: a task with a cron and a time zone, and no payload. See [Scheduled tasks](scheduled-tasks.md).
- **Built-in event**: `runFailed` and `fanOutFailed`, emitted by the library on a final failure. Listen to them, never emit them. See [Retries and failures](retries-and-failures.md#react-to-a-failure).
- **Instance**: what `createNextasking` returns, listing every task and event. Server code calls it; `Register` types its names. See [Tasks and events](tasks-and-events.md#register-the-tasks-and-events).

## Runs

```ts
const { runId } = await nextasking.dispatch("sync-account", { accountId: "account-1" });
const { eventId } = await nextasking.emit("invoice.paid", { invoiceId: "inv-1" });
```

- **Dispatch**: start exactly one run of a task, by its name. See [Tasks and events](tasks-and-events.md#dispatch-a-task).
- **Emit**: announce an event, by its name. Each of its listeners gets one run. See [Tasks and events](tasks-and-events.md#emit-an-event).
- **Fan-out**: the turning of one emitted event into one run per listener, done by a worker after `emit` resolves. A fan-out that fails can be replayed. See [Retries and failures](retries-and-failures.md#replay-a-failed-run).
- **Run**: one execution of a task, from its dispatch to its final outcome, across all its attempts. It has a `runId` and one of eight states. See [Dashboard](dashboard.md#the-eight-states).
- **Attempt**: one try at completing a run. A retry is a new attempt of the same run; the handler re-enters from the top. See [Retries and failures](retries-and-failures.md).
- **Backoff**: the wait between a failed attempt and the next one, doubling each time. See [Retries and failures](retries-and-failures.md#set-retries-and-backoff).
- **Failed run**: a run whose last attempt failed, kept 14 days with its payload, steps and errors. **Replay** starts it again under its own `runId`. See [Retries and failures](retries-and-failures.md#replay-a-failed-run).

## Steps

```ts
await step.run("charge", ({ idempotencyKey }) => payments.charge(payload.invoiceId, { idempotencyKey }));
await step.sleep("wait-before-reminder", "3d");
```

- **Step**: a named part of a run whose result is saved, so a later attempt gets it back instead of running it again. See [Steps](steps.md).
- **Sleep**: a step that suspends the run until a time, holding no worker. The run resumes in the same attempt. See [Steps](steps.md#sleep-for-minutes-or-days).
- **Idempotency key**: a value `step.run` hands its `fn`, the same for one step of one run on every attempt. Pass it to the provider so it drops a repeat. See [Steps](steps.md#pass-the-idempotency-key-to-the-provider).

## Ids and repeats

```ts
await nextasking.dispatch("ship-order", { orderId }, { runId: `ship-order-${orderId}` });
```

- **Run id** and **event id**: a random UUID by default. Pass your own, built from the business fact, so a repeated call starts nothing new. See [Tasks and events](tasks-and-events.md#run-once-per-business-fact).
- **Dedup window**: how long a repeated `runId` or `eventId` is dropped: at least 24 hours, and as long as the run or fan-out is kept. See [Tasks and events](tasks-and-events.md#the-dedup-window).
- **Duplicate run**: a side effect executed twice. The library closes most paths to it; six stay open. See [Duplicate runs](duplicate-runs.md).

## The worker

```ts
// src/instrumentation.ts
export async function register() {
  if (process.env.NEXT_RUNTIME !== "nodejs" || process.env.NEXTASKING_WORKER !== "on") return;
  const { nextasking } = await import("@/nextasking");
  await nextasking.startWorker();
}
```

- **Worker**: the part of a process that executes attempts and fan-outs, started by `startWorker()` in `next start`. A process without one only dispatches and emits. See [Worker](worker.md).
- **Lock**: the hold an attempt has on its run while it executes, renewed by its worker. An attempt that loses it can no longer save anything, and its `abortSignal` aborts. See [Worker](worker.md#stop-a-handler-cleanly-with-abortsignal).
- **Stall**: an attempt that lost its lock because its process crashed, was killed or blocked its event loop. The run goes back to pending; the third stall with no step saved in between fails it. See [Worker](worker.md#stalls).
- **Duplicate attempt**: an attempt that starts while an earlier attempt of the same run, which lost its lock, still executes. The earlier one stops at its next step. See [Duplicate runs](duplicate-runs.md#a-duplicate-attempt-finishing-its-step).
- **Concurrency limit**: the most attempts of one task, or of one key of it, executing at once across every worker. See [Concurrency](concurrency.md).
- **Firing**: one time a schedule comes due, starting one run of its task. See [Scheduled tasks](scheduled-tasks.md).

## Releases

- **Release**: one deployed build of your app. During a deploy, an old and a new release run side by side, sharing one queue, so a run can be dispatched by one and executed by the other. See [Deploys](deploys.md).
