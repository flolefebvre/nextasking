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

- **Task**: a named piece of work you define, with a payload schema and a handler. See [Tasks and events](tasks-and-events.md#define-a-task).
- **Event**: a named fact with a payload schema, which tasks can subscribe to. See [Tasks and events](tasks-and-events.md#define-an-event).
- **Listener**: a task subscribed to exactly one event, whose payload is that event's payload. See [Tasks and events](tasks-and-events.md#define-a-listener).
- **Scheduled task**: a task with a cron and a time zone, and no payload. See [Scheduled tasks](scheduled-tasks.md).
- **Built-in event**: `runFailedEvent`, an event the library defines and emits itself when a run fails. Listen to it, never emit it. See [Retries and failures](retries-and-failures.md#react-to-a-failure).
- **Failure notice**: one emission of a built-in event for one final failure of a run, stored together with that failure. See [Retries and failures](retries-and-failures.md#react-to-a-failure).
- **Instance**: what `createNextasking` returns, listing every task and event. Server code calls it; `Register` types its names. See [Tasks and events](tasks-and-events.md#register-the-tasks-and-events).

## Runs

```ts
const { runId } = await nextasking.dispatch("sync-account", { accountId: "account-1" });
const { eventId } = await nextasking.emit("invoice.paid", { invoiceId: "inv-1" });
```

- **Dispatch**: start exactly one run of a task, by its name. See [Tasks and events](tasks-and-events.md#dispatch-a-task).
- **Emit**: announce an event, by its name, starting one run of each of its listeners, which may be none. See [Tasks and events](tasks-and-events.md#emit-an-event).
- **Fan-out**: the creation of one run per listener of an emitted event, done by the emit itself, all or nothing. No fan-out is left to replay: a failed listener run is replayed as a run. See [Tasks and events](tasks-and-events.md#emit-an-event).
- **Run**: one execution of a task, from its dispatch to its final outcome, across all its attempts. See [Dashboard](dashboard.md#the-eight-states) for its states.
- **Attempt**: one try at completing a run. A retry is a new attempt of the same run. See [Retries and failures](retries-and-failures.md).
- **Backoff**: the wait between a failed attempt and the next attempt of the same run. See [Retries and failures](retries-and-failures.md#set-retries-and-backoff).
- **Failed run**: a run whose final outcome is failure. See [Retries and failures](retries-and-failures.md#what-a-failed-run-keeps).
- **Replay**: start a failed run again under its own `runId`, with a fresh retry budget, reusing the steps it already saved. See [Retries and failures](retries-and-failures.md#replay-a-failed-run).

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
- **Dedup window**: how long a repeated dispatch or emit with the same id is dropped. See [Tasks and events](tasks-and-events.md#the-dedup-window).
- **Retention**: how long an ended run stays readable and replayable before a worker removes it. See [Tasks and events](tasks-and-events.md#start-the-worker) for completed runs and [Retries and failures](retries-and-failures.md#what-a-failed-run-keeps) for failed runs.
- **Duplicate run**: a side effect executed twice. The library closes most paths to it; a few stay open, each with its fix. See [Duplicate runs](duplicate-runs.md).

## The worker

```ts
// src/instrumentation.ts
export async function register() {
  if (process.env.NEXT_RUNTIME !== "nodejs" || process.env.NEXTASKING_WORKER !== "on") return;
  const { nextasking } = await import("@/nextasking");
  await nextasking.startWorker();
}
```

- **Worker**: the role of a Next.js server process that executes attempts, started by `startWorker()` from `instrumentation.ts`. A process without one only dispatches and emits. See [Worker](worker.md).
- **Lock**: the hold an attempt has on its run while it executes, renewed by its worker. An attempt that loses it can no longer save or complete anything, and another attempt of the run may start. See [Worker](worker.md#stop-a-handler-cleanly-with-abortsignal).
- **Stall**: an attempt that lost its lock because its process crashed, was killed, or blocked its event loop past the lock duration. The run goes back to pending; the third stall with no step saved in between fails it. See [Worker](worker.md#stalls).
- **Duplicate attempt**: an attempt of a run that starts while another attempt of the same run is still executing, after the first lost its lock. See [Duplicate runs](duplicate-runs.md#a-duplicate-attempt-finishing-its-step).
- **Concurrency limit**: the most attempts of one task allowed to execute at the same time, counted across the whole task or per concurrency key. See [Concurrency](concurrency.md).
- **Firing**: one time a schedule comes due, starting one run of its task. See [Scheduled tasks](scheduled-tasks.md).

How the worker and the queue fit together:

- The library keeps its own queue in your Valkey or Redis, under the instance's `prefix`. A run changes by one atomic script per transition on the server, so each transition is stored whole or not at all. For what a read sees while runs move, see [Dashboard](dashboard.md#what-reads-see-and-what-they-cost).
- A worker holds a lock on each run it executes and renews it while the attempt runs. When a worker is lost, a worker finds its runs stalled once their locks expire and sends them back to pending. The lock's duration and how soon a stall is found are on [Worker](worker.md#stalls).
- The worker runs inside the Next.js server process, `next start` or `next dev`; the package ships no worker process of its own. Run it on one instance or many, with the same code and configuration. See [Worker](worker.md#choose-which-processes-run-a-worker).
- Every instant the queue decides on is the server's: when a run is due, when a lock expires, when a schedule fires. A worker stops an attempt whose lock it could not renew by its own clock. See [Worker](worker.md#when-redis-is-down).

## Releases

- **Release**: one deployed build of your app. During a deploy, an old and a new release run side by side on one queue, so a run stored by one can be executed by the other. See [Deploys](deploys.md).
- **Hand back**: return a run to pending without executing it, because the worker doesn't define its task, so that a worker of another release picks it up. A run no worker takes for long enough fails. See [Worker](worker.md#overlapping-releases).
- **Pass over**: leave a pending or due run in place, untouched, because its record is newer than the worker can read, so that a worker of a newer release takes it. See [Worker](worker.md#overlapping-releases).

A run's `retries` and `concurrency.key` are fixed when it is stored, by the release that stored it: the one that dispatched, emitted or fired it, or that failed the run a failure notice reports. A later release changes them only for the runs it stores. Which release decides each other option is on [Deploys](deploys.md#which-release-decides-what).
