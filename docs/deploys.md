# Deploys

During a deploy, the old and the new release run side by side on one queue: a run dispatched by one can be executed by the other, and runs stored weeks ago (delayed, asleep, backing off, failed and replayed) meet today's code. This page says which release decides each option, how to change a schema or a step safely, and how to remove a task, a listener or an event without stranding its runs.

## Which release decides what

| What                                                 | Decided by                                                                                                                                                                                                                                         | Details                                                                       |
| ---------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| Payload validation, and the payload the handler gets | The release that dispatched or emitted. The schema's output is stored, and no worker validates it again                                                                                                                                            | [Change a payload schema](#change-a-payload-schema)                           |
| The listeners an emit reaches                        | The release that emitted                                                                                                                                                                                                                           | [Tasks and events](tasks-and-events.md#emit-an-event)                         |
| `delay`                                              | The `dispatch` call                                                                                                                                                                                                                                | [Tasks and events](tasks-and-events.md#start-a-run-later)                     |
| `retries`                                            | Fixed when the run is stored: by the release that dispatched; for a listener run, by the worker that ran the fan-out, 5 if its release doesn't define the listener; for a firing's run, by the worker that processed the firing. A replay keeps it | [Retries and failures](retries-and-failures.md#set-retries-and-backoff)       |
| `backoff`                                            | The worker that ran the failed attempt                                                                                                                                                                                                             | [Retries and failures](retries-and-failures.md#set-retries-and-backoff)       |
| `timeout`                                            | The worker running the attempt                                                                                                                                                                                                                     | [Retries and failures](retries-and-failures.md#bound-an-attempt-with-timeout) |
| `concurrency.limit`, and whether there is a limit    | The worker running the attempt                                                                                                                                                                                                                     | [Concurrency](concurrency.md)                                                 |
| `concurrency.key`                                    | Computed when the run is stored: by the release that dispatched; for a listener run, by the worker that ran the fan-out. The worker running the attempt computes it only for a run stored without one                                              | [Concurrency](concurrency.md#limit-per-key)                                   |
| The handler and its steps                            | The worker running the attempt. Steps an earlier attempt saved, on any release, are reused by id                                                                                                                                                   | [Rename or rework a step](#rename-or-rework-a-step)                           |
| A schedule's `cron` and `timezone`                   | The last worker to boot                                                                                                                                                                                                                            | [Scheduled tasks](scheduled-tasks.md#what-a-deploy-does-to-schedules)         |
| Whether a firing starts a run                        | The worker that processes the firing: none when its release doesn't schedule the task                                                                                                                                                              | [Scheduled tasks](scheduled-tasks.md#what-a-deploy-does-to-schedules)         |
| Whether a run or a fan-out executes at all           | The worker that picks it up: one whose release doesn't define the name hands it back, and it fails 30 minutes after a worker first did                                                                                                             | [Worker](worker.md#overlapping-releases)                                      |

- An option the worker running the attempt decides applies to the next attempt of every run, old runs included.
- An option fixed when the run is stored keeps its old value for the runs stored before the change.

## Change a payload schema

Make additive changes only, or define the task under a new name. A payload stored by the old release reaches the new handler as it was stored, and during the overlap a payload stored by the new release can reach an old worker.

```ts
// before
schema: z.object({ userId: z.string(), email: z.email() }),

// after: additive, read with a fallback
schema: z.object({ userId: z.string(), email: z.email(), locale: z.string().optional() }),
handler: async ({ payload }) => {
  await email.send({ to: payload.email, template: "welcome", locale: payload.locale ?? "en" });
},
```

- Add a field as optional, and handle its absence in the handler. A schema default doesn't help: it applies at dispatch only, never to a payload already stored.
- Don't remove or rename a field, change its type, or make an optional field required.
- For a breaking change, define a new task, such as `send-welcome-email-v2`, dispatch it from the new release, and remove the old one with [the two-deploy recipe](#remove-a-task-a-listener-or-an-event).
- An event's schema follows the same rules: its listeners receive the payload stored at the emit.
- Stored payloads live as long as their run: a delay, a sleep of days, a failed run kept 14 days and replayed.

## Rename or rework a step

A step's saved result is found by its id, on whichever release runs the next attempt.

```ts
// before
await step.run("send-email", async ({ idempotencyKey }) => sendWelcome(payload, idempotencyKey));

// after: a run that saved "send-email" sends again under "send-welcome-email", with a new idempotency key
await step.run("send-welcome-email", async ({ idempotencyKey }) => sendWelcome(payload, idempotencyKey));
```

- Renaming a step makes the runs in progress run it again. Its `idempotencyKey` changes with its id, so the provider doesn't drop the repeat. Keep ids stable once deployed.
- A step whose `fn` changes but whose id stays returns the old result to runs in progress, in the old shape. Give it a new id when the result's shape changes, and read old results defensively until those runs finish.
- A saved step whose id no longer appears in the code is ignored.
- Renaming a task changes every step's `idempotencyKey` too: it is derived from the task name, the run id and the step id.
- See [Steps](steps.md#step-ids) and, to try a change in dev, [Dev](dev.md#change-a-tasks-steps).

## Remove a task, a listener or an event

Remove a definition in two deploys: the first stops new work and keeps the code that runs the work already stored; the second deletes the code once that work is done. A run or a fan-out whose name no worker defines is handed back, then fails after 30 minutes with `reason: "unknown task"`.

### A task

1. Deploy 1: remove every `dispatch` of the task, the handlers' included. Keep its definition in `tasks`.
2. Wait until the task has no unfinished run, and replay or give up its failed runs.
3. Deploy 2: delete the definition and its file.

```ts
const { completed, failed, ...unfinishedCounts } = (await nextasking.countRuns())["send-legacy-report"];
const unfinishedRunCount = Object.values(unfinishedCounts).reduce((total, count) => total + count, 0);
```

- Wait for `unfinishedRunCount` to be 0 and for the deploy overlap to have ended: the old release may still dispatch until its last process stops.
- After deploy 2, `replay`, `getRun` and `listRuns` throw `UnknownTaskError` for the name: replay its failed runs before, or accept losing them.
- A scheduled task is removed in one deploy: the new release's boot removes its schedule, so nothing fires afterwards. A run an old worker started from a firing during the overlap, and that no old worker finished, is handed back and fails `unknown task` 30 minutes later. The new release's reads don't list that task's runs, so check `countRuns` for it before the deploy, deploy away from its firing times, and point a BullMQ tool at the queue to find a run left behind.

### A listener

1. Deploy 1: turn the listener into a plain task of the same name, with the event's schema. New emits no longer reach it, and the runs and fan-outs already carrying its name still run.
2. Wait as for a task.
3. Deploy 2: delete it.

```ts
// before
export const recordInvoicePayment = defineListener("record-invoice-payment", {
  on: invoicePaid,
  handler: async ({ payload }) => {
    await ledger.record(payload);
  },
});

// deploy 1
export const recordInvoicePayment = defineTask("record-invoice-payment", {
  schema: invoicePaid.schema,
  handler: async ({ payload }) => {
    await ledger.record(payload);
  },
});
```

- Keep the event's schema: the stored payloads were validated by it, and the handler reads them as they are.
- Keep the task in `tasks`.
- An emit by the old release during the overlap still fans out to it; the plain task runs those runs.

### An event

1. Deploy 1: remove every `emit` of the event, the handlers' included. Keep the event in `events` and its listeners in `tasks`: a listener needs its event listed.
2. Wait until its listeners have no unfinished run, and the overlap has ended.
3. Deploy 2: delete the event and its listeners.

- After deploy 2, `getEvent` and `replayEvent` throw `UnknownEventError` for the name: replay its failed fan-outs before.
- A fan-out still stored after deploy 2 is handed back, then fails after 30 minutes.

## Rename a task or an event

A rename is an addition and a removal: the stored runs carry the old name.

1. Deploy 1: add the definition under the new name, dispatch or emit the new name, and keep the old definition for the runs already stored.
2. Deploy 2: remove the old definition, as in [Remove a task, a listener or an event](#remove-a-task-a-listener-or-an-event).

- A `runId` or `eventId` is scoped to its name: a repeat under the new name isn't deduplicated against a call under the old one.
- Renaming a scheduled task moves its schedule at the next boot. See [Scheduled tasks](scheduled-tasks.md#what-a-deploy-does-to-schedules).
