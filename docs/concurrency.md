# Concurrency

`concurrency` on a task caps how many of its attempts execute at once, across every worker and instance, for the whole task or per key. Runs beyond the cap wait for a slot: they are never dropped and use none of their retries.

This is not `startWorker({ concurrency })`, which caps the attempts of every task in one process. See [Worker](worker.md#cap-the-attempts-in-flight-with-concurrency).

## Limit a task across every instance

```ts
// src/nextasking/tasks/rebuild-search-index.ts
export const rebuildSearchIndex = defineTask("rebuild-search-index", {
  schema: z.object({ reason: z.string() }),
  concurrency: { limit: 1 },
  handler: async () => {
    await searchIndex.rebuild();
  },
});
```

- `limit` is a whole number, 1 or more, or `createNextasking` throws.
- `{ limit: 1 }` makes the task run one attempt at a time, whatever the number of workers.

## Limit per key

Give `key` a function of the payload. Runs sharing a key share the limit; runs of other keys flow freely.

```ts
// src/nextasking/tasks/sync-account.ts
export const syncAccount = defineTask("sync-account", {
  schema: z.object({ accountId: z.string() }),
  concurrency: { key: (payload) => payload.accountId, limit: 2 },
  handler: async ({ payload }) => {
    await crm.sync(payload.accountId);
  },
});
```

- `key` receives the schema's output and returns a non-empty string.
- A key is scoped to its task: two tasks returning the same key don't share a limit.
- The key is computed once, when the run is stored: at `dispatch`, or at `emit` for each listener. It is never computed again.
- A run stored before its task had a `key` has none: it shares the limit with the other runs stored without one, not with the runs of a key. During the deploy that adds a `key`, a run stored before it and a run of the same key stored after can execute together.
- A `key` that throws, or returns something other than a non-empty string, makes `dispatch` throw an `Error` naming the task, and `emit` an `Error` naming the listener: `Task "<listener>" has an invalid concurrency.key: it threw "<message>"`. Nothing is stored, for any listener of the event. For a `runFailedEvent` listener, see [Retries and failures](retries-and-failures.md#react-to-a-failure).
- A listener takes `concurrency` too, with the event's payload.

## Keep a scheduled task's firings from overlapping

```ts
export const purgeExpiredSessions = defineScheduledTask("purge-expired-sessions", {
  schedule: { cron: "*/5 * * * *" },
  concurrency: { limit: 1 },
  handler: async () => {
    await db.session.deleteMany({ where: { expiresAt: { lt: new Date() } } });
  },
});
```

- A scheduled task takes `{ limit }` only: it has no payload to key on.
- A firing due while the previous run executes waits for it, then runs.

## What waiting costs

- A slot is held only while an attempt executes. A run in backoff between retries, or asleep in `step.sleep`, holds none.
- Waiting uses no retry and no stall: `attempt` is the same when the run finally executes.
- Runs of one key start roughly in dispatch order. A retried or resumed run joins the back of the line. There is no fairness across keys.
- A slot frees as soon as its attempt ends, and wakes the oldest waiting runs at once, as many as there are free slots.
- A slot is held under its attempt's lock. A worker killed while executing holds its slot until its lock expires: a key at its limit stays blocked until the killed worker's lock expires and the stall is found. See [Worker](worker.md#stalls).
- An attempt that loses its lock is aborted. See [Worker](worker.md#stop-a-handler-cleanly-with-abortsignal).
- The limit is read from the release running the attempt; a change applies to the attempts it starts. The key is the one stored with the run, computed by the release that stored it. See [Deploys](deploys.md#which-release-decides-what).
- A raised limit's new slots go to the runs fetched from then on. A run already waiting starts when a slot of its key frees; that freed slot then wakes as many waiting runs as the new limit leaves free.

## Log lines

| Level   | Message                              | Fields                     |
| ------- | ------------------------------------ | -------------------------- |
| `debug` | `run waiting for a concurrency slot` | `task`, `runId`, `attempt` |

- `run waiting for a concurrency slot` is written once each time the run finds its slots all taken and waits; a run may wait more than once. A run that finds a free slot at once doesn't write it.
- A waiting run woken for a free slot writes no line of its own: its next line is `attempt started` or `run resumed`.

See [Logging](logging.md) for the other lines.
