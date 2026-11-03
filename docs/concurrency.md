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
- The key is computed once, when the run is stored: at `dispatch`, or at the fan-out of an emit for a listener. A run stored without one, by a release whose task had no `key` yet, gets it when a worker picks it up.
- A `key` that throws or returns an empty string makes `dispatch` throw an `Error` naming the task. For a listener, it fails the fan-out of the emit, which retries.
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
- A slot frees as soon as its attempt ends, and wakes the oldest waiting run at once.
- A waiting run is also picked up again every 2 minutes, as a safety net, and goes back to waiting if no slot is free. A key with 1,000 waiting runs costs about 8 pickups per second.
- A worker killed while executing holds its slot until the slot expires, 2 minutes after its last renewal: a key at its limit stays blocked for 2 to 3 minutes, until the killed run comes back as a stall or a waiting run's safety-net pickup finds the slot free.
- An attempt that can no longer renew its slot is aborted as having lost its lock, like an attempt that loses its run's lock. See [Worker](worker.md#stop-a-handler-cleanly-with-abortsignal).
- The limit and `key` are read from the release running the attempt; a change applies to the attempts it starts.

## Log lines

| Level   | Message                              | Fields                     |
| ------- | ------------------------------------ | -------------------------- |
| `debug` | `run waiting for a concurrency slot` | `task`, `runId`, `attempt` |
| `debug` | `run got a concurrency slot`         | `task`, `runId`, `attempt` |

- `run waiting for a concurrency slot` is written each time the run finds no free slot, its safety-net pickups included.
- `run got a concurrency slot` is written when a run that waited takes a slot, before its `attempt started` or `run resumed`. A run that finds a free slot at once writes neither line.

See [Logging](logging.md) for the other lines.
