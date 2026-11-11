# Worker

`startWorker()` runs a worker inside `next start`: the process that serves your pages also executes the attempts. This page covers how it boots, its options, what a deploy does to the attempts it is running, how a handler stops cleanly, stalls, overlapping releases, the server it needs and what it does when Redis is down.

## Start the worker with options

```ts
// src/instrumentation.ts
export async function register() {
  if (process.env.NEXT_RUNTIME !== "nodejs" || process.env.NEXTASKING_WORKER !== "on") return;
  const { nextasking } = await import("@/nextasking");
  await nextasking.startWorker({ concurrency: 20, shutdownTimeout: "25s" });
}
```

| Option               | Default   | What it does                                                                                                                              |
| -------------------- | --------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `concurrency`        | unlimited | The most attempts this process runs at once, a whole number 1 or more                                                                     |
| `shutdownTimeout`    | `"25s"`   | How long a shutdown waits for running attempts before it aborts them                                                                      |
| `onWorkerNotStarted` | none      | Called with the error when the boot that waited for Redis fails. See [Exit when the worker can't start](#exit-when-the-worker-cant-start) |

`startWorker()` boots in this order, and resolves once the last step is done:

1. It checks the options, and rejects an invalid one before opening any connection.
2. It opens the instance's connection to Redis, and rejects at once when `connection.url` is missing: `Missing connection.url: …`. See [Startup checks](tasks-and-events.md#startup-checks).
3. It reads the server's `maxmemory-policy` and version once, and rejects a server that would lose runs or is below the floor. See [Give queues their own Valkey or Redis](#give-queues-their-own-valkey-or-redis).
4. It reconciles the schedules stored in Redis with the instance's scheduled tasks, in one step: the last worker to boot sets them. See [Scheduled tasks](scheduled-tasks.md#what-a-deploy-does-to-schedules).
5. It opens a second connection, on which an idle worker waits to hear of a new run, starts taking attempts and writes `worker started`.

- With Redis down, `startWorker()` resolves anyway and the boot finishes in the background. See [When Redis is down](#when-redis-is-down).
- An instance with a worker holds two connections: one for commands, shared by your calls and the worker's, and the one the worker waits on. An instance with no worker holds one, opened by its first call.
- One worker per process: calling `startWorker()` again in the same process throws `The nextasking worker is already started in this process`. A call that rejected started no worker, so `startWorker()` can be called again.
- Type the options with `StartWorkerOptions`.
- An option `startWorker` doesn't take doesn't compile, written inline, held in a variable or spread in; the error names it: `"concurency" is not an option of this call`.

## Cap the attempts in flight with concurrency

Without `concurrency`, a worker starts every run that is pending: a backlog of 5,000 runs starts 5,000 attempts at once in one process. Set it when your handlers hold memory, database connections or rate-limited API calls.

```ts
await nextasking.startWorker({ concurrency: 10 });
```

- It counts this process only: two instances with `concurrency: 10` run up to 20 attempts.
- It caps every task together, in this process. To cap one task across every instance, or per account, use the task's own `concurrency: { limit }` or `{ key, limit }`; see [Concurrency](concurrency.md).
- A sleeping run holds no slot: it frees it until it resumes.
- A run of a task this release doesn't define counts against `concurrency` while the worker holds it before handing it back. See [Overlapping releases](#overlapping-releases).

## Let a deploy finish the running attempts

A deploy sends SIGTERM. Set `NEXT_MANUAL_SIG_HANDLE` in the environment of `next start`, and the worker handles it:

```json
// package.json
{
  "scripts": {
    "start": "NEXT_MANUAL_SIG_HANDLE=1 NEXTASKING_WORKER=on next start"
  }
}
```

On the first SIGTERM or SIGINT (a repeat is ignored):

1. The worker stops taking new attempts at once and keeps renewing the locks of the running ones. The HTTP server keeps answering.
2. A running attempt that reaches a step boundary (any `step.run`, `step.sleep`, `step.sleepUntil`, `emit` or `dispatch`) lets its steps in flight finish and save, then goes back to pending at once. Another instance resumes it from its saved steps. This uses no retry and counts no stall.
3. An attempt that ends before reaching a boundary completes or fails as usual.
4. At `shutdownTimeout`, the attempts still running get their `abortSignal` aborted. One that stops on the abort goes back to pending like a step boundary; one that returns keeps its completion. The worker waits up to one second more for them.
5. The worker closes its connections, waiting up to one second for a clean close, writes `worker stopped`, and the process exits with code 0.

- The whole shutdown takes at most `shutdownTimeout` plus about 2 seconds, whatever the state of the connection. Keep that under your platform's grace period before SIGKILL (30 seconds on Kubernetes and most hosts).
- An attempt still running when the process exits is cut. Its run comes back as a stall. See [Stalls](#stalls).
- During an outage the yields can't reach Redis: the attempts cut that way come back as stalls once their locks expire.
- From the start of the shutdown, a call on the instance throws `InstanceClosedError`; so does a call still waiting for Redis when the connection closes.
- Without `NEXT_MANUAL_SIG_HANDLE`, Next exits on SIGTERM at once and cuts every running attempt. The worker then warns at startup with `NEXT_MANUAL_SIG_HANDLE is not set: on SIGTERM Next exits at once and cuts the attempts running in this worker`, except under `NODE_ENV=development`. See [Dev](dev.md#what-ctrl-c-does-to-the-attempt-in-flight).
- With `NEXT_MANUAL_SIG_HANDLE` set, Next leaves both signals to your code: the worker exits the process once it has stopped, with code 1 if stopping itself failed.

## Choose which processes run a worker

`instrumentation.ts` starts the worker behind an environment variable you own, `NEXTASKING_WORKER` in these examples. Set it in each process that should run tasks:

| Process                    | Command                                                    | Runs a worker |
| -------------------------- | ---------------------------------------------------------- | ------------- |
| Web server with the worker | `NEXT_MANUAL_SIG_HANDLE=1 NEXTASKING_WORKER=on next start` | yes           |
| Web server only            | `next start`                                               | no            |

- A process that never calls `startWorker()` only dispatches and emits.
- Run as many workers as you need, with the same code and configuration: each takes attempts from the same queues. `concurrency` counts per process.
- Move CPU-heavy work off the event loop, to a worker thread or another service, so it doesn't stall the attempts of its process. See [Stalls](#stalls).

## Stop a handler cleanly with abortSignal

The handler receives `abortSignal`. It aborts when the attempt must stop: at the shutdown deadline, or when the attempt lost its lock. Pass it to the calls that accept one, and check it in long loops.

```ts
export const exportOrdersTask = defineTask("export-orders", {
  schema: z.object({ accountId: z.string() }),
  handler: async ({ payload, step, abortSignal }) => {
    const pageCount = await step.run("count-pages", () => countOrderPages(payload.accountId));
    for (let page = 0; page < pageCount; page += 1) {
      await step.run(`export-page-${page}`, async () => {
        const response = await fetch(exportUrl(payload.accountId, page), { method: "POST", signal: abortSignal });
        return (await response.json()) as { fileId: string };
      });
    }
  },
});
```

- `abortSignal.reason` is an `Error` whose message says why:

  ```text
  The attempt of run "<runId>" was aborted: the worker reached its shutdown deadline while the attempt was running.
  The attempt of run "<runId>" was aborted: it lost its lock, so another attempt may own the run now.
  ```

- Once it has aborted, every `step.*`, `emit` and `dispatch` of the attempt throws that reason without running.
- Let that error through. At the shutdown deadline the run goes back to pending and resumes elsewhere from its saved steps, using no retry.
- An attempt that lost its lock can no longer complete, save or read anything, whatever the handler does: another attempt may own the run. Stop rather than finish the work twice.
- Test `abortSignal.aborted` to know whether the attempt was aborted.

## Stalls

An attempt holds a lock on its run for 2 minutes, and its worker renews it once less than a minute is left. A stall is an attempt that lost its lock: its process crashed, was killed, or blocked its event loop longer than the lock. Every second, each worker looks for runs whose lock expired and sends them back to pending, where the next attempt reuses the saved steps.

- A stall is found within about a second of the lock expiring: a cut attempt's run is back in about 2 minutes, and a concurrency key it held at its limit stays blocked until then, and no longer.
- The third stall with no step saved in between fails the run with `reason: "stalled"`, whatever retries it has left. A saved step, a handler's `emit` or `dispatch`, and a new attempt start the count again.
- A stall uses no retry: `attempt` stays the same.
- An outage longer than the lock, in which a worker lost the reply that handed it runs, can count one stall on a run no attempt executed.
- After a stall, one log line can misreport an outcome. See [Logging](logging.md#lifecycle-lines), under `attempt outcome discarded: lock lost`.
- These timings are fixed, the same in dev and in production.

## Overlapping releases

During a deploy, an old and a new release run side by side, on the same queues.

A worker that takes a run of a task its release doesn't define hands it back for another worker instead of failing it:

- It holds the run 10 seconds, then returns it to pending in its place in line, using no retry and counting no stall. A worker of the release that defines the task runs it as usual, with `attempt` unchanged.
- While held, the run counts against `concurrency` and keeps its concurrency slot; the slot frees at each hand back. A key at its limit waits for the hand back, and the runs of a key keep their order.
- The first time a worker finds the task unknown is saved with the run. If no worker has run it 30 minutes later, the run fails with `reason: "unknown task"` and an error record named `UnknownTaskRunError`, kept like any failed run, and its failure notice reaches the listeners. See [Retries and failures](retries-and-failures.md#what-a-failed-run-keeps).
- Each hand back writes `run handed back: unknown task`, and no `attempt started`.

A worker that meets a run written by a newer release, which it can't read, passes it over:

- It leaves the run in place, untouched and unlocked, and goes on with the runs it can read. A newer worker completes it.
- When only newer runs are ahead, it waits 10 seconds before looking again. An old release whose every run ahead is newer does nothing more until you stop it.
- A run waiting for a time (a `delay`, a sleep, a backoff) is passed over the same way once it is due.
- It writes `runs passed over: newer record version` once per newer version it meets.

These timings are fixed. See [Deploys](deploys.md) for which release decides each option, and how to remove a task, a listener or an event without stranding its runs.

## Give queues their own Valkey or Redis

Point `connection` at a Valkey or Redis that holds only queues, not your cache or sessions. A cache evicts data when memory runs short; a queue loses runs that way. A dedicated server also keeps a cache flush or a busy cache from stalling attempts.

Set `maxmemory-policy noeviction` on it. The worker reads the policy at boot from the `maxmemory_policy` line of `INFO`, and from `CONFIG GET maxmemory-policy` when `INFO` fails or has no such line. Managed services that block `CONFIG` still answer `INFO`, so the check runs there too:

- A policy other than `noeviction` makes `startWorker()` reject with a one-line error naming it. An evicted lock lets a second worker run the same attempt.
- A policy neither command yields writes `maxmemory-policy unreadable` at warn, `err` being an `AggregateError` of both failures, and the worker starts. Check the policy in your provider's console.

The server must be Valkey 8 or Redis 7.4, or later. The worker reads the version from the same `INFO`:

- A server below the floor makes `startWorker()` reject with an error naming the server and version it found.
- A version it can't read writes `server version unreadable` at warn, with `err`, and the worker starts.
- See [Setup](setup.md#run-valkey-or-redis) for the servers nextasking is tested on.

```text
maxmemory-policy is "allkeys-lru": the Valkey or Redis holding the queues must use "noeviction", or it evicts locks and runs
Redis 7.2.4 is below the floor: Valkey 8 or Redis 7.4
```

## When Redis is down

`startWorker()` never blocks `next start` on Redis:

- When Redis doesn't answer its first command within `connection.giveUpAfter` (default `"5s"`), `startWorker()` resolves anyway and writes `connection error`. Pages keep rendering; route handlers calling the instance get `RedisUnavailableError`, see [Tasks and events](tasks-and-events.md#when-redis-is-down).
- The boot waits for Redis in the background: once it answers, the worker checks the policy and the version, reconciles the schedules, then writes `worker started` and takes attempts. No attempt starts before.
- An evicting policy or a server below the floor found in the background writes `worker not started` at error, `err` saying why, and the worker stays off until the process restarts.
- A SIGTERM while the boot waits for Redis stops it at once: nothing to drain.

Once started, the worker waits out an outage instead of failing attempts:

- Taking runs, renewing locks, and a handler's `step`, `emit` and `dispatch` wait for Redis to come back. No attempt fails and no retry is spent because Redis was away.
- Each attempt keeps its lock's deadline by the worker's own clock. Past it with no renewal confirmed, the attempt is aborted with the lock-lost reason without hearing from Redis, and its run comes back as a stall. See [Stalls](#stalls).
- A command whose reply was lost is sent again and gets the same answer: the same runs under the same locks, a save or a completion already stored, a firing already started. Nothing is left half-done, and nothing is handed back after a reconnect.
- A server that stops answering without closing the connections counts as down once a call has waited `connection.giveUpAfter`: the worker reconnects both connections, the one it waits on included, even while every slot runs a handler.
- A server answering the worker's writes with `READONLY`, as a replica or a demoted primary does, is waited out the same way: the worker sends the writes again, attempts wait, no retry is spent. A failover that closes the connection the worker waits on writes nothing: the worker reconnects.
- A server out of memory (`maxmemory` reached under `noeviction`) refuses writes: the worker waits and sends the refused write again about once a second. Your calls to `dispatch` and `emit` throw `RedisUnavailableError` meanwhile.
- An outage writes one `connection error` and one `connection restored` per process. See [Logging](logging.md#lifecycle-lines) for when each is written.

### Exit when the worker can't start

`startWorker()` fails in one of two ways: it rejects when it finds the problem at once, or, when the boot waited for Redis, it has already resolved and calls `onWorkerNotStarted` instead. Handle both with the same function, flush your logs, then exit so your platform restarts the process:

```ts
// src/instrumentation.ts
export async function register() {
  if (process.env.NEXT_RUNTIME !== "nodejs" || process.env.NEXTASKING_WORKER !== "on") return;
  const { nextasking } = await import("@/nextasking");
  const exitOnWorkerNotStarted = async (error: unknown) => {
    const { appLogger, flushLogs } = await import("@/logger");
    appLogger.error({ err: error }, "exiting: the nextasking worker did not start");
    await flushLogs();
    process.exit(1);
  };
  try {
    await nextasking.startWorker({ onWorkerNotStarted: exitOnWorkerNotStarted });
  } catch (error) {
    await exitOnWorkerNotStarted(error);
  }
}
```

- Exit yourself under `next start`. In production Next doesn't await `register()` before serving, and a rejected `register()` leaves the server answering 500 to every request without exiting (observed with Next 16.3.5). Rethrowing or setting `process.exitCode` doesn't stop it.
- `onWorkerNotStarted` is called once, right after `worker not started` is written, with the same error. It isn't called when `startWorker()` rejects, when the worker starts, or when the process was shut down before the boot finished.
- A callback that throws or rejects writes `onWorkerNotStarted failed` at error with `err`. The library never exits the process for you under `next start`.

## Bundling

Turbopack, the default bundler of `next dev` and `next build`, needs no `next.config` entry. With webpack (`next build --webpack`), list the library in `serverExternalPackages`:

```ts
// next.config.ts
import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  serverExternalPackages: ["@flefebvre/nextasking"],
};

export default nextConfig;
```

- `output: "standalone"` needs no `next.config` entry: the standalone output carries the library, and its `server.js` runs the worker, even copied out of the project.

## Log lines

| Level   | Message                                  | Fields                                |
| ------- | ---------------------------------------- | ------------------------------------- |
| `debug` | `run yielded at shutdown`                | `task`, `runId`, `attempt`            |
| `debug` | `attempt aborted: lock lost`             | `task`, `runId`, `attempt`            |
| `debug` | `run handed back: unknown task`          | `task`, `runId`, `attempt`            |
| `debug` | `runs passed over: newer record version` | `recordVersion`, `supportedVersion`   |
| `warn`  | `attempt stalled`                        | `task`, `runId`, `attempt`, `stalls`  |
| `warn`  | `attempt outcome discarded: lock lost`   | `task`, `runId`, `attempt`, `outcome` |
| `warn`  | `attempts aborted at shutdown deadline`  | none                                  |
| `warn`  | `maxmemory-policy unreadable`            | `err`                                 |
| `warn`  | `server version unreadable`              | `err`                                 |
| `error` | `worker shutdown failed`                 | `err`                                 |
| `error` | `worker not started`                     | `err`                                 |
| `error` | `onWorkerNotStarted failed`              | `err`                                 |
| `error` | `worker error`                           | `err`                                 |

See [Logging](logging.md#lifecycle-lines) for what each line means and for the other lines.
