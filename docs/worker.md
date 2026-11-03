# Worker

`startWorker()` runs one worker in the process that calls it: `next start`, or the `nextasking worker` bin. This page covers its options, where it runs, what a deploy does to the attempts it is running, how a handler stops cleanly, and what a stall is.

## Start the worker with options

```ts
// src/instrumentation.ts
export async function register() {
  if (process.env.NEXT_RUNTIME !== "nodejs" || process.env.NEXTASKING_WORKER !== "on") return;
  const { nextasking } = await import("@/nextasking");
  await nextasking.startWorker({ concurrency: 20, shutdownTimeout: "25s" });
}
```

| Option            | Default   | What it does                                                          |
| ----------------- | --------- | --------------------------------------------------------------------- |
| `concurrency`     | unlimited | The most attempts this process runs at once, a whole number 1 or more |
| `shutdownTimeout` | `"25s"`   | How long a shutdown waits for running attempts before it aborts them  |

- `startWorker` rejects an invalid option before opening any connection.
- Then it checks the server's `maxmemory-policy`. See [Give queues their own Valkey or Redis](#give-queues-their-own-valkey-or-redis).
- Before it takes its first attempt, `startWorker` reconciles the schedules stored in Redis with the instance's scheduled tasks. See [Scheduled tasks](scheduled-tasks.md#what-a-deploy-does-to-schedules).
- Then it re-sends the failure notices a crash lost among the failures of the last 24 hours. See [Retries and failures](retries-and-failures.md#react-to-a-failure).
- These boot steps finish before `worker started` is written and before `startWorker()` resolves, unless Redis is down. See [When Redis is down at boot](#when-redis-is-down-at-boot).
- Type the options with `StartWorkerOptions`.

## Cap the attempts in flight with concurrency

Without `concurrency`, a worker starts every run that is pending: a backlog of 5,000 runs starts 5,000 attempts at once in one process. Set it when your handlers hold memory, database connections or rate-limited API calls.

```ts
await nextasking.startWorker({ concurrency: 10 });
```

- It counts this process only: two instances with `concurrency: 10` run up to 20 attempts.
- It caps every task together, in this process. To cap one task across every instance, or per account, use the task's own `concurrency: { limit }` or `{ key, limit }`; see [Concurrency](concurrency.md).
- A sleeping run holds no slot: it frees it until it resumes.

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

1. The worker stops taking new attempts. The HTTP server keeps answering.
2. A running attempt that reaches a step boundary (any `step.run`, `step.sleep`, `step.sleepUntil`, `emit` or `dispatch`) lets its steps in flight finish and save, then goes back to pending at once. Another instance resumes it from its saved steps. This uses no retry and counts no stall.
3. An attempt that ends before reaching a boundary completes or fails as usual.
4. At `shutdownTimeout`, the attempts still running get their `abortSignal` aborted. The worker waits up to one second more for them to stop, then closes and the process exits with code 0.

- Keep `shutdownTimeout` plus one second under your platform's grace period before SIGKILL (30 seconds on Kubernetes and most hosts).
- An attempt still running when the process exits is cut. Its run comes back as a stall. See [Stalls](#stalls).
- Without `NEXT_MANUAL_SIG_HANDLE`, Next exits on SIGTERM at once and cuts every running attempt. The worker then warns at startup with `NEXT_MANUAL_SIG_HANDLE is not set: on SIGTERM Next exits at once and cuts the attempts running in this worker`, except under `NODE_ENV=development`. See [Dev](dev.md#what-ctrl-c-does-to-the-attempt-in-flight).
- With `NEXT_MANUAL_SIG_HANDLE` set, Next leaves both signals to your code: the worker exits the process once it has stopped, with code 1 if stopping itself failed.
- The `nextasking worker` bin sets `NEXT_MANUAL_SIG_HANDLE` itself. See [Run the worker in its own process](#run-the-worker-in-its-own-process).

## Run the worker in its own process

Run CPU-heavy or long tasks in a process of their own with the `nextasking worker` bin, on the same build as `next start`:

```json
// package.json
{
  "scripts": {
    "build": "next build",
    "start": "next start",
    "worker": "NEXTASKING_WORKER=on nextasking worker"
  }
}
```

- Run it after `next build`, from the app's directory. It loads that build the way `next start` does, so `register()` in `instrumentation.ts` runs and task code resolves the same modules, `server-only` and `@/` imports included.
- It answers no HTTP and binds no port. Deploy it as a worker component with no route and no port.
- It sets `NEXT_MANUAL_SIG_HANDLE` itself, and `NODE_ENV=production` when `NODE_ENV` is unset. A SIGTERM drains the attempts in flight as in [Let a deploy finish the running attempts](#let-a-deploy-finish-the-running-attempts), then the process exits 0.
- When `register()` doesn't call `startWorker()`, the bin exits 1 with this line on stderr. Turn on, in the bin's environment, the variable your `instrumentation.ts` checks:

```text
nextasking worker: register() started no worker. Turn on the environment variable your instrumentation.ts checks before calling startWorker().
```

- `nextasking worker` takes no other argument; anything else prints `Usage: nextasking worker` and exits 1.

### Choose which processes run a worker

`instrumentation.ts` starts the worker behind an environment variable you own, `NEXTASKING_WORKER` in these examples. Set it in each process that should run tasks:

| Process                    | Command                                                    | Runs a worker |
| -------------------------- | ---------------------------------------------------------- | ------------- |
| Web server with the worker | `NEXT_MANUAL_SIG_HANDLE=1 NEXTASKING_WORKER=on next start` | yes           |
| Web server only            | `next start`                                               | no            |
| Worker only                | `NEXTASKING_WORKER=on nextasking worker`                   | yes           |

- A process that never calls `startWorker()` only dispatches and emits.
- Run as many workers as you need, embedded, separate or both: each takes attempts from the same queues. `concurrency` counts per process.

### Run the bin with `output: "standalone"`

Next bundles the library into its server chunks, so the standalone output holds no copy of the bin unless you list it. Add both files to `outputFileTracingIncludes`; `package.json` makes Node load the bin as an ES module:

```ts
// next.config.ts
import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: "standalone",
  outputFileTracingIncludes: {
    "/*": ["./node_modules/@flefebvre/nextasking/package.json", "./node_modules/@flefebvre/nextasking/dist/bin.js"],
  },
};

export default nextConfig;
```

The standalone output has no `node_modules/.bin`, so run the bin by its path, from inside the standalone directory or a copy of it:

```sh
# the web process
cd .next/standalone && node server.js
# the worker process
cd .next/standalone && NEXTASKING_WORKER=on node node_modules/@flefebvre/nextasking/dist/bin.js worker
```

- The bin reads the build's config from `.next/required-server-files.json`, as the generated `server.js` does, and runs with `NODE_ENV=production`.
- The standalone directory runs on its own: copy it into an image without the project's `node_modules`.

## Stop a handler cleanly with abortSignal

The handler receives `abortSignal`. It aborts when the attempt must stop: at the shutdown deadline, or when the attempt lost its lock. Pass it to the calls that accept one, and check it in long loops.

```ts
export const exportOrders = defineTask("export-orders", {
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

- Once it has aborted, every `step.*`, `emit` and `dispatch` of the attempt throws its `reason` without running: an `Error` whose message says why.
- Let that error through. At the shutdown deadline the run goes back to pending and resumes elsewhere from its saved steps, using no retry.
- An attempt that lost its lock can no longer complete or save anything, whatever the handler does: another attempt may own the run. Stop rather than finish the work twice.
- Test `abortSignal.aborted` to know whether the attempt was aborted.

## Stalls

An attempt holds a lock on its run for 2 minutes, renewed while it runs. A stall is an attempt that lost its lock: its process crashed, was killed, or blocked its event loop longer than the lock. Every 30 seconds a worker looks for stalled runs and sends them back to pending, where the next attempt reuses the saved steps.

- The third stall with no step saved in between fails the run with `reason: "stalled"`, whatever retries it has left. Each saved step starts the count again.
- A stall uses no retry: `attempt` stays the same.
- A cut attempt comes back within about three minutes: the lock expires, then a stall check finds it.
- These timings are fixed, the same in dev and in production.
- Keep CPU-heavy work off the event loop (a worker thread, a separate service), so a long computation doesn't stall its own attempt.

## Overlapping releases

During a deploy, an old and a new release run side by side. A worker that picks up a run of a task it doesn't define, or a fan-out of an event it doesn't define, hands it back for another worker instead of failing it:

- It hands it back 10 seconds later, using no retry, no stall and no concurrency slot. A worker of the release that defines the name runs it as usual, with `attempt` unchanged.
- The first time a worker finds the name unknown is saved with the run. If no worker has taken it 30 minutes later, the run fails with `reason: "unknown task"` and an error record named `UnknownTaskRunError`, kept like any failed run; see [Retries and failures](retries-and-failures.md#what-a-failed-run-keeps). A fan-out fails the same way, and starts no listener run.
- Each hand-back writes `run handed back: unknown task`, or `fan-out handed back: unknown event`, at debug.
- These timings are fixed.

## Give queues their own Valkey or Redis

Point `connection` at a Valkey or Redis that holds only queues, not your cache or sessions. A cache evicts keys when memory runs short; a queue loses runs that way. A dedicated server also keeps a cache flush or a busy cache from stalling attempts.

Set `maxmemory-policy noeviction` on it. The worker reads the policy at boot:

- A readable policy other than `noeviction` makes `startWorker()` reject with a one-line error naming it. An evicted lock lets a second worker run the same attempt.
- A policy it can't read, `CONFIG` being disabled or denied as on many managed services, writes `maxmemory-policy unreadable` at warn with `err`, and the worker starts. Check the policy in your provider's console.

```text
maxmemory-policy is "allkeys-lru": the Valkey or Redis holding the queues must use "noeviction", or it evicts locks and runs
```

## When Redis is down at boot

`startWorker()` never blocks `next start` on Redis:

- When Redis doesn't answer its first command within `connection.giveUpAfter` (default `"5s"`), `startWorker()` resolves anyway and writes `connection error`. Pages keep rendering; route handlers calling the instance get `RedisUnavailableError`, see [Tasks and events](tasks-and-events.md#when-redis-is-down).
- The boot steps wait for Redis in the background: once it answers, the worker checks the policy, reconciles the schedules, re-sends the lost failure notices, then writes `worker started` and takes attempts. No attempt starts before.
- An evicting policy found in the background writes `worker not started` at error, `err` naming the policy, and the worker stays off until the process restarts.
- A SIGTERM while the worker waits for Redis stops it at once: nothing to drain.

Once started, the worker waits out an outage: its commands, and a handler's `emit` and `dispatch`, wait for Redis to come back instead of failing the attempt. A run whose completion was stored just before the cut, its reply lost, is read back after the reconnect: it writes `run completed`, or `run completion unconfirmed` when Redis can't tell; see [Logging](logging.md#lifecycle-lines). A lock that expires during a long outage aborts the attempt like any lost lock; see [Stalls](#stalls).

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

## Log lines

| Level   | Message                                 | Fields                     |
| ------- | --------------------------------------- | -------------------------- |
| `debug` | `run yielded at shutdown`               | `task`, `runId`, `attempt` |
| `debug` | `run handed back: unknown task`         | `task`, `runId`, `attempt` |
| `debug` | `fan-out handed back: unknown event`    | `eventName`, `eventId`     |
| `debug` | `attempt aborted: lock lost`            | `task`, `runId`, `attempt` |
| `warn`  | `attempt stalled`                       | `task`, `runId`, `attempt` |
| `warn`  | `attempts aborted at shutdown deadline` | none                       |
| `warn`  | `maxmemory-policy unreadable`           | `err`                      |
| `error` | `worker shutdown failed`                | `err`                      |
| `error` | `worker not started`                    | `err`                      |
| `error` | `worker error`                          | `err`                      |
| `error` | `queue error`                           | `err`                      |

See [Logging](logging.md) for the other lines.
