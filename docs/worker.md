# Worker

`startWorker()` runs one worker in the process that calls it. This page covers its options, what a deploy does to the attempts it is running, how a handler stops cleanly, and what a stall is.

## Start the worker with options

```ts
// src/instrumentation.ts
export async function register() {
  if (process.env.NEXT_RUNTIME !== "nodejs" || process.env.NEXTASKING_WORKER !== "embedded") return;
  const { nextasking } = await import("@/nextasking");
  await nextasking.startWorker({ concurrency: 20, shutdownTimeout: "25s" });
}
```

| Option            | Default   | What it does                                                          |
| ----------------- | --------- | --------------------------------------------------------------------- |
| `concurrency`     | unlimited | The most attempts this process runs at once, a whole number 1 or more |
| `shutdownTimeout` | `"25s"`   | How long a shutdown waits for running attempts before it aborts them  |

- `startWorker` rejects an invalid option before opening any connection.
- Before it takes its first attempt, `startWorker` reconciles the schedules stored in Redis with the instance's scheduled tasks. See [Scheduled tasks](scheduled-tasks.md#what-a-deploy-does-to-schedules).
- Then it re-sends the failure notices a crash lost among the failures of the last 24 hours. See [Retries and failures](retries-and-failures.md#react-to-a-failure).
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
    "start": "NEXT_MANUAL_SIG_HANDLE=1 NEXTASKING_WORKER=embedded next start"
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
| `error` | `worker shutdown failed`                | `err`                      |

See [Logging](logging.md) for the other lines.
