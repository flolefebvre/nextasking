# Logging

nextasking writes a fixed line when a run, an attempt or the worker changes state, through the logger you pass. Without one, it writes nothing.

## Pass a logger

Pass `logger` to `createNextasking`. It takes any object with `debug`, `info`, `warn` and `error`, each called `(fields, message)`: pino's order.

```ts
// src/nextasking/index.ts
import { createNextasking } from "@flefebvre/nextasking";
import pino from "pino";

export const nextasking = createNextasking({
  connection: { url: process.env.REDIS_URL ?? "redis://localhost:6379" },
  logger: pino({ name: "nextasking" }),
  tasks: [sendWelcomeEmailTask],
});
```

- `logger: console` works as it is, for development: each line prints its fields, then its message.
- Leave `logger` out and the library is silent.
- Wrap another library's logger in an object of the four functions if its argument order differs.

## Lifecycle lines

Each message is fixed: filter and alert on it.

| Level   | Message                                                                                                         | Fields                                                                                                                                         |
| ------- | --------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `debug` | `run dispatched`                                                                                                | `task`, `runId`                                                                                                                                |
| `debug` | `duplicate dispatch dropped`                                                                                    | `task`, `runId` for a dispatch; `eventName`, `eventId` for an emit; `task`, `runId`, `eventName`, `eventId` for a listener skipped on a repeat |
| `debug` | `run waiting for a concurrency slot`                                                                            | `task`, `runId`, `attempt`                                                                                                                     |
| `debug` | `attempt started`                                                                                               | `task`, `runId`, `attempt`                                                                                                                     |
| `debug` | `run suspended`                                                                                                 | `task`, `runId`, `attempt`, `resumesAt`                                                                                                        |
| `debug` | `run resumed`                                                                                                   | `task`, `runId`, `attempt`                                                                                                                     |
| `debug` | `run yielded at shutdown`                                                                                       | `task`, `runId`, `attempt`                                                                                                                     |
| `debug` | `attempt aborted: lock lost`                                                                                    | `task`, `runId`, `attempt`                                                                                                                     |
| `debug` | `run handed back: unknown task`                                                                                 | `task`, `runId`, `attempt`                                                                                                                     |
| `debug` | `runs passed over: newer record version`                                                                        | `recordVersion`, `supportedVersion`                                                                                                            |
| `info`  | `schedule added`                                                                                                | `task`, `cron`, `timezone`                                                                                                                     |
| `info`  | `schedule changed`                                                                                              | `task`, `cron`, `timezone`                                                                                                                     |
| `info`  | `schedule removed`                                                                                              | `task`, `cron`, `timezone`                                                                                                                     |
| `info`  | `run completed`                                                                                                 | `task`, `runId`, `attempt`, `runDurationInMilliseconds`, `executionDurationInMilliseconds`                                                     |
| `info`  | `run replayed`                                                                                                  | `task`, `runId`                                                                                                                                |
| `info`  | `worker started`                                                                                                | none                                                                                                                                           |
| `info`  | `worker stopped`                                                                                                | none                                                                                                                                           |
| `info`  | `connection restored`                                                                                           | none                                                                                                                                           |
| `warn`  | `no listeners for event`                                                                                        | `eventName`, `eventId`                                                                                                                         |
| `warn`  | `attempt failed`                                                                                                | `task`, `runId`, `attempt`, `err`, `retriesLeft`, `backoffInMilliseconds`                                                                      |
| `warn`  | `attempt stalled`                                                                                               | `task`, `runId`, `attempt`, `stalls`                                                                                                           |
| `warn`  | `attempt outcome discarded: lock lost`                                                                          | `task`, `runId`, `attempt`, `outcome`                                                                                                          |
| `warn`  | `attempts aborted at shutdown deadline`                                                                         | none                                                                                                                                           |
| `warn`  | `maxmemory-policy unreadable`                                                                                   | `err`                                                                                                                                          |
| `warn`  | `server version unreadable`                                                                                     | `err`                                                                                                                                          |
| `warn`  | `NEXT_MANUAL_SIG_HANDLE is not set: on SIGTERM Next exits at once and cuts the attempts running in this worker` | none                                                                                                                                           |
| `error` | `run failed`                                                                                                    | `task`, `runId`, `attempt`, `err`, `reason`, `runDurationInMilliseconds`, `executionDurationInMilliseconds`                                    |
| `error` | `worker shutdown failed`                                                                                        | `err`                                                                                                                                          |
| `error` | `connection error`                                                                                              | `err`                                                                                                                                          |
| `error` | `worker not started`                                                                                            | `err`                                                                                                                                          |
| `error` | `onWorkerNotStarted failed`                                                                                     | `err`                                                                                                                                          |
| `error` | `worker error`                                                                                                  | `err`                                                                                                                                          |

### Fields every line of a run carries

- Every line about an attempt has `task`, `runId` and `attempt`. `run dispatched`, `duplicate dispatch dropped` and `run replayed` come before an attempt, so they have no `attempt`.
- A listener's run adds `eventName` and `eventId` to each of its lines, the handler's own included. A run a firing started adds `scheduledAt`, an ISO string, the same way.
- Payloads are never logged, at any level.

### Dispatch and emit

- `run dispatched` is written by the `dispatch` that created the run: your app's, in its process, or a handler's, in the worker's process, through the instance's logger. A listener's run created by an emit has no such line, nor has a run a firing created: their first line comes from the worker that picks them up.
- `emit` writes no line itself, unless the event has no listeners (`no listeners for event`) or the call repeats an `eventId`. A built-in event with no listener writes nothing.
- `duplicate dispatch dropped` replaces `run dispatched` when the call repeats an id within its dedup window: a dispatch's `runId` (`task`, `runId`), a whole emit's `eventId` (`eventName`, `eventId`), or, on a repeat of an `eventId` past its window, each listener whose run still blocks its id (`task`, `runId`, `eventName`, `eventId`). See [Tasks and events](tasks-and-events.md#the-dedup-window).
- A handler's `emit` or `dispatch` already sent by an earlier attempt is skipped and writes nothing.
- `run replayed` is written by `replay`, in the process that calls it, before the run's next attempt. See [Retries and failures](retries-and-failures.md#replay-a-failed-run).

### Attempts

- `attempt started` is written when a worker picks the run up. `run resumed` replaces it only after a sleep suspended the run; every other pickup, after a yield or a stall, writes `attempt started` again with the same `attempt`.
- `run waiting for a concurrency slot` is written once each time a run finds its task's or key's slots all taken and waits; a run may wait more than once. See [Concurrency](concurrency.md).
- `run suspended` is written when a sleep suspends the run, with `resumesAt` as an ISO string. See [Steps](steps.md#sleep-for-minutes-or-days).
- `run completed` is written once the completion is stored. `attempt failed` is written when a retry follows, and `run failed` when the run fails for good: `err` is the attempt's `Error`, `reason` is `"retries exhausted"`, `"non-retriable"`, `"stalled"` or `"unknown task"`. See [Retries and failures](retries-and-failures.md).
- `runDurationInMilliseconds` runs from the dispatch to the completion or the final failure; `executionDurationInMilliseconds` from the last `attempt started` or `run resumed` to the end.
- A command whose reply was lost in an outage and sent again writes its line once: `run completed`, `run failed`, `attempt failed`, `run suspended` or `run yielded at shutdown`, from what was stored.

### Locks and stalls

- `attempt aborted: lock lost` is written at debug when an attempt is cut because it lost its lock, its handler still running. The worker that finds the stall warns with `attempt stalled`. See [Worker](worker.md#stalls).
- `attempt outcome discarded: lock lost` is written when a handler finished but its attempt had lost its lock, so its outcome was thrown away: a duplicate attempt ran or is running. `outcome` is what was thrown away: `complete`, `fail`, `retry`, `suspend` or `yield`.
- That line can misreport: when the reply that stored an outcome was lost in an outage, and another attempt took the run and ended before the worker sent it again, the line is written for an outcome that was stored. The run's state is right; the line is not.
- `attempt stalled` is written by the worker that found the stall, which may be another instance than the one that ran the attempt. `stalls` is 1 or 2. The third stall writes no `attempt stalled`: it writes `run failed` with `reason: "stalled"`, once, even when two workers find it.
- Lines from different calls of a worker come in no promised order: `attempt stalled` and the `attempt started` of the pickup that follows can arrive either way round, and so can a stall's or a third stall's line next to the line of a pickup or an attempt. Match them by `runId` and `attempt`.

### Deploy overlaps

- `run handed back: unknown task` is written by a worker whose release doesn't define the run's task, at each hand back, with no `attempt started` before it. A run given up later writes `run failed` with `reason: "unknown task"`. See [Worker](worker.md#overlapping-releases).
- `runs passed over: newer record version` is written by a worker that met runs a newer release wrote, once per newer version it meets in the life of its process. `recordVersion` is that newer version, missing when the worker could not read it; `supportedVersion` is the worker's own.
- `schedule added`, `schedule changed` and `schedule removed` are written by `startWorker()`, one per schedule it reconciles, before `worker started`. `cron` and `timezone` are the new values, or for `schedule removed` the stored ones. See [Scheduled tasks](scheduled-tasks.md#what-a-deploy-does-to-schedules).

### Boot and shutdown

- `worker started` is written once `startWorker()` has checked the server, reconciled the schedules and started taking attempts. With Redis down at boot, that happens once Redis is back, after `startWorker()` has resolved. The `NEXT_MANUAL_SIG_HANDLE` warning, when it applies, comes right after it, or before it when the boot waits for Redis.
- `maxmemory-policy unreadable` is written at boot when neither `INFO` nor `CONFIG GET maxmemory-policy` yields the policy: `err` is an `AggregateError` holding both failures, and the worker starts. `server version unreadable` is written when `INFO` yields no version, with `err`, and the worker starts. See [Worker](worker.md#give-queues-their-own-valkey-or-redis).
- `worker not started` is written only when the boot ran in the background because Redis was down, and failed, such as on an evicting `maxmemory-policy` or a server below the floor; `err` says why. `onWorkerNotStarted` is called right after it, and `onWorkerNotStarted failed` is written when that callback throws or rejects, `err` being what it threw. See [Worker](worker.md#exit-when-the-worker-cant-start).
- `run yielded at shutdown` is written for each attempt that went back to pending at shutdown; `attempts aborted at shutdown deadline` once, when `shutdownTimeout` aborted at least one attempt. `worker stopped` is written once the worker has stopped and both its connections are closed. `worker shutdown failed` is written when stopping failed, before the process exits with code 1. See [Worker](worker.md#let-a-deploy-finish-the-running-attempts).

### Connection and errors

- `connection error` is written when a connection of the instance loses Redis and no outage is already logged, `err` being the first error; `connection restored` when every connection of the instance is back. One pair per outage and per instance, however many connections and calls it affects. A `next start` process holds up to three instances, so its logs can show up to three pairs for one outage, one for each instance that holds a connection. See [Worker](worker.md#start-the-worker-with-options).
- A server that stops answering without closing the connections is noticed by the first call left unanswered for `giveUpAfter`. An instance nobody calls notices it too: each of its connections is checked once quiet for 10 seconds, so it writes `connection error` within 10 seconds plus `giveUpAfter`, about 15 seconds by default.
- After a server that stopped answering without closing the connections, `connection restored` is written once every connection has answered again or reconnected, up to `giveUpAfter` plus one second after the server answers again.
- A server answering the worker's writes with `READONLY`, as a replica or a demoted primary does, is an outage once it has done so for `giveUpAfter`: `connection error` is written then, `err` being the first `READONLY` reply, and `connection restored` once the server accepts one of those writes again. A shorter stretch, such as a failover, writes nothing. A connection drop during that stretch adds no line. On an idle worker, `connection restored` can come up to about a second after the server accepts writes again.
- A server refusing the worker's writes because it is out of memory is an outage from its first refusal: `connection error` is written then, `err` being the server's reply, an `Error` whose message holds `OOM command not allowed`, and `connection restored` once the server accepts one of those writes again. Reads it still answers in between don't end it.
- An instance with no worker writes neither line for a `READONLY` or out-of-memory server: its calls throw `RedisUnavailableError` instead. See [Tasks and events](tasks-and-events.md#when-redis-is-down).
- `worker error` is written when the worker meets something it can't act on that isn't a lost connection: a server error or a reply it can't read; a run or the stored schedules that a newer release rewrote, which this release leaves as they are; a stored `cron` it can't parse, once per stored `cron`; or a listener left out of a failure notice because its concurrency `key` threw. `err` says which. The worker goes on.

### What writes no line

- A run becoming due after its `delay`, sleep or backoff, a run a firing or an emit creates, a waiting run woken for a free slot, a lock renewed, a step saved or read back, ended runs removed by retention, and a worker that loses a race to fire a schedule or fail a third stall write no line.

## Log from a handler

Use the handler's `logger`, not your app logger: every line it writes carries `task`, `runId` and `attempt`, and in a listener `eventName` and `eventId` too.

```ts
export const sendWelcomeEmailTask = defineTask("send-welcome-email", {
  schema: z.object({ userId: z.string(), email: z.email() }),
  handler: async ({ payload, logger }) => {
    const { messageId } = await email.send({ to: payload.email, template: "welcome" });
    logger.info({ userId: payload.userId, messageId }, "welcome email sent");
  },
});
```

- It writes through the logger passed to `createNextasking`, and writes nothing when none was passed.
- `task`, `runId`, `attempt`, `eventName`, `eventId` and `scheduledAt` always hold the run's values, even if your fields use the same names.
- Log ids, not payload values: the payload is yours to keep out of the logs.

## Ship the lines

The `logger` is the one output: there is no hook and no built-in sink.

- Ship the lines through your app's logger, such as a pino transport to Better Stack, Datadog or Axiom.
- React to a failed run with a listener on the built-in event `runFailedEvent`, which runs with retries, not from a log line. See [Retries and failures](retries-and-failures.md#react-to-a-failure).
