# Logging

nextasking writes a fixed line when a run or the worker changes state, through the logger you pass. Without one, it writes nothing.

## Pass a logger

Pass `logger` to `createNextasking`. It takes any object with `debug`, `info`, `warn` and `error`, each called `(fields, message)`: pino's order.

```ts
// src/nextasking/index.ts
import { createNextasking } from "@flefebvre/nextasking";
import pino from "pino";

export const nextasking = createNextasking({
  connection: { url: process.env.REDIS_URL ?? "redis://localhost:6379" },
  logger: pino({ name: "nextasking" }),
  tasks: [sendWelcomeEmail],
});
```

- `logger: console` works as it is, for development: each line prints its fields, then its message.
- Leave `logger` out and the library is silent.
- Wrap another library's logger in an object of the four functions if its argument order differs.

## Lifecycle lines

Each message is fixed: filter and alert on it.

| Level   | Message                                                                                                         | Fields                                                                                                      |
| ------- | --------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `debug` | `run dispatched`                                                                                                | `task`, `runId`                                                                                             |
| `debug` | `duplicate dispatch dropped`                                                                                    | `task`, `runId` for a dispatch; `eventName`, `eventId` for an emit                                          |
| `debug` | `run waiting for a concurrency slot`                                                                            | `task`, `runId`, `attempt`                                                                                  |
| `debug` | `run got a concurrency slot`                                                                                    | `task`, `runId`, `attempt`                                                                                  |
| `debug` | `attempt started`                                                                                               | `task`, `runId`, `attempt`                                                                                  |
| `debug` | `run suspended`                                                                                                 | `task`, `runId`, `attempt`, `wakeAt`                                                                        |
| `debug` | `run resumed`                                                                                                   | `task`, `runId`, `attempt`                                                                                  |
| `debug` | `run yielded at shutdown`                                                                                       | `task`, `runId`, `attempt`                                                                                  |
| `debug` | `attempt aborted: lock lost`                                                                                    | `task`, `runId`, `attempt`                                                                                  |
| `debug` | `run handed back: unknown task`                                                                                 | `task`, `runId`, `attempt`                                                                                  |
| `debug` | `fan-out handed back: unknown event`                                                                            | `eventName`, `eventId`                                                                                      |
| `debug` | `firing dropped: task not scheduled`                                                                            | `task`, `scheduledAt`                                                                                       |
| `info`  | `schedule added`                                                                                                | `task`, `cron`, `timezone`                                                                                  |
| `info`  | `schedule changed`                                                                                              | `task`, `cron`, `timezone`                                                                                  |
| `info`  | `schedule removed`                                                                                              | `task`, `cron`, `timezone`                                                                                  |
| `info`  | `run completed`                                                                                                 | `task`, `runId`, `attempt`, `runDurationInMilliseconds`, `executionDurationInMilliseconds`                  |
| `info`  | `run replayed`                                                                                                  | `task`, `runId`                                                                                             |
| `info`  | `worker started`                                                                                                | none                                                                                                        |
| `info`  | `worker stopped`                                                                                                | none                                                                                                        |
| `info`  | `connection restored`                                                                                           | none                                                                                                        |
| `warn`  | `no listeners for event`                                                                                        | `eventName`, `eventId`                                                                                      |
| `warn`  | `attempt failed`                                                                                                | `task`, `runId`, `attempt`, `err`, `retriesLeft`, `backoffInMilliseconds`                                   |
| `warn`  | `attempt stalled`                                                                                               | `task`, `runId`, `attempt`                                                                                  |
| `warn`  | `attempts aborted at shutdown deadline`                                                                         | none                                                                                                        |
| `warn`  | `maxmemory-policy unreadable`                                                                                   | `err`                                                                                                       |
| `warn`  | `NEXT_MANUAL_SIG_HANDLE is not set: on SIGTERM Next exits at once and cuts the attempts running in this worker` | none                                                                                                        |
| `error` | `run failed`                                                                                                    | `task`, `runId`, `attempt`, `err`, `reason`, `runDurationInMilliseconds`, `executionDurationInMilliseconds` |
| `error` | `fan-out failed`                                                                                                | `eventName`, `eventId`, `err`                                                                               |
| `error` | `worker shutdown failed`                                                                                        | `err`                                                                                                       |
| `error` | `connection error`                                                                                              | `err`                                                                                                       |
| `error` | `worker not started`                                                                                            | `err`                                                                                                       |
| `error` | `queue error`                                                                                                   | `err`                                                                                                       |
| `error` | `worker error`                                                                                                  | `err`                                                                                                       |

- `run dispatched` comes before any attempt, so it has no `attempt`. Every line about an attempt has `task`, `runId` and `attempt`.
- A listener's run adds `eventName` and `eventId` to each of its lines, the handler's own included.
- `run dispatched` is written by `dispatch` only: a listener's run started by an emit has no such line. Neither has a scheduled task's run started by a firing, and a repeated firing writes no `duplicate dispatch dropped`: it collapses into the same run through its run id. `emit` itself writes no line, unless the event has no listeners or the call repeats an `eventId`.
- `duplicate dispatch dropped` replaces `run dispatched` when the call repeats a `runId` or an `eventId` still within its dedup window. See [Tasks and events](tasks-and-events.md#the-dedup-window).
- `run waiting for a concurrency slot` is written when a run finds its task's or key's slots all taken and waits; `run got a concurrency slot` when a run that waited takes one, before its `attempt started` or `run resumed`. See [Concurrency](concurrency.md).
- `run suspended` is written when a sleep suspends the run, with `wakeAt` as an ISO string. `run resumed` replaces `attempt started` when the run is picked up again after it: the attempt is the same. See [Steps](steps.md#sleep-for-minutes-or-days).
- `runDurationInMilliseconds`: from the dispatch to the completion or the final failure. `executionDurationInMilliseconds`: from the start, or the resume, of the last attempt to its end.
- `attempt failed` is written only when a retry follows, and `run failed` when the run fails for good: `err` is the attempt's `Error`, `reason` is `"retries exhausted"`, `"non-retriable"`, `"stalled"` or `"unknown task"`. See [Retries and failures](retries-and-failures.md).
- `fan-out failed` is written when a fan-out fails for good, stalls and an event no worker defines included: `err` is the `Error` it failed with. No `no listeners for event` line is written for a built-in event. See [Retries and failures](retries-and-failures.md#react-to-a-failure).
- `run completed` is written once the completion is stored: an attempt that lost its lock never writes it.
- `run replayed` is written by `replay`, in the process that calls it, before the run's next attempt; it has no `attempt`. See [Retries and failures](retries-and-failures.md#replay-a-failed-run).
- `run yielded at shutdown`, `attempt aborted: lock lost`, `attempt stalled`, `attempts aborted at shutdown deadline`, `worker shutdown failed` and the `NEXT_MANUAL_SIG_HANDLE` warning come from the worker's shutdown and lock handling. See [Worker](worker.md).
- `attempt stalled` is written by the worker that found the stall, which may be another instance than the one that ran the attempt.
- `run handed back: unknown task` and `fan-out handed back: unknown event` are written by a worker that doesn't define the run's task or the fan-out's event, each time it hands it back during a deploy overlap. See [Worker](worker.md#overlapping-releases).
- `schedule added`, `schedule changed` and `schedule removed` are written by `startWorker()`, one per schedule it reconciles, before `worker started`. `cron` and `timezone` are the new values, or for `schedule removed` the stored ones. `firing dropped: task not scheduled` is written when a firing reaches a release that no longer schedules its task, with `scheduledAt` as an ISO string. See [Scheduled tasks](scheduled-tasks.md#what-a-deploy-does-to-schedules).
- `worker started` is written when `startWorker()` has checked the `maxmemory-policy`, reconciled the schedules, re-sent the failure notices a crash lost, and started the worker. With Redis down at boot, that happens once Redis is back, after `startWorker()` has resolved. The `NEXT_MANUAL_SIG_HANDLE` warning, when it applies, comes right after it, or before it when the boot waits for Redis.
- `connection error` is written when a client of the instance loses Redis and no outage is already logged, `err` being the first error; `connection restored` when every client of the instance is connected again. One pair per outage and per process, however many clients and calls it affects.
- `queue error` and `worker error` are written when the BullMQ queue or worker under the instance reports an error that isn't a lost connection: a Lua script error, a lock it could not renew, a corrupted job. `err` is that error. A lost connection writes `connection error` instead, never these.
- `maxmemory-policy unreadable` and `worker not started` are written by `startWorker()` at boot. `worker not started` is written only when the boot ran in the background because Redis was down, and failed, such as on an evicting `maxmemory-policy`; `err` says why. See [Worker](worker.md#when-redis-is-down-at-boot).
- Payloads are never logged, at any level.

## Log from a handler

Use the handler's `logger`, not your app logger: every line it writes carries `task`, `runId` and `attempt`, and in a listener `eventName` and `eventId` too.

```ts
export const sendWelcomeEmail = defineTask("send-welcome-email", {
  schema: z.object({ userId: z.string(), email: z.email() }),
  handler: async ({ payload, logger }) => {
    const { messageId } = await email.send({ to: payload.email, template: "welcome" });
    logger.info({ userId: payload.userId, messageId }, "welcome email sent");
  },
});
```

- It writes through the logger passed to `createNextasking`, and writes nothing when none was passed.
- `task`, `runId`, `attempt`, `eventName` and `eventId` always hold the run's values, even if your fields set the same keys.
- Log ids, not payload values: the payload is yours to keep out of the logs.
