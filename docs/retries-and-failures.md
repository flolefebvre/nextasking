# Retries and failures

A handler that throws fails its attempt. The run retries after a backoff until its retries are spent, then fails for good and is kept 14 days. Throw `NonRetriableError` to fail it at once.

## Set retries and backoff

```ts
// src/nextasking/tasks/sync-account.ts
import { defineTask } from "@flefebvre/nextasking";
import { z } from "zod";

export const syncAccount = defineTask("sync-account", {
  schema: z.object({ accountId: z.string() }),
  retries: 3,
  backoff: "1m",
  handler: async ({ payload }) => {
    await crm.sync(payload.accountId);
  },
});
```

- `retries` (default `5`) counts the retries after the first attempt: `retries: 0` runs once, the default makes up to 6 attempts. It must be a whole number, 0 or more, or `createNextasking` throws.
- `backoff` (default `"30s"`) is the wait before the first retry. Each later wait doubles.
- Each wait is jittered to between 80% and 100% of its value, so runs that failed together don't retry together.
- The default schedule waits up to 30 s, 1 min, 2 min, 4 min, then 8 min: about 12 to 16 minutes in all, enough to outlast a dependency's deploy.
- The retry count is fixed when the run is stored: by the release that dispatched, for a listener run by the worker that ran the fan-out, for a firing's run by the worker that processed the firing. Each wait uses the `backoff` of the worker that ran the failed attempt. See [Deploys](deploys.md#which-release-decides-what).
- A listener takes the same options, for its own runs.
- Make the handler safe to run again: a retry starts it from the top. Wrap each side effect in `step.run` so a retry reuses its result instead of repeating it; see [Steps](steps.md).

## Fail a run at once

Throw `NonRetriableError` when retrying can't help: a declined card, a deleted account, a payload the handler can't use.

```ts
import { defineTask, NonRetriableError } from "@flefebvre/nextasking";

export const chargeCard = defineTask("charge-card", {
  schema: z.object({ invoiceId: z.string() }),
  handler: async ({ payload }) => {
    const invoice = await db.invoice.find(payload.invoiceId);
    if (invoice === null) {
      throw new NonRetriableError(`Invoice ${payload.invoiceId} no longer exists`);
    }
    await payments.charge(invoice);
  },
});
```

- The run fails after this attempt, whatever retries are left, with `reason: "non-retriable"`.
- Only the thrown error counts: a `NonRetriableError` wrapped as the `cause` of another error is retried like any error.
- `NonRetriableError` takes the arguments of `Error`: a message, and `{ cause }`.
- A `StepMisuseError` fails the run at once the same way, even when the handler catches it. See [Steps](steps.md#misuse-fails-the-run-at-once).

## Bound an attempt with timeout

```ts
export const renderReport = defineTask("render-report", {
  schema: z.object({ reportId: z.string() }),
  timeout: "2m",
  handler: async ({ payload }) => {
    await reports.render(payload.reportId);
  },
});
```

- `timeout` bounds each attempt. Without it, an attempt has no time limit.
- In a run that sleeps, `timeout` bounds each stretch: from the start or the resume to the sleep, and from the last resume to the end. Time spent sleeping never counts. See [Steps](steps.md#sleep-for-minutes-or-days).
- An attempt still running at its timeout fails with an error named `AttemptTimeoutError`, whose message names the task and the timeout, and retries like any failure.
- The handler isn't stopped: its promise keeps running in the background after the timeout. Don't rely on a timeout to cancel work.

## What a failed run keeps

A run fails for good when its last attempt fails, when it throws `NonRetriableError` or misuses a step, when it stalls a third time with no step saved in between (see [Worker](worker.md#stalls)), or when no worker defines its task for 30 minutes (see [Worker](worker.md#overlapping-releases)). It is then kept:

- for 14 days, and at most 10,000 failed runs and fan-outs per queue: the oldest are removed first;
- with its payload and its saved steps;
- with one error record per failed attempt: `{ attempt, name, message, stack, failedAt }`, `failedAt` an ISO string;
- with its `reason`: `"retries exhausted"`, `"non-retriable"`, `"stalled"` or `"unknown task"`; a stalled run's last error record is named `StalledRunError`, an unknown task's `UnknownTaskRunError`.

A thrown value that isn't an `Error`, such as `throw "boom"`, is recorded and logged as an `Error` whose message is that value as a string.

A repeat of a failed run's `runId` is dropped as long as the run is kept: a failure doesn't let the same `runId` run again. See [Tasks and events](tasks-and-events.md#the-dedup-window). Replay it instead.

## Replay a failed run

Deploy the fix, then replay the run from server code: an admin route, a server action, a script.

```ts
// src/app/admin/runs/actions.ts
"use server";
import { RunNotReplayableError } from "@flefebvre/nextasking";
import { nextasking } from "@/nextasking";

export async function replayRun(runId: string) {
  try {
    await nextasking.replay("sync-account", runId);
  } catch (error) {
    if (error instanceof RunNotReplayableError) {
      return { replayed: false, message: error.message };
    }
    throw error;
  }
  return { replayed: true };
}
```

- `replay(taskName, runId)` resolves with `{ runId }`. The run keeps its `runId` and its payload, and goes back to pending.
- It reuses the steps the run saved: a step that succeeded before the failure returns its saved result and doesn't run again.
- The retry budget starts over: the next attempt is `attempt` 1, and the run gets the retry count it was stored with.
- The failed life's `errors` and `reason` are cleared. Read them with `getRun` before you replay if you need them.
- It throws `RunNotReplayableError` when no run with that id is kept (unknown, or removed after 14 days) or when the run isn't failed: it is still pending or retrying, it completed, or it was already replayed. It throws `UnknownTaskError` for a task name the instance doesn't list.
- A replayed run that fails again is a new failure: it keeps its new errors and `reason`, and can be replayed again.

## Replay a failed fan-out

A fan-out fails when the worker can't turn an event into its listener runs: Redis kept failing, the worker stalled, or no worker defined the event for 30 minutes. Replay it with the event's name and id:

```ts
await nextasking.replayEvent("invoice.paid", eventId);
```

- `replayEvent(eventName, eventId)` resolves with `{ eventId }`. The fan-out runs again from the listener names it saved when it was emitted.
- A listener that already has a run is skipped, whatever that run's state: replaying a fan-out never runs a listener twice.
- It throws `FanOutNotReplayableError` when no fan-out with that id is kept or when it isn't failed, and `UnknownEventError` for an event name the instance doesn't list.
- A failed listener run is a failed run: replay it on its own with `replay(listenerName, runId)`. The other listeners don't run again. `getEvent` lists each listener's `runId`; see [Dashboard](dashboard.md).

## React to a failure

The library emits a built-in event on each final failure. Listen to it with an ordinary listener, so the alert is itself a retried, durable run.

```ts
// src/nextasking/tasks/alert-on-run-failed.ts
import { defineListener, runFailed } from "@flefebvre/nextasking";

export const alertOnRunFailed = defineListener("alert-on-run-failed", {
  on: runFailed,
  handler: async ({ payload, step }) => {
    await step.run("page-on-call", () =>
      pager.trigger(`${payload.task} failed (${payload.reason}): ${payload.error.message}`, { runId: payload.runId }),
    );
  },
});
```

```ts
// src/nextasking/index.ts
export const nextasking = createNextasking({
  connection: { url: process.env.REDIS_URL! },
  events: [invoicePaid],
  tasks: [syncAccount, recordInvoicePayment, alertOnRunFailed],
});
```

- List the listener in `tasks`. Don't list `runFailed` or `fanOutFailed` in `events`: they are added automatically, and `createNextasking` throws if you list them.
- `runFailed` (`nextasking/run.failed`) carries `{ task, runId, reason, error: { name, message, stack }, attempt, failedAt }`, plus `eventName` and `eventId` when the failed run is a listener's. `failedAt` is an ISO string.
- `fanOutFailed` (`nextasking/fan-out.failed`) carries `{ eventName, eventId, error: { name, message, stack }, failedAt }`. Replay the fan-out with `replayEvent(eventName, eventId)`.
- Neither carries the failed run's payload. Read it with `getRun(task, runId)` if the alert needs it.
- One notice per final failure: a run that is replayed and fails again sends a second one. A failed run sends its notice right after the failure is saved. If a crash loses it, the next `startWorker()` re-sends the unsent notices among the failures of the last 24 hours. A re-sent notice keeps its event id, derived from the failure, so it falls within the [dedup window](tasks-and-events.md#the-dedup-window) and still starts one run per listener.
- A failed run of a listener of a built-in event sends no notice, and neither does a failed fan-out of a built-in event, so an alert that fails can't loop. Its failure is still logged and replayable.
- With no listener on a built-in event, its notices are dropped silently: no `no listeners for event` line. A listener added later doesn't receive past failures.
- `emit` doesn't take the built-in events, in its types or at runtime, and `isEventName` is false for them. `EventName`, `getEvent`, `replayEvent` and `eventDefinitions` include them.
- The types are exported as `RunFailedPayload`, `FanOutFailedPayload` and `FailureNoticeError`.

## Log lines

| Level   | Message          | Fields                                                                                                      |
| ------- | ---------------- | ----------------------------------------------------------------------------------------------------------- |
| `info`  | `run replayed`   | `task`, `runId`                                                                                             |
| `warn`  | `attempt failed` | `task`, `runId`, `attempt`, `err`, `retriesLeft`, `backoffInMilliseconds`                                   |
| `error` | `run failed`     | `task`, `runId`, `attempt`, `err`, `reason`, `runDurationInMilliseconds`, `executionDurationInMilliseconds` |
| `error` | `fan-out failed` | `eventName`, `eventId`, `err`                                                                               |

- `run replayed` is written by the process that calls `replay`, through its own instance's logger.
- `attempt failed` is written only when a retry follows; a final failure writes `run failed` alone.
- `fan-out failed` is written when a fan-out fails for good, stalls included.
- `err` is the `Error` the attempt failed with, under pino's `err` key so pino serializes it.
- `backoffInMilliseconds` is the wait actually applied before the next attempt.
- See [Logging](logging.md) for the other lines.
