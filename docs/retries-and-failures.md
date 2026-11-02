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
- The retry count is fixed when the run is stored, from the `retries` of the release that dispatched or emitted. Each wait uses the `backoff` of the worker that ran the failed attempt.
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
- An attempt still running at its timeout fails with an error named `AttemptTimeoutError`, whose message names the task and the timeout, and retries like any failure.
- The handler isn't stopped: its promise keeps running in the background after the timeout. Don't rely on a timeout to cancel work.

## What a failed run keeps

A run fails for good when its last attempt fails, or when it throws `NonRetriableError` or misuses a step. It is then kept:

- for 14 days, and at most 10,000 failed runs and fan-outs per queue: the oldest are removed first;
- with its payload and its saved steps;
- with one error record per failed attempt: `{ attempt, name, message, stack, failedAt }`, `failedAt` an ISO string;
- with its `reason`: `"retries exhausted"` or `"non-retriable"`.

A thrown value that isn't an `Error`, such as `throw "boom"`, is recorded and logged as an `Error` whose message is that value as a string.

A repeat of a failed run's `runId` is dropped as long as the run is kept: a failure doesn't let the same `runId` run again. See [Tasks and events](tasks-and-events.md#the-dedup-window).

## Log lines

| Level   | Message          | Fields                                                                                                      |
| ------- | ---------------- | ----------------------------------------------------------------------------------------------------------- |
| `warn`  | `attempt failed` | `task`, `runId`, `attempt`, `err`, `retriesLeft`, `backoffInMilliseconds`                                   |
| `error` | `run failed`     | `task`, `runId`, `attempt`, `err`, `reason`, `runDurationInMilliseconds`, `executionDurationInMilliseconds` |

- `attempt failed` is written only when a retry follows; a final failure writes `run failed` alone.
- `err` is the `Error` the attempt failed with, under pino's `err` key so pino serializes it.
- `backoffInMilliseconds` is the wait actually applied before the next attempt.
- See [Logging](logging.md) for the other lines.
