# Dashboard

The instance reads its runs back, so you can build your own dashboard: one run with its state, payload, errors and saved steps; a page of runs in one state; the backlog per task; one event from its fan-out to each listener's run; each schedule's firings; a "run now" picker. Every read works from any process with a connection, with or without a worker.

## Protect the dashboard

A dashboard route, page or server action is server code that reads payloads and error stacks. Check the user's rights before you call a read. Nothing in the library checks them for you.

```ts
// src/app/admin/runs/[task]/[runId]/page.tsx
import { notFound } from "next/navigation";

import { nextasking } from "@/nextasking";
import { requireAdmin } from "@/auth";

export default async function RunPage({ params }: { params: Promise<{ task: string; runId: string }> }) {
  await requireAdmin();
  const { task, runId } = await params;
  if (!nextasking.isTaskName(task)) {
    notFound();
  }
  const run = await nextasking.getRun(task, runId);
  if (run === undefined) {
    notFound();
  }
  return <RunDetails run={run} />;
}
```

## Read a run

```ts
const run = await nextasking.getRun("send-welcome-email", runId);
```

`getRun(task, runId)` returns the `Run`, or `undefined` when the queue no longer holds it (it was never dispatched, or retention removed it). An unknown task name throws `UnknownTaskError`.

| Field          | What it holds                                                                                                   |
| -------------- | --------------------------------------------------------------------------------------------------------------- |
| `task`         | The task's name; checking it narrows `payload` to that task's payload                                           |
| `runId`        | The run id                                                                                                      |
| `state`        | One of the eight states below; checking it narrows the state's own field                                        |
| `payload`      | The validated payload, `undefined` for a scheduled task                                                         |
| `attempt`      | The attempt the run is on: the same `attempt` as the run's latest log line                                      |
| `dispatchedAt` | When it was dispatched                                                                                          |
| `finishedAt`   | When it completed or failed for good; absent before                                                             |
| `errors`       | One `RunError` per failed attempt: `{ attempt, name, message, stack, failedAt }`                                |
| `steps`        | The saved `step.run` results, `{ id, result }`, in no set order; empty once the run completes, which drops them |

Every time is a `Date`, so a `Run` passes unchanged from a server component to a client component.

## The eight states

```ts
switch (run.state) {
  case "delayed":
    return `starts at ${run.startsAt.toISOString()}`;
  case "suspended":
    return `asleep until ${run.resumesAt.toISOString()}`;
  case "backingOff":
    return `attempt ${run.attempt} at ${run.nextAttemptAt.toISOString()}`;
  case "failed":
    return `failed: ${run.reason}`;
  default:
    return run.state;
}
```

| `state`                     | The run                                                                                      | Own field       |
| --------------------------- | -------------------------------------------------------------------------------------------- | --------------- |
| `delayed`                   | Was dispatched with `delay` and waits for it to pass                                         | `startsAt`      |
| `pending`                   | Is due and waits for a worker                                                                | none            |
| `waitingForConcurrencySlot` | Waits for a slot of its `concurrency` limit; see [Concurrency](concurrency.md)               | none            |
| `executing`                 | Has an attempt executing                                                                     | none            |
| `suspended`                 | Sleeps in `step.sleep` or `step.sleepUntil`; see [Steps](steps.md)                           | `resumesAt`     |
| `backingOff`                | Failed an attempt and waits out its backoff; `attempt` is the attempt `nextAttemptAt` starts | `nextAttemptAt` |
| `completed`                 | Completed                                                                                    | none            |
| `failed`                    | Failed for good; see [Retries and failures](retries-and-failures.md#what-a-failed-run-keeps) | `reason`        |

- A run handed back during a deploy overlap, because the worker that picked it up doesn't define its task, shows `pending`: it is due and waits for a worker that defines it. See [Worker](worker.md#overlapping-releases).
- A run yielded at shutdown or come back from a stall shows `pending` too.
- `RunState` is the type of `state`; `FailureReason` the type of `reason`.

## Page through failed runs

```ts
const { runs, nextCursor } = await nextasking.listRuns({ state: "failed", limit: 20, cursor });
```

`listRuns({ state, task?, limit?, cursor? })` returns `{ runs, nextCursor? }`:

- `limit` defaults to 50; it must be a whole number, 1 or more.
- `task` keeps the runs of one task; the runs come typed by that task and by the state.
- Pass `nextCursor` back as `cursor` for the next page. It is an opaque string; a cursor from another state, or one `listRuns` didn't return, throws.
- No `nextCursor` means the walk is done. The last page may come back empty.
- Each listed run has the fields of a `Run` but `payload`, `errors` and `steps`, and adds `lastErrorMessage`, the message of its last error, when it has one. Call `getRun` for the rest.
- Order: `completed` and `failed` most recently finished first; `executing` most recently started first; the delayed kinds latest due first; `pending` newest dispatched first, then the handed-back runs.
- A page is a position in the queue, not a snapshot: a run that changes state between two pages can be skipped or listed twice.

```ts
// src/app/admin/failed/page.tsx
export default async function FailedRunsPage({ searchParams }: { searchParams: Promise<{ cursor?: string }> }) {
  await requireAdmin();
  const { cursor } = await searchParams;
  const { runs, nextCursor } = await nextasking.listRuns({ state: "failed", ...(cursor ? { cursor } : {}) });
  return <FailedRunsTable runs={runs} nextCursor={nextCursor} />;
}
```

Give each row a replay button backed by a server action that calls `replay(run.task, run.runId)`. See [Retries and failures](retries-and-failures.md#replay-a-failed-run).

## Count the backlog

```ts
const runCounts = await nextasking.countRuns();
runCounts["send-welcome-email"].pending;
```

`countRuns()` returns `{ [task]: { [state]: number } }`: every task the instance lists, with all eight states, zero included.

## Follow an event

```ts
const { fanOut, runsByListener } = await nextasking.getEvent("invoice.paid", eventId);
runsByListener["record-invoice-payment"]?.state;
```

`getEvent(event, eventId)` returns `{ event, eventId, fanOut, runsByListener }`. An unknown event name throws `UnknownEventError`.

- `fanOut` is `"pending"` until a worker has turned the event into listener runs, then `"completed"`, or `"failed"` when the fan-out failed for good; replay a failed one with `replayEvent(event, eventId)`, see [Retries and failures](retries-and-failures.md#replay-a-failed-fan-out). It is `undefined` when the queue holds no fan-out for that id: never emitted, emitted with no listener, or removed by retention.
- `runsByListener` has one key per listener the code defines for the event, each holding that listener's `Run`, typed by its listener, or `undefined` while it has none.
- A listener the code no longer defines is left out, even when the emitting release fanned out to it. A listener added after the emit shows `undefined`.

## Check that a schedule fired

```ts
for (const schedule of await nextasking.listSchedules()) {
  const previousRun =
    schedule.previousFiring && (await nextasking.getRun(schedule.task, schedule.previousFiring.runId));
}
```

`listSchedules()` returns one entry per scheduled task the code defines, in the order of `tasks`: `{ task, cron, timezone, nextFiringAt?, previousFiring? }`.

- `cron` and `timezone` come from the code; `timezone` is `"UTC"` when the task sets none.
- `nextFiringAt` is the next planned firing. It is absent until a worker has booted with the schedule; see [Scheduled tasks](scheduled-tasks.md).
- `previousFiring` is `{ scheduledAt, runId }`: the cron's last planned instant before `nextFiringAt`, and the run id derived from it. It is absent until the schedule has fired once.
- `previousFiring` says when the schedule was due, not that it ran. Call `getRun(task, previousFiring.runId)`: `undefined` means no run was started for that firing, or retention removed it.
- After a change of `cron`, `previousFiring` follows the new cron.

## Build a "run now" picker

```ts
// src/app/admin/run-now/page.tsx
export default async function RunNowPage() {
  await requireAdmin();
  const scheduledTaskNames = nextasking.taskDefinitions
    .filter((task) => task.schedule !== undefined)
    .map((task) => task.name);
  return <RunNowForm taskNames={scheduledTaskNames} />;
}

// src/app/admin/run-now/actions.ts
"use server";

export async function runNow(taskName: TaskName) {
  await requireAdmin();
  if (taskName === "purge-expired-sessions") {
    await nextasking.dispatch(taskName);
  }
}
```

- `taskDefinitions` lists every task as `{ name, event?, schedule? }`: `event` is a listener's event name, `schedule` a scheduled task's `{ cron, timezone }`.
- `eventDefinitions` lists every event as `{ name, listenerNames }`.
- Both are plain, frozen arrays, read with no connection, typed with `TaskName` and `EventName`. They carry no schema and no handler, so they pass to a client component.
- A scheduled task takes no payload; pass a task with a schema its payload, checked by `dispatch`, which throws `InvalidPayloadError` on a payload the schema refuses.

## What reads see, and what they cost

- A read sees only the runs of tasks the instance lists. Runs of a task the running release doesn't define, removed from the code or known only to a newer release, never appear in `listRuns` or `countRuns`. Point a BullMQ tool at the queue to see those.
- Fan-outs and firings never appear as runs.
- Retention is shared: completed runs are kept 24 hours and at most 1,000, failed runs 14 days and at most 10,000, per queue, across every task, fan-out and firing. A busy task can push another task's completed runs out within minutes.
- No read is a snapshot. `countRuns` and `listRuns` walk the queue in batches while runs keep moving, so totals can be off by the runs that moved during the walk.

| Read                                  | Cost                                                                                                            |
| ------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `getRun`                              | Three round trips, whatever the queue's size                                                                    |
| `listRuns`                            | Reads 200 runs per round trip, payloads included, until the page is full; a `task` or a delayed kind reads more |
| `countRuns`                           | Reads every run's task name, and the stored data of every delayed run, in batches of 200: one walk of the queue |
| `getEvent`                            | Two round trips per listener, run in parallel                                                                   |
| `listSchedules`                       | One read of every stored schedule                                                                               |
| `taskDefinitions`, `eventDefinitions` | Nothing: no connection                                                                                          |

Call `countRuns` on page load, not in a tight poll: with 10,000 failed runs kept, one call reads more than 50 batches.
