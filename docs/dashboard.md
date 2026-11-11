# Dashboard

The instance reads its runs back, so you can build your own dashboard: one run with its state, payload, errors and saved steps; a page of runs in one state; the backlog per task; the run of each listener of one event; each schedule's firings; a "run now" picker. Every read works from any process with a connection, with or without a worker.

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

`getRun(task, runId)` returns the `Run`, or `undefined` when the queue doesn't hold it: it was never dispatched, retention removed it, or a newer release wrote it. An unknown task name throws `UnknownTaskError`.

| Field          | What it holds                                                                                                                                                                                                                                                                  |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `task`         | The task's name; checking it narrows `payload` to that task's payload                                                                                                                                                                                                          |
| `runId`        | The run id                                                                                                                                                                                                                                                                     |
| `state`        | One of the eight states below; checking it narrows the state's own field                                                                                                                                                                                                       |
| `payload`      | The validated payload, `undefined` for a scheduled task                                                                                                                                                                                                                        |
| `attempt`      | The attempt the run is on: the same `attempt` as the run's latest log line                                                                                                                                                                                                     |
| `dispatchedAt` | When it was dispatched                                                                                                                                                                                                                                                         |
| `finishedAt`   | When it completed or failed for good; absent before                                                                                                                                                                                                                            |
| `errors`       | One `RunError` per failed attempt: `{ attempt, name, message, stack, failedAt }`                                                                                                                                                                                               |
| `steps`        | The saved `step.run` results and the handler's sends, `{ id, result }`, in no set order; empty once the run completes, which drops them. A `dispatch` or `emit` of the handler shows as `{ id: <send id>, result: <runId or eventId> }`, the same shape as a `step.run` result |

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

- A run fetched by a worker that doesn't define its task, during a deploy overlap, shows `executing` for the 10 seconds that worker holds it, then `pending` again, with `attempt` unchanged. Until a worker that defines the task takes it, a dashboard can show it flip between the two. See [Worker](worker.md#overlapping-releases).
- A run yielded at shutdown or come back from a stall shows `pending` too.
- `RunState` is the type of `state`; `FailureReason` the type of `reason`.

## Page through failed runs

```ts
const { runs, totalRunCount, firstRunPosition, previousCursor, nextCursor } = await nextasking.listRuns({
  state: "failed",
  limit: 20,
  cursor,
});
```

`listRuns({ state, task?, limit?, cursor? })` returns `{ runs, totalRunCount, firstRunPosition?, previousCursor?, nextCursor? }`:

- `limit` defaults to 50; it must be a whole number, 1 or more.
- `task` keeps the runs of one task; the runs come typed by that task and by the state.
- `totalRunCount` is how many runs the list holds: the runs in `state`, of `task` or of every task the instance lists.
- `firstRunPosition` is the position of the page's first run in the list, counted from 1. It is absent only when the list is empty or the page lies past its end.
- `nextCursor` is present exactly when a run follows the page; `previousCursor` exactly when a run comes before it. Pass either back as `cursor`, with the same `state` and `task`, for the page after or the page before. The direction is inside the cursor: there is no other option to set.
- A cursor is an opaque string; any other cursor throws:

```text
Invalid listRuns cursor: it pages through state "completed", not "failed"
Invalid listRuns cursor: it pages through task "send-welcome-email", not task "create-default-workspace"
Invalid listRuns cursor: it pages through every task, not task "send-welcome-email"
Invalid listRuns cursor: it pages through task "send-welcome-email", not every task
Invalid listRuns cursor: pass back a nextCursor or previousCursor that listRuns returned
```

- A page may come back shorter than `limit`, even empty; only a missing `nextCursor` ends the walk.
- `totalRunCount` and `firstRunPosition` count the list at the instant its page is read. A run written by a newer release, or one that leaves the state between that instant and the read of its fields, is counted but not listed: a page can hold fewer runs than its position and the total suggest, down to none, with `runs` empty while `firstRunPosition` is present. Two pages read at different instants can disagree on the total.
- Each listed run has the fields of a `Run` but `payload`, `errors` and `steps`, and adds `lastErrorMessage`, the message of its last error, when it has one. Call `getRun` for the rest.
- Order: `completed` and `failed` most recently finished first; `executing` most recently started first; `delayed`, `suspended` and `backingOff` latest due first; `pending` and `waitingForConcurrencySlot` most recently queued first. A run queues anew when it is dispatched, comes due after a delay, a sleep or a backoff, or is replayed; a handed-back, yielded or stalled run keeps its place.
- A page read through `previousCursor` comes in the same order as any other page.
- A cursor is a place in that order, not a page number and not a run that must still exist: `nextCursor` lists the runs after the page's last run, `previousCursor` the runs before its first. It stays valid after that run leaves the list, and reading it again returns the same runs, with a higher `firstRunPosition` when runs arrived before them.
- A run that leaves the state between two pages is skipped, and one that leaves and comes back further along the order can be listed again.
- A page read through `previousCursor` that reaches the head of the list is the first page: when fewer than `limit` runs come before the place, it returns what a call without `cursor` returns, full, with no `previousCursor`. Runs that arrive at the head while a dashboard pages back never leave it a short first page.
- A page past the end of the list, its runs all gone since the cursor was handed, holds no run and no `nextCursor`; its `previousCursor` returns the last page of the list.
- A run written by a newer release is left out.

```ts
// src/app/admin/failed/page.tsx
export default async function FailedRunsPage({ searchParams }: { searchParams: Promise<{ cursor?: string }> }) {
  await requireAdmin();
  const { cursor } = await searchParams;
  const { runs, totalRunCount, firstRunPosition, previousCursor, nextCursor } = await nextasking.listRuns({
    state: "failed",
    limit: 5,
    ...(cursor ? { cursor } : {}),
  });
  return (
    <>
      {firstRunPosition !== undefined && runs.length > 0 && (
        <p>
          Runs {firstRunPosition} to {firstRunPosition + runs.length - 1} of {totalRunCount}
        </p>
      )}
      <FailedRunsTable runs={runs} />
      {previousCursor && <Link href={`?cursor=${previousCursor}`}>Previous</Link>}
      {nextCursor && <Link href={`?cursor=${nextCursor}`}>Next</Link>}
    </>
  );
}
```

With 20 failed runs, the second page reads "Runs 6 to 10 of 20", with both links; the first page has no Previous link, the fourth no Next link. If four runs fail while the second page is open, reading it again shows the same five runs as "Runs 10 to 14 of 24", Previous then returns runs 5 to 9, and Previous again the first page, runs 1 to 5: only four runs come before run 5, so that page fills up from the head of the list and shows run 5 again.

Give each row a replay button backed by a server action that calls `replay(run.task, run.runId)`. See [Retries and failures](retries-and-failures.md#replay-a-failed-run).

## Count the backlog

```ts
const runCounts = await nextasking.countRuns();
runCounts["send-welcome-email"].pending;
```

`countRuns()` returns `{ [task]: { [state]: number } }`: every task the instance lists, with all eight states, zero included.

- It counts the runs a newer release wrote too, which `getRun` and `listRuns` leave out: during a deploy overlap, the counts of an old release can exceed what it lists.

## Read the listener runs of an event

```ts
const { runsByListener } = await nextasking.getEvent("invoice.paid", eventId);
runsByListener["record-invoice-payment"]?.state;
```

`getEvent(event, eventId)` returns `{ event, eventId, runsByListener }`. An unknown event name throws `UnknownEventError`.

- `runsByListener` has one key per listener the code defines for the event, each holding that listener's `Run`, typed by its listener, or `undefined` when there is none.
- Every listener reads `undefined` for an event never emitted, or whose runs retention removed.
- A listener the code no longer defines is left out, even when the emitting release stored a run for it. A listener added after the emit shows `undefined`.
- A failed listener run is replayed like any run, with `replay(listenerName, runId)`. See [Retries and failures](retries-and-failures.md#replay-a-failed-run).

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
- `previousFiring` is `{ scheduledAt, runId }`: the planned instant of the last firing a worker fired, and the id of the run it started. It is absent until the schedule has fired once.
- Both are absent while the stored schedule was written by a newer release: during a deploy overlap, the old release's dashboard shows a schedule that fired for months with neither.
- Call `getRun(task, previousFiring.runId)` to see how that run went: `undefined` means retention removed it.
- After a change of `cron`, `previousFiring` stays the last real firing, under the old cron; only `nextFiringAt` follows the new one.
- A run started with `dispatch`, or with `fireSchedule` from `./testing`, is not a firing and leaves `previousFiring` as it was.

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
- `eventDefinitions` lists every event as `{ name, listenerNames }`, the built-in `nextasking/run.failed` included.
- Both are plain, frozen arrays, read with no connection, typed with `TaskName` and `EventName`. They carry no schema and no handler, so they pass to a client component.
- A scheduled task takes no payload; pass a task with a schema its payload, checked by `dispatch`, which throws `InvalidPayloadError` on a payload the schema refuses.

## What reads see, and what they cost

- A read sees only the runs of tasks the instance lists. Runs of a task the running release doesn't define, removed from the code or known only to a newer release, never appear in `listRuns` or `countRuns`. A run left behind by a removed task fails `unknown task` 30 minutes after a worker first found it, then leaves through retention; see [Deploys](deploys.md#remove-a-task-a-listener-or-an-event).
- Retention is per task: each task keeps its completed runs for a time and up to a count, and its failed runs for a longer time and up to a larger count. A run older than that time is removed, and so is the oldest run past that count; `getRun` then reads it as `undefined`. The figures are on [Tasks and events](tasks-and-events.md#start-the-worker) for completed runs and [Retries and failures](retries-and-failures.md#what-a-failed-run-keeps) for failed runs; the counts are the caps below. A busy task never pushes out another task's runs.
- A worker removes up to 200 ended runs per second in all, and up to 180 per second of one task that never stops overflowing. The caps hold while runs end below that rate per worker; a burst above it is caught up afterwards, at that rate. With several workers, the rates add up.
- A worker visits one task per second, in turn, so a cap is reached again at each visit rather than held at every instant: between two visits a task sits over its cap by the runs it ended meanwhile, about a minute's worth with 50 tasks.
- No read is a snapshot: runs keep moving while a read runs, so two reads, or two counts of one `countRuns`, can disagree by the runs that moved in between.
- One run is read at one instant: the record, payload, errors and steps `getRun` returns are of the same moment, and so are each listed run's state and `lastErrorMessage`. A run replayed during the read is read whole, as it was before the `replay` or after it, never `failed` without the errors it failed with. Two runs of one `getEvent` or one `listRuns` page can still be of different instants.
- One `listRuns` page is placed in its list at one instant: which runs fall on the page, `totalRunCount` and `firstRunPosition` are read together, across every task listed. Each run's fields are read afterwards, at an instant of its own.

| Read            | Cost                                                                                                                                                                                                                                               |
| --------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `getRun`        | One round trip: record, payload, errors and steps together                                                                                                                                                                                         |
| `listRuns`      | Two round trips: one script reading a page and the counts of each task the instance lists (one task with `task`), all at one instant, then the page's runs with their last error, each at its own instant; one when no task has a run left to list |
| `countRuns`     | One round trip: one count per task and state, whatever the queue holds                                                                                                                                                                             |
| `getEvent`      | One round trip: every listener's run together                                                                                                                                                                                                      |
| `listSchedules` | One round trip: one read per scheduled task the instance lists                                                                                                                                                                                     |
