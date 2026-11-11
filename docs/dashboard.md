# Dashboard

The instance reads its runs back, so you can build your own dashboard: one run with its state, payload, errors and saved steps; a page of runs in one state, several, or all of them; the backlog per task; the run of each listener of one event; each schedule's firings; a "run now" picker. Every read works from any process with a connection, with or without a worker.

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
  case "executing":
    return `executing since ${run.startedAt.toISOString()}`;
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
| `executing`                 | Has an attempt executing; `startedAt` is when a worker last took it                          | `startedAt`     |
| `suspended`                 | Sleeps in `step.sleep` or `step.sleepUntil`; see [Steps](steps.md)                           | `resumesAt`     |
| `backingOff`                | Failed an attempt and waits out its backoff; `attempt` is the attempt `nextAttemptAt` starts | `nextAttemptAt` |
| `completed`                 | Completed                                                                                    | none            |
| `failed`                    | Failed for good; see [Retries and failures](retries-and-failures.md#what-a-failed-run-keeps) | `reason`        |

- A run fetched by a worker that doesn't define its task, during a deploy overlap, shows `executing` for the 10 seconds that worker holds it, then `pending` again, with `attempt` unchanged. Until a worker that defines the task takes it, a dashboard can show it flip between the two. See [Worker](worker.md#overlapping-releases).
- A run yielded at shutdown or come back from a stall shows `pending` too.
- `startedAt` is when a worker last took the run, not when its first attempt started: a run that resumed from a sleep, retried, or was handed back, yielded or stalled and then taken again, shows the later time.
- `RunState` is the type of `state`; `FailureReason` the type of `reason`.

## Page through failed runs

```ts
const { runs, totalRunCount, firstRunPosition, previousCursor, nextCursor } = await nextasking.listRuns({
  state: "failed",
  limit: 20,
  cursor,
});
```

`listRuns({ state?, task?, from?, until?, order?, limit?, cursor? })` returns `{ runs, totalRunCount, firstRunPosition?, previousCursor?, nextCursor? }`:

- `state` takes one state or a list of states; left out, the list holds every state. See [List several states](#list-several-states).
- `from` and `until` keep the runs of a time range, by the time their state is ordered by. See [List the runs of a time range](#list-the-runs-of-a-time-range).
- `order` is `"descending"`, the default, or `"ascending"`. See [Order](#order).
- `limit` defaults to 50; it must be a whole number, 1 or more.
- `task` takes one task name or a list of names, and keeps their runs; left out, the list holds every task the instance lists. The runs come typed by the tasks and the states listed. See [List several tasks](#list-several-tasks).
- `totalRunCount` is how many runs the list holds: the runs in the states listed, of the tasks listed.
- `firstRunPosition` is the position of the page's first run in the list, counted from 1. It is absent only when the list is empty or the page lies past its end.
- `nextCursor` is present exactly when a run follows the page; `previousCursor` exactly when a run comes before it. Pass either back as `cursor`, with the same `state`, `task`, `from`, `until` and `order`, for the page after or the page before. The direction is inside the cursor: there is no other option to set.
- A cursor is an opaque string; any other cursor throws:

```text
Invalid listRuns cursor: it pages through state "completed", not "failed"
Invalid listRuns cursor: it pages through states "completed" and "failed", not "failed"
Invalid listRuns cursor: it pages through every state, not "completed" and "failed"
Invalid listRuns cursor: it pages through task "send-welcome-email", not task "create-default-workspace"
Invalid listRuns cursor: it pages through every task, not task "send-welcome-email"
Invalid listRuns cursor: it pages through task "send-welcome-email", not every task
Invalid listRuns cursor: it pages through tasks "send-welcome-email" and "create-default-workspace", not task "send-welcome-email"
Invalid listRuns cursor: it pages through runs from 2026-11-11T08:00:00.000Z until 2026-11-11T10:00:00.000Z, not runs from 2026-11-11T08:05:00.000Z until 2026-11-11T10:05:00.000Z
Invalid listRuns cursor: it pages through runs in ascending order, not in descending order
Invalid listRuns cursor: pass back a nextCursor or previousCursor that listRuns returned
```

- A page may come back shorter than `limit`, even empty; only a missing `nextCursor` ends the walk.
- `totalRunCount` and `firstRunPosition` count the list at the instant its page is read. A run written by a newer release, or one that leaves the state between that instant and the read of its fields, is counted but not listed: a page can hold fewer runs than its position and the total suggest, down to none, with `runs` empty while `firstRunPosition` is present. Two pages read at different instants can disagree on the total.
- Each listed run has the fields of a `Run` but `payload`, `errors` and `steps`, and adds `lastErrorMessage`, the message of its last error, when it has one. Call `getRun` for the rest.
- The order is told by order group, in [Order](#order).
- A page read through `previousCursor` comes in the same order as any other page.
- A cursor is a place in that order, not a page number and not a run that must still exist: `nextCursor` lists the runs after the page's last run, `previousCursor` the runs before its first. It stays valid after that run leaves the list, and reading it again returns the same runs, with a higher `firstRunPosition` when runs arrived before them.
- A run that leaves the states listed between two pages is skipped, and one that leaves and comes back further along the list can be listed again.
- A run that moves between two states listed stays in the list. Inside one order group the only moves are between `pending` and `waitingForConcurrencySlot`, and the run keeps its place in the queue, so a walk lists it once. Between two order groups it moves to the block of its new state, at its new place: a walk lists it twice when it was listed already and its new place is still ahead, and misses it when its new place is behind and its old one was not reached yet.
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

## List several states

The eight states fall into four order groups. The runs of one group share one order; the runs of two groups never do.

| Order group        | States                                 | Ordered by             |
| ------------------ | -------------------------------------- | ---------------------- |
| executing          | `executing`                            | When it started        |
| queued             | `pending`, `waitingForConcurrencySlot` | Its place in the queue |
| waiting for a time | `delayed`, `suspended`, `backingOff`   | When it is due         |
| ended              | `completed`, `failed`                  | When it finished       |

States of one group merge into one order: completed and failed runs interleave by finish time.

```ts
const { runs } = await nextasking.listRuns({ state: ["completed", "failed"], limit: 20 });
```

States of several groups come as blocks, one per group, always in the sequence executing, queued, waiting for a time, ended, whatever order the call lists the states in and in both [orders](#order). Each block keeps its group's order, and a page fills across a block boundary: it is short only at the end of the list.

```ts
const { runs } = await nextasking.listRuns({ state: ["failed", "pending", "executing"] });
```

Leave `state` out to list every run of the instance, in four blocks:

```ts
const { runs, totalRunCount, firstRunPosition, nextCursor } = await nextasking.listRuns({ limit: 50 });
```

- Each run carries its `state`: draw a heading where the order group of `state` changes.
- `totalRunCount`, `firstRunPosition`, `previousCursor` and `nextCursor` run through the whole list, blocks included: the position of a page's first run counts the runs of the blocks before it.
- The runs come typed by the states listed: one state gives that state's run, a list the union, no `state` every state.
- An empty list throws: list at least one state, or leave `state` out.
- The same states in another order are the same list, and accept each other's cursors. A list of all eight states is not the same list as `state` left out: their cursors refuse each other.

## Order

`order` reverses each order group; it never reorders the blocks.

| Order group        | `"descending"`, the default  | `"ascending"`         |
| ------------------ | ---------------------------- | --------------------- |
| executing          | Most recently started first  | Longest running first |
| queued             | Most recently queued first   | Next to run first     |
| waiting for a time | Latest due first             | Soonest due first     |
| ended              | Most recently finished first | Oldest first          |

```ts
const { runs } = await nextasking.listRuns({ state: "pending", order: "ascending", limit: 20 });
```

- An ascending list is exactly the descending list read from its end, runs of equal time included.
- With several order groups listed, the blocks keep the sequence executing, queued, waiting for a time, ended, in both orders; each block is reversed inside.
- `totalRunCount`, `firstRunPosition` and both cursors work the same in both orders. A cursor carries its order, and is refused with the other one.
- To reach the oldest runs, list in ascending order: its first page is the end of the descending list. No option opens a last page.
- The oldest ended runs are the ones retention removes next: a run on the first ascending page of `completed` or `failed` can be gone by the time `getRun` reads it.
- A run queues anew when it is dispatched, comes due after a delay, a sleep or a backoff, or is replayed; a handed-back, yielded or stalled run keeps its place in the queue.
- `order` takes a time range too: `{ state: "failed", from, until, order: "ascending" }` lists the oldest failures of the range first.

## List several tasks

```ts
const { runs, nextCursor } = await nextasking.listRuns({
  state: "failed",
  task: ["send-welcome-email", "create-default-workspace"],
});
```

- The runs of the tasks listed merge into one order, as the runs of every task do. The call reads the indexes of those tasks only, so it costs less than listing every task.
- `totalRunCount`, `firstRunPosition` and both cursors count the tasks listed only.
- The runs come typed by the tasks listed: one name gives that task's run, a list the union.
- A name the instance doesn't list throws `UnknownTaskError`, wherever it is in the list. An empty list throws: list at least one task, or leave `task` out.
- The same names in another order are the same list, and accept each other's cursors. Another set is refused, naming both, and so is a list of every task against `task` left out:

```text
Invalid listRuns cursor: it pages through tasks "send-welcome-email" and "create-default-workspace", not every task
Invalid listRuns cursor: it pages through every task, not tasks "send-welcome-email" and "create-default-workspace"
```

## List the runs of a time range

`from` and `until` keep the runs whose time falls between them, both included. Each takes a `Date` and works alone. The time is the one the order group of the listed states is ordered by:

| Order group        | States                               | `from` and `until` apply to                              |
| ------------------ | ------------------------------------ | -------------------------------------------------------- |
| executing          | `executing`                          | `startedAt`                                              |
| waiting for a time | `delayed`, `suspended`, `backingOff` | When it is due: `startsAt`, `resumesAt`, `nextAttemptAt` |
| ended              | `completed`, `failed`                | `finishedAt`                                             |

Runs that failed in the last two hours:

```ts
const now = Date.now();
const { runs, totalRunCount } = await nextasking.listRuns({
  state: "failed",
  from: new Date(now - 2 * 60 * 60 * 1000),
  until: new Date(now),
});
```

Runs executing for more than ten minutes, their attempt taken ten minutes ago or earlier:

```ts
const { runs } = await nextasking.listRuns({
  state: "executing",
  until: new Date(Date.now() - 10 * 60 * 1000),
});
```

Runs due in the next thirty minutes:

```ts
const now = Date.now();
const { runs } = await nextasking.listRuns({
  state: ["delayed", "suspended", "backingOff"],
  from: new Date(now),
  until: new Date(now + 30 * 60 * 1000),
});
```

- `totalRunCount`, `firstRunPosition` and both cursors count the runs inside the time range only, and paging in either direction stays inside it.
- A cursor carries its `from` and `until`, and is refused with any other bounds. Compute the bounds once, on the first page, and pass the same bounds with every cursor: keep them beside the cursor, in the URL for instance. A page that computes `from` again from the current time is refused.

```ts
// src/app/admin/recent-failures/page.tsx
export default async function RecentFailuresPage({
  searchParams,
}: {
  searchParams: Promise<{ from?: string; until?: string; cursor?: string }>;
}) {
  await requireAdmin();
  const params = await searchParams;
  const until = params.until ? new Date(Number(params.until)) : new Date();
  const from = params.from ? new Date(Number(params.from)) : new Date(until.getTime() - 2 * 60 * 60 * 1000);
  const { runs, previousCursor, nextCursor } = await nextasking.listRuns({
    state: "failed",
    from,
    until,
    limit: 20,
    ...(params.cursor ? { cursor: params.cursor } : {}),
  });
  const pageLinkOf = (cursor: string) => `?from=${from.getTime()}&until=${until.getTime()}&cursor=${cursor}`;
  return (
    <>
      <FailedRunsTable runs={runs} />
      {previousCursor && <Link href={pageLinkOf(previousCursor)}>Previous</Link>}
      {nextCursor && <Link href={pageLinkOf(nextCursor)}>Next</Link>}
    </>
  );
}
```

- The bounds are compared with times the server wrote from its own clock: the app's clock and the server's can differ by their skew.
- A run that yielded, retried, resumed, or was handed back and taken again has the `startedAt` of its latest take, and is filtered on it.
- The call costs the same two round trips as without a time range.
- A time range is refused, before any round trip, with a message that says why:

```text
Invalid listRuns range: the states listed are of several order groups, each ordered its own way: "executing" by startedAt; "failed" by finishedAt. A range takes the states of one order group ordered by a time: executing, waiting for a time, or ended
Invalid listRuns range: "pending" runs are ordered by their place in the queue, not by a time. A range takes the states of one order group ordered by a time: executing, waiting for a time, or ended
Invalid listRuns range: from 2026-11-11T10:00:00.000Z is after until 2026-11-11T08:00:00.000Z; pass a from at or before until
Invalid listRuns from: pass a Date, got "2026-11-11T08:00:00.000Z"
Invalid listRuns until: pass a valid Date, got an Invalid Date
```

- A time range takes the states of one order group ordered by a time: neither `state` left out, nor `pending` or `waitingForConcurrencySlot`, takes one. Queued runs are ordered by their place in the queue, and a run holds no time at which it became pending.

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
- One `listRuns` page is placed in its list at one instant: which runs fall on the page, `totalRunCount` and `firstRunPosition` are read together, across every state and task listed. Each run's fields are read afterwards, at an instant of its own.

| Read            | Cost                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `getRun`        | One round trip: record, payload, errors and steps together                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `listRuns`      | Two round trips, whatever the number of states: one script reading the counts of one index per state and task listed (the tasks the instance lists, or those of `task`), and at most `limit` entries from each index of an order group the page reaches, none from the others, all at one instant; then the page's runs with their last error, each at its own instant. One round trip when no task has a run left to list. Every state of 50 tasks is 400 indexes in that one script |
| `countRuns`     | One round trip: one count per task and state, whatever the queue holds                                                                                                                                                                                                                                                                                                                                                                                                                |
| `getEvent`      | One round trip: every listener's run together                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `listSchedules` | One round trip: one read per scheduled task the instance lists                                                                                                                                                                                                                                                                                                                                                                                                                        |
