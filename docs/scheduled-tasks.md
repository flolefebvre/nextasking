# Scheduled tasks

A scheduled task is a task with a cron and no payload. Each firing of its schedule starts one run, however many instances run a worker. Define it next to your other tasks, list it in `tasks`, and the worker keeps its schedule.

## Define a scheduled task

```ts
// src/nextasking/tasks/purge-expired-sessions.ts
import { defineScheduledTask } from "@flefebvre/nextasking";

export const purgeExpiredSessions = defineScheduledTask("purge-expired-sessions", {
  schedule: { cron: "0 3 * * *", timezone: "Europe/Paris" },
  retries: 2,
  timeout: "10m",
  handler: async ({ scheduledAt, step }) => {
    const expiredBefore = scheduledAt ?? new Date();
    await step.run("purge", async () => {
      await db.session.deleteMany({ where: { expiresAt: { lt: expiredBefore } } });
    });
  },
});
```

```ts
// src/nextasking/index.ts
export const nextasking = createNextasking({
  connection: { url: process.env.REDIS_URL ?? "redis://localhost:6379" },
  tasks: [sendWelcomeEmail, purgeExpiredSessions],
});
```

- `schedule.cron`: five fields (minute, hour, day of month, month, day of week), or six with seconds first (`"*/30 * * * * *"`).
- `schedule.timezone`: an IANA time zone name such as `"Europe/Paris"` or `"America/New_York"`. It defaults to `"UTC"`.
- It takes the task options `retries`, `backoff`, `timeout` and `concurrency: { limit }`, with the same defaults as a task. It takes no `schema` and no `concurrency.key`: there is no payload.
- The handler receives the same object as a task's, without `payload` and with `scheduledAt`.
- A scheduled task is a task: its name is in `TaskName`, `isTaskName` is true for it, and it retries, suspends and fails like any other.

### Startup checks

`createNextasking` throws, with no connection, on a cron the parser refuses or a time zone name the platform doesn't know:

```text
Task "purge-expired-sessions" has an invalid schedule.cron: "61 * * * *" is not a cron expression: Constraint error, got value 61 expected range 0-59
Task "purge-expired-sessions" has an invalid schedule.timezone: "Europe/Pariss" is not an IANA time zone
```

## What scheduledAt is

`scheduledAt` is the `Date` the firing was planned for, not the time the run started: `0 3 * * *` gives 03:00:00.000 in the schedule's time zone, even when the run starts seconds or minutes later.

- Use it as the boundary of the work the run covers ("sessions expired before `scheduledAt`"), so a late or retried run does the same work.
- It stays the same across every attempt of the run.
- It is `undefined` when the run was started with `dispatch`. See [Run it now](#run-it-now).
- The run id is derived from the task name and `scheduledAt`, so one firing starts one run even when several instances or a repeated firing try to start it.

## Run it now

Dispatch a scheduled task with its name alone to start one run now, outside its schedule:

```ts
await nextasking.dispatch("purge-expired-sessions");
await nextasking.dispatch("purge-expired-sessions", undefined, { runId: "purge-after-incident-42", delay: "5m" });
```

- Its `scheduledAt` is `undefined`.
- `runId` and `delay` work as for any task: pass `undefined` as the payload to reach them.
- It doesn't move the schedule: the next firing comes at its usual time.
- A payload doesn't compile, and if one slips past the types, `dispatch` throws before storing anything:

```text
Scheduled task "purge-expired-sessions" takes no payload: dispatch it by its name alone to run it now
```

## What a deploy does to schedules

Each `startWorker()` reconciles the schedules stored in Redis with the scheduled tasks the instance lists, before the worker takes its first attempt:

- A scheduled task with no stored schedule gets one: `schedule added`.
- A stored schedule whose cron or time zone differs from the code is replaced: `schedule changed`. Its planned firing is dropped and the next one follows the new cron.
- A stored schedule that no scheduled task declares is removed, whether its task is gone or is now a plain task: `schedule removed`.
- A schedule that matches the code is left as it is, and logs nothing.

Each line is logged at `info` with `task`, `cron` and `timezone`. See [Logging](logging.md).

- Nothing coordinates the boots: when two releases boot side by side during a rolling deploy, the last one to boot sets the schedules.
- A firing that reaches a worker whose release no longer schedules its task starts no run, and logs `firing dropped: task not scheduled` at `debug`.
- A process that never calls `startWorker()` never touches the schedules.
- Renaming a scheduled task removes the old name's schedule and adds the new one.

## Daylight saving time

A schedule follows the wall clock of its time zone. Around a daylight saving change, with `timezone: "Europe/Paris"`:

- **Skipped hour** (spring, 02:00 jumps to 03:00): a firing whose local time doesn't exist that day runs once, one hour later. `30 2 * * *` fires at 03:30 that day, then at 02:30 again the next day.
- **Repeated hour** (autumn, 03:00 goes back to 02:00): a firing at a fixed time inside the repeated hour runs once, at its first occurrence. `30 2 * * *` fires once.
- An hour field of `*` fires on every real hour: `0 * * * *` fires at both 02:00s in autumn, and has no 02:00 in spring.
- Keep the default `"UTC"`, or schedule outside 02:00–03:00 local time, when the run must come exactly every 24 hours.

## Missed firings are not caught up

While no worker runs (every instance down, or Redis unreachable), firings don't start runs, and they are not replayed later:

- When a worker comes back, the one firing that was planned next when the workers stopped runs at once, late, with its original `scheduledAt`.
- Every firing that came due after it is skipped. The schedule then goes on from the current time.
- A schedule changed by that boot drops even the late firing, and only follows the new cron.

If missing a firing matters, make the handler cover everything since the last successful run (store your own high-water mark) rather than one period.
