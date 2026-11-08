# Testing

Test app code and handlers without mocking the library:

- `fakeNextasking(nextasking)` tests code that dispatches or emits, without Redis. It switches your own instance to recording mode in place: code that imports `nextasking` is faked with no module mocking. Every check still runs for real; only the Redis write is replaced, by a record of what would have been written.
- `executeAttempt(nextasking, taskName, payload)` tests a whole handler in one call, without Redis, on the production step code.
- `createNextaskingControl({ connection, prefix })` drives a real queue from an e2e test or a script in offline dev: wait until it is idle, skip delays and sleeps, fire a schedule, start clean.

Import them from `@flefebvre/nextasking/testing`, in test files and dev scripts only. They have no test-runner dependency: assert with your runner's own `expect`.

## Fake the instance in a test

```ts
// src/app/signup/actions.test.ts
import { fakeNextasking } from "@flefebvre/nextasking/testing";
import { expect, test } from "vitest";

import { nextasking } from "@/nextasking";
import { signUp } from "./actions";

test("signing up sends the welcome email and announces the user", async () => {
  using fake = fakeNextasking(nextasking);

  await signUp({ email: "ada@example.com" });

  expect(fake.dispatches).toEqual([
    { task: "send-welcome-email", payload: { email: "ada@example.com", locale: "en" }, runId: expect.any(String) },
  ]);
  expect(fake.emits).toEqual([{ event: "user.signed-up", payload: { userId: "user-1" }, eventId: "signed-up-user-1" }]);
});
```

- `using` restores the instance when the block ends, even when the test throws. It needs `esnext` or `esnext.disposable` in your tsconfig's `lib`; a Next app's generated tsconfig has `esnext`.
- Without `using`, call `fake.restore()` yourself, in an `afterEach` or a `finally`:

```ts
const fakeTheInstance = () => fakeNextasking(nextasking);
let fake: ReturnType<typeof fakeTheInstance>;

beforeEach(() => {
  fake = fakeTheInstance();
});

afterEach(() => {
  fake.restore();
});
```

## What it records

| Array        | One entry per                                         | Entry                                                           |
| ------------ | ----------------------------------------------------- | --------------------------------------------------------------- |
| `dispatches` | Run that `dispatch` would have written                | `{ task, payload, runId, delay? }`                              |
| `emits`      | Fan-out that `emit` would have written                | `{ event, payload, eventId }`                                   |
| `replays`    | `replay` or `replayEvent` call that passed its checks | `{ task, runId }` for a run, `{ event, eventId }` for a fan-out |

- Entries are in call order, and the arrays keep them after `restore()`.
- `payload` is what the schema returned, defaults and transforms applied, not what you passed. A scheduled task's `payload` is `undefined`.
- `delay` is the `Duration` you passed, present only when you passed one.
- `runId` and `eventId` are yours when you passed one, a random id otherwise: match them with `expect.any(String)`.
- Every type comes from the instance you pass, not from `Register`: checking `task` narrows `payload` to that task's payload, and `emits` lists only your own events.
- The fake never emits a built-in event: the worker sends those, and a faked instance runs no worker.

## The checks are real

The fake throws where production throws, before recording anything:

- an unknown task or event name: `UnknownTaskError`, `UnknownEventError`;
- a payload the schema refuses: `InvalidPayloadError`;
- an empty `runId` or `eventId`, a bad `delay`, a payload given to a scheduled task;
- a `concurrency.key` function that throws or returns no non-empty string;
- an `emit` of a built-in event.

Duplicates are dropped as in production: a second `dispatch` with the same task and `runId`, or a second `emit` with the same event and `eventId`, is recorded once and logs `duplicate dispatch dropped`. The fake keeps them for its own lifetime: a new fake starts empty.

An `emit` of an event no task listens to writes nothing in production, so the fake records nothing; the `no listeners for event` warning still reaches your logger.

`replay` and `replayEvent` check the name only: whether the run or fan-out failed is a Redis read, so the fake records the call as replayed.

## What throws on a faked instance

- `getRun`, `listRuns`, `countRuns`, `getEvent` and `listSchedules` reject, naming the call: they read Redis.
- `startWorker()` rejects: a faked instance runs no worker.
- `fakeNextasking` throws on an instance that is already faked: `restore()` the first fake before faking it again.
- `fakeNextasking` throws on an instance whose worker is started, since that worker would keep writing to Redis. Don't call `startWorker` in a test process that fakes the instance.

`isTaskName`, `isEventName`, `taskDefinitions` and `eventDefinitions` keep working.

## Test a whole handler in one call

`executeAttempt` runs one attempt of a task's handler in your test process, with the production step code, and resolves with what the attempt did. Sleeps are recorded and never waited: the handler goes on at once, so one call runs it to the end. Nothing reaches Redis.

```ts
// src/tasks/onboard-user.test.ts
import { executeAttempt } from "@flefebvre/nextasking/testing";
import { expect, test } from "vitest";

import { nextasking } from "@/nextasking";

test("onboarding creates the account, waits a day, then sends the tips email", async () => {
  const attempt = await executeAttempt(nextasking, "onboard-user", { userId: "user-1" });

  expect(attempt.steps).toContainEqual({ id: "create-account", result: { accountId: "account-1" } });
  expect(attempt.sleeps).toEqual([{ id: "wait-a-day", resumesAt: expect.any(Date) }]);
  expect(attempt.dispatches).toEqual([
    { task: "send-tips-email", payload: { userId: "user-1" }, runId: expect.any(String) },
  ]);
  expect(attempt.emits).toEqual([
    { event: "user.onboarded", payload: { userId: "user-1" }, eventId: expect.any(String) },
  ]);
});
```

It resolves with:

| Field        | What it holds                                                                                                                                                                |
| ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `steps`      | `{ id, result }` per saved step: the `savedSteps` you passed, then the ones this attempt saved, each `dispatch` or `emit` as `{ id: <send id>, result: <runId or eventId> }` |
| `sleeps`     | `{ id, resumesAt }` per `step.sleep` or `step.sleepUntil` the handler reached, in call order; `resumesAt` a Date                                                             |
| `dispatches` | The handler's `dispatch` calls, as a fake's `dispatches` records them                                                                                                        |
| `emits`      | The handler's `emit` calls, as a fake's `emits` records them                                                                                                                 |

- The payload goes through the task's schema first, as `dispatch` does: a refused payload rejects with `InvalidPayloadError` before the handler runs, and the handler gets what the schema returned.
- The step rules are the production ones: the 1 MiB and 10 MiB ceilings, unique step ids, the JSON round trip of each result, and the idempotency key derived from the task, the `runId` and the step id. A misuse rejects with `StepMisuseError`, even when the handler catches it.
- The handler's `dispatch` and `emit` run every check for real (an unknown name or a refused payload throws inside the handler) and are recorded, never written. They never go through the instance's own `dispatch` and `emit`, so an active fake records none of them. Their `runId` and `eventId` are derived from the run as in production: two attempts with the same `runId` record the same ids. A send passed back among `savedSteps` is skipped: it is not recorded again, and resolves with the saved id.
- Every type comes from the instance you pass, not from `Register`: `taskName` is one of its task names, `payload` that task's schema input, and a scheduled task takes no payload.
- The task's `timeout` and `retries` are not enforced: they belong to the worker, not to the attempt.

### Options

Pass them as the fourth argument; a scheduled task takes `undefined` as its payload.

| Option        | Default                  | What it does                                                                                                                         |
| ------------- | ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------ |
| `savedSteps`  | none                     | The step results an earlier attempt saved, as `{ id, result }[]`: their `fn` doesn't run, and the sends among them aren't sent again |
| `attempt`     | `1`                      | The handler's `attempt`                                                                                                              |
| `runId`       | a random id              | The handler's `runId`, which the idempotency keys and the send ids derive from                                                       |
| `abortSignal` | a signal never aborted   | The handler's `abortSignal`: abort it to make the next step throw                                                                    |
| `scheduledAt` | the time of the call     | A scheduled task's `scheduledAt`                                                                                                     |
| `logger`      | a logger writing nothing | The handler's `logger`, with `task`, `runId` and `attempt` added to each line, as production                                         |

### Assert on a failure

A throw rejects with the handler's own error, unwrapped:

```ts
await expect(executeAttempt(nextasking, "charge-card", { cardId: "card-expired" })).rejects.toThrow(NonRetriableError);
```

### Simulate a retry or a resume

Pass the steps of one attempt as the `savedSteps` of the next. A retry raises `attempt`; a resume after a sleep keeps it.

```ts
const firstAttempt = await executeAttempt(nextasking, "send-invoice", { invoiceId: "invoice-1" }, { runId: "run-1" });

const retry = await executeAttempt(
  nextasking,
  "send-invoice",
  { invoiceId: "invoice-1" },
  { runId: "run-1", attempt: 2, savedSteps: firstAttempt.steps },
);
```

When the first attempt rejects, build `savedSteps` yourself from the steps you expect it saved: `[{ id: "create-pdf", result: { pdfId: "pdf-1" } }]`. Results and sends are carried; sleeps are not, so the next attempt records them again. A send you leave out is sent again under the same id.

### Simulate a shutdown

Once the signal is aborted, the next step, `dispatch` or `emit` throws the abort reason, and the attempt rejects with it unless the handler handles it. Abort it from a mocked dependency the handler calls inside a step, to stop the handler at the step after it:

```ts
const shutdown = new AbortController();
vi.mocked(fetchContacts).mockImplementation(async () => {
  shutdown.abort(new Error("shutting down"));
  return [];
});

await expect(
  executeAttempt(nextasking, "import-contacts", { fileId: "file-1" }, { abortSignal: shutdown.signal }),
).rejects.toThrow("shutting down");
expect(saveContacts).not.toHaveBeenCalled();
```

## Drive the queue from an e2e test

`createNextaskingControl` works on the queue in Redis directly, next to a running app and its worker. It takes no tasks and no events, and reads no runs: point it at the same `connection` and `prefix` as your `createNextasking` call (the prefix defaults to `"nextasking"` in both).

```ts
// e2e/signup.spec.ts
import { test, expect } from "@playwright/test";
import { createNextaskingControl } from "@flefebvre/nextasking/testing";

const control = createNextaskingControl({ connection: { url: process.env.REDIS_URL ?? "redis://localhost:6379" } });

test.beforeEach(async () => {
  await control.reset();
});

test.afterAll(async () => {
  await control.close();
});

test("a new user gets the tips email the day after signing up", async ({ page }) => {
  await page.goto("/signup");
  await page.getByLabel("Email").fill("ada@example.com");
  await page.getByRole("button", { name: "Sign up" }).click();
  await control.waitForIdle();

  await control.makeRunsDueNow({ task: "onboard-user" });
  await control.waitForIdle();

  await page.goto("/admin/emails");
  await expect(page.getByText("Tips for ada@example.com")).toBeVisible();
});
```

| Call                        | What it does                                                                                                     |
| --------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `waitForIdle({ timeout? })` | Resolves once the queue is idle; rejects at `timeout` (a `Duration`, default `"30s"`) with the counts still busy |
| `makeRunsDueNow({ task? })` | Starts every `delayed`, `suspended` and `backingOff` run now, of one task when given; resolves with them         |
| `fireSchedule(taskName)`    | Starts one run of a scheduled task now; resolves with its `runId`                                                |
| `reset({ timeout? })`       | Waits until no attempt executes, removes every key under the prefix, and keeps the schedules                     |
| `close()`                   | Closes the control's connection; `await using` calls it for you                                                  |

### When the queue is idle

`waitForIdle` waits while any of these is left:

- a run `pending`, `executing` or `waitingForConcurrencySlot`;
- a fan-out waiting, executing, or delayed for a retry;
- a schedule firing waiting or executing, which is about to start its run.

It doesn't wait for a run `delayed`, `suspended` or `backingOff`, nor for a firing still to come: those can wait for hours. Call `makeRunsDueNow()` then `waitForIdle()` again to get past them.

### Get past a delay, a sleep or a backoff

`makeRunsDueNow` starts at once the runs dispatched with a `delay`, the runs asleep in `step.sleep` or `step.sleepUntil`, and the runs waiting out a retry backoff. It resolves with `{ task, runId }` per run started.

- A sleeping run resumes in the same attempt, past the sleep, as if its resume time had come: every sleep of that run still to come is moved to now.
- A run in backoff starts its next attempt.
- A run waiting for a concurrency slot and a firing still to come are left alone.

### Fire a schedule

`fireSchedule(taskName)` starts one run of a scheduled task now, through the same path as a firing of its cron: the handler gets `scheduledAt` set to now, and the retries of the task. It throws when no schedule is stored for that task: start the worker once first, since the worker stores the schedules at boot.

### Start each test clean

`reset()` removes every key under the prefix with `SCAN` and `UNLINK`, never `FLUSHDB`: every run, fan-out and saved step is gone, and keys under another prefix stay. The schedules are kept, each with its next firing. A running worker keeps working.

- It first waits until no attempt executes, up to `timeout` (default `"30s"`), then rejects naming how many still execute.
- It throws before touching anything when the calling process has `NODE_ENV=production`.
- Give your e2e app a prefix of its own, so a reset never reaches your dev data.

## Fire a schedule or skip a sleep in offline dev

Save this script once, and run it with `tsx` while the app and its worker run under `next dev`, to check a flow without waiting for a cron or a sleep:

```ts
// scripts/nextasking-control.mts
import { createNextaskingControl } from "@flefebvre/nextasking/testing";

const control = createNextaskingControl({
  connection: { url: process.env.REDIS_URL ?? "redis://localhost:6379" },
});
const [command, taskName] = process.argv.slice(2);

try {
  if (command === "fire" && taskName !== undefined) {
    console.log(await control.fireSchedule(taskName));
  } else if (command === "skip") {
    console.log(await control.makeRunsDueNow(taskName === undefined ? {} : { task: taskName }));
  } else {
    console.log("usage: tsx scripts/nextasking-control.mts fire <scheduled-task> | skip [task]");
  }
  await control.waitForIdle();
} finally {
  await control.close();
}
```

```sh
npx tsx scripts/nextasking-control.mts fire purge-expired-sessions
npx tsx scripts/nextasking-control.mts skip onboard-user
```

The `.mts` extension makes it an ES module, for the top-level `await`, whatever your `package.json` says. Pass the same `prefix` as your `createNextasking` call when it sets one.

## Exports

| Export                     | What it is                                                                                                       |
| -------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `executeAttempt`           | Runs one attempt of a handler in the test process and resolves with its `ExecutedAttempt`                        |
| `ExecutedAttempt`          | The type `executeAttempt` resolves with: `steps`, `sleeps`, `dispatches` and `emits`                             |
| `ExecuteAttemptOptions`    | The type of `executeAttempt`'s options: `savedSteps`, `attempt`, `runId`, `abortSignal`, `scheduledAt`, `logger` |
| `RecordedSleep`            | The type of one entry of `sleeps`: `id` and `resumesAt`                                                          |
| `fakeNextasking`           | Switches an instance to recording mode and returns its `NextaskingFake`                                          |
| `NextaskingFake`           | The type `fakeNextasking` returns: `dispatches`, `emits`, `replays`, `restore` and `[Symbol.dispose]`            |
| `RecordedDispatch`         | The type of one entry of `dispatches`                                                                            |
| `RecordedEmit`             | The type of one entry of `emits`                                                                                 |
| `RecordedReplay`           | The type of one entry of `replays`                                                                               |
| `createNextaskingControl`  | Opens a control on a queue: `waitForIdle`, `makeRunsDueNow`, `fireSchedule`, `reset` and `close`                 |
| `NextaskingControl`        | The type `createNextaskingControl` returns                                                                       |
| `NextaskingControlOptions` | The type of `createNextaskingControl`'s argument: `connection` and `prefix`                                      |
