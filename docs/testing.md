# Testing

Test app code that dispatches or emits without mocking the library and without Redis. `fakeNextasking(nextasking)` switches your own instance to recording mode in place: code that imports `nextasking` is faked with no module mocking. Every check still runs for real; only the Redis write is replaced, by a record of what would have been written.

Import it from `@flefebvre/nextasking/testing`, in test files only. It has no test-runner dependency: assert with your runner's own `expect`.

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

## Exports

| Export             | What it is                                                                                            |
| ------------------ | ----------------------------------------------------------------------------------------------------- |
| `fakeNextasking`   | Switches an instance to recording mode and returns its `NextaskingFake`                               |
| `NextaskingFake`   | The type `fakeNextasking` returns: `dispatches`, `emits`, `replays`, `restore` and `[Symbol.dispose]` |
| `RecordedDispatch` | The type of one entry of `dispatches`                                                                 |
| `RecordedEmit`     | The type of one entry of `emits`                                                                      |
| `RecordedReplay`   | The type of one entry of `replays`                                                                    |
