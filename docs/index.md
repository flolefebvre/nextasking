# nextasking

Events and background tasks for a Next.js app, on BullMQ with Redis or Valkey. Define tasks and events, list them in `createNextasking`, dispatch a task or emit an event by name from server code, and run them in a worker started from `instrumentation.ts`, in `next start` or in the `nextasking worker` bin.

## Rules that hold everywhere

- If a snippet in this manual disagrees with the installed `.d.ts`, trust the `.d.ts`.
- Import everything from the package root, `@flefebvre/nextasking`, except the test helpers: those come from `@flefebvre/nextasking/testing`, in test files and dev scripts only.
- Call the instance from server code only: route handlers, server actions, `instrumentation.ts`.

## Where to go

| Job                                                | Page                                            |
| -------------------------------------------------- | ----------------------------------------------- |
| Learn the words this manual uses                   | [Core concepts](core-concepts.md)               |
| Add nextasking to an app, step by step             | [Setup](setup.md)                               |
| Lay out the definition files and name them         | [Setup](setup.md)                               |
| Set the environment variables a worker needs       | [Setup](setup.md)                               |
| Point the app's agents at this manual              | [Setup](setup.md)                               |
| Define a task and its payload schema               | [Tasks and events](tasks-and-events.md)         |
| Define an event                                    | [Tasks and events](tasks-and-events.md)         |
| Define a listener that reacts to an event          | [Tasks and events](tasks-and-events.md)         |
| Register tasks and events in `createNextasking`    | [Tasks and events](tasks-and-events.md)         |
| Dispatch a task                                    | [Tasks and events](tasks-and-events.md)         |
| Start a run later with `delay`                     | [Tasks and events](tasks-and-events.md)         |
| Emit an event, with or without listeners           | [Tasks and events](tasks-and-events.md)         |
| Run once per business fact with your own id        | [Tasks and events](tasks-and-events.md)         |
| Know how long a repeated id is dropped             | [Tasks and events](tasks-and-events.md)         |
| Dispatch or emit a name known only at runtime      | [Tasks and events](tasks-and-events.md)         |
| Handle dispatch and emit errors                    | [Tasks and events](tasks-and-events.md)         |
| Answer 503 when Redis is down                      | [Tasks and events](tasks-and-events.md)         |
| Set how long a call waits with `giveUpAfter`       | [Tasks and events](tasks-and-events.md)         |
| Write a duration                                   | [Tasks and events](tasks-and-events.md)         |
| Fix an error `createNextasking` throws at startup  | [Tasks and events](tasks-and-events.md)         |
| Start the embedded worker                          | [Tasks and events](tasks-and-events.md)         |
| Pass `startWorker` options                         | [Worker](worker.md)                             |
| Cap the attempts in flight with `concurrency`      | [Worker](worker.md)                             |
| Let a deploy finish running attempts               | [Worker](worker.md)                             |
| Run the worker in its own process                  | [Worker](worker.md)                             |
| Choose which processes run a worker                | [Worker](worker.md)                             |
| Run the worker bin with `output: "standalone"`     | [Worker](worker.md)                             |
| Stop a handler cleanly with `abortSignal`          | [Worker](worker.md)                             |
| Know what a stall is and when a run fails stalled  | [Worker](worker.md)                             |
| Know what happens to runs during a deploy overlap  | [Worker](worker.md)                             |
| Know which release decides each task option        | [Deploys](deploys.md)                           |
| Change a payload schema without breaking runs      | [Deploys](deploys.md)                           |
| Rename or rework a step that is already deployed   | [Deploys](deploys.md)                           |
| Remove a task, a listener or an event              | [Deploys](deploys.md)                           |
| Rename a task or an event                          | [Deploys](deploys.md)                           |
| Pick the Valkey or Redis for the queues            | [Worker](worker.md)                             |
| Set `maxmemory-policy` to `noeviction`             | [Worker](worker.md)                             |
| Know what the worker does when Redis is down       | [Worker](worker.md)                             |
| Configure webpack with `serverExternalPackages`    | [Worker](worker.md)                             |
| Limit a task's attempts across every instance      | [Concurrency](concurrency.md)                   |
| Limit a task's attempts per account or other key   | [Concurrency](concurrency.md)                   |
| Keep a scheduled task's firings from overlapping   | [Concurrency](concurrency.md)                   |
| Know what a run waiting for a slot costs           | [Concurrency](concurrency.md)                   |
| Run a task on a cron, in a time zone               | [Scheduled tasks](scheduled-tasks.md)           |
| Know what `scheduledAt` holds                      | [Scheduled tasks](scheduled-tasks.md)           |
| Run a scheduled task now                           | [Scheduled tasks](scheduled-tasks.md)           |
| Know what a deploy does to schedules               | [Scheduled tasks](scheduled-tasks.md)           |
| Know what daylight saving time does to a schedule  | [Scheduled tasks](scheduled-tasks.md)           |
| Know what happens to firings missed in downtime    | [Scheduled tasks](scheduled-tasks.md)           |
| Make a side effect run once across retries         | [Steps](steps.md)                               |
| Name steps, in a loop too                          | [Steps](steps.md)                               |
| Return a step result that compiles                 | [Steps](steps.md)                               |
| Run steps in parallel                              | [Steps](steps.md)                               |
| Pass an idempotency key to a provider              | [Steps](steps.md)                               |
| Sleep for minutes or days without holding workers  | [Steps](steps.md)                               |
| Skip work a sleep made unnecessary                 | [Steps](steps.md)                               |
| Emit or dispatch from a handler, once per run      | [Steps](steps.md)                               |
| Know the step size limits                          | [Steps](steps.md)                               |
| Fix a `StepMisuseError`                            | [Steps](steps.md)                               |
| Know what nextasking guarantees against duplicates | [Duplicate runs](duplicate-runs.md)             |
| Close the duplicate windows that stay open         | [Duplicate runs](duplicate-runs.md)             |
| Send an email or charge a card only once           | [Duplicate runs](duplicate-runs.md)             |
| Know how long a provider keeps an idempotency key  | [Duplicate runs](duplicate-runs.md)             |
| Set how often and how soon a failed run retries    | [Retries and failures](retries-and-failures.md) |
| Fail a run at once with `NonRetriableError`        | [Retries and failures](retries-and-failures.md) |
| Bound an attempt with `timeout`                    | [Retries and failures](retries-and-failures.md) |
| Know what a failed run keeps, and for how long     | [Retries and failures](retries-and-failures.md) |
| Replay a failed run after deploying a fix          | [Retries and failures](retries-and-failures.md) |
| Replay a failed fan-out without duplicates         | [Retries and failures](retries-and-failures.md) |
| Alert on a failed run or fan-out with a listener   | [Retries and failures](retries-and-failures.md) |
| Pass a logger and read the lifecycle lines         | [Logging](logging.md)                           |
| Protect a dashboard route                          | [Dashboard](dashboard.md)                       |
| Read one run: state, payload, errors, steps        | [Dashboard](dashboard.md)                       |
| Know what each of the eight run states means       | [Dashboard](dashboard.md)                       |
| Page through failed runs                           | [Dashboard](dashboard.md)                       |
| Count the backlog per task and state               | [Dashboard](dashboard.md)                       |
| Follow an event from its fan-out to each listener  | [Dashboard](dashboard.md)                       |
| Check that last night's scheduled run happened     | [Dashboard](dashboard.md)                       |
| Build a typed "run now" picker                     | [Dashboard](dashboard.md)                       |
| Know what a read costs and what it can miss        | [Dashboard](dashboard.md)                       |
| Log from a handler                                 | [Logging](logging.md)                           |
| Test code that dispatches or emits, without Redis  | [Testing](testing.md)                           |
| Assert on the recorded dispatches and emits        | [Testing](testing.md)                           |
| Know which checks a faked instance still runs      | [Testing](testing.md)                           |
| Know what throws on a faked instance               | [Testing](testing.md)                           |
| Test a whole handler in one call                   | [Testing](testing.md)                           |
| Simulate a retry or a resume of a handler          | [Testing](testing.md)                           |
| Simulate a shutdown in a handler test              | [Testing](testing.md)                           |
| Wait in an e2e test until the queue is idle        | [Testing](testing.md)                           |
| Get an e2e test past a delay, a sleep or a backoff | [Testing](testing.md)                           |
| Fire a schedule from a test or in offline dev      | [Testing](testing.md)                           |
| Skip a sleep in offline dev with a script          | [Testing](testing.md)                           |
| Start each e2e test from a clean queue             | [Testing](testing.md)                           |
| Run the worker under `next dev`                    | [Dev](dev.md)                                   |
| Pick up a task edit                                | [Dev](dev.md)                                   |
| Know what Ctrl-C does to the attempt in flight     | [Dev](dev.md)                                   |
| Change a task's steps                              | [Dev](dev.md)                                   |

## Import map

```ts
import {
  createNextasking,
  defineEvent,
  defineListener,
  defineScheduledTask,
  defineTask,
  fanOutFailed,
  FanOutNotReplayableError,
  InvalidPayloadError,
  NonRetriableError,
  RedisUnavailableError,
  runFailed,
  RunNotReplayableError,
  StepMisuseError,
  UnknownEventError,
  UnknownTaskError,
} from "@flefebvre/nextasking";
import type {
  ConnectionConfig,
  DispatchOptions,
  Duration,
  EmitOptions,
  EventDefinition,
  EventDefinitionSummary,
  EventId,
  EventName,
  EventWithRuns,
  FailureNoticeError,
  FailureReason,
  FanOutFailedPayload,
  FanOutState,
  ListedRun,
  ListedSchedule,
  ListenerDefinition,
  ListRunsOptions,
  Logger,
  PreviousFiring,
  Register,
  ReplayedEvent,
  ReplayedRun,
  Run,
  RunCounts,
  RunError,
  RunFailedPayload,
  RunId,
  RunPage,
  RunState,
  SavedStepResult,
  Schedule,
  ScheduledTaskContext,
  ScheduledTaskDefinition,
  StartWorkerOptions,
  StepContext,
  StepTools,
  TaskContext,
  TaskDefinition,
  TaskDefinitionSummary,
  TaskName,
} from "@flefebvre/nextasking";
import { createNextaskingControl, executeAttempt, fakeNextasking } from "@flefebvre/nextasking/testing";
import type {
  ExecuteAttemptOptions,
  ExecutedAttempt,
  NextaskingControl,
  NextaskingControlOptions,
  NextaskingFake,
  RecordedDispatch,
  RecordedEmit,
  RecordedReplay,
  RecordedSleep,
} from "@flefebvre/nextasking/testing";
```

| Export                     | What it is                                                                                                                           |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `defineTask`               | Defines a task: a name, a Standard Schema payload schema and a handler                                                               |
| `defineEvent`              | Defines an event: a name and a Standard Schema payload schema                                                                        |
| `defineListener`           | Defines a listener: a task subscribed to one event, taking that event's payload                                                      |
| `defineScheduledTask`      | Defines a scheduled task: a name, a cron with its time zone and a handler, and no payload                                            |
| `runFailed`                | The built-in event `nextasking/run.failed`, emitted on each final failure of a run; listen to it, never emit it                      |
| `fanOutFailed`             | The built-in event `nextasking/fan-out.failed`, emitted on each final failure of a fan-out; listen to it, never emit it              |
| `createNextasking`         | Builds the instance from the connection, the prefix, the logger, the events and the tasks                                            |
| `UnknownTaskError`         | Thrown by `dispatch`, `replay`, `getRun` and `listRuns` for a task name the instance doesn't list                                    |
| `UnknownEventError`        | Thrown by `emit`, `replayEvent` and `getEvent` for an event name the instance doesn't list                                           |
| `RunNotReplayableError`    | Thrown by `replay` for a run that is unknown, removed or not failed                                                                  |
| `FanOutNotReplayableError` | Thrown by `replayEvent` for a fan-out that is unknown, removed or not failed                                                         |
| `InvalidPayloadError`      | Thrown by `dispatch` and `emit` for a payload the schema refuses                                                                     |
| `NonRetriableError`        | Thrown by a handler to fail its run at once, skipping the retries left                                                               |
| `RedisUnavailableError`    | Thrown by every call on the instance when Redis doesn't answer within `giveUpAfter`; carries `runId` or `eventId`                    |
| `StepMisuseError`          | Thrown by a step on a bug: a repeated id, a nested step, a bad result, duration or wake time                                         |
| `ConnectionConfig`         | The type of `createNextasking`'s `connection`: `url` and `giveUpAfter` (default `"5s"`)                                              |
| `DispatchOptions`          | The type of `dispatch`'s third argument: `runId` and `delay`                                                                         |
| `Duration`                 | The type of a duration option: a whole number and `s`, `m`, `h` or `d`                                                               |
| `EmitOptions`              | The type of `emit`'s third argument: `eventId`                                                                                       |
| `EventDefinition`          | The type `defineEvent` returns                                                                                                       |
| `EventDefinitionSummary`   | The type of one entry of `eventDefinitions`: `name` and `listenerNames`                                                              |
| `EventId`                  | The type of an event id: a string                                                                                                    |
| `EventName`                | The union of the registered instance's event names, the built-in events included; `never` without `Register`                         |
| `EventWithRuns`            | The type `getEvent` returns: `event`, `eventId`, `fanOut` and `runsByListener`                                                       |
| `FailureReason`            | The type of a failed run's `reason`: `"retries exhausted"`, `"non-retriable"`, `"stalled"` or `"unknown task"`                       |
| `FailureNoticeError`       | The type of a failure notice's `error`: `name`, `message` and `stack`                                                                |
| `FanOutFailedPayload`      | The type of `fanOutFailed`'s payload: `eventName`, `eventId`, `error` and `failedAt`                                                 |
| `FanOutState`              | The type of `getEvent`'s `fanOut`: `"pending"`, `"completed"` or `"failed"`                                                          |
| `ListedRun`                | The type of a run in `listRuns`' page: a `Run` without `payload`, `errors` and `steps`, with `lastErrorMessage`                      |
| `ListedSchedule`           | The type of one entry `listSchedules` returns: `task`, `cron`, `timezone`, `nextFiringAt` and `previousFiring`                       |
| `ListenerDefinition`       | The type `defineListener` returns                                                                                                    |
| `ListRunsOptions`          | The type of `listRuns`' argument: `state`, `task`, `limit` and `cursor`                                                              |
| `Logger`                   | The type of the `logger` option and of the handler's `logger`                                                                        |
| `PreviousFiring`           | The type of a listed schedule's `previousFiring`: `scheduledAt` and `runId`                                                          |
| `Register`                 | The interface you augment with your instance's type; it types names and a handler's calls                                            |
| `ReplayedEvent`            | The type `replayEvent` returns: `eventId`                                                                                            |
| `ReplayedRun`              | The type `replay` returns: `runId`                                                                                                   |
| `Run`                      | The type `getRun` returns: a run of a registered task, narrowed by `task` and by `state`                                             |
| `RunCounts`                | The type `countRuns` returns: a count per registered task and per state                                                              |
| `RunError`                 | The type of one entry of a run's `errors`: `attempt`, `name`, `message`, `stack` and `failedAt`                                      |
| `RunFailedPayload`         | The type of `runFailed`'s payload: `task`, `runId`, `reason`, `error`, `attempt`, `failedAt`, and the source event                   |
| `RunId`                    | The type of a run id: a string                                                                                                       |
| `RunPage`                  | The type `listRuns` returns: `runs` and `nextCursor`                                                                                 |
| `RunState`                 | The type of a run's `state`: one of the eight run states                                                                             |
| `SavedStepResult`          | The type of one entry of a run's `steps`: `id` and `result`                                                                          |
| `Schedule`                 | The type of a scheduled task's `schedule` option: `cron` and `timezone`                                                              |
| `ScheduledTaskContext`     | The type of the object a scheduled task's handler receives: no `payload`, and `scheduledAt`                                          |
| `ScheduledTaskDefinition`  | The type `defineScheduledTask` returns                                                                                               |
| `StartWorkerOptions`       | The type of `startWorker`'s argument: `concurrency` and `shutdownTimeout`                                                            |
| `StepContext`              | The type of the object a step's `fn` receives: `idempotencyKey`                                                                      |
| `StepTools`                | The type of the handler's `step`: `step.run`, `step.sleep` and `step.sleepUntil`                                                     |
| `TaskContext`              | The type of the object a handler receives                                                                                            |
| `TaskDefinition`           | The type `defineTask` returns                                                                                                        |
| `TaskDefinitionSummary`    | The type of one entry of `taskDefinitions`: `name`, `event` and `schedule`                                                           |
| `TaskName`                 | The union of the registered instance's task names, listeners and scheduled tasks included; `never` without `Register`                |
| `createNextaskingControl`  | From `./testing`: opens a control on a queue for e2e tests and offline dev: `waitForIdle`, `makeRunsDueNow`, `fireSchedule`, `reset` |
| `NextaskingControl`        | From `./testing`: the type `createNextaskingControl` returns                                                                         |
| `NextaskingControlOptions` | From `./testing`: the type of `createNextaskingControl`'s argument: `connection` and `prefix`                                        |
| `executeAttempt`           | From `./testing`: runs one attempt of a handler in a test, with recorded sleeps and sends                                            |
| `ExecutedAttempt`          | From `./testing`: the type `executeAttempt` resolves with: `steps`, `sleeps`, `dispatches` and `emits`                               |
| `ExecuteAttemptOptions`    | From `./testing`: the type of `executeAttempt`'s options: `savedSteps`, `attempt`, `runId`, `abortSignal`, `scheduledAt`, `logger`   |
| `RecordedSleep`            | From `./testing`: the type of one entry of an attempt's `sleeps`: `id` and `wakeAt`                                                  |
| `fakeNextasking`           | From `./testing`: switches an instance to recording mode in a test and returns its `NextaskingFake`                                  |
| `NextaskingFake`           | From `./testing`: the type `fakeNextasking` returns: `dispatches`, `emits`, `replays` and `restore`                                  |
| `RecordedDispatch`         | From `./testing`: the type of one entry of a fake's `dispatches`: `task`, `payload`, `runId` and `delay`                             |
| `RecordedEmit`             | From `./testing`: the type of one entry of a fake's `emits`: `event`, `payload` and `eventId`                                        |
| `RecordedReplay`           | From `./testing`: the type of one entry of a fake's `replays`: `task` and `runId`, or `event` and `eventId`                          |
