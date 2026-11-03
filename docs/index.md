# nextasking

Events and background tasks for a Next.js app, on BullMQ with Redis or Valkey. Define tasks and events, list them in `createNextasking`, dispatch a task or emit an event by name from server code, and run them in a worker started from `instrumentation.ts`.

## Rules that hold everywhere

- If a snippet in this manual disagrees with the installed `.d.ts`, trust the `.d.ts`.
- Import everything from the package root, `@flefebvre/nextasking`.
- Call the instance from server code only: route handlers, server actions, `instrumentation.ts`.

## Where to go

| Job                                               | Page                                            |
| ------------------------------------------------- | ----------------------------------------------- |
| Define a task and its payload schema              | [Tasks and events](tasks-and-events.md)         |
| Define an event                                   | [Tasks and events](tasks-and-events.md)         |
| Define a listener that reacts to an event         | [Tasks and events](tasks-and-events.md)         |
| Register tasks and events in `createNextasking`   | [Tasks and events](tasks-and-events.md)         |
| Dispatch a task                                   | [Tasks and events](tasks-and-events.md)         |
| Start a run later with `delay`                    | [Tasks and events](tasks-and-events.md)         |
| Emit an event, with or without listeners          | [Tasks and events](tasks-and-events.md)         |
| Run once per business fact with your own id       | [Tasks and events](tasks-and-events.md)         |
| Know how long a repeated id is dropped            | [Tasks and events](tasks-and-events.md)         |
| Dispatch or emit a name known only at runtime     | [Tasks and events](tasks-and-events.md)         |
| Handle dispatch and emit errors                   | [Tasks and events](tasks-and-events.md)         |
| Write a duration                                  | [Tasks and events](tasks-and-events.md)         |
| Fix an error `createNextasking` throws at startup | [Tasks and events](tasks-and-events.md)         |
| Start the embedded worker                         | [Tasks and events](tasks-and-events.md)         |
| Pass `startWorker` options                        | [Worker](worker.md)                             |
| Cap the attempts in flight with `concurrency`     | [Worker](worker.md)                             |
| Let a deploy finish running attempts              | [Worker](worker.md)                             |
| Stop a handler cleanly with `abortSignal`         | [Worker](worker.md)                             |
| Know what a stall is and when a run fails stalled | [Worker](worker.md)                             |
| Pick the Valkey or Redis for the queues           | [Worker](worker.md)                             |
| Configure webpack with `serverExternalPackages`   | [Worker](worker.md)                             |
| Limit a task's attempts across every instance     | [Concurrency](concurrency.md)                   |
| Limit a task's attempts per account or other key  | [Concurrency](concurrency.md)                   |
| Keep a scheduled task's firings from overlapping  | [Concurrency](concurrency.md)                   |
| Know what a run waiting for a slot costs          | [Concurrency](concurrency.md)                   |
| Run a task on a cron, in a time zone              | [Scheduled tasks](scheduled-tasks.md)           |
| Know what `scheduledAt` holds                     | [Scheduled tasks](scheduled-tasks.md)           |
| Run a scheduled task now                          | [Scheduled tasks](scheduled-tasks.md)           |
| Know what a deploy does to schedules              | [Scheduled tasks](scheduled-tasks.md)           |
| Know what daylight saving time does to a schedule | [Scheduled tasks](scheduled-tasks.md)           |
| Know what happens to firings missed in downtime   | [Scheduled tasks](scheduled-tasks.md)           |
| Make a side effect run once across retries        | [Steps](steps.md)                               |
| Name steps, in a loop too                         | [Steps](steps.md)                               |
| Return a step result that compiles                | [Steps](steps.md)                               |
| Run steps in parallel                             | [Steps](steps.md)                               |
| Pass an idempotency key to a provider             | [Steps](steps.md)                               |
| Sleep for minutes or days without holding workers | [Steps](steps.md)                               |
| Skip work a sleep made unnecessary                | [Steps](steps.md)                               |
| Emit or dispatch from a handler, once per run     | [Steps](steps.md)                               |
| Know the step size limits                         | [Steps](steps.md)                               |
| Fix a `StepMisuseError`                           | [Steps](steps.md)                               |
| Set how often and how soon a failed run retries   | [Retries and failures](retries-and-failures.md) |
| Fail a run at once with `NonRetriableError`       | [Retries and failures](retries-and-failures.md) |
| Bound an attempt with `timeout`                   | [Retries and failures](retries-and-failures.md) |
| Know what a failed run keeps, and for how long    | [Retries and failures](retries-and-failures.md) |
| Pass a logger and read the lifecycle lines        | [Logging](logging.md)                           |
| Log from a handler                                | [Logging](logging.md)                           |
| Run the worker under `next dev`                   | [Dev](dev.md)                                   |
| Pick up a task edit                               | [Dev](dev.md)                                   |
| Know what Ctrl-C does to the attempt in flight    | [Dev](dev.md)                                   |
| Change a task's steps                             | [Dev](dev.md)                                   |

## Import map

```ts
import {
  createNextasking,
  defineEvent,
  defineListener,
  defineScheduledTask,
  defineTask,
  InvalidPayloadError,
  NonRetriableError,
  StepMisuseError,
  UnknownEventError,
  UnknownTaskError,
} from "@flefebvre/nextasking";
import type {
  DispatchOptions,
  Duration,
  EmitOptions,
  EventDefinition,
  EventId,
  EventName,
  ListenerDefinition,
  Logger,
  Register,
  RunId,
  Schedule,
  ScheduledTaskContext,
  ScheduledTaskDefinition,
  StartWorkerOptions,
  StepContext,
  StepTools,
  TaskContext,
  TaskDefinition,
  TaskName,
} from "@flefebvre/nextasking";
```

| Export                    | What it is                                                                                                            |
| ------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `defineTask`              | Defines a task: a name, a Standard Schema payload schema and a handler                                                |
| `defineEvent`             | Defines an event: a name and a Standard Schema payload schema                                                         |
| `defineListener`          | Defines a listener: a task subscribed to one event, taking that event's payload                                       |
| `defineScheduledTask`     | Defines a scheduled task: a name, a cron with its time zone and a handler, and no payload                             |
| `createNextasking`        | Builds the instance from the connection, the prefix, the logger, the events and the tasks                             |
| `UnknownTaskError`        | Thrown by `dispatch` for a name the instance doesn't list                                                             |
| `UnknownEventError`       | Thrown by `emit` for a name the instance doesn't list                                                                 |
| `InvalidPayloadError`     | Thrown by `dispatch` and `emit` for a payload the schema refuses                                                      |
| `NonRetriableError`       | Thrown by a handler to fail its run at once, skipping the retries left                                                |
| `StepMisuseError`         | Thrown by a step on a bug: a repeated id, a nested step, a bad result, duration or wake time                          |
| `DispatchOptions`         | The type of `dispatch`'s third argument: `runId` and `delay`                                                          |
| `Duration`                | The type of a duration option: a whole number and `s`, `m`, `h` or `d`                                                |
| `EmitOptions`             | The type of `emit`'s third argument: `eventId`                                                                        |
| `EventDefinition`         | The type `defineEvent` returns                                                                                        |
| `EventId`                 | The type of an event id: a string                                                                                     |
| `EventName`               | The union of the registered instance's event names; `never` without the `Register` augmentation                       |
| `ListenerDefinition`      | The type `defineListener` returns                                                                                     |
| `Logger`                  | The type of the `logger` option and of the handler's `logger`                                                         |
| `Register`                | The interface you augment with your instance's type; it types names and a handler's calls                             |
| `RunId`                   | The type of a run id: a string                                                                                        |
| `Schedule`                | The type of a scheduled task's `schedule` option: `cron` and `timezone`                                               |
| `ScheduledTaskContext`    | The type of the object a scheduled task's handler receives: no `payload`, and `scheduledAt`                           |
| `ScheduledTaskDefinition` | The type `defineScheduledTask` returns                                                                                |
| `StartWorkerOptions`      | The type of `startWorker`'s argument: `concurrency` and `shutdownTimeout`                                             |
| `StepContext`             | The type of the object a step's `fn` receives: `idempotencyKey`                                                       |
| `StepTools`               | The type of the handler's `step`: `step.run`, `step.sleep` and `step.sleepUntil`                                      |
| `TaskContext`             | The type of the object a handler receives                                                                             |
| `TaskDefinition`          | The type `defineTask` returns                                                                                         |
| `TaskName`                | The union of the registered instance's task names, listeners and scheduled tasks included; `never` without `Register` |
