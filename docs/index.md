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
| Set how often and how soon a failed run retries   | [Retries and failures](retries-and-failures.md) |
| Fail a run at once with `NonRetriableError`       | [Retries and failures](retries-and-failures.md) |
| Bound an attempt with `timeout`                   | [Retries and failures](retries-and-failures.md) |
| Know what a failed run keeps, and for how long    | [Retries and failures](retries-and-failures.md) |
| Pass a logger and read the lifecycle lines        | [Logging](logging.md)                           |
| Log from a handler                                | [Logging](logging.md)                           |

## Import map

```ts
import {
  createNextasking,
  defineEvent,
  defineListener,
  defineTask,
  InvalidPayloadError,
  NonRetriableError,
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
  TaskContext,
  TaskDefinition,
  TaskName,
} from "@flefebvre/nextasking";
```

| Export                | What it is                                                                                        |
| --------------------- | ------------------------------------------------------------------------------------------------- |
| `defineTask`          | Defines a task: a name, a Standard Schema payload schema and a handler                            |
| `defineEvent`         | Defines an event: a name and a Standard Schema payload schema                                     |
| `defineListener`      | Defines a listener: a task subscribed to one event, taking that event's payload                   |
| `createNextasking`    | Builds the instance from the connection, the prefix, the logger, the events and the tasks         |
| `UnknownTaskError`    | Thrown by `dispatch` for a name the instance doesn't list                                         |
| `UnknownEventError`   | Thrown by `emit` for a name the instance doesn't list                                             |
| `InvalidPayloadError` | Thrown by `dispatch` and `emit` for a payload the schema refuses                                  |
| `NonRetriableError`   | Thrown by a handler to fail its run at once, skipping the retries left                            |
| `DispatchOptions`     | The type of `dispatch`'s third argument: `runId` and `delay`                                      |
| `Duration`            | The type of a duration option: a whole number and `s`, `m`, `h` or `d`                            |
| `EmitOptions`         | The type of `emit`'s third argument: `eventId`                                                    |
| `EventDefinition`     | The type `defineEvent` returns                                                                    |
| `EventId`             | The type of an event id: a string                                                                 |
| `EventName`           | The union of the registered instance's event names; `never` without the `Register` augmentation   |
| `ListenerDefinition`  | The type `defineListener` returns                                                                 |
| `Logger`              | The type of the `logger` option and of the handler's `logger`                                     |
| `Register`            | The interface you augment with your instance's type                                               |
| `RunId`               | The type of a run id: a string                                                                    |
| `TaskContext`         | The type of the object a handler receives                                                         |
| `TaskDefinition`      | The type `defineTask` returns                                                                     |
| `TaskName`            | The union of the registered instance's task names, listeners included; `never` without `Register` |
