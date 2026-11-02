# nextasking

Background tasks for a Next.js app, on BullMQ with Redis or Valkey. Define a task, list it in `createNextasking`, dispatch it by name from server code, and run it in a worker started from `instrumentation.ts`.

## Rules that hold everywhere

- If a snippet in this manual disagrees with the installed `.d.ts`, trust the `.d.ts`.
- Import everything from the package root, `@flefebvre/nextasking`.
- Call the instance from server code only: route handlers, server actions, `instrumentation.ts`.

## Where to go

| Job                                        | Page                                    |
| ------------------------------------------ | --------------------------------------- |
| Define a task and its payload schema       | [Tasks and events](tasks-and-events.md) |
| Register tasks in `createNextasking`       | [Tasks and events](tasks-and-events.md) |
| Dispatch a task and handle dispatch errors | [Tasks and events](tasks-and-events.md) |
| Start the embedded worker                  | [Tasks and events](tasks-and-events.md) |
| Pass a logger and read the lifecycle lines | [Logging](logging.md)                   |
| Log from a handler                         | [Logging](logging.md)                   |

## Import map

```ts
import { createNextasking, defineTask, InvalidPayloadError, UnknownTaskError } from "@flefebvre/nextasking";
import type { Logger, Register, TaskContext, TaskDefinition } from "@flefebvre/nextasking";
```

| Export                | What it is                                                                        |
| --------------------- | --------------------------------------------------------------------------------- |
| `defineTask`          | Defines a task: a name, a Standard Schema payload schema and a handler            |
| `createNextasking`    | Builds the instance from the connection, the prefix, the logger and the task list |
| `UnknownTaskError`    | Thrown by `dispatch` for a name the instance doesn't list                         |
| `InvalidPayloadError` | Thrown by `dispatch` for a payload the task's schema refuses                      |
| `Logger`              | The type of the `logger` option and of the handler's `logger`                     |
| `Register`            | The interface you augment with your instance's type                               |
| `TaskContext`         | The type of the object a handler receives                                         |
| `TaskDefinition`      | The type `defineTask` returns                                                     |
