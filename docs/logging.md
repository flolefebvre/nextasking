# Logging

nextasking writes a fixed line when a run or the worker changes state, through the logger you pass. Without one, it writes nothing.

## Pass a logger

Pass `logger` to `createNextasking`. It takes any object with `debug`, `info`, `warn` and `error`, each called `(fields, message)`: pino's order.

```ts
// src/nextasking/index.ts
import { createNextasking } from "@flefebvre/nextasking";
import pino from "pino";

export const nextasking = createNextasking({
  connection: { url: process.env.REDIS_URL ?? "redis://localhost:6379" },
  logger: pino({ name: "nextasking" }),
  tasks: [sendWelcomeEmail],
});
```

- `logger: console` works as it is, for development: each line prints its fields, then its message.
- Leave `logger` out and the library is silent.
- Wrap another library's logger in an object of the four functions if its argument order differs.

## Lifecycle lines

Each message is fixed: filter and alert on it.

| Level   | Message                  | Fields                                                                                     |
| ------- | ------------------------ | ------------------------------------------------------------------------------------------ |
| `debug` | `run dispatched`         | `task`, `runId`                                                                            |
| `debug` | `attempt started`        | `task`, `runId`, `attempt`                                                                 |
| `info`  | `run completed`          | `task`, `runId`, `attempt`, `runDurationInMilliseconds`, `executionDurationInMilliseconds` |
| `info`  | `worker started`         | none                                                                                       |
| `info`  | `worker stopped`         | none                                                                                       |
| `warn`  | `no listeners for event` | `eventName`, `eventId`                                                                     |

- `run dispatched` comes before any attempt, so it has no `attempt`. Every line about an attempt has `task`, `runId` and `attempt`.
- A listener's run adds `eventName` and `eventId` to each of its lines, the handler's own included.
- `run dispatched` is written by `dispatch` only: a listener's run started by an emit has no such line. `emit` itself writes no line, unless the event has no listeners.
- `runDurationInMilliseconds`: from the dispatch to the completion. `executionDurationInMilliseconds`: from the start of the completing attempt to the completion.
- `worker started` is written when `startWorker()` has started the worker, before it has reached the server.
- Payloads are never logged, at any level.

## Log from a handler

Use the handler's `logger`, not your app logger: every line it writes carries `task`, `runId` and `attempt`, and in a listener `eventName` and `eventId` too.

```ts
export const sendWelcomeEmail = defineTask("send-welcome-email", {
  schema: z.object({ userId: z.string(), email: z.email() }),
  handler: async ({ payload, logger }) => {
    const { messageId } = await email.send({ to: payload.email, template: "welcome" });
    logger.info({ userId: payload.userId, messageId }, "welcome email sent");
  },
});
```

- It writes through the logger passed to `createNextasking`, and writes nothing when none was passed.
- `task`, `runId`, `attempt`, `eventName` and `eventId` always hold the run's values, even if your fields set the same keys.
- Log ids, not payload values: the payload is yours to keep out of the logs.
