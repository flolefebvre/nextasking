# Tasks and events

A task is a named piece of work with a payload schema and a handler. Define it, list it in `createNextasking`, dispatch it by name, and start a worker to run it.

## Define a task

Put each task in its own file under `src/nextasking/tasks/`. Name tasks in kebab-case.

```ts
// src/nextasking/tasks/send-welcome-email.ts
import { defineTask } from "@flefebvre/nextasking";
import { z } from "zod";

export const sendWelcomeEmail = defineTask("send-welcome-email", {
  schema: z.object({ userId: z.string(), email: z.email() }),
  handler: async ({ payload }) => {
    await email.send({ to: payload.email, template: "welcome" });
  },
});
```

- `schema` takes any [Standard Schema](https://standardschema.dev) library: Zod, Valibot, ArkType and others.
- The handler receives one object:
  - `payload`: the schema's output, already validated
  - `runId`: the id `dispatch` returned for this run
  - `attempt`: the attempt number, starting at 1
- Keep the payload small and JSON: ids and short values, not whole records. Load the rest in the handler.

## Register the tasks

Create the instance once, in `src/nextasking/index.ts`, and augment `Register` with its type.

```ts
// src/nextasking/index.ts
import { createNextasking } from "@flefebvre/nextasking";
import { sendWelcomeEmail } from "@/nextasking/tasks/send-welcome-email";

export const nextasking = createNextasking({
  connection: { url: process.env.REDIS_URL ?? "redis://localhost:6379" },
  tasks: [sendWelcomeEmail],
});

declare module "@flefebvre/nextasking" {
  interface Register {
    nextasking: typeof nextasking;
  }
}
```

- `createNextasking` opens no connection. Importing the instance is safe anywhere, `next build` included; the first `dispatch` connects.
- `prefix` (default `"nextasking"`) puts every key of the instance under that prefix. Give two apps sharing one server different prefixes.

## Dispatch a task

```ts
// src/app/sign-up/actions.ts
"use server";
import { nextasking } from "@/nextasking";

export async function signUp(formData: FormData) {
  const user = await createUser(formData);
  await nextasking.dispatch("send-welcome-email", { userId: user.id, email: user.email });
}
```

- `dispatch(name, payload)` starts one run and resolves with `{ runId }`, a random UUID.
- The name is checked at compile time: a name the instance doesn't list doesn't compile, and the error lists the valid names.
- The payload is typed from the schema's input: a wrong payload doesn't compile.
- At runtime, `dispatch` validates the payload and stores the schema's output; the handler receives that output.

### Errors

```ts
import { InvalidPayloadError, UnknownTaskError } from "@flefebvre/nextasking";

try {
  await nextasking.dispatch(name, payload);
} catch (error) {
  if (error instanceof InvalidPayloadError) {
    return { status: 400, issues: error.issues };
  }
  throw error;
}
```

- `UnknownTaskError`: the name isn't in `tasks`. Its message lists the valid task names. Reached only through a cast or plain JavaScript.
- `InvalidPayloadError`: the schema refused the payload. `issues` lists each issue as `{ path, message }`, with `path` dotted (`address.zipCode`) or `(root)`. Messages are redacted of payload values (`[redacted]`), so the error is safe to log.
- Both are thrown before anything is stored.

## Start the worker

Start the worker from `instrumentation.ts`, behind an environment variable you own, so you choose which processes run tasks.

```ts
// src/instrumentation.ts
export async function register() {
  if (process.env.NEXT_RUNTIME !== "nodejs" || process.env.NEXTASKING_WORKER !== "embedded") return;
  const { nextasking } = await import("@/nextasking");
  await nextasking.startWorker();
}
```

- `startWorker()` runs one worker in this process, executing every task the instance lists.
- A process that never calls `startWorker()` only dispatches.
- Calling `startWorker()` a second time in the same process throws.
- Completed runs are kept 24 hours, and at most 1,000.
