# @flefebvre/nextasking

[![npm](https://img.shields.io/npm/v/%40flefebvre%2Fnextasking)](https://www.npmjs.com/package/@flefebvre/nextasking)
[![license](https://img.shields.io/npm/l/%40flefebvre%2Fnextasking)](./LICENSE)

Typed background tasks and events for Next.js, on Redis or Valkey. Define tasks and events in a few files, list them in one `createNextasking` call, dispatch a task or emit an event by name from server code, and run them in a worker embedded in `next start`.

**Requires** Next.js 16.0.10 or later, Node `^22.22`, `^24.5` or 26 and later, long-running Node hosting for the worker, and a Valkey 8 or Redis 7.4 or later. TypeScript is optional; with it, use 5.9 or later.

## Install

```sh
pnpm add @flefebvre/nextasking zod
```

Payload schemas take any [Standard Schema](https://standardschema.dev) library: Zod, Valibot, ArkType and others.

## At a glance

Define a task:

```ts
// src/nextasking/tasks/send-welcome-email.ts
import { defineTask } from "@flefebvre/nextasking";
import { z } from "zod";

export const sendWelcomeEmailTask = defineTask("send-welcome-email", {
  schema: z.object({ userId: z.string(), email: z.email() }),
  handler: async ({ payload }) => {
    await email.send({ to: payload.email, template: "welcome" });
  },
});
```

List it in the instance, and register the instance's type:

```ts
// src/nextasking/index.ts
import { createNextasking } from "@flefebvre/nextasking";
import { sendWelcomeEmailTask } from "@/nextasking/tasks/send-welcome-email";

export const nextasking = createNextasking({
  connection: { url: process.env.REDIS_URL ?? "redis://localhost:6379" },
  tasks: [sendWelcomeEmailTask],
});

declare module "@flefebvre/nextasking" {
  interface Register {
    nextasking: typeof nextasking;
  }
}
```

Dispatch it by name from a server action or a route handler; the name and the payload are typed:

```ts
// src/app/sign-up/actions.ts
"use server";
import { nextasking } from "@/nextasking";

export async function signUp(formData: FormData) {
  const user = await createUser(formData);
  await nextasking.dispatch("send-welcome-email", { userId: user.id, email: user.email });
}
```

Start the worker from `instrumentation.ts`:

```ts
// src/instrumentation.ts
export async function register() {
  if (process.env.NEXT_RUNTIME !== "nodejs" || process.env.NEXTASKING_WORKER !== "on") return;
  const { nextasking } = await import("@/nextasking");
  await nextasking.startWorker();
}
```

[Setup](docs/setup.md) walks through the rest: the Valkey or Redis, the environment, and the checklist that says when you are done.

## What it covers

- **Tasks and events**: tasks dispatched by name, events emitted to any number of listeners, a delay before a run starts, deduplication by your own id. See [Tasks and events](docs/tasks-and-events.md).
- **Retries and steps**: retries with backoff, durable steps whose results a retry reuses, and sleeps that free the worker. See [Retries and failures](docs/retries-and-failures.md) and [Steps](docs/steps.md).
- **Scheduled tasks**: cron schedules in a time zone, reconciled across deploys. See [Scheduled tasks](docs/scheduled-tasks.md).
- **Concurrency**: a cap per task across every instance, or per account or any other key. See [Concurrency](docs/concurrency.md).
- **The worker**: embedded in `next start`, with graceful shutdown on deploys. See [Worker](docs/worker.md) and [Deploys](docs/deploys.md).
- **A read API for dashboards**: runs, events and schedules, read from the instance. See [Dashboard](docs/dashboard.md).
- **Testing**: a fake for the code that dispatches and emits, and an in-process attempt for the handlers, from `@flefebvre/nextasking/testing`. See [Testing](docs/testing.md).

The [manual](docs/index.md) routes every job to its page.

## For agents

Add this line to your `AGENTS.md` or `CLAUDE.md`:

```md
For @flefebvre/nextasking, read `node_modules/@flefebvre/nextasking/docs/index.md` first — it is the
manual for the exact installed version.
```

The manual ships in the package, so what an agent reads is version-locked to the code next to it. There is no separate skill content to go stale, and nothing prints at install time.

## License

[MIT](./LICENSE)
