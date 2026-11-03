# Setup

Add nextasking to a Next.js app: install it, give it a Valkey or Redis, lay out the definitions under `src/nextasking/`, start the worker from `instrumentation.ts`, and set the environment. Work through the sections in order, then tick the [Done when](#done-when) checklist.

## Install

```sh
pnpm add @flefebvre/nextasking zod
```

- Add one [Standard Schema](https://standardschema.dev) library for payload schemas: Zod, Valibot, ArkType or another. This manual's examples use Zod.
- nextasking needs Next.js 16 or later and Node `^22.22`, `^24.5` or 26 and later. Run the worker on long-running Node hosting; a serverless function can only dispatch and emit.
- TypeScript is optional; with it, use 5.9 or later.

## Run Valkey or Redis

The queues live in a Valkey or Redis that the app and every worker reach. Give the queues a server of their own, not your cache's, and set its eviction policy:

```sh
docker run -d --name nextasking-valkey -p 6379:6379 valkey/valkey:9 --maxmemory-policy noeviction
```

- `maxmemory-policy` must be `noeviction`: a worker refuses to start on another policy it can read. See [Worker](worker.md#give-queues-their-own-valkey-or-redis).
- nextasking is tested on Valkey 8 and 9 and on Redis 8.
- Use a server without failover, or accept the gap a failover leaves. See [Duplicate runs](duplicate-runs.md#a-redis-failover).
- Give dev its own server or its own `prefix`. See [Dev](dev.md#run-the-worker-under-next-dev).

## Lay out the definitions

Put every definition under `src/nextasking/`, one per file, the file named after the definition:

```text
src/
  instrumentation.ts                 starts the worker
  nextasking/
    index.ts                         the instance and the Register augmentation
    events/
      invoice-paid.ts                defineEvent("invoice.paid", …)
    tasks/
      send-welcome-email.ts          defineTask("send-welcome-email", …)
      record-invoice-payment.ts      defineListener("record-invoice-payment", { on: invoicePaid, … })
      purge-expired-sessions.ts      defineScheduledTask("purge-expired-sessions", …)
```

| What           | Name                                   | Example                           |
| -------------- | -------------------------------------- | --------------------------------- |
| Task           | kebab-case, what it does               | `send-welcome-email`              |
| Listener       | kebab-case, what it does, under tasks/ | `record-invoice-payment`          |
| Scheduled task | kebab-case, under tasks/               | `purge-expired-sessions`          |
| Event          | dotted, what happened                  | `invoice.paid`, `user.signed-up`  |
| File           | the definition's name, kebab-case      | `tasks/send-welcome-email.ts`     |
| Exported const | the name in camelCase                  | `sendWelcomeEmail`, `invoicePaid` |

- Tasks and events share one namespace: two definitions with the same name make `createNextasking` throw.
- Names starting with `nextasking/` are reserved for the library.
- Choose names to keep: a name is stored with every run, and renaming takes two deploys. See [Deploys](deploys.md#rename-a-task-or-an-event).

See [Tasks and events](tasks-and-events.md) to write each kind of definition.

## Create the instance

```ts
// src/nextasking/index.ts
import { createNextasking } from "@flefebvre/nextasking";
import { invoicePaid } from "@/nextasking/events/invoice-paid";
import { recordInvoicePayment } from "@/nextasking/tasks/record-invoice-payment";
import { sendWelcomeEmail } from "@/nextasking/tasks/send-welcome-email";

export const nextasking = createNextasking({
  connection: { url: process.env.REDIS_URL ?? "redis://localhost:6379" },
  events: [invoicePaid],
  tasks: [sendWelcomeEmail, recordInvoicePayment],
});

declare module "@flefebvre/nextasking" {
  interface Register {
    nextasking: typeof nextasking;
  }
}
```

- List every event in `events` and every task, listeners and scheduled tasks included, in `tasks`.
- Keep the `Register` augmentation next to the instance: it types every name, and a handler's `emit` and `dispatch`.
- `createNextasking` opens no connection, so `next build` never needs Redis.
- Pass your app's logger as `logger`, or `console` while you set up; without one the library writes nothing. See [Logging](logging.md).

See [Register the tasks and events](tasks-and-events.md#register-the-tasks-and-events) for `prefix`, `giveUpAfter` and the startup checks.

## Start the worker

```ts
// src/instrumentation.ts
export async function register() {
  if (process.env.NEXT_RUNTIME !== "nodejs" || process.env.NEXTASKING_WORKER !== "on") return;
  const { nextasking } = await import("@/nextasking");
  await nextasking.startWorker();
}
```

- Import the instance inside `register()`, after the runtime check, so the edge runtime never loads it.
- The variable is yours: only the processes that set it run tasks. Every other process dispatches and emits only.
- To run tasks in a process of their own, use the `nextasking worker` bin on the same build. See [Worker](worker.md#run-the-worker-in-its-own-process).

## Set the environment

```json
// package.json
{
  "scripts": {
    "dev": "NEXTASKING_WORKER=on next dev",
    "build": "next build",
    "start": "NEXT_MANUAL_SIG_HANDLE=1 NEXTASKING_WORKER=on next start",
    "worker": "NEXTASKING_WORKER=on nextasking worker"
  }
}
```

| Variable                 | Owner    | Set it in                            | Why                                                                                                               |
| ------------------------ | -------- | ------------------------------------ | ----------------------------------------------------------------------------------------------------------------- |
| `REDIS_URL`              | your app | every process                        | `index.ts` reads it for `connection.url`                                                                          |
| `NEXTASKING_WORKER`      | your app | each process that runs tasks         | `instrumentation.ts` starts the worker when it is `on`                                                            |
| `NEXT_MANUAL_SIG_HANDLE` | Next.js  | `next start` when it runs the worker | A deploy's SIGTERM lets running attempts finish; see [Worker](worker.md#let-a-deploy-finish-the-running-attempts) |

- Without `NEXT_MANUAL_SIG_HANDLE`, a `next start` running the worker warns at startup that a deploy cuts the attempts in flight. The `nextasking worker` bin sets it itself.
- The `worker` script is for a separate worker process. Leave it out to run tasks in `next start` alone; to keep tasks off the web servers, drop `NEXTASKING_WORKER=on` from `start` instead. See [Choose which processes run a worker](worker.md#choose-which-processes-run-a-worker).

## Webpack only: serverExternalPackages

Turbopack, the default bundler, needs nothing. With `next build --webpack`, list the library:

```ts
// next.config.ts
import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  serverExternalPackages: ["@flefebvre/nextasking"],
};

export default nextConfig;
```

See [Worker](worker.md#bundling). For `output: "standalone"`, see [Worker](worker.md#run-the-bin-with-output-standalone).

## Point your agent at the manual

Add this line to the app's `AGENTS.md` or `CLAUDE.md`, so every agent reads the manual of the installed version before writing code:

```md
For @flefebvre/nextasking, read `node_modules/@flefebvre/nextasking/docs/index.md` first — it is the
manual for the exact installed version.
```

## Done when

- [ ] `@flefebvre/nextasking` and a Standard Schema library are in `dependencies`.
- [ ] The Valkey or Redis for the queues answers at `REDIS_URL` and has `maxmemory-policy noeviction`.
- [ ] Every definition lives in its own file under `src/nextasking/tasks/` or `src/nextasking/events/`, named by the conventions above.
- [ ] `src/nextasking/index.ts` lists every event and task and augments `Register`: `nextasking.dispatch("no-such-task")` doesn't compile.
- [ ] `src/instrumentation.ts` starts the worker behind `NEXTASKING_WORKER`, after the `NEXT_RUNTIME` check.
- [ ] Each process that runs tasks sets `NEXTASKING_WORKER=on`, and `next start` running a worker sets `NEXT_MANUAL_SIG_HANDLE=1`.
- [ ] `next build` passes with Redis stopped.
- [ ] A task dispatched from a route handler or a server action completes: `getRun` returns `state: "completed"`.
- [ ] With webpack only, `serverExternalPackages` lists `@flefebvre/nextasking`.
- [ ] The app's `AGENTS.md` or `CLAUDE.md` points at `node_modules/@flefebvre/nextasking/docs/index.md`.
