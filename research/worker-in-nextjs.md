# Running a BullMQ worker inside Next.js

Research for issue #5. Question: what it takes to run a BullMQ worker inside a Next.js server process reliably, and to run the same task code as a standalone Node process.

## Versions and method

- Next.js **16.4.3** (npm `latest` on 2026-11-01), read at tag `v16.4.3`. BullMQ **6.5.3**, read at tag `v6.5.3`. ioredis 5.11.1 (6.0.0 exists; BullMQ's peer range is `>=5.0.0`). React 19.2.0, Node 24.16.0, Valkey 8 in Docker.
- Prior art: Payload **3.92.1** (tag `v3.92.1`), Inngest JS **4.26.2**.
- Facts marked **(observed)** come from a throwaway Next 16.4.3 app: an `instrumentation.ts` that starts a BullMQ worker, a route that enqueues jobs, and a tsx entry that starts the same worker without Next. Everything else cites source or docs. Source links point at the pinned tags.

## Summary

- `register()` runs **once per Node process**, eagerly at boot, in `next start`, in `output: 'standalone'` (`server.js`), and in each `next dev` child process (observed). Next caches the call in a module-level promise. It is skipped during `next build` (`NEXT_PHASE === 'phase-production-build'`). Every request awaits it, so a `register()` that throws leaves the process up but returns 500 on every request (observed).
- `next dev` runs a parent CLI plus one forked `next-server` child. Editing a task file, `instrumentation.ts` or a page does **not** re-run `register()` or re-evaluate the worker's modules. The worker keeps running stale code until the child restarts, under both Turbopack and webpack (observed). A `next.config` change forks a new child, which calls `register()` again. The old child calls `process.exit()` without closing the worker, so its active jobs stall (observed).
- One file imported from both `instrumentation.ts` and an app route is evaluated **twice** in the same process, as separate module instances. Only `globalThis` is shared (observed). Modules imported by routes are also evaluated during `next build` ("Collecting page data"), in build worker processes (observed).
- The instrumentation bundle resolves packages with the **`react-server`** export condition in both bundlers, allows `server-only`, and gets Next's vendored RSC React (`React.useState` is `undefined`, observed). A plain Node process uses the default conditions, so `import 'server-only'` throws there (observed). `--conditions=react-server` fixes that but breaks `react-dom/server`, which Next's bundle aliases to a working build (observed). No Node flag reproduces Next's resolution exactly.
- BullMQ and ioredis are **bundled** into the instrumentation output by default. That works with Turbopack. Webpack warns on BullMQ's optional `@valkey/valkey-glide` require. `serverExternalPackages: ['bullmq']` externalizes BullMQ in both bundlers and works (observed). In BullMQ 6, `ioredis` is an optional peer dependency.
- Signals: without `NEXT_MANUAL_SIG_HANDLE`, Next's own SIGTERM handler calls `process.exit(143)` about 12 ms after the signal. That cuts short any `worker.close()` the app started, and the in-flight job is abandoned (observed). With `NEXT_MANUAL_SIG_HANDLE=true` and an app handler, the in-flight job finishes and the process exits cleanly (observed). The app handler then owns the whole shutdown. Manual handling is not available in `next dev`, which SIGKILLs the child 100 ms after forwarding the signal.
- CPU-bound handlers block rendering and BullMQ's lock renewal, which runs on the same event loop. In the observed run, a 10 s synchronous job with `lockDuration` 4 s caused an 8 s page response. The job also ran **twice**: another worker re-queued it as stalled, both runs hit `Missing lock`, and the job ended failed (observed). With the defaults, a synchronous block of roughly 30 s is enough to lose a lock.
- Prior art: Payload runs its job queue in the Next process from the first request, not from `register()`. It caches instances on `global`, detects the build with `NEXT_PHASE` plus `npm_lifecycle_event`, and recommends a separate bin process on dedicated servers. Inngest's long-lived `connect` worker handles SIGINT/SIGTERM itself and moves heartbeats and lease extension to a worker thread by default.

---

## 1. Starting from `instrumentation.ts` `register()`

### When Next calls it

- Docs: `register` "is called **once** when a new Next.js server instance is initiated, and must complete before the server is ready to handle requests" ([instrumentation.mdx](https://github.com/vercel/next.js/blob/v16.4.3/docs/01-app/03-api-reference/03-file-conventions/instrumentation.mdx)).
- Source: `ensureInstrumentationRegistered()` stores the call in a module-level `registerInstrumentationPromise`, and the module is a non-bundled `.external` file. That makes it once per process, shared by `NextNodeServer` and the route-module entry point ([instrumentation-globals.external.ts](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/lib/router-utils/instrumentation-globals.external.ts), [route-module.ts L790-L809](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/route-modules/route-module.ts#L790-L809)).
- Production start is eager. The `NextNodeServer` constructor fires `this.prepare()` without awaiting it when not in dev, and `prepareImpl()` calls `runInstrumentationHookIfAvailable()` ([next-server.ts L306-L315, L401-L408](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/next-server.ts#L306-L315)). `handleRequest` awaits `prepare()` ([base-server.ts L877-L883](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/base-server.ts#L877-L883)), so requests wait for `register()`.
- **(observed)** `next start` logs `✓ Ready` _before_ `register()` runs, and `register()` runs before the first request arrives. A `register()` that throws logs `Failed to prepare server`, and every request then returns 500 with `An error occurred while loading instrumentation hook: …`. The promise is cached, so it never retries. The process keeps running.

### Mode by mode (observed, Next 16.4.3)

| Mode                              | Processes                                 | `register()` calls                                                                                                                                                                                  |
| --------------------------------- | ----------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `next start`                      | 1 (`next-server (v16.4.3)`)               | once, at boot                                                                                                                                                                                       |
| `node .next/standalone/server.js` | 1                                         | once, at boot. The generated `server.js` calls the same `startServer()` ([build/utils.ts L1595-L1640](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/build/utils.ts#L1595-L1640)) |
| `next dev` (Turbopack, default)   | CLI parent + 1 forked `next-server` child | once per child, at boot. Again in a **new child** after a `next.config` change                                                                                                                      |
| `next dev --webpack`              | same                                      | same                                                                                                                                                                                                |
| `next build`                      | build workers                             | **never**. The instrumentation module is not even evaluated                                                                                                                                         |

What restarts the dev child: a `next.config.*` change, deletion of the dist directory, and the dev memory threshold. Each one calls `process.exit(RESTART_EXIT_CODE)` directly ([start-server.ts L255-L276, L597-L626](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/lib/start-server.ts#L597-L626)). No signal reaches the old child, so only synchronous `exit` handlers run, and an async `worker.close()` cannot finish.

### `NEXT_RUNTIME` guard

- Next inlines `process.env.NEXT_RUNTIME` per bundle as `'nodejs'` or `'edge'` ([define-env.ts L164-L168](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/build/define-env.ts#L164-L168)). The docs recommend `if (process.env.NEXT_RUNTIME === 'nodejs') await import('./…')` inside `register` ([guides/instrumentation.mdx](https://github.com/vercel/next.js/blob/v16.4.3/docs/01-app/02-guides/instrumentation.mdx)).
- **(observed)** Turbopack also compiles `instrumentation.ts` for the edge runtime with no middleware or proxy present. It warns ("A Node.js API is used … not supported in the Edge Runtime") on every Node API used _outside_ the guard, but nothing inside the guarded dynamic import triggered a warning. Webpack drops the edge instrumentation entry when it is the only edge entry ([entries.ts L798-L802](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/build/entries.ts#L798-L802)).

### `NEXT_PHASE`

- `registerInstrumentation()` returns early when `process.env.NEXT_PHASE === 'phase-production-build'` (source above). `next build` sets that variable before it spawns the static-generation workers ([build/index.ts L2339](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/build/index.ts#L2339)). At runtime under `next start`, `NEXT_PHASE` is undefined (observed).
- **(observed)** A route that imports the worker module evaluates it during the build's "Collecting page data" step, once per build worker process. A route-level `new Queue(...)` at module scope also ran during the build: it threw the missing-ioredis error before ioredis was installed. Guarding `register()` therefore does not protect module top-level side effects in files that app code also imports.
- Payload's guard also checks `npm_lifecycle_event === 'build'` ([isNextBuild.ts](https://github.com/payloadcms/payload/blob/v3.92.1/packages/payload/src/utilities/isNextBuild.ts)).

```ts
// instrumentation.ts: the shape used in the experiment
export async function register() {
  if (process.env.NEXT_RUNTIME !== "nodejs") return;
  const { startWorker } = await import("./worker/start");
  await startWorker(); // must not throw and should not block on Redis: requests wait for register()
}
```

## 2. `next dev` re-evaluation, duplicate workers, HMR of task code

**(observed)** The run used Next 16.4.3, both bundlers, and a worker started from `register()`. Each job logged a `VERSION` constant read through a static import and through a per-job `await import()`.

| Edit                                | `register()` re-run         | Worker module re-evaluated | Version the job saw           |
| ----------------------------------- | --------------------------- | -------------------------- | ----------------------------- |
| task file (`worker/tasks/hello.ts`) | no                          | no                         | stale (v1 static and dynamic) |
| `instrumentation.ts`                | no                          | no                         | stale                         |
| `app/page.tsx`                      | no                          | no                         | stale                         |
| `next.config.ts`                    | yes, in a new child process | yes (fresh process)        | new (v2)                      |

- No duplicate worker came from `register()` within a process: one call per process in every mode tested. The duplication risks that do exist:
  - **Two module instances in one process.** `worker/start.ts` imported by both `instrumentation.ts` and an app route was evaluated twice in the same pid, with different module-level tokens. `globalThis` was shared (observed, `next start`). Module-level singletons, such as a task registry or a cached `Queue`, are therefore not process-wide. `globalThis` is.
  - **Process replacement in dev.** A restarted child starts a new worker while the old one died without `close()`. Its active jobs stay locked until `lockDuration` expires, then the stalled check re-runs them. Several jobs abandoned by killed processes reappeared as `stalled` and ran again on the next worker start (observed).
- Dev shutdown: the CLI parent forwards SIGINT/SIGTERM to the child, then sends `SIGKILL` after `NEXT_EXIT_TIMEOUT_MS`, which defaults to 100 ms ([next-dev.ts L118-L160](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/cli/next-dev.ts#L118-L160)). The self-hosting docs say "Manual signal handling is not available in `next dev`" ([self-hosting.mdx](https://github.com/vercel/next.js/blob/v16.4.3/docs/01-app/02-guides/self-hosting.mdx)).
- Singleton patterns in use:
  - A `globalThis` key, as in the experiment (`globalThis.__w5_workers`).
  - Payload keeps `global._payload = new Map()` keyed by instance and reuses it across module re-evaluations ([index.ts L1121-L1132](https://github.com/payloadcms/payload/blob/v3.92.1/packages/payload/src/index.ts#L1121-L1132)).
  - For dev reloads, Payload opens a WebSocket to Next's HMR endpoint and marks its instance for reload on `serverComponentChanges` (Next 16: `data.type`; Next 15: `data.action`) ([index.ts L1224-L1250](https://github.com/payloadcms/payload/blob/v3.92.1/packages/payload/src/index.ts#L1224-L1250)). That is an existing way to learn about edits from inside the server process. Whether a reloaded import would then return fresh task code was **not verified**: the per-job dynamic import returned stale code in the experiment.

```ts
// a process-wide guard keyed on globalThis (works across module instances)
const KEY = Symbol.for("nextasking.worker");
const g = globalThis as { [KEY]?: Worker };
g[KEY] ??= new Worker(queueName, processor, { connection });
```

## 3. Module resolution: Next's bundle vs a standalone process

### What the instrumentation bundle does

- **`react-server` condition.**
  - Webpack: the `instrument` layer resolves with `reactServerConditionNames` ([webpack-config.ts L1737-L1745](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/build/webpack-config.ts#L1737-L1745)).
  - Turbopack: `ServerContextType::Instrumentation` is in `should_use_react_server_condition()` ([next_server/context.rs L111-L121, L198-L200](https://github.com/vercel/next.js/blob/v16.4.3/crates/next-core/src/next_server/context.rs#L111-L121)).
  - The `next-js` condition (with `cacheComponents`) is excluded for instrumentation.
  - **(observed)** A local package with `exports: { "react-server": …, "default": … }` resolved to `react-server` inside Next.
- **`server-only` / `client-only`.** `instrument` belongs to `GROUP.serverOnly`, so `server-only` is allowed and `client-only` fails the build ([constants.ts L178-L186](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/lib/constants.ts#L178-L186), [webpack-config.ts L1430-L1490](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/build/webpack-config.ts#L1430-L1490)).
- **React is Next's vendored RSC build.** For node and server layers, `react` aliases to `next/dist/server/route-modules/app-page/vendored/rsc/react`, and `react-dom/server` aliases to `next/dist/compiled/react-dom/server.node` ([create-compiler-aliases.ts L479-L495](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/build/create-compiler-aliases.ts#L479-L495)). **(observed)** `typeof React.useState === 'undefined'`, yet `renderToStaticMarkup` from `react-dom/server` worked in a job (dev, Turbopack and webpack).
- **Externals.** Only packages in `serverExternalPackages` are external. The default list has neither `bullmq` nor `ioredis` ([server-external-packages.jsonc](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/lib/server-external-packages.jsonc)). Turbopack uses only the server-external plugin for instrumentation (context.rs L242-L248).
  - **(observed)** By default, BullMQ and ioredis are bundled; only Node built-ins are external. Turbopack builds cleanly. Webpack warns `Can't resolve '@valkey/valkey-glide'`, from BullMQ's optional `require` in `valkey-glide-client` ([source](https://github.com/taskforcesh/bullmq/blob/v6.5.3/src/classes/valkey-glide-client.ts#L61)).
  - **(observed)** With `serverExternalPackages: ['bullmq']`, webpack emits `require("bullmq")` and the warning goes away. Turbopack emits `require("bullmq-<hash>")` through a symlink `.next/node_modules/bullmq-<hash> -> ../../node_modules/bullmq`. Jobs ran in both cases.
- **`output: 'standalone'` tracing (observed).** `.next/standalone/node_modules` contained `bullmq` and `ioredis` (traced from `instrumentation.js.nft.json`).
- **Process error handlers.** Next installs `uncaughtException`/`unhandledRejection` handlers that log instead of crashing ([process-error-handlers.ts](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/node-environment-extensions/process-error-handlers.ts)). A plain Node process keeps Node's default behaviour: it crashes on an unhandled rejection.

### BullMQ 6 specifics that interact with bundling

- `ioredis` is an **optional peer**, loaded lazily with `require('ioredis')` only when the user passes connection options rather than a client instance ([package.json](https://github.com/taskforcesh/bullmq/blob/v6.5.3/package.json), [redis-connection.ts L84-L102](https://github.com/taskforcesh/bullmq/blob/v6.5.3/src/classes/redis-connection.ts#L84-L102)). When it is missing, the `Queue`/`Worker` constructor throws. The error text advises passing a constructed client "in a native ESM environment". BullMQ 6 also accepts node-redis and Bun clients.
- Lua scripts ship as generated JS modules (`src/scripts`, built from `src/commands/*.lua`), so they bundle fine. **Sandboxed processors** take a _file path_. BullMQ forks it, or starts it as a worker thread, through its own `main.js`/`main-worker.js` resolved from its `dist` directory ([worker.ts L375-L395](https://github.com/taskforcesh/bullmq/blob/v6.5.3/src/classes/worker.ts#L375-L395), [child.ts L84-L95](https://github.com/taskforcesh/bullmq/blob/v6.5.3/src/classes/child.ts#L84-L95)). The processor file then loads under plain Node rules, not Next's.

### The same code as a standalone process (observed, tsx)

| Import                             | Inside Next (instrumentation)             | Plain Node                                                       | Plain Node + `--conditions=react-server`                |
| ---------------------------------- | ----------------------------------------- | ---------------------------------------------------------------- | ------------------------------------------------------- |
| package with `react-server` export | `react-server` entry                      | `default` entry                                                  | `react-server` entry                                    |
| `react`                            | vendored RSC build (`useState` undefined) | app's React (`useState` present)                                 | `react-server` build (`useState` undefined)             |
| `import 'server-only'`             | ok                                        | **throws** ("cannot be imported from a Client Component module") | ok                                                      |
| `react-dom/server`                 | ok (aliased)                              | ok                                                               | **throws** ("not supported in React Server Components") |

The `server-only` package is `exports: { "react-server": "./empty.js", "default": "./index.js" }`, and `index.js` throws. `react-dom`'s `./server` export maps `react-server` to a file that throws.

Facts this puts on a library and on user task files:

- Any library entry that the worker imports runs under two resolutions, `react-server` inside Next and default outside. An entry that exists under only one condition, or that behaves differently under each, splits the two runtimes.
- A user task file that imports `server-only`, directly or through a dependency (common in Next code: data-access layers, auth helpers), cannot be loaded by a plain Node worker unless the worker runs with `--conditions=react-server`. That flag in turn breaks `react-dom/server` (email rendering, for example).
- Code reached from `instrumentation.ts` gets Next's vendored React, which can differ from the app's `node_modules/react` that a standalone worker gets.
- Top-level side effects in files that app routes also import run during `next build`, and run once per module instance (twice per process when imported by both instrumentation and a route).
- Path aliases (`@/…` from `tsconfig`) are resolved by Next's bundler. A standalone runner needs its own resolver for them (tsx resolves `tsconfig` paths). That is general Node behaviour and was not specifically tested here.

## 4. Signals: SIGTERM inside `next start`

- Source: after `Ready`, `startServer()` registers `process.on('SIGINT'|'SIGTERM', cleanup)` unless `process.env.NEXT_MANUAL_SIG_HANDLE` is set. `cleanup` closes the HTTP server and awaits pending requests, then calls `nextServer.close()`, then `process.exit(143)` for SIGTERM or `process.exit(130)` for SIGINT ([start-server.ts L404-L492](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/lib/start-server.ts#L404-L492)). `output: 'standalone'` goes through the same `startServer()`.
- Docs: set `NEXT_MANUAL_SIG_HANDLE=true` "directly in the `package.json` script, and not in the `.env` file". It is not available in `next dev` ([self-hosting.mdx](https://github.com/vercel/next.js/blob/v16.4.3/docs/01-app/02-guides/self-hosting.mdx)). The check runs before Next loads config and env (start-server.ts L489 vs L495).
- **(observed)** The run used a 6 s job, with SIGTERM sent 2 s into it:

| Setup                                                       | Result                                                                                                             |
| ----------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| no app handler                                              | exit 143 about 12 ms after SIGTERM; job left active (later recovered as stalled and re-run)                        |
| app handler calling `worker.close()`, Next's handler active | both ran; Next's `process.exit(143)` landed about 13 ms after the signal, before `close()` finished; job abandoned |
| `NEXT_MANUAL_SIG_HANDLE=true` + app handler                 | job finished about 4 s later, `close()` resolved, exit 0                                                           |
| same three, `node .next/standalone/server.js`               | identical                                                                                                          |

- **(observed)** Inside `register()`, `process.listenerCount('SIGTERM')` is `1` with Next's handling active and `0` with `NEXT_MANUAL_SIG_HANDLE`. Next's listener is therefore already installed when `register()` runs. Removing or wrapping it would rely on Next internals. That was not tried.
- With manual handling, Next closes nothing. Stopping new HTTP connections, waiting for in-flight requests, closing the worker and exiting are all the app handler's job. `Worker.close()` waits for active jobs **with no built-in timeout** in the TypeScript SDK ([graceful-shutdown.md](https://github.com/taskforcesh/bullmq/blob/v6.5.3/docs/gitbook/guide/workers/graceful-shutdown.md), [worker.ts L1380-L1425](https://github.com/taskforcesh/bullmq/blob/v6.5.3/src/classes/worker.ts#L1380-L1425)). A deadline against the host's grace period is not provided. `close(true)` skips waiting.

```jsonc
// package.json
{ "scripts": { "start": "NEXT_MANUAL_SIG_HANDLE=true next start" } }
```

```ts
// the handler used in the experiment (registered once, from register())
process.on("SIGTERM", async () => {
  await worker.close(); // waits for in-flight jobs; no timeout
  process.exit(0); // HTTP server is not closed by Next in this mode
});
```

## 5. CPU-bound handlers, rendering, and lock renewal

- Lock mechanics (source, BullMQ 6.5.3):
  - Defaults are `lockDuration: 30000`, `stalledInterval: 30000`, `maxStalledCount: 1`, and `lockRenewTime = lockDuration / 2` ([worker.ts L303-L345](https://github.com/taskforcesh/bullmq/blob/v6.5.3/src/classes/worker.ts#L303-L345)).
  - `LockManager` re-arms a `setTimeout` every `lockRenewTime / 2` (7.5 s). On each tick it extends the locks whose last extension is older than `lockRenewTime / 2` ([lock-manager.ts](https://github.com/taskforcesh/bullmq/blob/v6.5.3/src/classes/lock-manager.ts)).
  - Extending a lock sets `<job>:lock` with `PX lockDuration` and removes the job from the `stalled` set ([extendLocks-1.lua](https://github.com/taskforcesh/bullmq/blob/v6.5.3/src/commands/extendLocks-1.lua)).
  - The stalled check, run by any worker every `stalledInterval`, has two phases. One check marks active jobs in `stalled`. The next check moves the still-marked jobs whose lock is missing back to wait, or fails them once `stc > maxStalledCount` ("job stalled more than allowable limit") ([moveStalledJobsToWait-9.lua](https://github.com/taskforcesh/bullmq/blob/v6.5.3/src/commands/moveStalledJobsToWait-9.lua)).
  - All of this runs on the worker's event loop. BullMQ offers no separate bookkeeping thread. Its documented mitigation is sandboxed processors ([sandboxed-processors.md](https://github.com/taskforcesh/bullmq/blob/v6.5.3/docs/gitbook/guide/workers/sandboxed-processors.md), [stalled.md](https://github.com/taskforcesh/bullmq/blob/v6.5.3/docs/gitbook/guide/jobs/stalled.md)).
- Consequence: a synchronous block longer than about `lockDuration`, minus the time since the last extension, lets the lock expire. A worker in another process then re-queues the job while the first run is still going, which is a duplicate execution. When the first run finishes, `moveToFinished` reports `Missing lock for job <id>`.
- **(observed)** The setup was `next start` with an embedded worker (`lockDuration` 4000, `stalledInterval` 2000), a 10 s busy-loop job, and a standalone worker started 1 s later:
  - `GET /` sent during the burn took **8.0 s**; after the burn it took 6 ms.
  - The standalone worker emitted `stalled` for the job and **started it again** while the Next process was still burning.
  - The Next run finished and got `Missing lock for job 25. moveToFinished`.
  - The second run blocked its own loop, stalled again and was failed: "job stalled more than allowable limit". It then also finished with `Missing lock`.
  - Net effect: two executions, final state `failed`.
- Embedded in Next, every handler, async or not, shares the event loop with rendering. Async I/O handlers interleave with requests. CPU-bound work delays every request for as long as it runs. `concurrency` above 1 multiplies both effects.
- Prior art for the bookkeeping problem: Inngest's `connect` worker runs the WebSocket, heartbeater and lease extender in a worker thread by default (`isolateExecution`, default `true`), "to prevent userland code from blocking heartbeats and lease extensions" ([connect/types.ts](https://github.com/inngest/inngest-js/blob/inngest%404.26.2/packages/inngest/src/components/connect/types.ts), [strategies/README.md](https://github.com/inngest/inngest-js/blob/inngest%404.26.2/packages/inngest/src/components/connect/strategies/README.md)).

## 6. How other libraries run background work inside Next.js

- **Next.js docs / observability SDKs.** The documented use of `register()` is OpenTelemetry-style setup (`registerOTel`), with runtime-specific code behind `NEXT_RUNTIME` and dynamic `import()` ([guides/instrumentation.mdx](https://github.com/vercel/next.js/blob/v16.4.3/docs/01-app/02-guides/instrumentation.mdx)). Graceful shutdown is documented only through `NEXT_MANUAL_SIG_HANDLE`.
- **Payload 3.92.1 (jobs queue).**
  - `jobs.autoRun` starts `croner` crons inside the Next process that call `payload.jobs.run()`, using `protect: true` so runs do not overlap. They are skipped when `isNextBuild()` ([index.ts L714-L772](https://github.com/payloadcms/payload/blob/v3.92.1/packages/payload/src/index.ts#L714-L772)).
  - The crons do not start from `register()`. They start on the first `getPayload({ cron: true })`, which Payload's REST/GraphQL request handler calls ([createPayloadRequest.ts L34-L38](https://github.com/payloadcms/payload/blob/v3.92.1/packages/payload/src/utilities/createPayloadRequest.ts#L34-L38)).
  - The instance is cached on `global._payload`. `payload.destroy()` stops the crons. Nothing in core listens for SIGTERM.
  - Docs: autoRun "runs within your Next.js process", is for "a dedicated server that is always running", "should not be used on serverless platforms", and "for dedicated servers, consider using bin scripts instead, as they run in a separate process" ([Payload docs, Queues](https://payloadcms.com/docs/v3/jobs-queue/queues)).
- **Inngest 4.26.2.**
  - In serve mode, Next route handlers (`serve()` from `inngest/next`) receive HTTP calls from the Inngest server, so no worker loop runs in the Next process ([next.ts](https://github.com/inngest/inngest-js/blob/inngest%404.26.2/packages/inngest/src/next.ts)).
  - In `connect` mode (a long-lived worker over WebSocket), it installs SIGINT/SIGTERM handlers by default (`handleShutdownSignals`, default `["SIGINT","SIGTERM"]`) that close the connection gracefully. It isolates lease extension in a worker thread (section 5).
- No first-party BullMQ guidance for Next.js exists: the BullMQ v6.5.3 docs do not mention Next.js or `instrumentation`.

## Open points surfaced (not decided here)

- Dev reload of task code: `register()` and the module cache do not pick up edits, so a worker started there runs stale code until the dev child restarts. Whether a reload hook (for example Next's HMR WebSocket, as Payload uses) can deliver fresh task modules to a worker was not verified.
- Shutdown deadline: `Worker.close()` has no timeout, and `NEXT_MANUAL_SIG_HANDLE` hands all of shutdown, HTTP included, to the app. The deadline relative to the host's kill timeout is open.
- Bundling BullMQ vs `serverExternalPackages: ['bullmq']`: both run. Externalizing removes the webpack warning and keeps sandboxed-processor paths valid, but it needs a line in the user's `next.config`.
- The resolution split (`react-server` inside Next vs default outside) constrains which user code a standalone worker can load. Whether task files may import `server-only` code is open.
