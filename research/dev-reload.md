# Reloading task code under `next dev`

Research for [Reloading task code under next dev](https://github.com/flolefebvre/nextasking/issues/34). Question: on Next 16.4.3, can a worker started from `register()` under `next dev` run the current task code after an edit, without a dev-server restart? For each candidate: what it needs from the consumer, whether it relies on Next internals, and how it interacts with the library-owned SIGTERM drain and with steps saved by an attempt that ran the old code.

This builds on two earlier notes, whose findings are not repeated:

- [`research/worker-in-nextjs.md`](https://github.com/flolefebvre/nextasking/blob/research/worker-in-nextjs/research/worker-in-nextjs.md): `register()` runs once per dev child, edits to task files never re-run it, a file imported by both instrumentation and a route is evaluated twice in one process, and only `globalThis` is shared.
- [`research/standalone-worker.md`](https://github.com/flolefebvre/nextasking/blob/research/standalone-worker/research/standalone-worker.md), section 7: a route that imports the registry re-evaluates it lazily on its next request after an edit, under both bundlers; a worker that looks up handlers in the newest evaluation on `globalThis` gets fresh code from then on; a dev-only self-request to a library route before each job makes every job fresh; the HMR WebSocket announces edits with different messages per bundler.

[Where the worker runs and how it starts](https://github.com/flolefebvre/nextasking/issues/11) rejected the self-request because it needs a route file in the app, and settled on "restart the dev server after editing a task" for now.

## Versions and method

- Next.js **16.4.3** (source read at tag `v16.4.3`), BullMQ **6.5.3**, ioredis 5.11.1, React 19.2.0, Node 24.16.0, Valkey 8 in Docker, Linux (WSL2).
- **(observed)** facts come from a throwaway Next 16.4.3 app:
  - `instrumentation.ts` uses the documented `if (process.env.NEXT_RUNTIME === "nodejs") { await import(...) }` guard and starts a BullMQ worker;
  - `src/tasks/hello.ts` exports `VERSION` and a handler that waits `ms` milliseconds; `src/tasks/registry.ts` imports it and pushes each of its evaluations onto `globalThis`;
  - the job handler logs the version of the registry it imported (`own`) and of the newest one on `globalThis` (`newest`);
  - the worker's drain is `await worker.close()` then `process.exit()`, the shape of the library's SIGTERM drain without its `shutdownTimeout`;
  - `lockDuration` was 10 s and `stalledInterval` 5 s so that abandoned jobs come back quickly. The library fixes `lockDuration` at 2 minutes, and BullMQ's default `stalledInterval` is 30 s (`node_modules/bullmq/dist/esm/classes/worker.js`, constructor defaults).
- Timings come from one run on a tiny app. Treat them as orders of magnitude.

## Summary

| #   | Option                                                                      | Fresh code for                                                     | Consumer adds                                    | Next internals                                              | HTTP during reload                            | Drain                          |
| --- | --------------------------------------------------------------------------- | ------------------------------------------------------------------ | ------------------------------------------------ | ----------------------------------------------------------- | --------------------------------------------- | ------------------------------ |
| A   | Newest registry on `globalThis`, refreshed by app requests                  | attempts started after any request that imports the app's instance | nothing                                          | none                                                        | unaffected                                    | not involved                   |
| B   | A + self-request to a library route before each job                         | every attempt                                                      | a route file                                     | none                                                        | unaffected                                    | not involved                   |
| C   | File watcher in the dev child, drain, then `process.exit(77)`               | everything, boot work included                                     | the paths to watch (or a convention)             | exit code 77 and the `next dev` parent that respawns        | down about 1.5 s (Turbopack), 2.7 s (webpack) | library drains before exiting  |
| D   | Separate dev worker: `next({ dev: true }).prepare()` restarted by a watcher | everything, boot work included                                     | a `distDir` keyed on an env var, a second script | none, but Next rewrites `tsconfig.json` and `next-env.d.ts` | unaffected                                    | SIGTERM drain runs in full     |
| E   | Dev-server hooks as edit signals                                            | nothing by themselves                                              | depends                                          | experimental or undocumented                                | —                                             | —                              |
| —   | Today: restart the dev server by hand                                       | everything                                                         | nothing                                          | none                                                        | down for the restart                          | cut after 100 ms by `next dev` |

- A dev worker gets new code only from a fresh evaluation of the task modules. Inside the dev child, only a request to an app route or page triggers one, because instrumentation does not take part in server HMR. Outside it, only a new process does.
- In every option, an attempt keeps the code it started with. New code applies from the next attempt or resume, which reuses the steps saved under the same ids by the old code.
- Stopping `next dev` with Ctrl-C or SIGTERM cuts the drain after 100 ms, even with `NEXT_MANUAL_SIG_HANDLE`. Next's own restarts (a `next.config` edit, the dev overlay's restart button, the memory threshold) call `process.exit(77)` with no signal at all. In both cases the attempt in flight comes back as stalled after up to the 2-minute lock plus the stall-check interval (observed).

---

## Why a worker cannot re-import the registry by itself

- Turbopack's server HMR covers "App Router entries on the Node.js runtime" ([hot-reloader-turbopack.ts L705-L706](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/dev/hot-reloader-turbopack.ts#L705-L706)). Instrumentation is written to disk once, when the entrypoints are processed ([turbopack-utils.ts L692-L714](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/dev/turbopack-utils.ts#L692-L714)), and its module is cached for the life of the process (`cachedInstrumentationModule` in [instrumentation-globals.external.ts](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/lib/router-utils/instrumentation-globals.external.ts)).
- A dynamic `import()` inside instrumentation code is resolved at compile time into the instrumentation bundle, so calling it again returns the same module. Reading the bundle's chunks from `.next/dev/server` would rely on Turbopack's output layout, which the standalone-worker note found undocumented and different per bundler.
- So fresh code reaches the dev child only when a request builds an app route or page that imports the task modules ([hot-reloader-turbopack.ts L2183](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/dev/hot-reloader-turbopack.ts#L2183): "The only server HMR pull, driven by the request being built — which is what makes evaluating a changed module lazy"). The other way is a new process.

## A. Newest registry on `globalThis`, refreshed by the app's own requests

How it works: in dev, each evaluation of `createNextasking(...)` records itself on `globalThis`, and the worker looks up the handler in the newest one when an attempt starts. Code that dispatches or emits imports the app's instance, which imports every task. So the request that dispatches after an edit re-evaluates the registry before the job is enqueued. The standalone-worker note observed this: after an edit and one `GET /api/dispatch`, the next job ran the new version (section 7 table).

Observed again here, with Turbopack: an edit to v2, then `GET /api/dispatch` logged `route: dispatch registry=v2#2`, and the job it dispatched logged `own=v1 newest=v2#2`.

- **Consumer adds**: nothing.
- **Next internals**: none. It relies on `globalThis` being shared between instrumentation and route code in one process, which the earlier note observed.
- **Fresh for**: attempts that start after any request whose route or page imports the app's instance. The common loop, "edit a task, then click what dispatches it", is covered.
- **Stale for**: schedule firings, retries and resumes of runs already in the queue, and dispatches from handlers, until such a request happens. The worker cannot tell that it is running stale code.
- **Partial reload**: only handlers are fresh. Work done once in `startWorker()` is not redone: schedule reconciliation ([Scheduled tasks: declaration and reconciliation across deploys](https://github.com/flolefebvre/nextasking/issues/18)) and the boot-time config checks. A task added or a schedule changed in an edit is therefore half-applied.
- **Cost on the library**: `createNextasking` gains one dev-only side effect, a `globalThis` write (no connection). The library must decide which evaluation is "the same instance" when an app has several (by `prefix`, for example) and keep only the newest, since each route that imports the registry adds one evaluation per edit.
- **Drain**: not involved; nothing restarts.

## B. A plus a self-request to a library route

Already observed in the standalone-worker note (section 7): a dev-only `fetch` to a route that only imports the registry, sent before each job, made every job fresh under both bundlers. It cost 3 to 18 ms per job when nothing had changed, and 261 ms (Turbopack) or 1,457 ms (webpack) for the first compile. [Where the worker runs and how it starts](https://github.com/flolefebvre/nextasking/issues/11) rejected it for its route file.

- **Consumer adds**: one route file, for example `app/api/nextasking/dev/route.ts` re-exporting a library handler. The dev server's port is also needed; Payload reads `process.env.PORT || '3000'` (cited in the standalone-worker note).
- **Variant not tried**: the library writing that file itself under `app/` at dev start. That needs to know whether the app uses `app/` or `src/app/`, writes into the consumer's tree, and needs a `.gitignore` line.
- **Next internals**: none.
- **Partial reload**: same as A.
- **Drain**: not involved.

## C. A file watcher in the dev child that exits with code 77

How it works: in dev, `startWorker()` watches the task paths with `fs.watch`. On a change it drains like on SIGTERM, then calls `process.exit(77)`. The `next dev` parent treats 77 as "restart" and forks a new child ([next-dev.ts L610-L628](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/cli/next-dev.ts#L610-L628), `RESTART_EXIT_CODE = 77` in [utils.ts L336](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/lib/utils.ts#L336)). That child runs `register()` again with the new code. Next uses the same exit code itself when `next.config` changes ([start-server.ts L605-L616](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/lib/start-server.ts#L605-L616)) and for the dev overlay's restart button (`POST /__nextjs_restart_dev`, [restart-dev-server-middleware.ts L64-L68, L91](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/next-devtools/server/restart-dev-server-middleware.ts#L64-L68)).

**(observed)** A 6 s job was in flight when `hello.ts` went from v1 to v2:

| Event                                   | Turbopack   | webpack     |
| --------------------------------------- | ----------- | ----------- |
| `fs.watch` fires after the edit         | under 1 ms  | under 1 ms  |
| drain waits for the job in flight       | 5.0 s       | 5.0 s       |
| exit 77 to new `register()`             | 0.79 s      | 1.76 s      |
| HTTP refused (polling `/` every 200 ms) | about 1.5 s | about 2.7 s |
| next job                                | `own=v2`    | `own=v2`    |

- HTTP kept serving during the drain. The worker's `close()` stops only jobs.
- **Consumer adds**: what to watch. The library cannot see the instrumentation module graph, and watching the whole project restarts the server on every UI edit, which loses the point of Fast Refresh. So it needs a path option or a convention, such as a `src/nextasking/` directory.
- **Next internals**: exit code 77 is not documented. It works only when the process is a child forked by `next dev`, which sets `__NEXT_DEV_SERVER=1` and `NEXT_PRIVATE_WORKER=1` ([next-dev.ts L508-L517](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/cli/next-dev.ts#L508-L517)). Under a custom dev server, 77 is a plain exit.
- **Fresh for**: everything, boot work included (reconciliation, config checks), since it is a new process.
- **Costs**: every edit to a watched file restarts the whole dev server. Turbopack's file-system cache, on by default in 16.4.3 (`turbopackFileSystemCacheForDev: true`, [config-shared.ts L2494](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/config-shared.ts#L2494)), kept the tiny app's restart under a second. Routes compile again on demand afterwards. The effect on an open browser tab was not checked.
- **Drain**: under the library's control. It drains before exiting, so in-flight attempts finish or yield at a step boundary, which [Steps in v1: primitives and semantics](https://github.com/flolefebvre/nextasking/issues/13) already provides for shutdown. The restart waits for the drain, up to `shutdownTimeout`.

## D. A separate dev worker process

How it works: the dev server runs with the worker off, gated by the env var the app already owns ([Where the worker runs and how it starts](https://github.com/flolefebvre/nextasking/issues/11)). A second process runs `next({ dev: true, dir }).prepare()` without `listen`, the dev twin of the `nextasking worker` bin, and a watcher restarts it on edits. Here the watcher was `node --watch-path=src`.

**(observed)**

- With the default `distDir`, the second process **failed**: "Another next dev server is already running", then exit 1. In dev, Next takes a lock on `distDir` (`experimental.lockDistDir`, default `true`, [setup-dev-bundler.ts L221-L240](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/lib/router-utils/setup-dev-bundler.ts#L221-L240), [config-shared.ts L1608-L1614, L2488](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/config-shared.ts#L1608-L1614)). The option's own doc says that two processes writing the same `distDir` "can mangle the state of the directory".
- With `distDir: process.env.NX_DIST ?? ".next"` in `next.config.ts` and `NX_DIST=.next-worker` for the worker, it **worked**:
  - `register()` ran inside `prepare()`, which resolved after 643 ms on a cold start;
  - no port was bound;
  - on a task edit with a 6 s job in flight, `node --watch` sent SIGTERM, printed "Waiting for graceful termination...", and waited for the drain to finish;
  - the new process ran `register()` with v2 582 ms after starting;
  - the web dev server kept answering 200 throughout.
- RSS (one sample, Turbopack): dev worker 344 MB, next to 356 MB for the web dev child and 79 MB for the `next dev` parent. It is a second Turbopack project, not a light process.
- Starting the worker made Next **rewrite consumer files**:
  - `tsconfig.json` gained `.next-worker/types/**/*.ts` and `.next-worker/dev/types/**/*.ts` in `include`;
  - `next-env.d.ts` switched its imports to `./.next-worker/dev/types/routes.d.ts`.

  The TypeScript setup runs on every dev start ([setup-dev-bundler.ts L178-L192, L469](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/lib/router-utils/setup-dev-bundler.ts#L178-L192)), and no option to skip it was found.

Assessment:

- **Consumer adds**: a `distDir` entry keyed on an env var the library sets, or a `withNextasking()` config wrapper that does it. Also a second dev script and a `.gitignore` line, and they live with the `tsconfig.json` churn. Overriding `distDir` without touching `next.config` would need `__NEXT_PRIVATE_STANDALONE_CONFIG`. That variable replaces the whole config with JSON, which drops function-valued options like `webpack()`, and skips the dev `distDir` suffix ([config.ts L2038-L2053, L1786-L1791](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/config.ts#L2038-L2053)). This was read in the source, not tried.
- **Next internals**: none for the process itself; `next({ dev: true })` is the documented custom-server API.
- **Fresh for**: everything, boot work included. Because a restart doesn't touch HTTP, the watcher can watch the whole source tree.
- **Drain**: the normal SIGTERM drain runs in full when the watcher waits for exit, as `node --watch` does. With `tsx watch` the standalone-worker note did not check this.
- **Also**: the worker compiles its own copy of every task module, so a type or build error in a task shows up in the worker's output, not in the browser overlay.

## E. Dev-server hooks as edit signals

None of these gives the worker new code. Each only tells it that something changed, so it has to be paired with B, C or D.

- **`experimental.onBeforeDeferredEntries`** (Turbopack, [config-shared.ts L1639-L1648](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/config-shared.ts#L1639-L1648)). It needs a non-empty `experimental.deferredEntries` and runs debounced by 500 ms after each Turbopack update ([hot-reloader-turbopack.ts L690-L702, L2237-L2241, L2318-L2321](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/dev/hot-reloader-turbopack.ts#L690-L702)). It runs in the dev child, where the worker is. **(observed)** It ran at startup and after each route compile. It **did not** run for a task edit made before any route had been compiled, and it did run for one made after (about 550 ms later). So it depends on which entries are subscribed. It is experimental and not in the docs.
- **A webpack plugin** added through `webpack()` in `next.config` (for example `compiler.hooks.done`) works only under `--webpack`. Turbopack, the default in 16, has no plugin hook.
- **The HMR WebSocket** at `/_next/hmr`: the standalone-worker note observed `serverComponentChanges` under webpack and only `built` under Turbopack. The protocol is undocumented, and its path changed in 16.3.0.
- **`fs.watch`** in the library needs no Next at all. `recursive` has worked on Linux since Node v19.1.0 ([Node 24 `fs.watch`](https://nodejs.org/docs/latest-v24.x/api/fs.html#fswatchfilename-options-listener)), and an `ignore` option exists. It is the signal used for C.

## Today's plan: restarting `next dev` by hand

[Where the worker runs and how it starts](https://github.com/flolefebvre/nextasking/issues/11) documents "restart the dev server after editing a task". These facts apply to it, and to any option that restarts the dev child:

- On SIGINT or SIGTERM, the `next dev` parent forwards the signal to the child and sends **SIGKILL after 100 ms** (`NEXT_EXIT_TIMEOUT_MS`, default `'100'`, undocumented, [next-dev.ts L117-L160](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/cli/next-dev.ts#L117-L160)).

  **(observed)** With `NEXT_MANUAL_SIG_HANDLE=1` and a 6 s job in flight, the drain started on Ctrl-C (SIGINT to the process group) and on SIGTERM to the parent alone, and the child died without finishing it. With `NEXT_EXIT_TIMEOUT_MS=10000`, the drain completed and the child exited 130. The child received SIGINT twice on Ctrl-C, once from the terminal and once from the parent, so a drain handler has to be idempotent.

- Next's own restarts call `process.exit(77)` in the child, with no signal and so no drain:
  - a `next.config` edit;
  - the dev overlay's restart button;
  - `devMemoryThresholdRestart` (default `true`, [start-server.ts L263-L275](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/lib/start-server.ts#L263-L275));
  - deleting the `distDir`.

  **(observed)** An edit to `next.config.ts` with a 6 s job in flight: the child exited 77 within a second, with no drain, and the new child logged `stalled 3` and re-ran the job 11 s later (10 s lock).

- The abandoned attempt comes back only after its lock expires and a stall check runs. With the library's fixed 2-minute `lockDuration` and BullMQ's 30 s default `stalledInterval`, that is up to about 2.5 minutes in dev. Each such restart counts as one stall toward `maxStalledCount` 3; saving a step resets the counter ([Steps in v1: primitives and semantics](https://github.com/flolefebvre/nextasking/issues/13)).

## Steps saved by an attempt that ran the old code

This is the same for every option, because each one switches code at an attempt boundary:

- An attempt in flight finishes, or yields at a step boundary, on the code it started with. With A or B it keeps running in the same process. With C, D or a manual restart it is drained, or else killed and re-run after the stall.
- The next attempt or resume of that run re-enters the new handler and reuses saved steps by id ([Steps in v1: primitives and semantics](https://github.com/flolefebvre/nextasking/issues/13)):
  - a step whose id is unchanged returns the old code's result, even if the new code would compute it differently or return another shape;
  - a removed id leaves an unused result;
  - a new id runs.
- A step that was in doubt when the process died runs again ([Duplicate runs: what nextasking guarantees for side effects](https://github.com/flolefebvre/nextasking/issues/14)). `replay` also reuses saved steps ([Retries, failures, replay and lifecycle logs](https://github.com/flolefebvre/nextasking/issues/15)).
- This is the deploy situation (old and new code over one run), which the map leaves to payload versioning. In dev it happens on nearly every edit to a task with steps. Options that don't need the library to know a code version:
  - document it ("dispatch a new run after changing steps");
  - in dev only, log when a resumed attempt finds saved steps that the handler never reached.

  Detecting a code change per run in dev (a hash of the handler's source, for example) was not explored.

## Open points surfaced (not decided here)

- Which option, if any, v1 ships:
  - A costs nothing for the consumer but is silently stale for schedules, retries and resumes, and half-applies boot work.
  - C is complete but restarts the whole dev server and leans on exit code 77.
  - D is complete and leaves HTTP alone, but needs a `distDir` entry, doubles dev memory and churns `tsconfig.json`.
- Whether the docs recommend `NEXT_EXIT_TIMEOUT_MS` (undocumented) for dev, so that Ctrl-C lets attempts drain. The alternative is to accept that a dev restart re-runs the attempt in flight after up to about 2.5 minutes.
- Whether the 2-minute lock and the 3-stall limit, fixed by the library, should be shorter in dev, where the dev child is killed often.
