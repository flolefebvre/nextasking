# Running the worker as its own process

Research for the standalone-worker decision. Question: what are the realistic ways to run the **worker** (the process role that executes attempts) as its own process, with the same task code and config as the worker embedded in `next start`? And in `next dev`, can a worker pick up edited task code without a restart?

This builds on [`research/worker-in-nextjs.md`](https://github.com/flolefebvre/nextasking/blob/research/worker-in-nextjs/research/worker-in-nextjs.md) (branch `research/worker-in-nextjs`). That note already covers when `register()` runs, the `react-server` resolution split, `NEXT_MANUAL_SIG_HANDLE`, and stale code in `next dev`. None of that is repeated here.

## Versions and method

- Next.js **16.4.3**, read at tag `v16.4.3`. BullMQ **6.5.3**, ioredis 5.11.1, React/React DOM 19.2.0, `server-only` 0.0.1, esbuild 0.28.2, tsx 4.23.15, `@react-email/render` 2.2.0, Node 24.16.0, Valkey 8 in Docker, on Linux (WSL2).
- Prior art: Trigger.dev **v4.8.3**, Payload **v3.92.1**, graphile-worker **v0.18.0**.
- **(observed)** facts come from a throwaway Next 16.4.3 app built to stand in for a consumer's app:
  - task files in `src/nextasking/` import `@/` aliases;
  - one task file imports a module with `import "server-only"`, a `.tsx` component rendered with `react-dom/server`, and `revalidateTag` from `next/cache`;
  - the registry module publishes each of its evaluations on `globalThis`;
  - `instrumentation.ts` starts a BullMQ worker;
  - a route dispatches jobs.

  Each approach ran the same task, with one job dispatched from a separate process.

- RSS is the process's `VmRSS` a few seconds after boot plus one to three jobs, from a single sample. Treat the numbers as rough comparisons on a tiny app, not as capacity figures.

## Summary

| #   | Approach                                                     | Works                                                  | Same semantics as embedded              | RSS (observed)      | Port  | Internal APIs                                                          |
| --- | ------------------------------------------------------------ | ------------------------------------------------------ | --------------------------------------- | ------------------- | ----- | ---------------------------------------------------------------------- |
| 1   | `next start` / standalone `server.js`, worker on, no traffic | yes                                                    | identical                               | 147–154 MB / 119 MB | binds | none                                                                   |
| 2   | `next({ dev: false }).prepare()`, no `listen`                | yes; standalone only with a private env var            | identical                               | 145–149 MB / 118 MB | none  | none (non-standalone); `__NEXT_PRIVATE_STANDALONE_CONFIG` (standalone) |
| 3   | `require('.next/server/instrumentation.js')` + `register()`  | yes, both bundlers, also standalone                    | same bundle, **no `.env` loading**      | 94–99 MB            | none  | yes: `next/dist/server/node-environment` preload, build output layout  |
| 4   | Separate bundle of a worker entry (esbuild)                  | yes, with a `server-only` stub and CJS output          | **no**: default conditions, app's React | 91–94 MB            | none  | none                                                                   |
| 5   | `src/worker.ts` at runtime via tsx (+ resolve hook)          | yes with a `server-only` hook; Node type stripping: no | **no**: same as 4                       | 107–109 MB          | none  | none                                                                   |
| 6   | `after()`, kept-alive route handler, custom server           | not a separate worker                                  | —                                       | —                   | —     | —                                                                      |

- `revalidateTag()` called from a task threw `Invariant: static generation store missing in revalidateTag("posts")` in **every** approach, including the worker embedded in `next start` (observed). Hosting the worker inside Next does not give task code a request or work store, so `next/cache` and `next/headers` are no argument for approaches 1–3.
- Static `import { renderToStaticMarkup } from "react-dom/server"` in a file reached from an App Route is a **build error** in Next 16.4.3 (observed, Turbopack): "You're importing a component that imports react-dom/server…". `react-dom/server` is in the RSC transform's `invalid_server_imports` ([react_server_components.rs L703-L708, L326-L327](https://github.com/vercel/next.js/blob/v16.4.3/crates/next-custom-transforms/src/transforms/react_server_components.rs#L703-L708)). A dynamic `await import("react-dom/server")` builds and works. That is what `@react-email/render` 2.2.0 does (`dist/node/index.mjs`: `await import("react-dom/server")`). Task files that both a route and the worker import are therefore already limited to dynamic imports of `react-dom/server`.
- Dev reload: the registry module is re-evaluated **only when a route that imports it is requested** after the edit, under both Turbopack and webpack (observed, section 7). A worker that looks up handlers in the newest registry on `globalThis` gets fresh code only after such a request. A dev-only self-request from the worker to a route that imports the registry, sent before each job, made every job see the edited code with no user action (observed, both bundlers).

---

## 1. `next start` (or `server.js`) with the worker enabled and no traffic

**(observed)**

- `node node_modules/next/dist/bin/next start`: one process (`next-server (v16.4.3)`), listening on `*:PORT`, RSS 147–154 MB with the worker and 122 MB with `register()` returning early. Launching through `npx next start` adds an `npm exec` parent of about 88 MB. Use the binary directly.
- `node .next/standalone/server.js` (`output: "standalone"`): one process, listening on `0.0.0.0:3000`, RSS 119 MB with the worker.
- Jobs ran, `.env` was loaded, `server-only` and the dynamic `react-dom/server` import worked (Next's own bundle).

What the consumer writes: nothing beyond the embedded setup, plus an env switch to say which processes run the worker (or serve traffic). For example, a DigitalOcean worker component with `run_command: next start` (or `node server.js`) and `NEXTASKING_WORKER=on`.

DigitalOcean App Platform (docs, fetched 2026-11-01):

- Workers are "Workloads which do not expose publicly-accessible HTTP services". The worker spec has `run_command`, `build_command`, `instance_count`, `instance_size_slug`, `termination.grace_period_seconds` (default 120, max 600) and `liveness_health_check`. It has **no `http_port` and no routes**, so a port the process binds receives no traffic ([app spec reference](https://docs.digitalocean.com/products/app-platform/reference/app-spec/), [manage workers](https://docs.digitalocean.com/products/app-platform/how-to/manage-workers/)).
- `liveness_health_check` is optional. Without `http_path` "a TCP health check [is] used instead", and failing it restarts the component (same reference). A process that binds no port can still use an HTTP or TCP liveness check only if it opens one itself.
- Services default `http_port` to 8080 and inject `PORT` (same reference). Nothing is said about `PORT` for workers, so `next start` in a worker falls back to its own default of 3000.

Costs:

- The full router server is initialised.
- A port is bound for nothing.
- Shutdown needs `NEXT_MANUAL_SIG_HANDLE=true` to let in-flight jobs finish (see the earlier note).
- No internal APIs are involved.

## 2. Next's programmatic API, `prepare()` without HTTP

```js
// what was run (ESM)
import next from "next";
process.env.NODE_ENV = "production";
const app = next({ dev: false, dir: process.cwd() });
await app.prepare(); // register() runs here; nothing listens
```

**(observed)**

- `prepare()` ran `instrumentation.register()`. The worker started before `prepare()` resolved, jobs ran, `.env` was loaded, and **no port was bound**. RSS was 145–149 MB, the same as `next start`.
- `minimalMode: true` (a `NextServerOptions` field the custom-server docs do not list) changed nothing measurable: 143 MB.
- Standalone output:
  - Running the same script inside `.next/standalone/` **failed**. Without config, Next walked up and loaded the project's `next.config.ts` (downloading SWC to compile it), then crashed with `ENOENT … .next/standalone/.next/static`, because standalone does not copy `.next/static`.
  - Passing `conf` from `.next/required-server-files.json` to `next({ conf })` **did not help**.
  - It worked only after also setting `process.env.__NEXT_PRIVATE_STANDALONE_CONFIG = JSON.stringify(conf)`, which is what the generated `server.js` does. RSS was then 118 MB.

Source:

- `NextCustomServer.prepare()` calls `getRequestHandlers({ dir, port, isDev, hostname, minimalMode, quiet })`, which does the router-server initialisation. It does **not** forward `conf` ([next.ts L475-L499](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/next.ts#L475-L499), [start-server.ts L146](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/lib/start-server.ts#L146)). Config comes from `loadConfig`, which reads `__NEXT_PRIVATE_STANDALONE_CONFIG` when it is set ([config.ts L2032-L2042](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/config.ts#L2032-L2042)).
- Next's SIGINT/SIGTERM `cleanup` handlers are installed inside `startServer()` only ([start-server.ts L490-L491](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/lib/start-server.ts#L490-L491)), and the custom-server path does not call it. Shutdown therefore belongs to the caller, with no `NEXT_MANUAL_SIG_HANDLE` needed. This was read from the source and not exercised.
- Docs: `import next from 'next'` with `app.prepare()` is the documented custom-server API ([custom-server.mdx](https://github.com/vercel/next.js/blob/v16.4.3/docs/01-app/02-guides/custom-server.mdx)). The same page says "When using standalone output mode, it does not trace custom server files … These cannot be used together", and the custom server "should only be used when the integrated router of Next.js can't meet your app requirements".

What the consumer writes: a script entry such as `"worker": "nextasking-worker"`, a library bin wrapping the four lines above, run after `next build`. It needs no `next.config` changes. With `output: "standalone"`, the bin must either set the private env var from `required-server-files.json`, as `server.js` does, or run from the project directory with `next.config.*` and `.next/static` present.

## 3. Loading Next's compiled instrumentation directly

```js
// what was run (CJS)
process.env.NODE_ENV = "production";
const mod = require(path.resolve(".next/server/instrumentation.js"));
await mod.register();
```

**(observed)**

- Plain `node` **failed** on both bundlers with `Invariant: AsyncLocalStorage accessed in runtime where it is not available` (`next/dist/server/app-render/async-local-storage.js`).
- With `node -r next/dist/server/node-environment` (Next's internal environment setup: AsyncLocalStorage globals, console and crypto extensions, unhandled-rejection handling; [node-environment.ts](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/node-environment.ts)), it **worked**:
  - Turbopack build: RSS 95 MB;
  - webpack build: 94 MB;
  - inside `.next/standalone` (the preload file is traced there): 99 MB.
- No port is bound.
- `server-only`, aliases and `react-dom/server` behave as embedded, because it is Next's bundle. But `.env` was **not** loaded (`process.env.NX_FROM_DOTENV` unset). Nothing calls `@next/env`.
- The output layout is internal and differs per bundler:
  - Turbopack's `instrumentation.js` is `require("./chunks/[turbopack]_runtime.js")("server/instrumentation.js")` plus `R.c(…)`/`R.m(<id>)` chunk calls;
  - webpack's is a webpack runtime that `require`s `next/dist/compiled/next-server/app-page.runtime.prod.js`.

  Neither file nor the preload is documented.

What the consumer writes: a library bin that preloads Next's internal module, requires the build output, calls `register()`, and loads env itself. Fragility: two undocumented contracts (the preload module and the compiled file's shape), on top of `register()`'s signature, which is documented.

## 4. A separately bundled worker entry

Setup: `src/worker.ts` imports the registry and starts the worker. It is built with esbuild 0.28.2 using `bundle: true, platform: "node", target: "node22"`. tsconfig `paths` are read natively by esbuild, and `.tsx` uses the tsconfig's `jsx: react-jsx`.

**(observed)**

| Variant                                                                   | Result                                                                                                                                             |
| ------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| ESM output, `packages: "external"`                                        | `ERR_MODULE_NOT_FOUND …/node_modules/next/cache`. `next` has no `exports` map, so `next/cache` (also `next/headers`, …) needs `.js` under Node ESM |
| CJS output, `packages: "external"`, default conditions                    | `server-only` throws at load ("cannot be imported from a Client Component module")                                                                 |
| CJS, `packages: "external"`, `conditions: ["react-server"]`               | same: esbuild conditions do not apply to external packages, which Node resolves with its defaults                                                  |
| CJS, bundle all, `conditions: ["react-server"]`                           | loads; the job fails: "react-dom/server is not supported in React Server Components"                                                               |
| CJS, `packages: "external"`, `alias: { "server-only": "<empty module>" }` | **works**, RSS 94 MB; the alias applies even with external packages                                                                                |
| CJS, bundle all (except `next`), same alias                               | **works**, RSS 91 MB, 3.6 MB bundle                                                                                                                |

So the working recipe is:

- **stub `server-only`** and keep the default conditions;
- **emit CJS**, or rewrite `next/*` specifiers;
- keep or bundle `node_modules`.

Facts about the result:

- Task code runs on the app's `node_modules/react` (`useState` present), not Next's vendored RSC React. Packages with a `react-server` export get their `default` entry. Both differ from the embedded worker (earlier note, section 3).
- `.env` is not loaded. Payload solves this with `@next/env`'s `loadEnvConfig` ("the same algorithm next.js uses", [loadEnv.ts](https://github.com/payloadcms/payload/blob/v3.92.1/packages/payload/src/bin/loadEnv.ts)).
- It adds a build step next to `next build`. The esbuild build of this app took well under a second.

Prior art:

- **Trigger.dev v4.8.3** bundles task code with esbuild: `format: "esm"`, `platform: "node"`, `bundle: true`, conditions `[...build.conditions, "trigger.dev", "module", "node"]`, and JSX loaders ([bundle.ts L180-L236](https://github.com/triggerdotdev/trigger.dev/blob/v4.8.3/packages/cli-v3/src/build/bundle.ts#L180-L236)).
  - "All code is bundled by default" and `external` opts packages out.
  - Custom `conditions` such as `["react-server"]` "will also be passed to the `node` runtime when running your tasks" ([config-file.mdx](https://github.com/triggerdotdev/trigger.dev/blob/v4.8.3/docs/config/config-file.mdx)).
  - Neither its Next.js guide nor its troubleshooting page mentions `server-only`. Its Next guide imports tasks into routes with `import type` only ([nextjs.mdx](https://github.com/triggerdotdev/trigger.dev/blob/v4.8.3/docs/guides/frameworks/nextjs.mdx)).
- **Payload v3.92.1** `payload jobs:run` does not bundle. Its bin:
  - runs `dist/bin/index.js` through tsx's `tsImport` by default, through `@swc-node/register` with `--use-swc`, or with no transpiling under `--disable-transpile` ([bin.js](https://github.com/payloadcms/payload/blob/v3.92.1/packages/payload/bin.js));
  - loads env with `@next/env`;
  - installs a `module.registerHooks` resolve hook for Lexical's entry points ([bin/index.ts L28-L48](https://github.com/payloadcms/payload/blob/v3.92.1/packages/payload/src/bin/index.ts#L28-L48)).

  Its docs recommend the bin for dedicated servers because it means "No Next.js overhead" ([queues.mdx](https://github.com/payloadcms/payload/blob/v3.92.1/docs/jobs-queue/queues.mdx)). For import errors they suggest `--use-swc` or another runtime ([outside-nextjs.mdx](https://github.com/payloadcms/payload/blob/v3.92.1/docs/local-api/outside-nextjs.mdx)). Nothing handles `server-only`.

- **graphile-worker v0.18.0** loads task files with native `import()`, including `.ts` via Node type stripping. It recommends compiling TS to JS "for performance and memory usage reasons", and for non-erasable syntax a compile step, `--experimental-transform-types`, or a loader ([tasks.md](https://github.com/graphile/worker/blob/v0.18.0/website/docs/tasks.md)). It has no Next-specific guidance. pg-boss ships no CLI: the user writes the worker script.

What the consumer writes, if the library owns the build:

```jsonc
// package.json (shape only)
{ "scripts": { "build": "next build && nextasking build", "worker": "node .nextasking/worker.cjs" } }
```

The library's build would need the alias for `server-only` (and `client-only` if desired), CJS output (or `.js`-suffixed `next/*` specifiers), and env loading. It needs no `next.config` lines.

## 5. Running `src/worker.ts` at runtime

**(observed)**

| Command                                                     | Result                                                                                                                             |
| ----------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `node --import tsx src/worker.ts`                           | `server-only` throws                                                                                                               |
| `node --conditions=react-server --import tsx src/worker.ts` | loads; the job fails on `react-dom/server`                                                                                         |
| `node --import ./stub-hooks.mjs --import tsx src/worker.ts` | **works**, RSS 107–109 MB; aliases resolved by tsx                                                                                 |
| `node src/worker.ts` (Node 24 type stripping)               | `ERR_MODULE_NOT_FOUND` for `@/nextasking`; `.tsx` gives `ERR_UNKNOWN_FILE_EXTENSION`; extensionless relative imports are not found |
| `tsx watch --import ./stub-hooks.mjs src/worker.ts`         | works; editing a task file restarted the process (new pid) and the next job ran the new code                                       |

`stub-hooks.mjs` is ten lines: `module.registerHooks({ resolve })` returning an empty module for `server-only`. `module.registerHooks` is the same mechanism Payload's bin uses.

- tsx documents `tsconfig.json` `paths` support. On production use it says the decision "depends on your specific needs and risk tolerance" and notes that esbuild "technically hasn't reached a stable release yet" ([faq.md](https://github.com/privatenumber/tsx/blob/v4.23.15/docs/faq.md)).
- The constraints on task code are the same as in approach 4, plus transpile-on-start cost and no `.env` loading.

## 6. Other candidates

- **`after()`** runs after a response, within the request's lifetime: "`after` will run for the platform's default or configured max duration of your route" ([after.mdx](https://github.com/vercel/next.js/blob/v16.4.3/docs/01-app/03-api-reference/04-functions/after.mdx)). It needs a request and is not a worker process.
- **A kept-alive route handler** runs in the web process, which makes it the embedded worker, not a standalone one.
- **A custom server** is approach 2 plus `http.createServer`, with the same standalone incompatibility.

No Next 16.4.3 config option for adding an extra non-route server entry was found in the files read for this note. A full search of `config-shared.ts` was not done.

## 7. Dev reload

Setup (observed, `next dev` with Turbopack and with `--webpack`):

- `register()` starts the worker.
- Each evaluation of `registry.ts` (which plays the part of `createNextasking(...)`) pushes `{ id, tasks }` onto `globalThis.__nxRegistries`.
- The job handler runs both the registry it imported at start (`own`) and the newest one on `globalThis` (`fresh`).
- The task's `VERSION` was edited from v1 to v2 to v3.

| Step                                             | Turbopack                                   | webpack |
| ------------------------------------------------ | ------------------------------------------- | ------- |
| job before any request                           | own v1, fresh v1                            | same    |
| `GET /api/dispatch` (imports the registry)       | registry evaluated again (new instance, v1) | same    |
| edit to v2, wait 5 s, job                        | **fresh still v1**: nothing re-evaluated    | same    |
| `GET /` (page that does not import the registry) | fresh still v1                              | same    |
| `GET /api/dispatch` again                        | registry evaluated, **v2**                  | same    |
| next job                                         | own v1, **fresh v2**                        | same    |
| edit to v3, `GET /api/dispatch`, job             | own v1, fresh v3                            | same    |

- The module instance imported by `instrumentation` never updates (`own` stayed v1), as the earlier note found. The `globalThis` lookup is what carries new code.
- Re-evaluation is **lazy and request-driven**:
  - Turbopack's server HMR covers "App Router entries on the Node.js runtime" ([hot-reloader-turbopack.ts L705-L706](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/dev/hot-reloader-turbopack.ts#L705-L706)), and instrumentation is excluded.
  - Its comment at [L2183](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/dev/hot-reloader-turbopack.ts#L2183) reads: "The only server HMR pull, driven by the request being built — which is what makes evaluating a changed module lazy."
  - webpack behaved the same in the experiment.
- Each route that imports the registry is its own module instance. Two such routes produced two evaluations per edit (observed), so the array grows in dev, and keeping only the newest entry is enough for a lookup.

**Self-request (observed).** In dev, the worker `fetch`ed `http://localhost:<port>/api/nextasking-dev` before each job. That route only imports the registry. The job dispatched right after an edit, with no user request in between, saw the new version under **both** bundlers.

- Cost per job: 3–18 ms when nothing changed; 261 ms (Turbopack) and 1,457 ms (webpack) for the first compile of the route.
- It needs a route file in the consumer's app and the dev server's port. Payload builds its dev URL from `process.env.PORT || '3000'` ([getNextJsHMRURL.ts](https://github.com/payloadcms/payload/blob/v3.92.1/packages/payload/src/utilities/getNextJsHMRURL.ts)).
- The request runs in the same dev child process, so it shares `globalThis` with the worker.

**HMR WebSocket as an edit signal (observed).**

- The dev socket is `/_next/hmr` in 16.4.3 ([router-server.ts L1087](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/lib/router-server.ts#L1087)). Payload switches between `/_next/hmr` and `/_next/webpack-hmr` on "16.3.0" (same file). A non-browser client connected without error.
- On a task-file edit, webpack sent `{"type":"serverComponentChanges"}` about 55 ms later. Turbopack sent **no** `serverComponentChanges` (no browser page was subscribed), only `{"type":"built","hash":…}` with a new hash about 50 ms later.
- So both bundlers do announce a change, but with different messages, on an undocumented protocol whose path already moved once. A self-request per job needs no signal.

**`register()` guard form (observed, webpack dev).** With `if (process.env.NEXT_RUNTIME !== "nodejs") return;` followed by `await import("./nextasking/worker")`, `next dev --webpack` compiled the worker into the edge instrumentation bundle and failed with `Module not found: Can't resolve 'stream'` (ioredis via `bullmq/dist/esm`). Every page and route then returned 500, even though the Node-side worker kept running. The documented form, `if (process.env.NEXT_RUNTIME === "nodejs") { await import(...) }`, compiled cleanly. `next build` with Turbopack accepted both forms.

**Separate dev worker.** `tsx watch` restarts the worker process on each edit (section 5), so a standalone dev worker reloads without any of the above. Whether the old process's `worker.close()` gets to run on restart was not tested.

## Open points surfaced (not decided here)

- Approaches 1–3 run Next's own bundle and keep the embedded semantics exactly. Approaches 4–5 run a second resolution of the same files, with a `server-only` stub, default conditions and the app's React. Which one nextasking promises its consumers is the decision.
- For approach 2 with `output: "standalone"`: rely on `__NEXT_PRIVATE_STANDALONE_CONFIG` as `server.js` does, or require running from the project directory.
- For dev reload: whether the consumer adds a library route file (self-request), or the dev worker runs as its own `tsx watch` process.
