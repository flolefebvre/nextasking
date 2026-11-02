# Invalidating Next's cache from outside a request

Research for [Invalidating Next's cache from outside a request](https://github.com/flolefebvre/nextasking/issues/35). Question: what does Next 16.4.3 offer for invalidating its cache from code that runs outside a request, in a worker started from `register()` or in the `nextasking worker` bin? For each option: does it work in both worker placements, does it reach every instance when there are N of them (with a shared cache handler and with the default per-instance cache), and does it rely on Next internals?

This builds on [`research/worker-in-nextjs.md`](https://github.com/flolefebvre/nextasking/blob/research/worker-in-nextjs/research/worker-in-nextjs.md) and [`research/standalone-worker.md`](https://github.com/flolefebvre/nextasking/blob/research/standalone-worker/research/standalone-worker.md). The second one already observed `revalidateTag()` throwing `static generation store missing` in every worker placement. This note explains why, and what else there is.

## Versions and method

- Next.js **16.4.3**, read at tag `v16.4.3` (npm `latest` is 16.4.4 on 2026-11-02; this note stays on 16.4.3). React 19.2.0, BullMQ 6.5.3, ioredis 5.11.1, node-redis 5.12.1 (used by Next's handler example), Node 24.16.0, Valkey 8 in Docker, on Linux (WSL2).
- Prior art: Payload **v3.92.1**, Trigger.dev **v4.8.3**, Inngest JS **4.26.2**.
- **(observed)** facts come from a throwaway Next 16.4.3 app:
  - `cacheComponents: true`; `/` renders a `'use cache'` function tagged `posts` with `cacheLife("max")`, `/other` one tagged `other`, both prerendered at build;
  - a route handler `POST /api/revalidate` that checks a secret header and calls `revalidateTag(tag, { expire: 0 })` or `revalidatePath(path)`;
  - a page with a Server Action that calls `updateTag("posts")`;
  - a BullMQ worker started from `register()` (embedded), or by a bin-style script that runs `next({ dev: false }).prepare()` with no HTTP server (the bin);
  - two `next start` instances, A and B, each from its own copy of the build, so each has its own `.next` and its own memory. In the embedded runs only A ran the worker; in the bin runs neither did.

  Each job tried one way of invalidating `posts`. A driver read `/` on A and B before the job and twice after it.

- Three cache setups:
  1. **default**: no handler configured;
  2. **shared**: the two handlers from Next's [`cache-handler-redis` example](https://github.com/vercel/next.js/tree/v16.4.3/examples/cache-handler-redis), unchanged, as `cacheHandler` and as both `cacheHandlers.default` and `cacheHandlers.remote`, with `cacheMaxMemorySize: 0`;
  3. **shared pages, in-memory `'use cache'`**: the same `cacheHandler`, and `cacheHandlers.remote` only, so `'use cache'` (the `default` kind) stays in Next's in-memory handler. Apart from `cacheMaxMemorySize`, this is how Next's example ships.

## Summary

- `next/cache` 16.4.3 has no export that works outside a request. `revalidateTag` and `revalidatePath` only queue tags on the request's work store, and throw `Invariant: static generation store missing in …` when there is none. `updateTag` throws `updateTag can only be called from within a Server Action` (observed, both placements). The docs scope `revalidateTag` and `revalidatePath` to "Server Functions and Route Handlers", and `updateTag` to Server Actions only.
- Next applies the queued tags after the route handler or action returns. It calls `updateTags(tags, durations)` on every `cacheHandlers` handler and `revalidateTag(tags, durations)` on the request's incremental cache (the `cacheHandler` side). `revalidatePath(p)` is the same call with the soft tag `_N_T_<p>`, a format the docs describe.
- With the **default cache**, tag state is an in-memory `Map` per process. An invalidation reaches only the process that made it. That holds for every option below, and the bin serves no pages, so nothing it does in its own process is visible to users (observed). Next's docs say so: "calling `revalidateTag()` on one instance only invalidates the cache on that instance".
- With **shared handlers on every layer**, every option that reaches the handlers reached both instances (observed). The bin can reach them with no Next internals by calling the app's own handler modules: `new CacheHandler().revalidateTag(tags)` and `handler.updateTags(tags)`. A soft tag `_N_T_/other` invalidated `/other` the same way (observed).
- **One in-memory layer left over defeats it.** With `'use cache'` still in memory (setup 3), the direct handler call changed nothing. An invalidation through A's route made B rebuild the page from its stale in-memory entry and write it back to the shared store, so both instances then served stale content (observed).
- **Calling a route handler with a secret** works from both placements and needs no internals. It costs a route file in the app, a URL the worker can reach (the bin has no port of its own) and a secret. It reaches one instance with the default cache, and all of them with fully shared handlers (observed). This is what Trigger.dev documents, and what the reporter of Payload's open issue falls back to.
- **The other options rely on internals.** Next's live handlers sit on undocumented globals: `globalThis.__incrementalCache` and `Symbol.for('@next/cache-handlers-set')`. Running `revalidateTag` inside a fabricated work store also works. Both work only in the embedded worker, and only after the process has served a request; `__incrementalCache` is never set in the bin (observed). A Server Action can be called over raw HTTP with its build-specific `Next-Action` id (observed), but that wire protocol is internal.

---

## 1. What `revalidateTag`, `revalidatePath` and `updateTag` do

### They only queue

- `revalidateTag(tag, profile)` and `revalidatePath(path, type?)` both call a private `revalidate(tags, expression, profile)`. It reads `workAsyncStorage.getStore()` and throws `Invariant: static generation store missing in ${expression}` when there is no store, or the store has no `incrementalCache`. Otherwise it pushes `{ tag, profile, revalidatedAt }` onto `store.pendingRevalidatedTags` ([revalidate.ts L37, L110, L138-L146](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/web/spec-extension/revalidate.ts#L138-L146)).
- `updateTag(tag)` throws unless there is a work store and its page does not end in `/route`, then queues the tag with no profile (immediate expiry) (same file, [L58-L70](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/web/spec-extension/revalidate.ts#L58-L70)). Compared with `revalidateTag(tag, { expire: 0 })`, all it adds is read-your-own-writes in the action's own response. A worker has no response to refresh.
- `revalidatePath(p)` queues `_N_T_` + `p` (plus `_N_T_/index` for `/`). Soft tags are described in [How Revalidation Works](https://github.com/vercel/next.js/blob/v16.4.3/docs/01-app/02-guides/how-revalidation-works.mdx): "Soft tags are automatically generated by Next.js based on the route path, prefixed with `_N_T_`".
- The full list of `next/cache` exports in 16.4.3: `unstable_cache`, `revalidatePath`, `revalidateTag`, `updateTag`, `refresh`, `unstable_noStore`, `io`, `cacheLife`, `cacheTag`, `navigation`, `prefetch` (installed `next/cache.d.ts`). None of them invalidates outside a work store.

### What applies the queued tags

- `executeRevalidates(workStore)` groups the tags by profile. Each group becomes `durations = { expire }`, with string profiles looked up in `workStore.cacheLifeProfiles`. For every handler in `getCacheHandlers()` it calls `handler.updateTags(tags, durations)`, then `incrementalCache.revalidateTag(tags, durations)` ([revalidation-utils.ts L88-L194](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/revalidation-utils.ts#L88-L194)).
- A route handler awaits that before its response is sent ([app-route/module.ts L398](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/route-modules/app-route/module.ts#L398)). **(observed)** The first read after the route's `200` already showed the new content. Payload's [#18490](https://github.com/payloadcms/payload/issues/18490) shows the flip side: calls made after the handler has returned are dropped silently.
- The docs, [revalidateTag.mdx](https://github.com/vercel/next.js/blob/v16.4.3/docs/01-app/03-api-reference/04-functions/revalidateTag.mdx) and [revalidatePath.mdx](https://github.com/vercel/next.js/blob/v16.4.3/docs/01-app/03-api-reference/04-functions/revalidatePath.mdx), say both "can be called in Server Functions and Route Handlers". [updateTag.mdx](https://github.com/vercel/next.js/blob/v16.4.3/docs/01-app/03-api-reference/04-functions/updateTag.mdx) says "It cannot be used in Route Handlers, Client Components, or any other context". For callers outside a Server Action, revalidateTag.mdx recommends `revalidateTag(tag, { expire: 0 })`, while `"max"` serves stale content once and regenerates in the background.

### Observed in the workers

| Call                                       | Embedded (`register()`)                                                       | Bin (`prepare()`, no HTTP) |
| ------------------------------------------ | ----------------------------------------------------------------------------- | -------------------------- |
| `revalidateTag("posts", { expire: 0 })`    | throws `Invariant: static generation store missing in revalidateTag("posts")` | same                       |
| `revalidatePath("/")`                      | throws `… missing in revalidatePath("/")`                                     | same                       |
| `updateTag("posts")`                       | throws `updateTag can only be called from within a Server Action. …`          | same                       |
| `workAsyncStorage.getStore()` inside a job | `undefined`                                                                   | `undefined`                |

The worker sees no store because `register()` runs before any request, so nothing it starts inherits a request's async context. Payload's job runner does inherit one, with a worse result; see section 5.

## 2. Where the cache state lives

### Default cache, no handler configured

- **Tag state is one in-memory `Map` per process**: `tagsManifest` in `tags-manifest.external.ts` ([L10](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/lib/incremental-cache/tags-manifest.external.ts#L10)). Both default handlers write to it:
  - the incremental cache's `FileSystemCache.revalidateTag` ([file-system-cache.ts L98-L131](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/lib/incremental-cache/file-system-cache.ts#L98-L131));
  - the built-in `'use cache'` handler's `updateTags` ([cache-handlers/default.ts L218-L240](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/lib/cache-handlers/default.ts#L218-L240)).
- Entries live in an in-memory LRU, plus disk for pages. With `cacheMaxMemorySize: 0`, the built-in `'use cache'` handler is a no-op in production (default.ts [L54](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/lib/cache-handlers/default.ts#L54)).
- Docs: "By default, calling `revalidateTag()` on one instance only invalidates the cache on that instance. Other instances continue serving stale content until they independently discover the invalidation" ([self-hosting.mdx, Multi-Instance Cache Coordination](https://github.com/vercel/next.js/blob/v16.4.3/docs/01-app/02-guides/self-hosting.mdx)).

### Custom handlers

- `cacheHandler` (singular) is a class with `get`, `set`, `revalidateTag` and `resetRequestCache`. It backs pages, route handlers, `fetch` and images ([incrementalCacheHandlerPath.mdx](https://github.com/vercel/next.js/blob/v16.4.3/docs/01-app/03-api-reference/05-config/01-next-config-js/incrementalCacheHandlerPath.mdx)). Next builds a `new IncrementalCache` per request, which instantiates the class ("incremental-cache is request specific", [next-server.ts L451](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/next-server.ts#L451)).
- `cacheHandlers` (plural) are objects with `get`, `set`, `refreshTags`, `getExpiration` and `updateTags`, one per kind (`default` for `'use cache'`, `remote` for `'use cache: remote'`). They are loaded once per process into `globalThis` symbols ([handlers.ts L14-L62](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/use-cache/handlers.ts#L14-L62), [next-server.ts L430](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/next-server.ts#L430)).
- Coordination across instances is the handler's job. `updateTags()` writes the invalidation to shared storage, and `refreshTags()` (called before each request's first read) pulls the invalidations other instances made ([cacheHandlers.mdx](https://github.com/vercel/next.js/blob/v16.4.3/docs/01-app/03-api-reference/05-config/01-next-config-js/cacheHandlers.mdx), [how-revalidation-works.mdx](https://github.com/vercel/next.js/blob/v16.4.3/docs/01-app/02-guides/how-revalidation-works.mdx)). The self-hosting guide asks for a handler that keeps its tag index "in the shared store, so a `revalidateTag` call on one instance invalidates the entry on every instance".
- The handler paths end up in the build output: `.next/required-server-files.json` → `config.cacheHandler` and `config.cacheHandlers` (observed).

### What a worker process can reach (observed)

| Global                                               | Embedded, at `register()` | Embedded, in a job after the first request | Bin, after `prepare()` |
| ---------------------------------------------------- | ------------------------- | ------------------------------------------ | ---------------------- |
| `globalThis.__incrementalCache`                      | unset                     | set (the last request's instance)          | **never set**          |
| `globalThis[Symbol.for('@next/cache-handlers-set')]` | unset                     | set                                        | set                    |

`__incrementalCache` is assigned while a request is handled ([base-server.ts L1616](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/base-server.ts#L1616), [route-module.ts L587](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/route-modules/route-module.ts#L587)). It is overwritten on every request, and it carries that request's headers. Neither global is documented.

## 3. The options, observed

"A/B" is what the driver read on instance A, then B, after the job. "Reached" means the new content was served.

| #   | Option                                                                                                                                                      | Embedded worker (on A)                        | Bin worker                                                                         | Default cache, N instances                                      | Shared handlers on every layer, N instances    | Next internals                                                                                                                            |
| --- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------- | ---------------------------------------------------------------------------------- | --------------------------------------------------------------- | ---------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | `revalidateTag` / `revalidatePath` / `updateTag` from `next/cache`                                                                                          | throws                                        | throws                                                                             | —                                                               | —                                              | —                                                                                                                                         |
| 2   | HTTP `POST` to an app route handler with a secret, which calls `revalidateTag`                                                                              | works (A/B: reached/stale)                    | works, through A's URL (reached/stale)                                             | only the instance that gets the request                         | all instances (reached/reached)                | none                                                                                                                                      |
| 3   | Raw HTTP call of a Server Action (`Next-Action: <id>`)                                                                                                      | not run from a job; curl to A worked          | same                                                                               | only the instance that gets the request                         | all instances (reached/reached)                | yes: per-build action id from `server-reference-manifest.json`, action wire format                                                        |
| 4   | Call Next's live handlers through globals (`__incrementalCache.revalidateTag` + `updateTags` on the handler set)                                            | works after the first request (reached/stale) | partial: only `cacheHandlers` are reachable; the page entry stayed, so stale/stale | only the process that calls it                                  | all instances, embedded only (reached/reached) | yes: undocumented globals                                                                                                                 |
| 5   | `revalidateTag` inside a fabricated store: `workAsyncStorage.run({ incrementalCache: globalThis.__incrementalCache, … })`, then `executeRevalidates(store)` | works after the first request (reached/stale) | throws (no incremental cache to put in the store)                                  | only the process that calls it                                  | all instances, embedded only (reached/reached) | yes: `next/dist/server/app-render/work-async-storage.external`, `next/dist/server/revalidation-utils`, store shape                        |
| 6   | Call the app's own handler modules: `new (require(cacheHandlerPath))().revalidateTag(tags)` and `handlers.updateTags(tags)`                                 | works (reached/reached)                       | works with no request ever served (reached/reached)                                | not applicable: the built-in handlers can't be reached this way | all instances                                  | none in Next. The library needs the handler modules: passed in by the app, or read from `.next/required-server-files.json` (build output) |

Notes:

- **Option 2 in the embedded worker** can find its own instance through `process.env.PORT`, which `startServer()` sets once it listens ([start-server.ts L370](https://github.com/vercel/next.js/blob/v16.4.3/packages/next/src/server/lib/start-server.ts#L370)). The bin binds no port, so it needs the app's URL from configuration. Behind a load balancer, that URL lands on one instance chosen by the balancer. Each call is also an HTTP hop that can fail on its own, separate from the job.
- **Option 3**: Server Actions are public endpoints, so the secret would have to be an action argument. The action id changes with every build. `updateTag` brings a worker nothing that `revalidateTag(tag, { expire: 0 })` in a route handler doesn't.
- **Option 6 and `revalidatePath`**: calling `revalidateTag(["_N_T_/other"])` and `updateTags(["_N_T_/other"])` on the handler modules invalidated `/other` on both instances (observed). It works with Next's example handlers because their `cacheHandler` indexes page entries under every tag in `x-next-cache-tags`, soft tags included (the Redis keys showed `nextjs:tag:_N_T_/`, `_N_T_/page`, `_N_T_/layout`, `_N_T_/index`). Another handler might index differently.
- **Option 6 bypasses `durations`.** Next derives `durations` from the profile (`"max"` → stale-while-revalidate). A direct call passes whatever the caller chooses, and the handler decides what it means. Next's example handlers ignore it and expire at once.

### The in-memory layer that defeats a shared cache (setup 3, observed)

Setup 3 is Next's example config: shared `cacheHandler`, shared `remote` only, `'use cache'` in Next's in-memory handler.

1. Option 6 from the bin removed the page entry from Redis. Each instance then re-rendered the page from its own in-memory `posts` entry, which nothing had invalidated. A/B: stale/stale.
2. Option 2 through A, with A read first: A's in-memory tag state was updated, so A rendered fresh data and wrote the page to Redis. B then read that page from Redis. A/B: reached/reached. The outcome depends on which instance renders first.
3. Option 2 through A, with B read first, and B holding a `posts` entry in memory: B re-rendered from its stale entry and wrote the stale page to Redis. A then read that stale page. A/B: stale/stale.

So "reaches every instance" holds only when **every layer that holds tagged data** is shared, or coordinated through `refreshTags`: the `cacheHandler`, and `cacheHandlers.default` as well as `remote`. A `cacheMaxMemorySize` above 0 adds an in-memory default `'use cache'` handler whenever `default` isn't customized.

## 4. The default per-instance cache with N instances

- No call made in one process changes another process's cache. The tag `Map` and the LRU are per process (section 2), and the `.next` cache directory is per container in the reference hosting.
- To reach every instance, the invalidation has to be **delivered to each web instance and applied there**. One shape: a subscriber that every web instance starts from `register()` (Redis pub/sub, for example), which then applies the tags locally. It can apply them through a request to its own route (option 2, using `process.env.PORT`) or through the internal globals (options 4 or 5). The bin can publish but not apply, because it serves no pages.
- This was **not built end to end**. The pieces were observed separately: option 2 against the instance itself, and options 4 and 5 in-process.
- Read from the source, not observed: plain pub/sub does not replay a message to an instance that was down when it was sent. The tag `Map` is also lost on restart, so an invalidation that no request had acted on yet may not survive the restart.

## 5. Prior art

- **Payload** (v3.92.1):
  - Its website template calls `revalidatePath` / `revalidateTag` straight from collection `afterChange` hooks. Callers that run outside a request, like the seed script, set `context.disableRevalidate` to skip them ([revalidatePage.ts](https://github.com/payloadcms/payload/blob/v3.92.1/templates/website/src/collections/Pages/hooks/revalidatePage.ts)).
  - In jobs, the open issue [#16392](https://github.com/payloadcms/payload/issues/16392) ("Cannot use `next/cache` revalidation like `revalidatePath()` in jobs") reports `Route /admin/[[...segments]] used "revalidatePath …" during render which is unsupported`. The error names a render, not a missing store. Payload's runner starts from the first request (see the earlier note), so a job inherits that request's async context. That cause is inferred from the error, not exercised. The reporter suggests calling an API route from the task instead.
  - Its docs under `docs/jobs-queue/` do not mention revalidation.
- **Trigger.dev** (v4.8.3): tasks run outside the app. The Next.js guide's answer is a route handler (`app/api/revalidate/path/route.ts`) that checks a `REVALIDATION_SECRET` and calls `revalidatePath`, plus a task that `fetch`es `${NEXTJS_APP_URL}/api/revalidate/path` ([nextjs.mdx, "Revalidation from your Trigger.dev tasks"](https://github.com/triggerdotdev/trigger.dev/blob/v4.8.3/docs/guides/frameworks/nextjs.mdx)). That is option 2, with the app's public URL, so on a multi-instance self-host it reaches one instance.
- **Inngest** (JS 4.26.2):
  - With `serve()`, Inngest calls the app's `GET`/`POST`/`PUT` route handlers, and function code runs inside that route request ([next.ts](https://github.com/inngest/inngest-js/blob/inngest%404.26.2/packages/inngest/src/next.ts)). `next/cache` calls are then in a route-handler context, on whichever instance received the call. This was read from the adapter, not exercised.
  - `connect`, Inngest's long-lived worker (see the earlier note), runs outside any request.
  - Searching the `inngest` and `triggerdotdev` organisations' code for `revalidateTag` turned up nothing but the Trigger.dev guide above.

## 6. Costs, by option (for the API decision)

- **Route handler + secret (2)**:
  - Costs: one route file in the app (the cost that ruled out the self-request dev reload in [Where the worker runs and how it starts](https://github.com/flolefebvre/nextasking/issues/11)); a reachable URL and a secret, both configuration; one HTTP hop per invalidation, which can fail apart from the job.
  - Reach: one instance under the default cache.
  - Internals: none.
  - Same model as Trigger.dev, and Inngest's `serve()` runs this way by construction.
- **App's handler modules (6)**:
  - Costs: the app must use custom handlers, and they must cover every layer, or it silently does nothing (setup 3). The library must find the handlers, either passed in by the app or read from the build output. It must call two different interfaces with tags in Next's soft-tag format, and it bypasses Next's `durations` mapping.
  - Reach: all instances, from either placement.
  - Internals: none in Next.
- **Globals or fabricated store (4, 5)**:
  - Costs: embedded worker only, after the first request; local-only under the default cache; undocumented globals and module paths that can change in any Next release.
  - Value: the one way to apply an invalidation inside a process without a route file, so it is a candidate for the "apply" half of a per-instance fan-out (section 4), with the same internals cost.
- **Server Action over raw HTTP (3)**: internal wire protocol and per-build ids, with nothing gained over option 2.
- **Fan-out to every instance (section 4)**: the only way to make the default per-instance cache consistent across N instances. It needs a subscriber in every web instance's `register()`, plus one of the "apply" mechanisms above.
