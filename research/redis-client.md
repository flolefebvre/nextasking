# Redis client: ioredis or node-redis

Research for the proposal that the consumer passes `createNextasking({ connection: { url } })` and the library depends on a Redis client directly and owns every client. The question: "ioredis? I thought nowadays it was more node-redis?" Facts only; the decision belongs to the design ticket.

This builds on [`research/bullmq-runtime.md`](https://github.com/flolefebvre/nextasking/blob/research/bullmq-runtime/research/bullmq-runtime.md) (branch `research/bullmq-runtime`), which already covers what each BullMQ class opens, the ioredis options BullMQ forces or rejects, the class detection of raw clients, and Valkey's idle timeout. None of that is repeated here.

## Versions read

| Source                   | Version                                                 | Where                                                                                                     |
| ------------------------ | ------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| BullMQ                   | 6.5.3 (npm `latest`, 2026-10-31), commit `a8ff126`      | [taskforcesh/bullmq](https://github.com/taskforcesh/bullmq/tree/a8ff12664de94f52a9bcc964c6129a4b23d00aa6) |
| ioredis                  | 6.0.0 (npm `latest`, 2026-07-31)                        | [redis/ioredis](https://github.com/redis/ioredis), npm tarball                                            |
| node-redis (`redis`)     | 6.3.1 (npm `latest`, 2026-10-29); `@redis/client` 6.3.1 | [redis/node-redis](https://github.com/redis/node-redis), npm tarball of `@redis/client`                   |
| iovalkey                 | 0.4.0 (npm `latest`, 2026-07-27)                        | [valkey-io/iovalkey](https://github.com/valkey-io/iovalkey)                                               |
| Valkey GLIDE             | `@valkey/valkey-glide` 2.5.3 (npm `latest`)             | [valkey-io/valkey-glide](https://github.com/valkey-io/valkey-glide)                                       |
| Valkey (experiment)      | 9.1.3, `valkey/valkey:9.1.3` docker image               | local                                                                                                     |
| Registry and GitHub data | read on 2026-11-01                                      | `npm view`, GitHub REST API                                                                               |

Below, `bullmq:` links are relative to `https://github.com/taskforcesh/bullmq/blob/a8ff126/`.

## Summary

- **Redis Inc. recommends node-redis for new projects.** ioredis's README says it is "a stable project and maintenance is done on a best-effort basis", and "For new projects, node-redis is the recommended client library". redis.io calls ioredis "an older library that is still supported". In practice ioredis is far from abandoned: it shipped a 6.0.0 major on 2026-07-31, is released by Redis staff, and merged 110 PRs between 2026-05-01 and 2026-11-01 (node-redis: 222).
- **That recommendation targets apps that call Redis themselves.** nextasking would not: BullMQ makes every call. For BullMQ, ioredis is still the default and the most tested client. node-redis is a supported adapter since May 2026 (BullMQ 5.77). BullMQ's CI runs its full suite on node-redis too, minus the ioredis-specific files (cluster, connection internals, sandboxed processors), but **only against Redis 7**. The Valkey 8 job runs on ioredis. The node-redis adapter has **no cluster support** (`isCluster = false; // TODO`).
- **BullMQ's CI tests ioredis 5.11.1 and node-redis 6.3.0** (its devDependencies), so ioredis 6 is not what BullMQ tests, and one ioredis 6 typing issue is open ([#4595](https://github.com/taskforcesh/bullmq/issues/4595)).
- **The choice does not leak** to a consumer who only passes a URL, as long as nextasking passes BullMQ a client instance (never bare options, and never the process-wide `RedisConnection.clientFactory`). BullMQ declares both clients as optional peers. It is server-only code, so "bundle size" is install size: about 2 MB for ioredis, 11.6 MB for `@redis/client`, 15.6 MB for `redis`.
- **Idle drop behaviour differs.** On a Valkey idle close, ioredis reconnected silently. node-redis emitted `'error'` (`Socket closed unexpectedly`) through BullMQ's Queue, then reconnected. Both then completed the next job. node-redis has a `pingInterval` option that prevents the idle close; ioredis has none.
- **Both clients map `rediss://` to TLS.** Experiment: BullMQ 6.5.3 with node-redis 6.3.1 and with ioredis 6.0.0, against Valkey 9.1.3, produced, processed and survived an idle close (see [Experiment](#experiment)).

## 1. ioredis today

- **README notice** (main branch, read 2026-11-01): "ioredis is a stable project and maintenance is done on a best-effort basis for relevant issues (contributions to ioredis will still be evaluated, reviewed, and merged when they benefit the project). For new projects, node-redis is the recommended client library." ([redis/ioredis README](https://github.com/redis/ioredis#readme))
- **Versions:** 6.x on `main` needs Node ≥ 20 and Redis 6.2 or later; 5.x lives on branch `v5` (README "Versions" table). An upgrade guide exists ([wiki: Upgrading from v5 to v6](https://github.com/redis/ioredis/wiki/Upgrading-from-v5-to-v6)). A `6.1.0-beta.1` is tagged `beta` on npm.
- **Release cadence** (npm `time`): 5.9.0 (2026-01-05), 5.9.1, 5.9.2, 5.9.3, 5.10.0, 5.10.1, 5.11.0, 5.11.1 (2026-06-04), 6.0.0 (2026-07-31). A 4.31.0 was released on 2026-01-05 too.
- **Activity** (GitHub API, 2026-11-01): last push 2026-10-30, 138 open issues and 33 open PRs, 110 PRs merged since 2026-05-01. Recent merges are mostly reconnect, cluster and sentinel fixes from outside contributors, merged by Redis staff ([PR list](https://github.com/redis/ioredis/pulls?q=is%3Apr+is%3Amerged)). The repo is not archived.
- **Who maintains it:** the repo is `redis/ioredis`. npm maintainers are `ioredis-robot` and `dmaier-redislabs` (a redis.com address). Recent maintenance PRs, such as CI publishing with npm trusted publishing (#2226), are by `PavelPashov`, who is also an npm maintainer of `@redis/client`.
- **What Redis Inc. recommends** ([redis.io: client libraries](https://redis.io/docs/latest/develop/clients/)): "`node-redis` is the recommended JavaScript client library for most use cases. `ioredis` is an older library that is still supported, but lacks some of the newer features and performance optimizations of `node-redis`." Its decision tree says "For new projects, node-redis is the recommended choice". Existing ioredis users without search, vector or JSON needs are told to "Continue using ioredis". Both are listed as "Supported: Yes".

## 2. node-redis, iovalkey and Valkey GLIDE

### node-redis (`redis`)

- **Current major is 6.** It went 5.12.1 (2026-04-14), then 6.0.0 (2026-05-28), up to 6.3.1 (2026-10-29), released roughly monthly. `redis` is a meta-package over `@redis/client` plus the Redis Stack modules (`@redis/json`, `@redis/search`, `@redis/time-series`, `@redis/bloom`) ([README: Packages](https://github.com/redis/node-redis#packages)). It needs Node ≥ 20.
- **Activity:** last push 2026-10-31, 106 open issues and 15 open PRs, 222 PRs merged since 2026-05-01. Five npm maintainers, Redis staff among them.
- **Defaults that matter here** (from `@redis/client` 6.3.1 `dist/`):
  - **RESP3 by default** (`DEFAULT_RESP = 3`, `RESP/types.js`). It connects with `HELLO 3`.
  - **Maintenance notifications.** `maintNotifications` defaults to `auto` under RESP3, which sends `CLIENT MAINT_NOTIFICATIONS ON` at connect. In `auto` mode a failed handshake is only logged to debug, so on a server without that command, such as Valkey, the failure is swallowed (`enterprise-maintenance-manager.js`).
  - **A 5000 ms default command timeout** (`DEFAULT_COMMAND_TIMEOUT`, `client/index.js#L466`). It is cleared once the command is written to the socket (`commands-queue.js`, around L428), so it bounds the wait to send, not the wait for a reply. Blocking `BZPOPMIN`/`XREAD BLOCK` are not cut by it, which the experiment confirmed with QueueEvents' 10 s `XREAD BLOCK`.
  - **Reconnect:** the default `reconnectStrategy` is `min(2^retries × 50, 2000)` ms plus 0–200 ms of jitter, forever, **except after a `SocketTimeoutError`, where it does not reconnect** (`socket.js#L429-L438`). `socketTimeout` is undefined by default.
  - **TCP keepalive:** on, with an initial delay of 30 000 ms (`DEFAULT_KEEPALIVE_INITIAL_DELAY`).
  - **`pingInterval`:** "Send `PING` command at interval (in ms). Useful with Redis deployments that do not honor TCP Keep-Alive." The timer starts on `ready` and re-arms after each PING, whatever other traffic there is. A PING failure is emitted as `'error'` (`client/index.js#L692-L704`).
  - **Offline queue:** on by default. `disableOfflineQueue: true` rejects commands while reconnecting, which is the node-redis equivalent of ioredis's `enableOfflineQueue: false`.
- **Valkey:** neither the node-redis README nor redis.io makes any Valkey claim. Valkey's own client list does not mention node-redis or ioredis. It says generally: "Anything that speaks the Redis 7.2 wire protocol works with Valkey 7.2 and later" ([valkey.io/clients](https://valkey.io/clients/)).

### iovalkey

- "A friendly fork of ioredis after [commit ec42c82]", supporting Valkey 7.0 or later ([README](https://github.com/valkey-io/iovalkey#readme)). Listed by Valkey as "A friendly fork of a widely used Node.js client, tracking Valkey" ([valkey.io/clients](https://valkey.io/clients/)).
- Small activity: 0.4.0 on 2026-07-27, after 0.3.3 on 2025-06-12. One npm maintainer (`matteo.collina`). 12 PRs merged in 2026, 35 open issues and PRs, 241 stars. It is still 0.x.
- **BullMQ has no iovalkey adapter.** A raw iovalkey instance would fall through to the ioredis adapter by structural detection (`bullmq: src/classes/redis-connection.ts#L118-L150`; inferred, untested). It is in no BullMQ CI job.

### Valkey GLIDE

- `@valkey/valkey-glide` 2.5.3, Apache-2.0, a Rust core with Node bindings. It ships prebuilt native packages as `optionalDependencies` for darwin x64/arm64 and linux x64/arm64 (gnu and musl) only, so there is no Windows build (npm `optionalDependencies`). Maintained under `valkey-io` (npm maintainers include `valkey-glide@amazon.com`). 546 open issues and PRs.
- BullMQ supports it through `createValkeyGlideClient`. A dedicated workflow runs `test:valkey-glide` against `valkey/valkey:8` with GLIDE 2.5.0 ([bullmq: .github/workflows/valkey-glide-test.yml](https://github.com/taskforcesh/bullmq/blob/a8ff126/.github/workflows/valkey-glide-test.yml)).

## 3. BullMQ 6.5.3 support per client

### Docs

- "By default, BullMQ creates Redis connections with ioredis, and the options you pass to BullMQ are passed to the ioredis constructor." The package "includes adapters for ioredis, node-redis, Bun's built-in Redis client and Valkey GLIDE" ([bullmq: docs/gitbook/guide/connections.md](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/guide/connections.md)). The README lists ioredis as "(default)" ([bullmq: docs/gitbook/README.md#L44](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/README.md)).
- The docs do not recommend one client over another. Every quick-start and production example uses ioredis ([quick-start.md](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/quick-start.md); [going-to-production.md](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/guide/going-to-production.md)).
- node-redis: "BullMQ does not create node-redis clients directly. Create the raw client in your application and wrap it with `createNodeRedisClient`", with `redis` v5 or later as the peer.

```ts
import { Queue, Worker, createNodeRedisClient } from "bullmq";
import { createClient } from "redis";

const connection = createNodeRedisClient(createClient({ url }));
const queue = new Queue("tasks", { connection });
const worker = new Worker("tasks", process, { connection });
```

### History

- node-redis and Bun support came with "refactored client code, adding support for node-redis and bun native redis client" ([#4170](https://github.com/taskforcesh/bullmq/pull/4170), merged 2026-05-21, in the 5.77 line of [changelog-v5](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/changelogs/changelog-v5.md)). The GLIDE adapter came in 5.81 ([#4321](https://github.com/taskforcesh/bullmq/pull/4321)).
- Merged node-redis fixes since then: [#4195](https://github.com/taskforcesh/bullmq/pull/4195) (types), [#4209](https://github.com/taskforcesh/bullmq/pull/4209) (accept a plain `createClient` result), and [#4952](https://github.com/taskforcesh/bullmq/pull/4952) (clear the ended status when `connect()` reopens), the latter being the only bug fix listed in 6.5.3 itself.
- The long-standing "Migrate to official node-redis" issue is still open ([#2490](https://github.com/taskforcesh/bullmq/issues/2490), opened 2024-03-26). The adapter answers it without changing the default.

### CI at `a8ff126`

| Job                                | Client         | Server               | Suite                                                                                                  |
| ---------------------------------- | -------------- | -------------------- | ------------------------------------------------------------------------------------------------------ |
| `ioredis-redis`, gating            | ioredis 5.11.1 | Redis 7              | full suite plus `test:ioredis` (cluster, connection internals, sandboxed processes), with coverage     |
| `ioredis-redis-extra`              | ioredis 5.11.1 | Redis 7, and Redis 6 | full suite plus `test:ioredis`, on Node lts/-1 and lts/-2                                              |
| `node-redis`                       | redis 6.3.0    | **Redis 7 only**     | `test:adapter-conformance` plus `test:node-redis`: the full suite **minus** the ioredis-specific files |
| `node-valkey`                      | ioredis 5.11.1 | **Valkey 8**         | full suite                                                                                             |
| `valkey-glide` (separate workflow) | GLIDE 2.5.0    | Valkey 8             | `test:valkey-glide`                                                                                    |

Sources: [bullmq: .github/workflows/test.yml#L122-L320](https://github.com/taskforcesh/bullmq/blob/a8ff126/.github/workflows/test.yml#L122-L320); [bullmq: vitest.node-redis.config.ts](https://github.com/taskforcesh/bullmq/blob/a8ff126/vitest.node-redis.config.ts) (excludes `cluster`, `connection`, `sandboxed_process`, `worker.ioredis`, `relay.ioredis`); [bullmq: vitest.node-redis.setup.ts](https://github.com/taskforcesh/bullmq/blob/a8ff126/vitest.node-redis.setup.ts); [bullmq: package.json](https://github.com/taskforcesh/bullmq/blob/a8ff126/package.json) devDependencies (`ioredis` 5.11.1, `redis` 6.3.0).

So **no BullMQ CI job runs node-redis against Valkey**, and **no job runs ioredis 6**.

### What BullMQ needs from each client

| Need                              | ioredis                                                                                                                      | node-redis (through `createNodeRedisClient`)                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| --------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Who creates the client            | BullMQ can build it from options, including `url` (`bullmq: src/classes/redis-connection.ts#L403-L411`), or take an instance | The caller only. BullMQ never imports `redis` (`bullmq: src/classes/node-redis-client.ts#L8-L24`). Without an instance, the only route is the process-wide `RedisConnection.clientFactory`                                                                                                                                                                                                                                                                                    |
| Retries for blocking connections  | `maxRetriesPerRequest: null`, forced or checked by BullMQ (see `bullmq-runtime.md`)                                          | No equivalent check. "configure that client's retry and reconnect behavior according to its own documentation" ([connections.md](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/guide/connections.md#maxretriesperrequest)). The production page names `socket.reconnectStrategy`, the connect timeout and command timeouts ([going-to-production.md#L51-L58](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/guide/going-to-production.md)) |
| Duplicating for blocking commands | ioredis `duplicate()`, wrapped                                                                                               | `raw.duplicate()` wrapped in a new adapter, with scripts copied and `CLIENT SETNAME` applied before `ready` (`node-redis-client.ts#L423-L441`)                                                                                                                                                                                                                                                                                                                                |
| Connecting                        | ioredis connects in its constructor                                                                                          | The adapter calls `raw.connect()` eagerly in its constructor if the client is not open. Connect errors surface only as `'error'` (`node-redis-client.ts#L280-L292`)                                                                                                                                                                                                                                                                                                           |
| Blocking `BZPOPMIN`               | native                                                                                                                       | `raw.bzPopMin`. "Disconnects client", "The client is closed" and "Connection is closed." become `ConnectionClosedError`, which the Worker loop treats as a connection error (`node-redis-client.ts#L40-L46, L764-L786`)                                                                                                                                                                                                                                                       |
| Lua scripts                       | `defineCommand`                                                                                                              | `EVALSHA` with an `EVAL` fallback, implemented in the adapter (`node-redis-client.ts#L450-L510`)                                                                                                                                                                                                                                                                                                                                                                              |
| Cluster                           | supported and tested                                                                                                         | **not supported**: `readonly isCluster = false; // TODO: cluster support` (`node-redis-client.ts#L238`)                                                                                                                                                                                                                                                                                                                                                                       |
| `keyPrefix`                       | rejected (`bullmq-runtime.md`)                                                                                               | n/a                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |

### Open issues that touch the choice

- [#4595](https://github.com/taskforcesh/bullmq/issues/4595) (open, 2026-08-20): ioredis 6 `RedisOptions` is not assignable to BullMQ 6 `ConnectionOptions`, because `retryStrategy` was widened to accept `null`. It is types only, and only bites when ioredis options are passed to BullMQ instead of an instance.
- No open BullMQ issue mentions a node-redis adapter bug (search "node-redis", 2026-11-01). The open connection issues ([#4656](https://github.com/taskforcesh/bullmq/issues/4656) `close()` hangs when Redis is unreachable, [#4861](https://github.com/taskforcesh/bullmq/issues/4861) a closing Worker reconnects its blocking client, [#4912](https://github.com/taskforcesh/bullmq/issues/4912) and [#4138](https://github.com/taskforcesh/bullmq/issues/4138) on clusters) are reported on ioredis, the client most people use.

## 4. Does the choice leak to the consumer?

- **Peer dependencies.** `bullmq@6.5.3` declares `ioredis >=5.0.0`, `redis >=5.0.0`, `pg` and `bullmq-otel` as peers, all with `optional: true` in `peerDependenciesMeta` (npm registry). If nextasking lists its client in `dependencies`, the consumer installs nothing extra and sees no peer warning.
- **Routing.** With ioredis, BullMQ does `require('ioredis')` from its own location when it is given options rather than an instance (`redis-connection.ts#L76-L99`). Under pnpm's isolated layout that resolution depends on how the optional peer is linked (not verified here). Passing BullMQ an **instance** always sidesteps it, for either client. With node-redis, BullMQ never `require`s anything.
- **Do not use `RedisConnection.clientFactory`.** It is a static on BullMQ's `RedisConnection` (`redis-connection.ts#L179`). If the consumer's app also uses BullMQ directly and both resolve to the same `bullmq` copy, setting it would change the consumer's own queues (inferred).
- **The consumer already uses another client.** With URL-only configuration no client object crosses the API, so a consumer on ioredis, node-redis or neither is unaffected. The only cost is a second client in `node_modules` and separate connections. Connection counts are the same either way (see `bullmq-runtime.md`).
- **Bundle and install size.** This is server-only code, so nothing reaches the browser bundle. Next.js's automatic `serverExternalPackages` list contains no Redis or BullMQ package ([vercel/next.js `server-external-packages.jsonc` at `29bbeab`](https://github.com/vercel/next.js/blob/29bbeab09981148ade186e8599ecdede15abc281/packages/next/src/lib/server-external-packages.jsonc)), so the server build may bundle whichever client is used. Measured `node_modules` after a clean `npm i`:

  | Install               | Size    | Packages                                                                              |
  | --------------------- | ------- | ------------------------------------------------------------------------------------- |
  | `ioredis@6.0.0`       | 1.98 MB | ioredis plus 6 small dependencies                                                     |
  | `@redis/client@6.3.1` | 11.6 MB | 1 package (zero dependencies; optional peers `@node-rs/xxhash`, `@opentelemetry/api`) |
  | `redis@6.3.1`         | 15.6 MB | client plus the 4 Redis Stack modules                                                 |

  `createNodeRedisClient` types its argument structurally, so `@redis/client` alone is enough. BullMQ's optional peer names `redis`, which would then be "missing" but is optional.

## 5. Valkey, TLS and idle reconnects

- **Valkey 8/9 with BullMQ.**
  - ioredis: tested in BullMQ CI against Valkey 8, with the full suite.
  - node-redis: no CI coverage on Valkey. It worked in the experiment against Valkey 9.1.3 with its defaults: RESP3, and an `auto` maintenance handshake that Valkey does not know and that is swallowed.
  - GLIDE: tested against Valkey 8.
  - iovalkey: untested.
- **TLS through `rediss://`.**
  - ioredis: `rediss://` enables TLS when the URL is parsed (`built/Redis.js#L707`, `built/utils/index.js#L248-L300` in 6.0.0).
  - node-redis: `parseURL` sets `socket.tls = protocol === 'rediss:'` and rejects any other scheme (`client/index.js#L183-L186` in `@redis/client` 6.3.1).
  - DigitalOcean managed Valkey "require[s] you to connect using SSL/TLS" with Let's Encrypt certificates, "which does not require that you download a CA certificate", so Node's default CA store suffices for both clients ([DO: connect to Valkey](https://docs.digitalocean.com/products/databases/valkey/how-to/connect/)). TLS was not exercised in the experiment.
- **Idle-connection reconnect.**
  - `bullmq-runtime.md` showed that only a producer-only Queue connection goes idle long enough to hit DO's 300 s `timeout`.
  - ioredis reconnects through `retryStrategy` without emitting `'error'` (experiment) and has no PING option.
  - node-redis surfaces the close as `'error'` (`Socket closed unexpectedly`), which BullMQ re-emits on the Queue, and then reconnects with its default strategy (experiment). `pingInterval` below the server timeout prevents the close altogether (experiment, with `pingInterval: 2000` against `timeout 5`).
  - node-redis does not reconnect after a `SocketTimeoutError` by default, which only matters if `socketTimeout` is set.

## Experiment

The script is in `.tmp/exp/run.mjs` (gitignored, not committed). It uses `bullmq@6.5.3`, `redis@6.3.1` and `ioredis@6.0.0` on Node 24.16.0, against `valkey/valkey:9.1.3` started with `--timeout 5`. Each variant uses separate producer and worker connections, plus a QueueEvents. It adds 3 jobs and waits for each, idles 8 s, then adds a 4th job.

```
[ioredis +0.0s] added 1..3 -> ok:1..3
[ioredis +8.2s] after idle added 4 -> ok:4

[node-redis +0.1s] added 1..3 -> ok:1..3
[node-redis +5.3s] queue error: Socket closed unexpectedly
[node-redis +5.3s] raw reconnecting
[node-redis +8.1s] after idle added 4 -> ok:4

[node-redis-ping +0.1s] added 1..3 -> ok:1..3        (pingInterval: 2000)
[node-redis-ping +8.1s] after idle added 4 -> ok:4  (no error, no reconnect)
```

All three closed cleanly (`worker.close()`, `events.close()`, `queue.close()`, then `quit()` on the instances).
