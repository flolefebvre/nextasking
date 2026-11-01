# BullMQ runtime behaviour

Research for issue #4 (BullMQ runtime behaviour), under map #1. Facts only; the design decisions belong to the HITL tickets.

## Versions read

| Source                     | Version                                                    | Where                                                                                                                                                            |
| -------------------------- | ---------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| BullMQ                     | **6.5.3** (npm `latest` on 2026-11-01), commit `a8ff126`   | [taskforcesh/bullmq](https://github.com/taskforcesh/bullmq/tree/a8ff12664de94f52a9bcc964c6129a4b23d00aa6)                                                        |
| ioredis                    | 6.0.0 (npm `latest`), commit `35cfa89`; 5.11.1 for compare | [redis/ioredis](https://github.com/redis/ioredis/tree/35cfa895726486f09badc0412091d24e38da57fd)                                                                  |
| Valkey                     | 9.1.3 (latest release)                                     | [valkey-io/valkey `src/timeout.c`](https://github.com/valkey-io/valkey/blob/9.1.3/src/timeout.c)                                                                 |
| Next.js                    | 16.4.3, commit `29bbeab`                                   | [vercel/next.js `start-server.ts`](https://github.com/vercel/next.js/blob/29bbeab09981148ade186e8599ecdede15abc281/packages/next/src/server/lib/start-server.ts) |
| DigitalOcean API (OpenAPI) | commit `112b376`                                           | [digitalocean/openapi](https://github.com/digitalocean/openapi/tree/112b3762423ee6e853d7b0e9c1777fe89f493e21)                                                    |

Below, `bullmq:` links are relative to `https://github.com/taskforcesh/bullmq/blob/a8ff126/`. Everything holds for BullMQ 6.5.3. BullMQ 6.0.0 (2026-07-30) broke several things relative to 5.x (see [CHANGELOG](https://github.com/taskforcesh/bullmq/blob/a8ff126/CHANGELOG.md#600-2026-07-30)), and these matter for nextasking:

- `ioredis` is now an **optional peer dependency** (`>=5.0.0`), so the consumer or the library must install it.
- Legacy repeatable jobs (`repeat` on `add`, `getRepeatableJobs`, `removeRepeatable*`) are gone. Only Job Schedulers remain.
- The raw client moved behind `getBackend()`. Pluggable backends (Redis, PostgreSQL) and client adapters (ioredis, node-redis, Bun, Valkey GLIDE) are new.

## Summary

- **Connections.** A Worker opens 2 connections: a command connection plus a dedicated blocking one for `BZPOPMIN`. A QueueEvents opens 1 blocking connection (`XREAD BLOCK`). A Queue opens 1. When you pass a client instance, BullMQ duplicates it wherever it needs a blocking connection, so one shared instance is safe. BullMQ forces `maxRetriesPerRequest: null` **only on blocking connections**. If you pass an ioredis instance that has a non-null value, `new Worker`/`new QueueEvents` throws. In 6.x, node-redis, Bun and GLIDE clients are detected and wrapped. Anything unrecognised is treated as ioredis.
- **Idle drops.** Valkey's idle `timeout` never closes clients that are blocked or in Pub/Sub. A running Worker sends a command at least every ~5 s (`drainDelay`) on its blocking connection and every 30 s (stalled check) on its command connection. A **producer-only Queue connection does go idle** and gets closed after DigitalOcean's 300 s default. ioredis then reconnects. TCP keepalive does not reset the server's idle timer.
- **Schedulers.** `upsertJobScheduler` is one atomic Lua script, and occurrence job IDs are deterministic (`repeat:<id>:<millis>`). N instances upserting the same scheduler therefore converge on a single pending job. Only the job that is the scheduler's current occurrence can produce the next one (`producerId` check). Missed occurrences are skipped, not caught up. A scheduler removed from code **keeps firing forever** until someone calls `removeJobScheduler`. Two edge cases can drop or duplicate an occurrence (see [Schedulers](#schedulers)).
- **Stalls.** The defaults are a 30 s lock, renewal every 7.5–15 s, a stalled check every 30 s (at most once per queue across all workers), and `maxStalledCount: 1`. A job is re-queued only when its lock key is gone **and** it was already flagged at the previous check. If the event loop blocks for more than about 15–30 s, the lock expires and **a second run starts while the first is still alive**. BullMQ does not abort the first run. It only emits `lockRenewalFailed`, and the first run's final write fails with `Missing lock`. `maxStalledCount: 0` turns every stall into a failure instead of a re-run.
- **Shutdown.** `worker.close()` stops fetching and **waits for active jobs with no timeout**. `close(true)` waits for nothing, stops lock renewal and drops the connections. Its in-flight jobs keep running in memory but cannot be completed, so they come back as stalled after ~30–60 s. On SIGTERM, `next start` closes its HTTP server and then calls `process.exit(143)`, so an embedded worker is not awaited unless `NEXT_MANUAL_SIG_HANDLE` is set. DigitalOcean App Platform sends TERM and then KILL after `grace_period_seconds` (default 120, max 600). Delayed jobs live in Redis and are unaffected by a kill.
- **Topology.** Concurrency, global concurrency, rate limits, priority ordering, pause and the event stream are all **per queue**. A Worker consumes every job name in its queue and cannot filter by name. Keys are `<prefix>:<queue>:<suffix>`, with `prefix` defaulting to `bull`. Queue names cannot contain `:`.
- **Retention.** By default jobs are **kept forever**, both completed and failed. A job's own `removeOnComplete`/`removeOnFail` takes precedence over the Worker's. Age-based removal happens only when another job finishes. BullMQ requires `noeviction`. It only `console.warn`s when another policy is set. Under `volatile-*`, the only BullMQ keys with a TTL are locks and similar control keys, so eviction removes exactly the keys that prevent double runs.

## Connections

### What each class opens

`createRedisBackend` ([bullmq: src/utils/create-backend.ts#L38-L104](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/utils/create-backend.ts#L38-L104)):

| Class                          | Connections                                                                                                                                                                                     | Blocking command                                                                                                                                                   |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `Queue`, `FlowProducer`        | 1, **shared** with a passed instance and never closed by BullMQ                                                                                                                                 | none                                                                                                                                                               |
| `Worker`                       | main: shared with a passed instance (non-blocking); plus a **dedicated blocking** connection, always a duplicate, owned and closed by BullMQ, named `<prefix>:<base64(queue)>[:w:<workerName>]` | `BZPOPMIN`, timeout ≥ `drainDelay` (default 5 s) ([worker.ts#L300-L313](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/worker.ts#L300-L313))       |
| `QueueEvents`                  | 1, `hasBlockingConnection = true`, so a passed instance is **duplicated**                                                                                                                       | `XREAD BLOCK`, `blockingTimeout` 10 000 ms ([queue-events.ts#L300-L307](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/queue-events.ts#L300-L307)) |
| `JobScheduler` inside a Worker | reuses the Worker's backend ([worker.ts#L604-L619](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/worker.ts#L604-L619))                                                         | none                                                                                                                                                               |

One ioredis instance can therefore be passed to every class. BullMQ itself duplicates the instance wherever a blocking command would otherwise stall shared traffic ([docs: Connections](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/guide/connections.md)). Closing works like this: for a passed instance BullMQ calls `quit()` only on the duplicates it made, and the caller closes the original after every Queue and Worker is closed ([redis-connection.ts#L776-L816](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/redis-connection.ts#L776-L816)).

```ts
import IORedis from "ioredis";
import { Queue, Worker } from "bullmq";

const connection = new IORedis(url, { maxRetriesPerRequest: null });
const queue = new Queue("tasks", { connection });
const worker = new Worker("tasks", process, { connection });

await worker.close();
await queue.close();
await connection.quit();
```

### ioredis options

- **`maxRetriesPerRequest`.** When BullMQ builds a connection from options, it sets `null` **only if the connection is blocking** ([redis-connection.ts#L226-L240](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/redis-connection.ts#L226-L240); test [connection.test.ts#L99-L110](https://github.com/taskforcesh/bullmq/blob/a8ff126/tests/connection.test.ts#L99-L110)). The Worker's own command connection keeps whatever the caller gave, falling back to the ioredis default of 20 ([ioredis RedisOptions.ts#L231-L241, L392](https://github.com/redis/ioredis/blob/35cfa895726486f09badc0412091d24e38da57fd/lib/redis/RedisOptions.ts#L231-L241)). This contradicts the docs' "When instantiating a Worker this option will always be set to null" ([going-to-production.md](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/guide/going-to-production.md#maxretriesperrequest)): the source sets it only on the blocking duplicate. With an option object that sets a non-null value, the blocking connection logs `console.error` and overrides it. With an **instance** that has a non-null value, the blocking duplicate inherits it and BullMQ **throws** `BullMQ: Your redis options maxRetriesPerRequest must be null.` ([redis-connection.ts#L261, L297-L307](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/redis-connection.ts#L297-L307)).
- **`retryStrategy`.** The default for connections BullMQ builds is `max(min(exp(times), 20000), 1000)` ms ([redis-connection.ts#L232-L234](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/redis-connection.ts#L232-L234)).
- **`enableOfflineQueue`.** The ioredis default is `true` ([RedisOptions.ts#L377](https://github.com/redis/ioredis/blob/35cfa895726486f09badc0412091d24e38da57fd/lib/redis/RedisOptions.ts#L377)). With the offline queue on, `queue.add()` during an outage waits up to `maxRetriesPerRequest` reconnect attempts. With the defaults above that sums to roughly 4 minutes (20 attempts: 6 × 1 s, then 1.1 s, 3 s, 8.1 s, then 11 × 20 s; derived). BullMQ's documented fail-fast recipe for producers is `enableOfflineQueue: false`. The Redis server must still be reachable when the Queue is constructed ([failing-fast-when-redis-is-down.md](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/patterns/failing-fast-when-redis-is-down.md)).
- **`keyPrefix`** (ioredis) is rejected. Passing an instance that has it throws. Use BullMQ's `prefix` instead ([redis-connection.ts#L248-L252](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/redis-connection.ts#L248-L252)).
- **Which errors count as connection errors.** Worker retry loops treat only `ConnectionClosedError`, "Connection is closed" and `ECONNREFUSED` as connection errors. Anything else, including ioredis `MaxRetriesPerRequestError`, is emitted as `'error'` ([utils/index.ts#L260-L270](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/utils/index.ts#L260-L270)).
- **ioredis loading.** BullMQ loads ioredis lazily with `require('ioredis')` and throws a friendly error if it is missing. The bullmq package has no `exports` map (`main` → CJS, `module` → ESM), so Node resolves `bullmq` to the CJS build ([package.json](https://github.com/taskforcesh/bullmq/blob/a8ff126/package.json); [redis-connection.ts#L76-L99](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/redis-connection.ts#L76-L99)).
- **Direct `console` output.** BullMQ writes to `console.warn`/`console.error` itself for eviction-policy, version and option warnings ([redis-connection.ts#L302, L457, L876](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/redis-connection.ts#L874-L880)). Lifecycle errors go through the `'error'` event.

### Non-ioredis clients

In 6.x, a raw instance passed as `connection` is classified structurally ([redis-connection.ts#L118-L150](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/redis-connection.ts#L118-L150)):

- an object that already implements `IRedisClient` is used as is;
- `sendCommand` together with `isOpen`/`isReady`, and no `defineCommand`, is wrapped as **node-redis**, which requires `redis >= 5` ([connections.md](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/guide/connections.md#using-node-redis));
- `send` together with `connected` is wrapped as **Bun**, which only runs on the Bun runtime;
- **anything else falls through to the ioredis adapter**, which means a foreign client fails at its first ioredis-specific call (`defineCommand`, `duplicate`, …), not at construction. This is inferred from the fall-through branch. Valkey GLIDE must be wrapped explicitly with `createValkeyGlideClient`.

Retries and reconnects on non-ioredis clients are the caller's job: "configure that client's retry and reconnect behavior according to its own documentation" ([connections.md](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/guide/connections.md#maxretriesperrequest)). `RedisConnection.clientFactory` is a process-wide static. In BullMQ 5.x only ioredis worked.

### Idle connections on managed Valkey (~300 s)

- DigitalOcean managed Valkey: `valkey_timeout` is the "Valkey idle connection timeout in seconds", **default 300**, range 0–31 536 000 ([DO OpenAPI valkey_advanced_config.yml#L50-L56](https://github.com/digitalocean/openapi/blob/112b3762423ee6e853d7b0e9c1777fe89f493e21/specification/resources/databases/models/advanced_config/valkey_advanced_config.yml#L50-L56)).
- Valkey closes a client after `maxidletime` without interaction, **except blocked clients ("No timeout for BLPOP") and Pub/Sub clients** ([valkey timeout.c#L54-L67](https://github.com/valkey-io/valkey/blob/9.1.3/src/timeout.c#L54-L67)). `last_interaction` advances on commands, not on TCP keepalive probes, which never reach the application layer.
- What this means for BullMQ (derived from the defaults above):
  - Worker blocking connection: blocked, or re-issuing `BZPOPMIN` every ≤ 5 s, so it is never idle.
  - Worker command connection: the stalled check runs every 30 s while the worker runs, plus lock renewals while jobs are active, so it is never idle for 300 s.
  - QueueEvents: blocked in `XREAD`, so it is exempt.
  - **Queue (producer) connection: idle whenever nothing is dispatched for 300 s. Valkey closes it, and ioredis reconnects through `retryStrategy`.**
- ioredis TCP keepalive (`keepAlive`, the initial delay in ms) defaults to **30 000 in 6.0.0** and **0 in 5.11.1**. A value of 0 means `setKeepAlive(true, 0)`, which leaves the OS default in place, 7200 s on Linux ([ioredis 6 RedisOptions.ts#L56-L61, L348](https://github.com/redis/ioredis/blob/35cfa895726486f09badc0412091d24e38da57fd/lib/redis/RedisOptions.ts#L56-L61); [ioredis 5.11.1 RedisOptions.ts#L240](https://github.com/redis/ioredis/blob/v5.11.1/lib/redis/RedisOptions.ts); [Node `socket.setKeepAlive`](https://nodejs.org/api/net.html#socketsetkeepaliveenable-initialdelay); [Linux `tcp_keepalive_time`](https://www.kernel.org/doc/html/latest/networking/ip-sysctl.html)). Keepalive helps detect silently dead sockets, for example behind NAT. It does not prevent the server-side idle close. Neither ioredis nor BullMQ has a periodic application-level PING option (searched both sources).

```ts
const producer = new IORedis(url, {
  enableOfflineQueue: false,
  keepAlive: 30_000,
});
setInterval(() => producer.ping().catch(() => {}), 240_000).unref();
```

### Valkey compatibility

- BullMQ's CI runs the full Node test suite against `valkey/valkey:8` with ioredis (job `node-valkey`), plus a GLIDE job ([.github/workflows/test.yml#L285-L320](https://github.com/taskforcesh/bullmq/blob/a8ff126/.github/workflows/test.yml#L285-L320)).
- Version detection reads `valkey_version` from `INFO` and sets `databaseType = 'valkey'`. The minimum-version check (Redis ≥ 5.0.0, 6.2.0 recommended) is skipped for Valkey because it compares only like-for-like database types ([redis-connection.ts#L842-L870](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/redis-connection.ts#L842-L870); [utils/index.ts#L303-L315](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/utils/index.ts#L303-L315)).
- Cluster mode needs every key of a queue in one hash slot: put the `prefix` (or the queue name) in `{…}` ([patterns/redis-cluster.md](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/patterns/redis-cluster.md)).

## Schedulers

### Model

- `queue.upsertJobScheduler(id, { pattern | every, tz?, startDate?, endDate?, limit?, immediately? }, { name?, data?, opts? })` ([queue.ts#L473-L500](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/queue.ts#L473-L500)). A scheduler always has exactly one pending occurrence, normally in `delayed`. The **next occurrence is created when the current one moves to active**: the worker calls `upsertJobScheduler(..., { override: false, producerId: job.id })` ([worker.ts#L985-L1010](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/worker.ts#L985-L1010); [docs](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/guide/job-schedulers/README.md)).
- Occurrence job IDs are `repeat:<schedulerId>:<millis>`, and custom job IDs are not allowed ([job-scheduler.ts#L513](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/job-scheduler.ts#L513)).
- For `pattern`, the client computes `nextMillis` from **its own `Date.now()`** with cron-parser (`interval.next()`, strictly after now). For `every`, it is computed inside Lua from the server-provided timestamp ([job-scheduler.ts#L180-L200](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/job-scheduler.ts#L180-L200); [L517-L540](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/job-scheduler.ts#L517-L540)).

### N instances all declaring the same scheduler

`addJobScheduler-11.lua` runs atomically ([source](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/commands/addJobScheduler-11.lua)). In order, it:

1. reads `prevMillis` from the `repeat` ZSET;
2. removes the previous occurrence `repeat:<id>:<prevMillis>` **if it is still in delayed, prioritized, wait or paused**. It never removes an active job;
3. for `every` with an unchanged interval, keeps `prevMillis`, so repeated upserts do not shift the schedule;
4. stores the scheduler hash and adds the new occurrence. If that job ID already exists, `every` walks forward up to 32 slots and `pattern` returns `-10` ("Cannot create job scheduler iteration - job ID already exists") ([L117-L200](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/commands/addJobScheduler-11.lua#L117-L200); [finished-errors.ts#L63-L71](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/finished-errors.ts#L63-L71)).

The next occurrence is produced by `updateJobScheduler-12.lua`. It only acts if `producerId == "repeat:<id>:<prevMillis>"`, so only the current occurrence can produce the next one. If the next job already exists, it emits a `duplicated` event instead of adding one ([L84-L112](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/commands/updateJobScheduler-12.lua#L84-L112)). BullMQ's stress tests cover "upsert many times respecting the guarantees" and "rapid successive upserts without creating duplicate schedulers" ([job_scheduler_stress.test.ts#L56, L350](https://github.com/taskforcesh/bullmq/blob/a8ff126/tests/job_scheduler_stress.test.ts)).

**Result:** concurrent identical upserts from N instances leave one pending occurrence, and each occurrence is produced at most once. Edge cases, inferred from the scripts above:

- **Dropped occurrence (`pattern`).** An occurrence can be due but not yet picked up: still in `delayed` because no worker promoted it, or sitting in `wait` behind busy workers. If an upsert runs at that moment, typically at boot during a deploy, step 2 deletes it and step 4 schedules the next cron time after `now`. That occurrence never runs. `every` keeps `prevMillis` and is not affected.
- **Duplicate with `immediately: true`.** Each upsert computes `nextMillis = Date.now()`, so every instance's boot-time upsert creates a fresh, distinct job ID. If the previous one has already gone active, it is not removed, and a second run happens.
- **Clock skew (`pattern`).** An instance whose clock is behind can compute a `nextMillis` whose job already completed or is active, and the upsert then throws `-10`.
- **Missed occurrences are not caught up.** After downtime, the one pending occurrence runs late, and the next is computed from `max(prevMillis, now)` ([job-scheduler.ts#L186-L188](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/job-scheduler.ts#L186-L188)).
- **Production rate.** "The scheduler will only generate new jobs when the last job begins processing." Busy workers therefore lower the frequency ([docs](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/guide/job-schedulers/README.md)).
- **Stalls.** Scheduler occurrences are **never failed for stalling**. They are moved back to wait however many times they stall ([moveStalledJobsToWait-9.lua#L78-L99](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/commands/moveStalledJobsToWait-9.lua#L78-L99)).

### A schedule removed from code

Nothing in BullMQ reconciles schedulers against code. A scheduler lives in `<prefix>:<queue>:repeat` (ZSET) and `…:repeat:<id>` (hash) until `queue.removeJobScheduler(id)` is called. That call deletes the pending delayed occurrence and the scheduler, and returns `false` if the scheduler was missing ([removeJobScheduler-3.lua](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/commands/removeJobScheduler-3.lua); [manage-job-schedulers.md](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/guide/job-schedulers/manage-job-schedulers.md)).

An orphan keeps producing occurrences, because the next one is created when the current one goes active, whatever the processor then does with it. A library has to list what exists (`getJobSchedulers(start, end, asc)`, `getJobSchedulersCount()`), compare it with what is declared, and remove the difference ([queue.ts#L598-L640](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/queue.ts#L598-L640)).

```ts
const declared = new Set(["daily-report", "cleanup"]);
for (const s of await queue.getJobSchedulers(0, -1)) {
  if (!declared.has(s.key)) await queue.removeJobScheduler(s.key);
}
```

A removed scheduler's **active** occurrence still finishes. Its `updateJobScheduler` call is then a no-op, because the scheduler no longer exists.

## Stalled jobs

### Lock and renewal

- The defaults are `lockDuration` 30 000 ms, `lockRenewTime = lockDuration / 2`, `stalledInterval` 30 000 ms and `maxStalledCount` 1 ([worker.ts#L300-L345](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/worker.ts#L300-L345); [worker-options.ts](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/interfaces/worker-options.ts)).
- On pickup, `moveToActive` sets `<jobKey>:lock = token PX lockDuration` and increments `ats` (attemptsStarted) ([prepareJobForProcessing.lua](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/commands/includes/prepareJobForProcessing.lua)).
- The `LockManager` timer fires every `lockRenewTime / 2` (7.5 s) and extends the locks of jobs not renewed in the last 7.5 s, so each lock is extended every 7.5–15 s. It does this in one `extendLocks` script, which checks the token, re-sets the TTL and `SREM`s the job from the `stalled` set ([lock-manager.ts#L97-L125](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/lock-manager.ts#L97-L125); [extendLocks-1.lua](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/commands/extendLocks-1.lua)).
- **When renewal fails, the job is not stopped.** The manager emits `lockRenewalFailed(jobIds)` and an `'error'` for each job, and that is all ([lock-manager.ts#L67-L79](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/lock-manager.ts#L67-L79)). `worker.cancelJob(id, reason)` aborts a job's `AbortSignal`, but only if the processor function **declares 3 parameters** (`processor.length >= 3`) ([worker.ts#L355, L576-L588](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/worker.ts#L355); [cancelling-jobs.md](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/guide/workers/cancelling-jobs.md)).

### Stall detection

Every worker runs a stalled-check loop every `stalledInterval`. The script takes a queue-wide guard key `stalled-check` with `PX stalledInterval`, so the check runs **at most once per interval for the whole queue**, whatever the number of instances ([worker.ts#L1441-L1555](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/worker.ts#L1441-L1555); [moveStalledJobsToWait-9.lua#L44-L118](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/commands/moveStalledJobsToWait-9.lua#L44-L118)). Each check:

1. takes every job in the `stalled` set that **also has no `:lock` key** and removes it from `active`;
2. increments `stc`. If `stc > maxStalledCount` and the job is not a scheduler occurrence, it sets `defa = "job stalled more than allowable limit"`;
3. pushes the job back to `wait` and emits `stalled`;
4. adds every job currently in `active` to the `stalled` set, ready for the next check. Successful renewals remove them again.

A job with `defa` set is failed with an `UnrecoverableError` when it is next picked up, without running ([worker.ts#L1072-L1090, L1157-L1169](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/worker.ts#L1157-L1169)). There is no "stalled" state, only the event ([jobs/stalled.md](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/guide/jobs/stalled.md)). `maxStartedAttempts` is a separate cap on `attemptsStarted`, with no default.

### What causes a stall

- **Process crash, SIGKILL or `close(true)`.** Renewals stop. The lock expires 15–30 s after the last renewal, and the next check that finds the job flagged and unlocked re-queues it. That is roughly 30–60 s after death with the defaults, and only if some worker on the queue is running (derived). The docs say "about 30 seconds" ([going-to-production.md](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/guide/going-to-production.md#gracefully-shut-down-workers)).
- **Blocked event loop.** The renewal timer cannot fire. If the block outlasts the remaining lock TTL (15–30 s with the defaults), the lock expires ([workers/stalled-jobs.md](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/guide/workers/stalled-jobs.md)). An embedded worker shares its event loop with Next.js request rendering. This is an inference: heavy synchronous work in either one delays the other's timers.
- **Lost connection to Redis for longer than the lock TTL.** Renewals cannot reach the server, and other instances can still run the check.
- **Eviction of the `:lock` key** under a `volatile-*` policy (see [Retention](#retention)).

### When this causes a second run while the first is alive

Whenever the lock expires but the process survives: a blocked loop, a network partition, an evicted lock, or `close(true)` followed by the process staying up.

- Another worker picks up the re-queued job and runs it **concurrently** with the first.
- When the first finishes, `moveToFinished` checks the token. It returns `-2` ("Missing lock for job …") or `-6` (token mismatch). The first run's result is discarded and an `'error'` is emitted, but its side effects have already happened ([removeLock.lua](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/commands/includes/removeLock.lua); [finished-errors.ts#L29-L45](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/finished-errors.ts#L29-L45)).

The knobs BullMQ exposes:

- a larger `lockDuration`, which widens the margin but slows crash recovery;
- `maxStalledCount: 0`, so the first stall fails the job instead of re-running it, at the cost of failing the jobs of crashed workers too;
- `lockRenewalFailed` plus `cancelJob`, to abort the zombie run cooperatively;
- sandboxed processors (a separate process or thread), which keep CPU work off the renewal loop ([workers/sandboxed-processors.md](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/guide/workers/sandboxed-processors.md)).

## Shutdown

### `worker.close(force = false)`

From [worker.ts#L1380-L1424](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/worker.ts#L1380-L1424):

- `close()` sets `closing`, which stops new fetches. It then waits for the main loop and every active job (`whenCurrentJobsFinished`), with **no built-in timeout** ("TypeScript … wait for active jobs to finish without a built-in timeout", [graceful-shutdown.md](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/guide/workers/graceful-shutdown.md)). After that it closes the lock manager, the child pool and the connections, and emits `closed`.
- `close(true)` skips the wait, stops lock renewal and disconnects. Jobs still running in memory cannot complete or fail, because `handleCompleted`/`handleFailed` are skipped once `backend.closing` is set ([worker.ts#L1171-L1180, L1210](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/worker.ts#L1171-L1180)). Their locks are not released, so they come back as stalled. `close(true)` does not abort processors. `cancelAllJobs(reason)` is a separate call.
- Bounding the wait is up to the caller:

```ts
const graceful = worker.close();
const timeout = new Promise((r) => setTimeout(r, budgetMs));
await Promise.race([graceful, timeout]);
worker.cancelAllJobs("shutdown");
await worker.close(true);
```

### `next start` and signals

`next start` registers SIGINT/SIGTERM handlers **unless `NEXT_MANUAL_SIG_HANDLE` is set**. On the signal it calls `server.close()` and `nextServer.close()`, then `process.exit(143)` for SIGTERM. There is no production hook to await other work: `cleanupListeners` exists only in dev ([start-server.ts#L403-L492](https://github.com/vercel/next.js/blob/29bbeab09981148ade186e8599ecdede15abc281/packages/next/src/server/lib/start-server.ts#L403-L492)).

An embedded worker that registers its own handler alongside Next's races with that `process.exit`, so its active jobs are cut and later recovered as stalled (inferred). With `NEXT_MANUAL_SIG_HANDLE=true`, the app owns signal handling entirely. It has to be set in the start script, not in `.env`, and does not work in `next dev` ([self-hosting.mdx#L277-L306](https://github.com/vercel/next.js/blob/29bbeab09981148ade186e8599ecdede15abc281/docs/01-app/02-guides/self-hosting.mdx)).

### DigitalOcean App Platform

- `termination.grace_period_seconds`, for services, workers and jobs: "seconds to wait between sending a TERM signal to a container and issuing a KILL", **default 120, max 600** ([app_service_spec_termination.yml](https://github.com/digitalocean/openapi/blob/112b3762423ee6e853d7b0e9c1777fe89f493e21/specification/resources/apps/models/app_service_spec_termination.yml); [app_worker_spec_termination.yml](https://github.com/digitalocean/openapi/blob/112b3762423ee6e853d7b0e9c1777fe89f493e21/specification/resources/apps/models/app_worker_spec_termination.yml)).
- Services also have `drain_seconds`: the delay between selecting an instance for termination (when the load balancer starts draining it) and sending TERM. Default 15, max 110.

### What happens to each kind of job on a deploy kill

- **Active job finished within the grace period:** completes normally, provided `close()` was awaited before exit.
- **Active job still running at KILL (or at `process.exit`):** its lock lapses and it is re-queued by a new instance's stalled check, roughly 30–60 s later. It is re-run from the start: `attemptsMade` does not increase, `stc` does. With `maxStalledCount: 1`, a job that stalls a second time is failed. The docs: "the code above does not guarantee that the jobs will never end up being stalled, as the job may take longer time than the grace period" ([going-to-production.md](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/guide/going-to-production.md#gracefully-shut-down-workers)).
- **Delayed jobs:** these are entries in the `delayed` ZSET in Redis, untouched by a process dying. Due jobs are promoted by whichever worker next runs `moveToActive` (`promoteDelayedJobs`), so they are late only while no worker on the queue is running ([moveToActive-11.lua#L52-L59](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/commands/moveToActive-11.lua#L52-L59)).
- **Waiting jobs:** unaffected.
- **Scheduler occurrences:** see the dropped-occurrence edge case in [Schedulers](#schedulers). The new release's boot-time upsert can delete a due occurrence that the old release did not pick up.
- **Old and new release overlap.** For up to `grace_period_seconds`, both releases' workers consume the same queue. The old one stops fetching once `close()` is called. A job enqueued by new code can therefore be picked up by an old worker that is still fetching (inferred).

## Topology

- **Per-queue mechanisms.**
  - `concurrency` is per Worker instance ([workers/concurrency.md](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/guide/workers/concurrency.md)).
  - `setGlobalConcurrency(n)` and `setGlobalRateLimit(max, ms)` are queue-wide, across all workers ([global-concurrency.md](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/guide/queues/global-concurrency.md); [global-rate-limit.md](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/guide/queues/global-rate-limit.md)).
  - `priority` (1 … 2 097 151, lower runs first, unprioritized jobs run before prioritized ones) orders jobs within one queue only ([jobs/prioritized.md](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/guide/jobs/prioritized.md)).
  - Pause, `drain`, `obliterate`, job counts and the event stream `<prefix>:<queue>:events` (approximately trimmed to `streams.events.maxLen`, default 10 000) are all per queue ([queue.ts#L247](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/queue.ts#L247)).
- **A Worker takes every job in its queue.** There is no name filter in `WorkerOptions`. BullMQ's documented pattern for several job types in one queue is a `switch (job.name)` inside a single processor ([patterns/named-processor.md](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/patterns/named-processor.md)).
- **One queue for everything** therefore means:
  - every worker process must be able to run every task;
  - a slow or flooding task shares concurrency slots and the rate limit with all others;
  - priority works across task types;
  - there is one stream to watch and one Worker per process, which is 2 connections.
- **One queue per task** means:
  - isolated concurrency, rate limits and pause per task, but no priority across tasks;
  - one Worker per queue per process, which is 2 connections each. N task types × M instances gives 2·N·M worker connections plus producers and QueueEvents;
  - one stalled-check loop and one blocking `BZPOPMIN` per queue per process.
- **Keys and prefixes.** Keys are `<prefix>:<queue>:<suffix>`, with `prefix` defaulting to `bull` ([queue-keys.ts](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/queue-keys.ts)). Queue names cannot contain `:` ([queue-base.ts#L90-L96](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/queue-base.ts#L90-L96)). The prefix lets several apps or environments share one Redis database. In cluster mode it must be a `{hash-tag}`, and BullMQ recommends different prefixes for different queues so they spread across nodes ([patterns/redis-cluster.md](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/patterns/redis-cluster.md)).
- **Inspection tools work per queue.** Bull Board registers each queue explicitly (`queues: [new BullMQAdapter(queue)]`) ([bull-board README, v9.11.0](https://github.com/felixmosh/bull-board/blob/v9.11.0/README.md)). A single queue shows up as one list filtered by job name. Per-task queues show up as separate entries with their own counts and pause controls.

## Retention

- **Default: keep everything.** If neither the job nor the Worker sets `removeOnComplete`/`removeOnFail`, the job is kept forever (`{ count: -1 }`). The job's own option (job opts or `defaultJobOptions`) takes precedence over the Worker's ([redis-queue-backend.ts#L1230-L1243](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/redis-queue-backend.ts#L1230-L1243); [queues/auto-removal-of-jobs.md](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/guide/queues/auto-removal-of-jobs.md)).
- **Accepted values.** `true` (remove), a number (keep the last N), or `{ age (s), count?, limit? }`. Removal runs inside `moveToFinished`, **only when another job reaches the same final state**. There is no background timer ([types/keep-jobs.ts](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/types/keep-jobs.ts)).
- **What a finished job keeps in memory.** Its hash (data, opts, return value or stack trace), its entry in the `completed`/`failed` ZSET, and logs and metrics keys if used. The events stream holds about 10 000 entries per queue.
- **`maxmemory-policy`.** BullMQ needs `noeviction`: it "is the **only** setting that guarantees the correct behavior of the queues" ([going-to-production.md](https://github.com/taskforcesh/bullmq/blob/a8ff126/docs/gitbook/guide/going-to-production.md#max-memory-policy)). With any other policy it only prints `IMPORTANT! Eviction policy is <p>. It should be "noeviction"` through `console.warn` when it connects ([redis-connection.ts#L874-L880](https://github.com/taskforcesh/bullmq/blob/a8ff126/src/classes/redis-connection.ts#L874-L880)).
- **The same Valkey also used as a cache.** Policy semantics are from [Valkey: key eviction](https://valkey.io/topics/lru-cache/). The BullMQ keys with a TTL are `:lock` (`PX lockDuration`), `stalled-check`, the rate-limiter key and deduplication keys that have a `ttl`.

  | Policy       | Effect on BullMQ                                                                                                                                                                                                           | Effect on the cache                     |
  | ------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------- |
  | `noeviction` | Correct. At `maxmemory`, writes are rejected, so `add`, `moveToActive` and `moveToFinished` start failing, along with every other write                                                                                    | cache writes fail at the limit too      |
  | `volatile-*` | Only TTL keys are candidates, which for BullMQ are mainly **`:lock` keys**. An evicted lock makes a live job look stalled, which leads to a **second concurrent run**. Behaves like `noeviction` when no TTL keys are left | cache keys with a TTL are evicted first |
  | `allkeys-*`  | Any key, including job hashes, `wait`/`active` lists and scheduler hashes, so data loss and corrupted queue state                                                                                                          | normal LRU/LFU behaviour                |

- **DigitalOcean managed Valkey.** The default policy is `noeviction`, and DO marks `allkeys-lru` as "Recommended" ([DO: choose eviction policies](https://docs.digitalocean.com/products/databases/valkey/how-to/choose-eviction-policies/)). The policy is set per cluster ([DO OpenAPI eviction_policy_model.yml](https://github.com/digitalocean/openapi/blob/112b3762423ee6e853d7b0e9c1777fe89f493e21/specification/resources/databases/models/eviction_policy_model.yml)). One cluster cannot apply `noeviction` to BullMQ keys and LRU to cache keys at the same time: `maxmemory-policy` is a server-wide setting, and logical databases share it.
