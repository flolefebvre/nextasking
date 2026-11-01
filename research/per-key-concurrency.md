# Per-key concurrency on OSS BullMQ

Research for issue #3. Question: how to serialise jobs per key on open-source BullMQ, so that two jobs with the same concurrency key never run at the same time and the second one waits instead of being dropped. Also covers a per-key limit above 1. Options and trade-offs only; no design is picked.

Versions read (2026-11-01):

- BullMQ `6.5.3`, commit [`a8ff126`][bq] (`B/` below means `https://github.com/taskforcesh/bullmq/blob/a8ff126…/`).
- groupmq `1.1.0` (npm `latest`), commit [`6439d4e`][gmq]. A `2.0.0-next.7` pre-release exists on npm's `next` tag; its README makes the same one-job-per-group claim.
- Inngest docs: the [concurrency guide][inn-guide] and the [concurrency reference][inn-ref], as published on the date above.
- Redis docs: [distributed locks][redis-lock], [`LMOVE`][redis-lmove].

## Summary

- OSS BullMQ has no per-key wait. Deduplication drops the second job (simple, throttle) or collapses it into the latest one (`keepLastIfActive`). Per-group concurrency is Pro-only, and the OSS group-key limiter was removed in 3.0 because the maintainers judged it not solid enough to fix ([issue 1322][bq-1322]).
- The only per-scope limit OSS BullMQ enforces in Redis is per queue: `queue.setGlobalConcurrency(n)` (`isQueueMaxed` compares `LLEN active` with the `concurrency` field of the queue meta hash). Hence "one queue per key" is the only option that uses a native BullMQ limit. It needs one `Worker`, so one blocking Redis connection, per key per instance.
- Everything else adds a key lease (a Redis lock or counting semaphore) beside BullMQ, then has to decide what a job does when its key is busy: wait inside the handler (holds a worker slot), re-delay itself (polling), or park and be woken by the holder (a userland group implementation on BullMQ primitives).
- BullMQ list mechanics matter for ordering. New jobs are `LPUSH`ed and workers `RPOPLPUSH` from the other end (FIFO). A job moved back with `WaitingError` is `RPUSH`ed to the front, so the same worker picks it up again at once. Delayed jobs and immediate retries go to the back. So "re-delay" and "move back to wait" break per-key FIFO, and an immediate retry breaks FIFO even inside a queue-per-key.
- A lost BullMQ job lock does not abort the processor (the worker only emits `lockRenewalFailed` and `error`). A stalled job can therefore run twice at once in BullMQ itself, whatever lease is added. Any key lease shares the same failure class: TTL expiry while the holder still runs breaks mutual exclusion unless the handler is aborted or the downstream writes are fenced.
- groupmq (MIT) is a separate Redis queue inspired by BullMQ, not a plugin. It gives strict per-group FIFO with a hard limit of 1 per group, and a retry with backoff blocks its group. Inngest's `concurrency: { key, limit }` is the reference semantics: excess work is queued, not dropped; FIFO within a key is best-effort, with retries as an exception; one key's backlog should not block other keys; the limit counts executing steps, not runs.

## Facts the options depend on

### BullMQ 6.5.3

| Fact                                                                                                                                                                                                                                                        | Source                                                                                                                                    |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| Standard add pushes with `LPUSH` (or `RPUSH` if `lifo`).                                                                                                                                                                                                    | `B/src/commands/addStandardJob-9.lua` L115-116                                                                                            |
| Workers take the next job with `RPOPLPUSH wait active`, then fall back to the prioritized set.                                                                                                                                                              | `B/src/commands/moveToActive-11.lua` L74-91                                                                                               |
| `job.moveToWait(token)` + `WaitingError` puts the job back with `RPUSH`, i.e. at the end workers pop from: it is the next job taken.                                                                                                                        | `B/src/commands/moveJobFromActiveToWait-9.lua` L51; `B/src/classes/job.ts` L698                                                           |
| Promoted delayed jobs are `LPUSH`ed (back of the line). Delayed scores are `timestamp * 0x1000 + counter`.                                                                                                                                                  | `B/src/commands/includes/promoteDelayedJobs.lua` L16-30                                                                                   |
| A retry with a backoff moves the job to `delayed`; a retry without one goes through `retryJob` with `LPUSH` (back) unless `lifo`.                                                                                                                           | `B/src/classes/job.ts` L774-796; `B/src/classes/redis-queue-backend.ts` L1898                                                             |
| `job.moveToDelayed(ts, token)` + `throw new DelayedError()` re-delays an active job with `skipAttempt: true`, so `attemptsMade` does not grow; `attemptsStarted` (`ats`) grows on every pickup, and `maxStartedAttempts` fails the job once it is exceeded. | `B/src/classes/job.ts` L1189-1199; `B/src/commands/includes/prepareJobForProcessing.lua` L45; `B/src/interfaces/worker-options.ts` L58-65 |
| The worker treats `DelayedError`, `WaitingError` and `WaitingChildrenError` as "moved", neither completed nor failed, and fetches the next job.                                                                                                             | `B/src/classes/worker.ts` L1217-1231                                                                                                      |
| `queue.setGlobalConcurrency(n)` is stored in Redis and caps active jobs per queue across all workers.                                                                                                                                                       | `B/docs/gitbook/guide/queues/global-concurrency.md`; `B/src/commands/includes/isQueueMaxed.lua`                                           |
| Every `Worker` duplicates its connection for blocking commands.                                                                                                                                                                                             | `B/docs/gitbook/guide/connections.md` L9; `B/src/classes/worker.ts` L312                                                                  |
| Defaults: `lockDuration` 30 s, `lockRenewTime` = `lockDuration / 2`, `stalledInterval` 30 s, `maxStalledCount` 1.                                                                                                                                           | `B/src/classes/worker.ts` L305-345; `B/docs/gitbook/guide/jobs/stalled.md`                                                                |
| A failed lock renewal only emits `lockRenewalFailed` and `error`; the processor keeps running. An `AbortSignal` is passed to the processor, but it is fired by `worker.cancelJob`, not by lock loss.                                                        | `B/src/classes/lock-manager.ts` L63-77; `B/docs/gitbook/guide/workers/cancelling-jobs.md`                                                 |
| Deduplication: simple mode ignores the new job while the first is unfinished; throttle ignores it for a TTL; debounce replaces it. `keepLastIfActive` keeps at most 1 active + 1 waiting job per id, the waiting one carrying the latest data only.         | `B/docs/gitbook/guide/jobs/deduplication.md`; `B/src/commands/includes/requeueDeduplicatedJob.lua`                                        |
| With `keepLastIfActive`, retries of the active job keep the stored next data; the next job is created once the active job completes or exhausts its retries.                                                                                                | `B/docs/gitbook/guide/jobs/deduplication.md` ("Keep Last If Active Mode")                                                                 |
| Manual rate limiting (`worker.rateLimit` + `RateLimitError`) is queue-wide, not per key. Group-key limiting was removed in 3.0.                                                                                                                             | `B/docs/gitbook/guide/rate-limiting.md`                                                                                                   |
| Pro groups (paid, out of scope): round-robin across groups, `group.concurrency` global across workers, per-group override with `setGroupConcurrency`.                                                                                                       | `B/docs/gitbook/bullmq-pro/groups/README.md`, `concurrency.md`, `local-group-concurrency.md`                                              |
| In Redis Cluster, a queue's keys share a hash slot through a `{…}` prefix, so a lease key on another slot cannot be touched in the same Lua script as BullMQ keys.                                                                                          | `B/docs/gitbook/patterns/redis-cluster.md`                                                                                                |

### Redis leases

- Single-instance lock: `SET key token NX PX ttl`; release with compare-and-delete (`DELEX key IFEQ token` on Redis 8.4+, or a `GET`/`DEL` Lua script on older Redis and Valkey). Mutual exclusion only holds while the holder finishes within the validity time ([Redis docs][redis-lock]).
- Replication is asynchronous, so a failover can lose a lease and admit a second holder ([Redis docs][redis-lock], "Why failover-based implementations are not enough").
- The Redis docs ask for fencing tokens for long work, and warn that TTL expiry does not use a monotonic clock ([Redis docs][redis-lock], "Disclaimer about consistency").
- Reliable list consumption: `LMOVE` into a processing list, `LREM` after processing, and a sweeper for items stuck too long ([`LMOVE` docs][redis-lmove], "Pattern: reliable queue").

### Inngest reference semantics

From the [guide][inn-guide] and [reference][inn-ref]:

- `concurrency: { key, limit, scope }`. `key` is an expression evaluated against the triggering event; each value is its own virtual queue. Work keeps the key it was given when it was enqueued.
- Over the limit, steps are queued and start when capacity frees up; rate limiting is the tool for skipping.
- The limit counts executing steps, not runs. Sleeping, `waitForEvent` and `invoke` do not hold a slot.
- Within one function and key: best-effort FIFO, oldest first, with retries as an exception. Across keys there is no ordering guarantee; one key's backlog should not block other keys. Explicit key-queue fairness is an Enterprise feature.
- Scopes `fn` (default), `env`, `account`; at most two concurrency constraints per function, and a step starts only if it fits both.

## The options

Shared building block for A-C: a key lease. With a limit of 1 it is a lock; with a limit above 1 it is a counting semaphore. One sorted-set script covers both, using server time so client clocks do not matter:

```lua
-- KEYS[1] = lease zset for the key; ARGV = holder, ttlMs, limit
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
if redis.call('ZSCORE', KEYS[1], ARGV[1])
   or redis.call('ZCARD', KEYS[1]) < tonumber(ARGV[3]) then
  redis.call('ZADD', KEYS[1], now + tonumber(ARGV[2]), ARGV[1])
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0
```

Renewing is the same `ZADD` guarded by `ZSCORE`; releasing is `ZREM`. The holder id is a design choice. Using the BullMQ `job.id` makes the lease re-entrant: a stalled job picked up again takes its own lease back, and may run alongside its zombie. Using the worker `token` makes the re-run wait until the zombie's lease expires.

### A. Lease around the handler, wait inside the handler

The worker takes the job, loops on `acquire` with a sleep, then runs the handler and releases in `finally`.

```ts
new Worker(queue, async (job, token, signal) => {
  while (!(await acquire(keyOf(job), job.id, ttlMs, limit))) await sleep(pollMs);
  const renew = setInterval(() => renewLease(keyOf(job), job.id, ttlMs), ttlMs / 2);
  try {
    return await handle(job, signal);
  } finally {
    clearInterval(renew);
    await release(keyOf(job), job.id);
  }
});
```

- Ordering: none between waiters; whoever polls first after a release wins.
- Fairness and throughput: each waiting job holds a worker slot. A burst on one key can fill every slot of every instance and stall all other keys (head-of-line blocking at the worker level).
- N instances: correct, since the lease is in Redis.
- Retries and backoff: the lease is released before BullMQ schedules the retry, so another job of the key can run first.
- Failure modes: a crashed holder blocks the key until the lease TTL; BullMQ moves its job back after the stall check. Lease expiry while still running admits a second holder. Waiting jobs renew their BullMQ lock normally, so they do not stall, but they burn `lockDuration`-long slots.
- Limit above 1: native to the semaphore.

### B. Lease, and re-delay the job when its key is busy

On a busy key the job moves itself to `delayed` and frees the slot.

```ts
new Worker(queue, async (job, token) => {
  if (!(await acquire(keyOf(job), job.id, ttlMs, limit))) {
    await job.moveToDelayed(Date.now() + backoffWithJitter(job), token);
    throw new DelayedError();
  }
  try {
    return await handle(job);
  } finally {
    await release(keyOf(job), job.id);
  }
});
```

- Ordering: lost. A re-delayed job returns at the back of the wait list, and a newer job of the same key can take the lease first. The `WaitingError` variant is worse: the job is `RPUSH`ed to the front and taken again at once, a hot loop.
- Fairness and throughput: polling. A hot key with M waiting jobs costs about M pickups per delay period, each one a fetch, a lock, a script call and a move. Starvation of one job is possible in theory.
- Attempts: re-delays do not count as attempts (`skipAttempt`), but each pickup increments `attemptsStarted`, so `maxStartedAttempts`, if set, fails a job that waited too often.
- N instances: correct.
- Retries and backoff: as in A.
- Failure modes: as in A, plus latency of up to one delay period after a release.
- Limit above 1: native to the semaphore.

### C. Lease, park busy jobs, and let the holder wake the next one

This is a userland group on BullMQ primitives. A busy job records itself in a per-key FIFO and moves to `delayed` with a long safety delay. On release, the holder promotes the head of that FIFO (`job.promote()`), so no polling is needed.

```ts
new Worker(queue, async (job, token) => {
  // Lua on own keys: take a slot if free and (parked is empty or job is its head); otherwise RPUSH parked (once)
  if (!(await acquireOrPark(keyOf(job), job.id, ttlMs, limit))) {
    await job.moveToDelayed(Date.now() + safetyMs, token);
    throw new DelayedError();
  }
  try {
    return await handle(job);
  } finally {
    const next = await releaseAndPeekHead(keyOf(job), job.id); // Lua: ZREM lease; LINDEX parked 0
    if (next) await (await Job.fromId(queue, next))?.promote().catch(() => {});
  }
});
```

- Ordering: FIFO per key in order of first pickup, which follows enqueue order except under concurrency races. The "or job is its head" check stops a new arrival from cutting in front of parked jobs.
- Fairness and throughput: a busy-key job costs one pickup and one park, then sits in `delayed` without consuming slots. Other keys flow at queue speed, but there is no round-robin: the wait list stays one FIFO across keys.
- N instances: correct; the lease and the park list are in Redis.
- Retries and backoff: a choice. Releasing on failure lets the next parked job run during the retry backoff (FIFO broken across retries, as Inngest accepts). Keeping the lease through the backoff, with lease TTL ≥ backoff, keeps strict order but blocks the key (groupmq's behaviour).
- Failure modes:
  - The wake is not atomic with BullMQ state: lease and park keys sit beside BullMQ's keys, and in Cluster on another slot. A wake can hit a job that is still active and not yet delayed, so `promote` fails. The safety delay bounds the stall to `safetyMs`.
  - A crashed holder never wakes anyone: the key frees at lease TTL, and the head re-checks at `safetyMs`.
  - Lease expiry while still running: as in A.
  - A parked job removed by hand leaves a dead head, so the script must skip ids whose job no longer exists.
- Limit above 1: the semaphore admits up to `limit`; a release wakes one head.
- Cost: this is the most code of A-E, against undocumented interplay of `moveToDelayed`/`promote` timing; the list-side facts above are from BullMQ's Lua, not from a documented contract.

### D. One queue per key, with `setGlobalConcurrency(limit)`

Each key gets its own BullMQ queue (for example `name:key`), with `setGlobalConcurrency(limit)` and its own `Worker` on every instance.

```ts
const q = new Queue(`charge:${customerId}`, { connection });
await q.setGlobalConcurrency(1);
await q.add("charge", payload);
// on every instance, for every known key:
new Worker(`charge:${customerId}`, processor, { connection, concurrency: 1 });
```

- Ordering: FIFO within the key while nothing fails. An immediate retry is `LPUSH`ed behind waiting jobs, and a retry with backoff leaves `active`, which frees the slot, so later jobs of the key run before the retry.
- Fairness: each key has its own workers, so a busy key never blocks another; there is no global cap unless one is added.
- Throughput and cost: one `Worker`, hence one blocking connection, per key per instance. Keys must be known to every instance, so dynamic keys (per user, per tenant) need discovery and teardown. That fits a small, fixed set of keys, not an unbounded one. BullMQ docs advise distinct prefixes for many queues in Cluster.
- N instances: native; the limit is in the queue meta hash.
- Failure modes: BullMQ's own stall handling; there is no extra lease to expire. The double-run window after a lost lock remains.
- Limit above 1: native.

### E. Per-key mailbox and one runner job per key

Dispatch appends the payload to a per-key Redis list and adds a runner job with `deduplication: { id: key, keepLastIfActive: true }`. The runner drains the list with `LMOVE` into a processing list (the reliable-queue pattern). `keepLastIfActive` guarantees a fresh runner after the active one finishes if anything was added meanwhile, and allows at most one runner per key.

```ts
await redis.rpush(`mbox:${key}`, JSON.stringify(payload));
await runners.add("drain", { key }, { deduplication: { id: key, keepLastIfActive: true } });

new Worker("runners", async (job) => {
  let item;
  while ((item = await redis.lmove(`mbox:${job.data.key}`, `mbox:${job.data.key}:wip`, "LEFT", "RIGHT"))) {
    await handle(JSON.parse(item));
    await redis.lrem(`mbox:${job.data.key}:wip`, 1, item);
  }
});
```

- Ordering: strict FIFO per key.
- Fairness and throughput: one slot per busy key; a long mailbox keeps its runner busy but takes no other slots. Many busy keys means many runners, bounded by worker concurrency.
- N instances: native through deduplication, and the mailbox is in Redis.
- Retries and backoff: per item, in userland. BullMQ retries the runner, not the item. The item's attempts, backoff, failed state, dashboards and events have to be rebuilt, which is most of what BullMQ gives per job.
- Failure modes: a runner crash leaves items in the processing list, so a sweeper must re-queue them. Runner stalls behave as in BullMQ.
- Limit above 1: not covered; deduplication allows exactly one runner per key.

### F. A group queue beside BullMQ, or BullMQ Pro

- groupmq 1.1.0 (MIT) is its own Redis data model: a per-group sorted set of jobs, a `ready` sorted set of groups scored by their head job, and a per-group active list. `reserve.lua` scans the first `reserveScanLimit` (default 20) ready groups and takes a group with no active job. Selection across groups is oldest head first, not round-robin. Ordering inside a group is strict, scored by `orderMs` and a sequence number, with optional reordering windows. The limit is fixed at 1 per group. A failing job is re-added to its group; with backoff it sits in `delayed` and "delayed jobs block their group". Stalled jobs are recovered when their `processing` deadline passes. ([README][gmq-readme], [`retry.lua`][gmq-retry], [`reserve.lua`][gmq-reserve])
- groupmq is not BullMQ: it has its own `Queue`/`Worker` API, no named jobs, and does not work with BullMQ dashboards or flows. Using it means a second queue engine next to BullMQ, against the map's "BullMQ only, no adapter layer" scope.
- BullMQ Pro groups do all of this, with round-robin and a per-group limit above 1, but Pro is out of scope.

## Comparison

| Option                | Per-key FIFO                           | Busy keys cost                               | Blocks other keys | N instances  | Limit > 1    | Extra moving parts                             |
| --------------------- | -------------------------------------- | -------------------------------------------- | ----------------- | ------------ | ------------ | ---------------------------------------------- |
| A. Wait in handler    | No                                     | A worker slot per waiting job                | Yes, under bursts | Yes          | Yes          | Lease and renewal                              |
| B. Re-delay           | No                                     | Polling: pickups per delay period            | No                | Yes          | Yes          | Lease and renewal                              |
| C. Park and wake      | Yes, except across retries if released | One pickup and one park per job              | No                | Yes          | Yes          | Lease, park list, wake path, safety delay      |
| D. Queue per key      | Yes, except across retries             | A worker and connection per key per instance | No                | Yes (native) | Yes (native) | Key discovery, worker lifecycle per key        |
| E. Mailbox and runner | Yes                                    | One runner slot per busy key                 | No                | Yes          | No           | Mailbox, sweeper, per-item retries in userland |
| F. groupmq            | Yes, strict                            | Native                                       | No                | Yes          | No           | A second queue engine                          |

## Cross-cutting points

- Lease TTL vs BullMQ `lockDuration`. When a holder crashes, BullMQ re-queues its job after the stall check, about `lockDuration` plus `stalledInterval`. A lease TTL much longer than that blocks the key after the job is back. A much shorter one lets the key go while a slow but alive holder still runs. Renewing the lease on the same timer as the job lock keeps them aligned.
- Lock loss is not an abort. Neither BullMQ nor a lease stops a handler that lost its lock. Closing that window means aborting the handler from the renewal failure (userland, through an `AbortSignal`), or passing a fencing token (a counter incremented on every grant) to the handler for downstream writes. Both are open design points for the effectively-once goal.
- Deploy overlap: the reference hosting keeps the old release up to about 10 minutes, then kills it. A killed holder's lease lives until its TTL, and both releases' workers compete for the same keys, so keys and lease formats must stay compatible across a deploy.
- Redis failover can drop a lease (asynchronous replication) and admit a second holder. This is the same class of risk as BullMQ's own locks on a replicated primary.
- Valkey: A-E use only `SET`, sorted sets, lists and Lua, which Valkey supports. `DELEX` is Redis 8.4-only, so compare-and-delete stays a Lua script.
- Limit above 1 and ordering: with a limit above 1, only start order can be FIFO; completion order cannot. Inngest promises best-effort FIFO only.
- Inngest counts executing steps, not runs. If nextasking adds steps with sleeps or waits (`step.waitForEvent` is on the map), matching Inngest means releasing the lease while a run is suspended and taking it again on resume. A lease held for the whole run gives different semantics.

## Open questions this surfaced

- Retries vs strict order: should a key stay blocked during a retry backoff (strict order) or let the next job run (Inngest-like, retries out of order)?
- Does the per-key slot count steps or whole runs once steps and `waitForEvent` exist?
- Is an unbounded, dynamic key space (per user, per tenant) required? It rules out D.
- How to close the lost-lock double-run window (handler abort on renewal failure, fencing token, or a documented window), since it applies to plain BullMQ jobs too.

[bq]: https://github.com/taskforcesh/bullmq/tree/a8ff12664de94f52a9bcc964c6129a4b23d00aa6
[bq-1322]: https://github.com/taskforcesh/bullmq/issues/1322
[gmq]: https://github.com/Openpanel-dev/groupmq/tree/6439d4e505b51925e143ebed38aef9f7fa12dc7e
[gmq-readme]: https://github.com/Openpanel-dev/groupmq/blob/6439d4e505b51925e143ebed38aef9f7fa12dc7e/README.md
[gmq-retry]: https://github.com/Openpanel-dev/groupmq/blob/6439d4e505b51925e143ebed38aef9f7fa12dc7e/src/lua/retry.lua
[gmq-reserve]: https://github.com/Openpanel-dev/groupmq/blob/6439d4e505b51925e143ebed38aef9f7fa12dc7e/src/lua/reserve.lua
[inn-guide]: https://www.inngest.com/docs/guides/concurrency
[inn-ref]: https://www.inngest.com/docs/functions/concurrency
[redis-lock]: https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/
[redis-lmove]: https://redis.io/docs/latest/commands/lmove/
