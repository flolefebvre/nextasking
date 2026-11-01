# Durable steps on OSS BullMQ

Research for issue #2. The question: how can Inngest-style durable steps (`step.run(id, fn)` memoized, `step.sleep` suspended and surviving deploys) be built on open-source BullMQ, and what does each option cost? This file reports options and trade-offs; it does not pick one.

Versions read (2026-11-01):

- BullMQ `6.5.3`, the latest on npm, source at commit `a8ff126` of `taskforcesh/bullmq`. Paths below are relative to that repo; `docs/` means `docs/gitbook/`.
- Inngest TypeScript SDK `inngest@4.26.2`, source at commit `204e05e` of `inngest/inngest-js` (`packages/inngest/src/`).
- Inngest docs: `inngest/website` `main` (`pages/docs/learn/*.mdx`) and inngest.com/docs pages, fetched 2026-11-01.

## Summary

- **Inngest's model is step memoization, not full replay.** Each step has a string ID, hashed with SHA-1, with a `:n` suffix added when an ID repeats. A run is re-entered from the top and completed steps return their stored result. Code between steps re-runs on every re-entry, so it must be deterministic. `step.sleep` also takes an ID: it is a memoized step. Retries are counted per step.
- **OSS BullMQ has every primitive a suspend/resume runner needs, but no step abstraction.** The official "process step jobs" pattern stores a cursor in `job.data` with `job.updateData`, and suspends with `moveToDelayed` + `DelayedError` or `moveToWaitingChildren` + `WaitingChildrenError`. Suspending does not increment `attemptsMade` and frees the worker slot.
- **Three places for step state fit:** (A) the job's own `data`, (B) a side Redis hash next to the job, (C) one child job per step, with results collected in the parent's `:processed` hash. A and B run steps back to back inside one active job, like Inngest v4's default checkpointing mode. C runs each step as its own job.
- **Three ways to sleep fit:** (S1) `moveToDelayed` + `DelayedError` on the run's own job, (S2) a delayed child job + `WaitingChildrenError`, (S3) a delayed continuation job. A sleeping run lives in Redis (a delayed ZSET or the waiting-children set), holds no worker, and survives any deploy that keeps Redis.
- **The main correctness gap is fencing.** `job.updateData` does not check the job lock. A worker that lost its lock (a stalled "zombie") can still write step state, and with option A it replaces the whole `data` blob. Completion and the move calls are lock-checked; `updateData` is not. Fencing means a custom Lua script that couples to BullMQ's undocumented key layout.
- **Deploys:** a job killed mid-step comes back as stalled after about 30–60 s by default. The stall counter `stc` is never reset, and with the default `maxStalledCount: 1` the second stall in a run's whole lifetime fails it. `attemptsStarted` also grows on every resume, so `maxStartedAttempts` counts sleeps.
- **Size:** BullMQ sets no payload limit. A Redis string is at most 512 MB. Practically, A rewrites and re-reads the whole state on every step and resume, and B and C write per step. Inngest's own limits are 4 MiB per step result, 32 MiB of run state and 1,000 steps per function.

## 1. How Inngest defines steps (the target semantics)

- **Execution model.** A run executes "step by step". Each completed step's result is persisted, and on the next execution "the SDK uses the hash of the step name to look up the result", injects it, and skips the step's code. Non-deterministic logic "must be placed within a `step.run()` call" ([how functions are executed](https://www.inngest.com/docs/learn/how-functions-are-executed), `learn/how-functions-are-executed.mdx`).
- **Step IDs.** "Each step in a function has a unique string identifier." The SDK "hashes the step's identifier along with a counter (enabling steps inside loops)" ([versioning](https://www.inngest.com/docs/learn/versioning)). In source, `hashId` is `sha1(id)` (`components/execution/engine.ts:3423`). A repeated ID gets the suffix `":" + n` (`STEP_INDEXING_SUFFIX`, `components/InngestStepTools.ts:305`), assigned by `resolveStepIdCollision` (`engine.ts:3464`). When the same ID shows up across parallel chains, the SDK warns `AUTOMATIC_PARALLEL_INDEXING` and keeps running (`engine.ts` ~2395–2415). Nesting a step inside a step is detected and warned about (`ErrCode.NESTING_STEPS`).
- **Determinism and evolution.** Determinism is "graceful": a new step runs when it is discovered, and a changed step order "logs a warning rather than failing". Changing the code inside a step but keeping its ID reuses the memoized result. Changing an ID forces the step to re-execute. A removed step's data is ignored. For incompatible rewrites, Inngest recommends a new function plus event routing ([versioning](https://www.inngest.com/docs/learn/versioning)).
- **Interruption.** In the classic model each step is "a separate HTTP request": the SDK stops the handler after a new step and is re-invoked with the state. In SDK v4, **checkpointing is on by default**: steps run "back to back within a single request" up to `maxRuntime`, and a checkpoint is sent as each step completes. A failed step or parallel steps fall back to the classic model ([checkpointing](https://www.inngest.com/docs/setup/checkpointing)).
- **Sleep.** The signature is `step.sleep(idOrOptions, time)` (`InngestStepTools.ts:944`). It is an async opcode that suspends the run, and "your function does not run for that time" ([steps](https://www.inngest.com/docs/learn/inngest-steps)). Note that the ticket's `step.sleep(duration)` has no ID. Inngest requires one because the sleep is itself a memoized step.
- **Retries.** "Each `step.run()` has its own retry counter". The default is four retries after the first attempt ([retries](https://www.inngest.com/docs/features/inngest-functions/error-retries/retries)).
- **Serialization.** Step results are typed as `Jsonify<T>` unless middleware changes that (`types.ts:953`): a `Date` comes back as a string.
- **Limits** ([usage limits](https://www.inngest.com/docs/usage-limits/inngest)): 1,000 steps per function, 4 MiB per step result, 32 MiB of run state, sleeps up to 1 year, run length capped by plan (30–366 days).

## 2. BullMQ primitives involved

| Primitive                                                   | Effect                                                                                                                                                              | Lock-checked?                                                                                                          | Attempts                                                                     | Source                                                                                                                       |
| ----------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `job.updateData(data)`                                      | `HSET jobKey data <JSON>`, replacing all of the data                                                                                                                | **No**: only checks `EXISTS`                                                                                           | n/a                                                                          | `src/classes/job.ts:542`, `src/commands/updateData-1.lua`                                                                    |
| `job.moveToDelayed(ts, token)` + `DelayedError`             | Active → delayed ZSET, lock released, `skipAttempt` set                                                                                                             | Yes when a token is passed; the backend defaults the token to `'0'`, which skips the check (`includes/removeLock.lua`) | `atm` not incremented                                                        | `job.ts:1189`, `src/commands/moveToDelayed-11.lua`                                                                           |
| `job.moveToWaitingChildren(token)` + `WaitingChildrenError` | Active → waiting-children if dependencies are pending; returns `false` if none are                                                                                  | Yes                                                                                                                    | not incremented                                                              | `job.ts:1208`, `moveToWaitingChildren-7.lua`                                                                                 |
| `job.moveToWait(token)` + `WaitingError`                    | Active → wait (yield the slot)                                                                                                                                      | Yes                                                                                                                    | not incremented                                                              | `job.ts:698`, `moveJobFromActiveToWait-9.lua`                                                                                |
| Worker handling of the special errors                       | `DelayedError`, `WaitingError` and `WaitingChildrenError` are neither completed nor failed; the worker fetches the next job                                         | n/a                                                                                                                    | "does **not** increment the attemptsMade property"; use `maxStartedAttempts` | `src/classes/worker.ts:1220`, `docs/patterns/process-step-jobs.md`                                                           |
| Child jobs / `FlowProducer`                                 | Parent waits in waiting-children; a child's `returnvalue` is copied into `parentKey:processed` on completion; read with `getChildrenValues()`                       | n/a                                                                                                                    | per child                                                                    | `includes/updateParentDepsIfNeeded.lua:10-11`, `job.ts:1015`                                                                 |
| Custom `jobId`                                              | Adding an existing ID is ignored ("duplicated" event); an existing completed child is still linked to the new parent and its result recorded                        | n/a                                                                                                                    | n/a                                                                          | `docs/guide/jobs/job-ids.md`, `includes/handleDuplicatedJob.lua`, `includes/updateExistingJobsParent.lua`                    |
| Locks and stalls                                            | Defaults `lockDuration 30000`, `stalledInterval 30000`, `maxStalledCount 1`; a lost renewal only emits `lockRenewalFailed`/`error` and does not abort the processor | —                                                                                                                      | `stc` is incremented per stall and never reset; `ats` per start              | `worker.ts:305-308`, `src/classes/lock-manager.ts`, `moveStalledJobsToWait-9.lua`, `includes/prepareJobForProcessing.lua:45` |
| Completion                                                  | `moveToFinished` requires the lock token; a zombie cannot complete or fail the job                                                                                  | Yes                                                                                                                    | —                                                                            | `moveToFinished-14.lua`                                                                                                      |
| `worker.cancelJob` / `cancelAllJobs`                        | Aborts the `AbortSignal` passed as the processor's third argument                                                                                                   | —                                                                                                                      | —                                                                            | `docs/guide/workers/cancelling-jobs.md`                                                                                      |

The official pattern (`docs/patterns/process-step-jobs.md`) stores a `step` cursor in `job.data`. Its delay example calls `moveToDelayed` **before** `updateData`. If the process dies between the two calls, the job wakes with the old cursor and re-runs the step that just finished. Persisting first closes that gap.

## 3. Where step state can live

### A. In the job's own `data`

```ts
type RunData = { payload: P; steps: Record<string, { v: unknown } | { wakeAt: number }> };

async function run<T>(job: Job<RunData>, id: string, fn: () => Promise<T>): Promise<T> {
  const hit = job.data.steps[id];
  if (hit && "v" in hit) return hit.v as T;
  const v = await fn();
  await job.updateData({ ...job.data, steps: { ...job.data.steps, [id]: { v } } });
  return v;
}
```

- Public API only, and visible in any BullMQ dashboard as job data.
- Every step rewrites the whole JSON (`HSET data`, `updateData-1.lua`), so bytes written over n steps grow O(n²). Every resume pulls the whole blob, because `moveToActive` returns `HGETALL jobKey` (`prepareJobForProcessing.lua:51`).
- `updateData` is not fenced. A zombie writes a stale snapshot that **erases** the steps the new owner memoized since, and those steps run again later.
- The payload and the state share one field, so payload validation and versioning see the step state too.
- Cleanup comes for free: `removeOnComplete`/`removeOnFail` delete the job hash (`includes/removeJobKeys.lua`).

### B. In a side Redis hash, one field per step

```lua
-- KEYS[1] = <jobKey>:lock   KEYS[2] = <jobKey>:steps
-- ARGV[1] = worker token    ARGV[2] = step key   ARGV[3] = JSON
if redis.call("GET", KEYS[1]) ~= ARGV[1] then return -1 end  -- fenced
redis.call("HSET", KEYS[2], ARGV[2], ARGV[3])
return 0
```

- Writes are O(step size), and reads can be one `HGETALL` per resume or one `HGET` per step.
- The step state stays out of `job.data`, so the payload stays the validated input.
- It can be fenced on the lock token, as shown above. The lock key is `jobKey + ':lock'` (`includes/removeLock.lua`), but that layout is internal and undocumented, so a BullMQ upgrade could break it. An unfenced `HSET` (no Lua) is simpler and still never erases other fields, but a zombie can overwrite the same field.
- On Redis Cluster, the script's keys must share a hash slot. BullMQ requires the queue prefix to be a hash tag (`docs/patterns/redis-cluster.md`), so deriving the key from the job key keeps it in the same slot.
- Cleanup is extra work: `removeJobKeys` deletes only `jobKey`, `:logs`, `:dependencies`, `:processed`, `:failed` and `:unsuccessful`. The runner needs a TTL, deletion on the final transition, or a sweeper. The state is also not visible in dashboards.

### C. One child job per step (flows)

```ts
// parent processor, at a step not yet in getChildrenValues():
await stepQueue.add(id, input, {
  jobId: `${job.id}:${id}`, // deterministic: re-adding is a no-op that re-links
  parent: { id: job.id!, queue: job.queueQualifiedName },
  failParentOnFailure: true,
});
if (await job.moveToWaitingChildren(token)) throw new WaitingChildrenError();
// on resume: (await job.getChildrenValues())[`${prefix}:steps:${job.id}:${id}`]
```

- Results persist in `parentKey:processed`, even when children use `removeOnComplete` (`updateParentDepsIfNeeded.lua:11`). Retries, backoff, concurrency and rate limits work per step natively, and a step can run on another instance or queue. The parent holds no slot while a step runs.
- Deterministic child IDs make re-adding a child after a crash idempotent: an already completed child just records its value into the parent (`updateExistingJobsParent.lua`).
- **A closure cannot be shipped in a job.** The step's code has to be addressable from the child: either steps are declared by name ahead of time, which changes the `step.run(id, fn)` API, or the child job re-invokes the whole handler in "execute step `id` only" mode, replaying memoized steps on the way. The second is Inngest's classic model, and it doubles the replay of between-step code.
- Each step costs a full trip through the queue (add, fetch, complete, release the parent, fetch the parent again). Latency is much higher than A/B, and there are more keys and events per run.
- By default a failed child leaves the parent in waiting-children. One of `failParentOnFailure`, `continueParentOnFailure`, `ignoreDependencyOnFailure` or `removeDependencyOnFailure` must be set (`docs/guide/flows/fail-parent.md`, `continue-parent.md`).

Hybrids are possible: A or B for `step.run`, with C only for steps that need their own queue or concurrency key.

### Per-step retries (Inngest semantics) on A/B

BullMQ `attempts` are per job, so a plain throw retries the whole run, and the memoized steps are skipped on the way back. To get per-step counters, the runner stores `attempt` in the step's record, then on failure calls `moveToDelayed(now + backoff, token)` and throws `DelayedError`, which skips `atm`. When the step is out of attempts, it rethrows so the job fails, or throws `UnrecoverableError`.

## 4. Suspending and resuming (`step.sleep`)

**S1. Delay the run's own job.**

```ts
async function sleep(job: Job, token: string, id: string, ms: number) {
  const rec = await state.get(id); // A or B
  const wakeAt = rec?.wakeAt ?? Date.now() + ms;
  if (!rec) await state.set(id, { wakeAt }); // persist BEFORE moving
  if (Date.now() >= wakeAt) return; // resumed: sleep is over
  await job.moveToDelayed(wakeAt, token);
  throw new DelayedError();
}
```

- On resume, the processor runs from the top again: memoized steps return their stored result, and the sleep sees `wakeAt <= now` and continues. Persisting `wakeAt` first makes a crash between the two calls harmless: the stalled job re-enters and computes the same `wakeAt`.
- The job sits in the `delayed` ZSET with score `wakeAt * 0x1000 + counter` (`includes/getDelayedScore.lua`). It holds no worker. Any worker on the queue promotes it, and timing is "not guaranteed … exact" (`docs/guide/jobs/delayed.md`).
- Every resume increments `ats`, so `maxStartedAttempts` would count sleeps.

**S2. A delayed child as a timer.** The run adds a child `{ delay: ms, jobId: runId:sleepId }` to a timer queue whose processor returns at once, then calls `moveToWaitingChildren` and throws `WaitingChildrenError`. This is the same suspend mechanism as C, so it suits a C-based design, and the same mechanism could later carry a run waiting on an event. It costs an extra job and a timer worker.

**S3. A continuation job.** The current job completes and adds a new delayed job carrying the memo state, for example `jobId: runId:seq`. Each segment is a fresh job, so `stc`, `ats` and `atm` reset per segment. On the other hand, the run's identity must be tracked outside BullMQ's job ID, and history is spread across jobs. Adding the next job and completing the current one are not atomic in OSS BullMQ: a crash between them leaves both, or neither if the order is reversed, unless deterministic IDs make the add idempotent.

## 5. Deploys and N instances

- **A sleeping run** is only Redis state (S1: `delayed`; S2: waiting-children). It survives any number of deploys and instance changes, provided Redis persists. BullMQ recommends AOF and requires `maxmemory-policy noeviction` (`docs/guide/going-to-production.md`). Whichever worker version is running when the run wakes runs the rest of it, so step IDs and between-step code must stay compatible across releases. Payload versioning is a separate map item.
- **An active run during a deploy.** `worker.close()` waits for active jobs, with no timeout in TypeScript (`docs/guide/workers/graceful-shutdown.md`). A runner can also yield at the next step boundary: persist, `job.moveToWait(token)`, throw `WaitingError`. The run then requeues at once for the new release, with no stall delay and no `stc` increment. A step still running when the old release is killed is lost, and the job comes back via the stall check after up to `lockDuration` + `stalledInterval` (about 30–60 s by default).
- **Stall budget.** `stc` is `HINCRBY`'d on each stall and appears in no other script (`moveStalledJobsToWait-9.lua`), so it accumulates over the job's whole life. With the default `maxStalledCount: 1`, the second mid-step kill of a long-lived run sets `defa` and the job fails. S3 resets the counter per segment; A, B and C with S1 or S2 need `maxStalledCount` sized for the run's lifetime.
- **N instances.** All options are multi-instance safe in the same way BullMQ is: one lock owner per job, plus the stalled-job checker. The remaining duplicate paths are listed below.

## 6. Size limits

- **BullMQ** sets no limit on job data. Data is a JSON string (`docs/guide/jobs/job-data.md`), so `undefined`, `Date`, `Map` and `BigInt` don't round-trip, and a step result of `undefined` needs a wrapper (`{ v }`) to stay distinguishable from "not run".
- **Redis:** "a single Redis string can be a maximum of 512 MB" ([Redis strings](https://redis.io/docs/latest/develop/data-types/strings/)). Valkey was not checked separately. This caps option A's whole state, and each field in B or each `returnvalue` in C.
- **Practical costs:** A rewrites and re-reads the whole state per step and resume. B and C are per step, but C's `getChildrenValues()` is an `HGETALL` of every result. Inngest's limits (4 MiB per step, 32 MiB per run, 1,000 steps) are a reference point for what a durable-steps API can impose.

## 7. Duplicate-run windows (effectively-once)

1. **Side effect done, memo not yet persisted** (crash, kill, or Redis error between `fn()` and the write). The step runs again on resume. This window is irreducible in every option, and in Inngest too; only the step's own idempotency (keys passed to the external system) closes it.
2. **Zombie after lock loss.** The worker keeps running its step after the lock lapses, because lock loss does not abort the processor. Meanwhile another worker re-runs the run. The zombie's side effect is not preventable. Its memo write is: A cannot block it (and it can erase state), B can with the fenced script, and C blocks it implicitly because a step's result is only recorded through lock-checked completion. Its completion is rejected in all options.
3. **Crash between persist and suspend** (S1). Harmless if `wakeAt` is persisted first, as in the sketch above. Done the other way round, as in the official doc example, the step re-runs.
4. **Parallel steps sharing a base ID.** Collision indexes depend on start order, which Inngest only warns about. A runner can index (Inngest's approach) or reject duplicate IDs.

## 8. Fit with OSS BullMQ

| Option               | Public API only                  | Fenced writes              | Step latency              | Per-step retry/concurrency   | `step.run(id, closure)`             | Cleanup                       |
| -------------------- | -------------------------------- | -------------------------- | ------------------------- | ---------------------------- | ----------------------------------- | ----------------------------- |
| A. job data          | yes                              | no (zombie can erase memo) | in-process                | emulated via `moveToDelayed` | yes                                 | free                          |
| B. side hash         | no if fenced (internal lock key) | yes, with Lua              | in-process                | emulated via `moveToDelayed` | yes                                 | runner's job                  |
| C. child per step    | yes                              | implicit (completion)      | one queue round trip/step | native                       | needs named steps or re-invoke mode | free (results kept in parent) |
| S1. self delay       | yes                              | lock-checked move          | —                         | —                            | —                                   | —                             |
| S2. delayed child    | yes                              | lock-checked move          | —                         | —                            | —                                   | timer jobs                    |
| S3. continuation job | yes                              | n/a                        | —                         | —                            | —                                   | many jobs per run             |

BullMQ Pro's observables and groups are out of scope (map #1), and the official step pattern notes that Pro makes "save next step" unnecessary (`process-step-jobs.md`).

## Open questions for the HITL tickets

- Does `step.sleep` take an ID, as in Inngest, or derive one from call order?
- Are duplicate step IDs auto-indexed (Inngest) or rejected?
- Is coupling to BullMQ's internal lock-key layout acceptable in exchange for fenced writes?
- Is the default `maxStalledCount` acceptable for runs that span deploys, or does the library set it, or segment runs (S3)?
- Does the library yield at step boundaries on shutdown, and how does that interact with the roughly 10-minute deploy overlap?
- Where do step results get validated, given that typing alone gives `Jsonify<T>`?
