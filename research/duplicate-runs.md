# Duplicate runs on BullMQ: where they come from and what closes each

Research for issue #8. The question: where can a job run twice on BullMQ (open-source, Redis/Valkey backend), and what closes each path? The driving example is a handler that sends an email, which must not be sent twice.

This file gathers facts and options. It does not pick a design.

**Versions read** (2026-11-01):

- BullMQ `6.5.3`, source at commit `a8ff126` (the `main` HEAD, which carries version 6.5.3). Docs come from the same tree (`docs/gitbook/`) and from Context7 `/taskforcesh/bullmq/v6.5.3`.
- Provider docs as published on 2026-11-01. Resend, Stripe, Brevo and AWS SES API v2 come from their API references. Postmark comes from its API reference and a support article modified 2026-10-26.
- Inngest docs through Context7 (`/inngest/website`), used for comparison only.

## Summary

| Source of the duplicate                                                                                                    | What BullMQ provides                                                                                                                                                                                                                                                                                                                                                                                | What a library can add                                                                                                                                                                                                                                                                                                                                                      | What remains open                                                                                                                                                                              |
| -------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **1. Same work dispatched twice** (double click, webhook re-delivered, `add()` retried after a network error)              | A custom `jobId`: a second add is ignored while the job key exists, including in `completed`/`failed`, until the job is removed. `deduplication` has three modes. Simple: the key lives until the job completes or finally fails. Throttle: `ttl` ms, kept even after completion or auto-removal. `keepLastIfActive`: at most 1 active + 1 waiting. A `deduplicated`/`duplicated` event is emitted. | A dispatch key per event, required or derived, mapped to `jobId` or `deduplication.id`. A window that does not depend on job retention, e.g. throttle mode with a long `ttl`, or the library's own `SET NX PX` ledger. A typed result for "deduplicated".                                                                                                                   | Re-delivery after the window closes. A dispatch that should run again after a final failure but gets suppressed. The window depends on Redis keeping the keys (durability, `noeviction`).      |
| **2. Retry after a partial success** (side effect done, a later part throws)                                               | `attempts` (default **1**, so no retry unless configured), `backoff`, and `UnrecoverableError` to stop retrying. `job.updateData()` can persist progress (the "process step jobs" pattern). Flows split the work into child jobs that are retried separately.                                                                                                                                       | Steps with memoized results (Inngest's `step.run` model): a completed step returns its stored result on re-run. The results go in a key written by a Lua script that checks the job's lock token, unlike `updateData`, which checks nothing. Per-step retry counts.                                                                                                         | The step that threw after its own side effect. That is source 4 inside a step.                                                                                                                 |
| **3. Stalled job re-run while the first run is still alive** (blocked event loop, or Redis unreachable, past the lock TTL) | A lock per job (`lockDuration` 30 s, renewed about every 15 s). A stall check every `stalledInterval` (30 s) moves a job to `wait` when its lock is gone. `maxStalledCount` (default 1). `lockRenewalFailed` event plus `worker.cancelJob()` → `AbortSignal`, but this is opt-in in Node. A completion from the old run is rejected (`Missing lock` / `Lock mismatch`).                             | Wire `lockRenewalFailed` → abort the run's signal. Check the lock (`job.extendLock(token, …)`) before each side-effecting step. Fence step-result writes on the lock token. Tune `lockDuration`. Use `maxStalledCount: 0` to turn a stall into a failure rather than a re-run. A per-key lock does **not** prevent the overlap: it is a lease with the same expiry problem. | A run paused after its last check still performs its side effect: a check cannot fence a pause (Kleppmann). Only the external resource can fence, through an idempotency key.                  |
| **4. Crash after the side effect, before completion is recorded** (SIGKILL at the end of a deploy, OOM, Redis write lost)  | Nothing beyond re-running: the lock expires, the stall check re-queues the job, and it runs again from the start.                                                                                                                                                                                                                                                                                   | A stable idempotency key per step, sent to the provider. An "in-doubt" marker written before the call, so a re-run knows the outcome is unknown and can apply a policy: resend, fail, or reconcile.                                                                                                                                                                         | Providers without idempotency keys: **Postmark** and **SES** have none. Provider windows are short: Brevo 30 min, Resend 24 h, Stripe v1 ≥24 h. A retry or re-run past the window sends again. |

## 1. The same work dispatched twice

### What BullMQ provides

**Custom `jobId`.**

- Job ids are unique per queue. "if you add a job with an existing id then that job will just be ignored and not added to the queue at all" ([job-ids.md][b-jobids]).
- In the add script, the check is `EXISTS jobIdKey`. On a hit, it emits a `duplicated` event and returns the existing id ([addStandardJob-9.lua L94-98][b-addstd], [handleDuplicatedJob.lua][b-handledup]).
- The window lasts **while the job hash exists**, so it covers `waiting`, `delayed`, `active`, `completed` and `failed`. "Jobs that are removed from the queue (either manually, or when using settings such as `removeOnComplete`/`removeOnFailed`) will **not** be considered as duplicates" ([job-ids.md][b-jobids]).
- Consequences:
  - The default `removeOnComplete`/`removeOnFail` keeps jobs forever ([worker-options.ts L84-105][b-wopts]). That makes the dedup permanent, at the cost of memory.
  - `removeOnComplete: { age }` caps the window. But eviction is "evaluated on a best-effort basis … a job is only removed once a subsequent job of the same kind … is processed after its age has expired" ([keep-jobs.ts][b-keepjobs]). So the window is "at least `age`", not exactly `age`.
  - A job that finally failed and is still in the `failed` set also blocks a re-dispatch with the same id. The re-dispatch is silently dropped, apart from the `duplicated` event.
- Constraints: an id must not contain `:`, and must not be digits only (`Custom Id cannot be integers`) ([job-ids.md][b-jobids]).

**`deduplication` option** ([deduplication.md][b-dedup], [deduplication-options.ts][b-dedupopts]). It stores a key `de:<id>` holding the winning job id. The add script checks `jobId` first ([addStandardJob-9.lua L94-106][b-addstd]).

- **Simple mode** (`{ id }`):
  - Uses `SET de:<id> jobId NX` with no expiry ([deduplicateJobWithoutReplace.lua][b-dedupnr]).
  - On completion or **final** failure, `moveToFinished` deletes the key if it has no TTL and still points to this job ([removeDeduplicationKeyIfNeededOnFinalization.lua][b-dedupfin]).
  - Retries go through `moveToDelayed`/`retryJob`, not `moveToFinished`, so the key survives them.
  - Window: from add until the job completes or finally fails. A webhook re-delivered **after completion runs again**.
- **Throttle mode** (`{ id, ttl }`):
  - Uses `SET de:<id> jobId PX ttl NX` ([deduplicateJobWithoutReplace.lua][b-dedupnr]).
  - At finalization the key is deleted only if `PTTL == 0` or `-1` ([removeDeduplicationKeyIfNeededOnFinalization.lua][b-dedupfin]), so a key with time left **survives completion**.
  - Auto-removal (`removeOnComplete`) calls `removeJob(…, false)` and does not touch the dedup key ([moveToFinished-14.lua L180-186][b-mtf], [removeJobsByMaxAge.lua][b-rmage], [removeJob.lua][b-rmjob]).
  - Window: exactly `ttl` from the first add, whatever happened to the job.
  - If the job is still running when `ttl` expires, a new add is accepted and can run **in parallel**.
- **Debounce** (`ttl` + `extend` + `replace` + `delay`) replaces a delayed job with the latest payload. It is for coalescing, not idempotency.
- **`keepLastIfActive`**:
  - A duplicate added while the first job is active is stored. One follow-up job is created when the active one finishes.
  - "No parallel execution: At most 1 job per deduplication ID is active at any time"; `ttl` is ignored ([deduplication.md][b-dedup]).
  - This runs the work again by design, so it is the opposite of idempotency for an email.
- **Manual removal** (`job.remove()`, `queue.removeDeduplicationKey(id)`, `job.removeDeduplicationKey()`) clears the key: "Any manual deletion will disable the deduplication" ([deduplication.md][b-dedup], [removeDeduplicationKeyIfNeededOnRemoval.lua][b-dedupremove]).
- **Stale key recovery**: a key with no TTL whose job hash no longer exists (for example after an outage) is discarded on the next add ([recoverStaleDeduplicationKey.lua][b-dedupstale]). This applies only to keys without expiry.
- A deduplicated add emits `deduplicated` with `jobId`, `deduplicationId` and `deduplicatedJobId` on `QueueEvents`. `queue.getDeduplicationJobId(id)` returns the winner ([deduplication.md][b-dedup]).

Both checks run inside the add Lua script, so concurrent adds from N instances are serialized by Redis. Two web instances receiving the same webhook at once produce one job.

The add call itself can be retried by the caller. Example: a network error after Redis applied the write, then the route handler retries. With a `jobId` or a dedup id, the retry is absorbed. Without one, it is a second job.

### What a library on top can add

- **A dispatch key.** It can be required or optional per event, supplied by the caller (webhook delivery id, `Idempotency-Key` header of the incoming request, business key) and mapped to `jobId` or `deduplication.id`. Comparison: Inngest dedups on a producer-set event `id` for 24 hours, and offers a separate per-function `idempotency` expression, also 24 hours ([Inngest idempotency][i-idem]).
- **A retention-independent window.**
  - Throttle mode with a long `ttl` already gives "ignore the same key for T", atomically in the add script, and survives auto-removal. Its gaps: a manual `job.remove()` clears it, and it also suppresses a legitimate re-dispatch after a final failure.
  - Alternatively, `jobId` plus `removeOnComplete: { age }` gives a window of "at least `age`".
  - Or a library-owned key (`SET … NX PX`) checked and set in the same script as the add. That needs custom Lua or a second round trip, and a second round trip is not atomic.
- **One job per listener.** When one event fans out to several listeners, each listener job needs its own id, derived from the event key and the listener name. Otherwise the second listener is deduplicated against the first.
- **A typed "deduplicated" outcome** for the dispatcher. BullMQ returns the existing job's id rather than throwing.
- **Id hygiene**: hash or encode caller keys so they never contain `:` and are never all digits ([job-ids.md][b-jobids]).

### What remains open

- Choosing the window: which length, and whether a final failure should reopen it.
- A re-delivery arriving after the window always runs again.
- The window lives in Redis, so it is only as durable as Redis (see [Other sources](#other-sources)).

## 2. A retry after a partial success

### What BullMQ provides

- `attempts` defaults to **1** ([base-job-options.ts L30-34][b-basejob]). A thrown error does not retry unless the user configures `attempts`/`backoff`. Duplicates from a stall still happen (source 3).
- `UnrecoverableError` fails the job without the remaining attempts. `attemptsMade` counts finished attempts (`atm`). `attemptsStarted` counts every start (`ats`, incremented in `prepareJobForProcessing.lua`), including starts after a stall ([job.ts L119-125][b-job], [prepareJobForProcessing.lua L45][b-prep]). `maxStartedAttempts` caps total starts ([worker-options.ts L58-65][b-wopts]).
- The "Process step jobs" pattern persists the current step with `job.updateData({ step })` between steps, so a retry resumes there ([process-step-jobs.md][b-steps]). `updateData` is a bare `HSET` with **no lock-token check** ([updateData-1.lua][b-updatedata]). A run that lost its lock (source 3) can still overwrite the step state.
- The "Idempotent jobs" pattern only advises making jobs small and atomic, and splitting them into flows ([idempotent-jobs.md][b-idem]). In a flow, each child is its own job with its own retries.

### What a library on top can add

- **Memoized steps.** A step that completed stores its result, and a re-run returns it without executing the body. This is Inngest's model: "When a step completes successfully, `step.run` memoizes its result and on any later replay of the run, that step returns its stored value instead of executing again"; "Each `step.run` call has its own retry count" ([Inngest step.run][i-steprun], [Inngest blog][i-blog]). On BullMQ the results can live in the job hash or in a sibling key.
- **Fenced step-result writes.** A Lua script compares `<job>:lock` with the run's token before writing. This is the same check `extendLock` and `moveToFinished` do ([extendLock-2.lua][b-extendlock], [removeLock.lua][b-removelock]). A run whose lock expired cannot record a step result.
- Step ids must be stable across deploys. Inngest matches completed steps by id, and "Renaming a step's ID makes it a new step, so it runs again" ([Inngest patterns][i-steps]).

### What remains open

Memoization protects a step that **finished and was recorded**. A step whose side effect succeeded but which threw, or crashed, before its result was saved runs again. That is source 4 at step granularity. Inngest documents the same limit: "Memoization protects a step that _finished_; it can't reach back and neutralize a side effect that already escaped before the step was recorded" ([Inngest blog][i-blog]).

## 3. A stalled job picked up again while the first run is still alive

### Mechanism (BullMQ 6.5.3)

- **Lock.** When a job moves to active, the worker holds `<job>:lock` = token, with `PX lockDuration`. The default `lockDuration` is 30000, `lockRenewTime` defaults to `lockDuration / 2`, and `stalledInterval` is 30000 ([worker.ts L305-308, L344-345][b-worker]).
- **Renewal.**
  - A timer fires every `lockRenewTime / 2` (7.5 s). It extends jobs whose last renewal is older than `lockRenewTime / 2`, which works out to about every 15 s ([lock-manager.ts L97-125][b-lockmgr]).
  - `extendLocks` succeeds only if the lock still exists with the same token. It never re-creates a lock that expired ([extendLocks-1.lua][b-extendlocks]).
  - On failure the worker emits `lockRenewalFailed(jobIds)` and an `error` ([lock-manager.ts L69-77][b-lockmgr]).
- **Stall check.** Any worker can run it. A queue-wide `stalled-check` key with `PX stalledInterval` lets it run at most once per interval across all instances ([moveStalledJobsToWait-9.lua L44-48][b-stalled], [redis-queue-backend.ts L2138-2158][b-backend]). Each run:
  1. Takes the members of the `stalled` set, which holds every active job at the previous check, minus those whose lock was renewed since (`SREM` on renewal).
  2. For each, if `<job>:lock` does not exist, moves the job back to `wait`, increments `stc` and emits `stalled`.
  3. If `stc > maxStalledCount`, marks a deferred failure. The job is still moved to wait, then failed with "job stalled more than allowable limit" on its next pickup, without running the processor ([moveStalledJobsToWait-9.lua L66-104][b-stalled], [worker.ts L1157-1168][b-worker]).
  4. Re-marks all currently active jobs into `stalled` ([L114-120][b-stalled]).
- **The first run is not stopped.**
  - In Node, the processor gets an `AbortSignal` only if it declares a third parameter ([worker.ts L354-355][b-worker]). The signal is aborted only by `worker.cancelJob()`/`cancelAllJobs()` ([worker.ts L568-588][b-worker]).
  - Lock loss does not abort it automatically. The docs show the opt-in wiring `worker.on('lockRenewalFailed', ids => ids.forEach(id => worker.cancelJob(id)))` ([cancelling-jobs.md, via Context7 v6.5.3][b-cancel]). The Elixir port cancels automatically ([elixir job_cancellation.md, via Context7][b-cancel]).
- **The old run's completion is rejected.** `moveToFinished` calls `removeLock`, which returns `-2` (lock missing) or `-6` (token mismatch) → `Missing lock for job …` / `Lock mismatch …` ([removeLock.lua][b-removelock], [finished-errors.ts][b-finerr]). The duplicate is in the side effects, not in the recorded result.

### How likely it is

The lock is lost when no renewal succeeds for `lockDuration`. With defaults, a renewal lands about every 15 s. A pause of duration B that starts t seconds after the last renewal loses the lock when `t + B > 30 s`. So a pause of **about 15 to 30 s or more**, depending on phase, can lose it, and any pause over 30 s does. This follows from the timer arithmetic above ([lock-manager.ts][b-lockmgr], [worker.ts][b-worker]).

Once the lock has expired, the next stall check **will** re-queue the job, even if the original run has woken up: the renewal cannot re-create the lock. The overlap then lasts from the re-queue until the original run ends.

Causes of a pause:

- **CPU-bound work on the event loop.** The docs: "make sure that your worker does not keep the Node.js event loop too busy … as long as you do not perform CPU operations exceeding [30 s] you should not get stalled jobs" ([stalled.md][b-stalledguide], [stalled-jobs.md][b-stalledworker]). Embedding the worker in `next start` shares the loop with request handling, including SSR.
- **Redis unreachable or slow** for longer than the lock TTL while the handler keeps going against other services. Renewals fail the same way.
- **A process frozen** (VM pause, container CPU throttling) — the pause is the same for the lock.

On one instance with `concurrency: 1`, the re-queued job waits until the zombie run frees the slot, so the two runs are sequential rather than overlapping, but it still runs twice. With `concurrency > 1` or N instances, they can overlap.

### Settings that narrow it

| Setting                                | Effect                                                                                                                                 | Cost                                                                                                                                                                  |
| -------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `lockDuration` ↑ (e.g. minutes)        | A longer pause is needed to lose the lock.                                                                                             | A crashed worker's job is recovered later: lock TTL + up to `stalledInterval`.                                                                                        |
| `stalledInterval`                      | Only how soon a lost lock is noticed. It does not prevent the loss.                                                                    | —                                                                                                                                                                     |
| `maxStalledCount: 0`                   | The first stall fails the job instead of re-running it (at-most-once for stalls) ([moveStalledJobsToWait-9.lua L95-98][b-stalled]).    | A SIGKILLed or crashed run is never retried automatically, which interacts with deploys (source 4). Jobs from a job scheduler are exempt from the stall-failure rule. |
| `maxStartedAttempts`                   | Caps total starts, stall re-runs included ([worker-options.ts L58-65][b-wopts]).                                                       | Same trade-off, counted over all starts.                                                                                                                              |
| Sandboxed processors                   | The processor runs in a child process, so the parent's loop (which renews) is not blocked by it ([stalled.md][b-stalledguide]).        | Process overhead. The handler is loaded from a file, which matters for bundling under Next.                                                                           |
| `skipLockRenewal` / `skipStalledCheck` | Disable renewal or checking on one worker. "other workers could still perform stalled checks" ([worker-options.ts L105-121][b-wopts]). | Not a fix.                                                                                                                                                            |

### Does a per-key lock prevent the overlap?

Not by itself.

- A Redis lock that can survive a crash must expire (`SET key token NX PX ttl`) ([Redis distributed locks][r-lock]). It is a lease of exactly the same kind as BullMQ's job lock: it expires under the same pause.
- Redis's own page: "a client may acquire the lock, get blocked performing some operation for longer than the lock validity time … and later remove the lock, that was already acquired by some other client". Its consistency disclaimer: "You should implement fencing tokens … don't assume that a lock is retained as long as the process that had acquired it is alive" ([Redis distributed locks][r-lock]).
- Kleppmann: "You cannot fix this problem by inserting a check on the lock expiry just before writing back to storage". A fencing token works only if the **storage** rejects older tokens ([Kleppmann 2016][k-lock]).
- A per-key lock does help in one case: two _different_ jobs for the same key, e.g. two dispatches that slipped past dedup, are serialized while both runs are healthy. BullMQ open-source has no per-key concurrency: `setGlobalConcurrency` is per queue ([global-concurrency.md][b-globalconc]), and groups are BullMQ Pro, which is out of scope.

### What a library on top can add

- **Abort on lock loss**: subscribe to `lockRenewalFailed`, call `worker.cancelJob(id)`, and pass the signal to handlers and steps.
- **Check before each side-effecting step**: `job.extendLock(token, duration)` returns `1` only if the token still owns the lock ([job.ts L639][b-job], [extendLock-2.lua][b-extendlock]). This narrows the window but cannot close it (Kleppmann).
- **Fence step-result writes** on the token (see source 2), so a zombie run cannot record results or advance state.
- **Expose `lockDuration`** per job type with a sane default, and document the event-loop budget.
- **A per-key lock** with a fencing token, if the user's own database can check the token, e.g. a column compared in the `UPDATE`.

### What remains open

A run paused after its last check, then waking up and calling the provider, sends a second email. No check on the worker side fences that. Only the external system can, which means an idempotency key sent with the call (source 4). How much of this window to accept, and how to tune it, is a design decision.

## 4. A crash right after the side effect, before completion is recorded

### What BullMQ provides

- Completion is recorded atomically by `moveToFinished` ([moveToFinished-14.lua][b-mtf]). Anything that kills the process between the side effect and that script leaves the job active with a lock that will expire. The stall check re-queues it, and it runs again from the start ([stalled-jobs.md][b-stalledworker]).
- On deploy: `worker.close()` waits for active jobs "without a built-in timeout" in TypeScript ([graceful-shutdown.md][b-shutdown]). A host that kills the old release after a grace period (about 10 min on the reference hosting) SIGKILLs longer runs. "If a worker is killed without waiting for their jobs to complete, these jobs will be marked as stalled and processed automatically … (with a waiting time of about 30 seconds by default)" ([going-to-production.md L91-93][b-prod]).

### Provider idempotency keys

| Provider       | Mechanism                                                                                                                                                                                                      | Key format       | Window                                                                    | Same key, different payload                                          | Concurrent same key                                | Source                                                                                                        |
| -------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------- | ------------------------------------------------------------------------- | -------------------------------------------------------------------- | -------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| **Resend**     | `Idempotency-Key` HTTP header. SMTP: `Resend-Idempotency-Key` email header. Send Email, Send Batch, and the inbox send/reply/forward endpoints.                                                                | 1–256 chars      | 24 h                                                                      | `409 invalid_idempotent_request`                                     | `409 concurrent_idempotent_requests` (retry later) | [Resend][p-resend]                                                                                            |
| **Postmark**   | **None**: "Postmark does not currently support an idempotency key feature". It suggests using response codes and webhooks. Outbound search can filter by `tag` or one `metadata_<field>` (for reconciliation). | —                | —                                                                         | —                                                                    | —                                                  | [Postmark support][p-postmark], [Postmark email API][p-postmark-api], [Postmark messages API][p-postmark-msg] |
| **AWS SES v2** | **None**: `SendEmail` has no idempotency or client-token parameter. It returns a `MessageId`.                                                                                                                  | —                | —                                                                         | —                                                                    | —                                                  | [SES SendEmail][p-ses]                                                                                        |
| **Brevo**      | `idempotencyKey` inside the JSON body's `headers` object, on `POST /v3/smtp/email`                                                                                                                             | UUID only        | 30 min                                                                    | n/a; a reused key returns `duplicate_parameter` and is not processed | not documented                                     | [Brevo][p-brevo]                                                                                              |
| **Stripe v1**  | `Idempotency-Key` header, on all `POST`s                                                                                                                                                                       | ≤255 chars       | Keys may be pruned once ≥24 h old; a reuse after pruning is a new request | Error (parameters compared)                                          | Not saved; retryable                               | [Stripe idempotent requests][p-stripe]                                                                        |
| **Stripe v2**  | `Idempotency-Key` header, on `POST` and `DELETE`. Auto-generated if absent.                                                                                                                                    | UUID recommended | 30 days, same API and same account/sandbox                                | —                                                                    | —                                                  | [Stripe API v2 overview][p-stripe2]                                                                           |

Notes:

- Stripe v1 saves and replays the first response "including `500` errors". Stripe v2 instead re-executes failed work without repeating side effects ([Stripe][p-stripe], [Stripe v2][p-stripe2]).
- Stripe: "Avoid using sensitive data (for example, email addresses or personal identifiers) as idempotency keys" ([Stripe][p-stripe]).
- Resend does not say whether keys are scoped per team or per API key. Stripe v2 scopes them per account or sandbox. Stripe v1 does not state a scope.

### Deriving a stable key per step

What the key must be:

- **Stable across every re-execution of the same logical step**: retries, stall re-runs and crash re-runs. BullMQ keeps the same job id and the same `job.data` across all three. The job is moved back, not re-created ([moveStalledJobsToWait-9.lua][b-stalled]).
- **Unique across logical operations**, including across a Redis reset. Auto-generated ids are "an increasing counter … scoped by queue" ([job-ids.md][b-jobids]). If Redis loses its data, the counter restarts. A key built from `queue:jobId:step` could then collide with a key already sent within the provider window, and the provider would return the old response **without sending**. A random nonce stored in `job.data` at dispatch, or a caller-supplied dispatch key, avoids this.
- **Distinct per step and per listener**: event key + listener name + step id (+ a loop index for repeated steps). Step ids must be stable across deploys (see source 2).
- **Fitted to the provider's format**: ≤255/256 chars for Stripe and Resend, a UUID for Brevo. A name-based UUIDv5 (SHA-1 of namespace + name, deterministic; [RFC 9562 §5.5][rfc9562]) satisfies all three, and also avoids putting personal data in the key.
- **The same payload on every attempt.** Resend and Stripe reject a reused key with different parameters ([Resend][p-resend], [Stripe][p-stripe]). A step that builds its request from values computed at run time (timestamps, a re-fetched record that changed) gets an error on re-run instead of a dedup.

### What a library on top can add

- Expose a per-step `idempotencyKey` to the handler, derived as above, for the user to pass to the provider SDK.
- Write an **"in-doubt" marker** before the call (fenced on the lock token). On re-run, a step with a marker but no recorded result has an unknown outcome. The library can apply a per-step policy: retry with the same key (safe where the provider dedups), fail without retrying (at-most-once), or call a user-supplied reconcile function (e.g. a Postmark outbound search by metadata).
- Warn when the backoff schedule outlasts the provider window. Exponential backoff over several attempts can exceed Brevo's 30 min.

### What remains open

- **Postmark and SES**: no key, so a crash between the provider accepting the email and the result being recorded sends twice on re-run, or is dropped if the policy is at-most-once. Reconciliation through Postmark's search is possible but its indexing delay is not documented. SES has no query API in `SendEmail`'s reference.
- **Windows**: a re-run after the provider window (Brevo 30 min, Resend 24 h, Stripe v1 ≥24 h) is a new request.
- Inngest documents the same residual window and the same fix: "A request can succeed before Inngest records the step result. A retry can then call the API again. Event and function deduplication do not remove that risk" ([Inngest idempotency][i-idem]).

## Other sources

These are outside the ticket's four, but they surface the same way.

- **Redis durability.**
  - BullMQ recommends AOF and says "Usually, 1 second per write is enough" ([going-to-production.md L9-13][b-prod]). With `appendfsync everysec`, "you may lose 1 second of data if there is a disaster" ([Redis persistence][r-persist]). A completion written in that second can be lost, and the job re-runs.
  - With asynchronous replication, a failover can lose a lock or a dedup key that was just written ([Redis distributed locks][r-lock]).
- **Eviction.** "it is very important to configure the `maxmemory-policy` setting to `noeviction`" ([going-to-production.md L17][b-prod]). An evicted lock, job or dedup key reopens every path above.
- **Manual retries** (`job.retry()`, or a dashboard pointed at the queues) re-run a completed or failed job on purpose. Memoized steps and provider keys still apply, since the job id and data are kept.

[b-jobids]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/docs/gitbook/guide/jobs/job-ids.md
[b-dedup]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/docs/gitbook/guide/jobs/deduplication.md
[b-dedupopts]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/src/types/deduplication-options.ts
[b-keepjobs]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/src/types/keep-jobs.ts
[b-addstd]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/src/commands/addStandardJob-9.lua
[b-handledup]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/src/commands/includes/handleDuplicatedJob.lua
[b-dedupnr]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/src/commands/includes/deduplicateJobWithoutReplace.lua
[b-dedupfin]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/src/commands/includes/removeDeduplicationKeyIfNeededOnFinalization.lua
[b-dedupremove]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/src/commands/includes/removeDeduplicationKeyIfNeededOnRemoval.lua
[b-dedupstale]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/src/commands/includes/recoverStaleDeduplicationKey.lua
[b-rmage]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/src/commands/includes/removeJobsByMaxAge.lua
[b-rmjob]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/src/commands/includes/removeJob.lua
[b-mtf]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/src/commands/moveToFinished-14.lua
[b-removelock]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/src/commands/includes/removeLock.lua
[b-finerr]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/src/classes/finished-errors.ts
[b-stalled]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/src/commands/moveStalledJobsToWait-9.lua
[b-backend]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/src/classes/redis-queue-backend.ts
[b-extendlock]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/src/commands/extendLock-2.lua
[b-extendlocks]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/src/commands/extendLocks-1.lua
[b-lockmgr]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/src/classes/lock-manager.ts
[b-worker]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/src/classes/worker.ts
[b-wopts]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/src/interfaces/worker-options.ts
[b-basejob]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/src/interfaces/base-job-options.ts
[b-job]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/src/classes/job.ts
[b-prep]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/src/commands/includes/prepareJobForProcessing.lua
[b-updatedata]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/src/commands/updateData-1.lua
[b-steps]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/docs/gitbook/patterns/process-step-jobs.md
[b-idem]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/docs/gitbook/patterns/idempotent-jobs.md
[b-stalledguide]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/docs/gitbook/guide/jobs/stalled.md
[b-stalledworker]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/docs/gitbook/guide/workers/stalled-jobs.md
[b-cancel]: https://github.com/taskforcesh/bullmq/blob/v6.5.3/docs/gitbook/guide/workers/cancelling-jobs.md
[b-shutdown]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/docs/gitbook/guide/workers/graceful-shutdown.md
[b-prod]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/docs/gitbook/guide/going-to-production.md
[b-globalconc]: https://github.com/taskforcesh/bullmq/blob/a8ff12664de94f52a9bcc964c6129a4b23d00aa6/docs/gitbook/guide/queues/global-concurrency.md
[p-resend]: https://resend.com/docs/dashboard/emails/idempotency-keys
[p-postmark]: https://postmarkapp.com/support/article/what-is-an-idempotency-key
[p-postmark-api]: https://postmarkapp.com/developer/api/email-api
[p-postmark-msg]: https://postmarkapp.com/developer/api/messages-api
[p-ses]: https://docs.aws.amazon.com/ses/latest/APIReference-V2/API_SendEmail.html
[p-brevo]: https://developers.brevo.com/docs/heterogenous-versions-batch-emails
[p-stripe]: https://docs.stripe.com/api/idempotent_requests
[p-stripe2]: https://docs.stripe.com/api-v2-overview#idempotency
[r-lock]: https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/
[r-persist]: https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/
[k-lock]: https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html
[rfc9562]: https://www.rfc-editor.org/rfc/rfc9562#section-5.5
[i-idem]: https://github.com/inngest/website/blob/main/pages/docs/durable-execution/guides-and-advanced/idempotency.mdx
[i-steprun]: https://github.com/inngest/website/blob/main/pages/docs/durable-execution/primitives/step-run.mdx
[i-blog]: https://github.com/inngest/website/blob/main/content/blog/your-agent-just-learned-to-spend-money.mdx
[i-steps]: https://github.com/inngest/website/blob/main/shared/Patterns/_patterns/process-large-datasets-without-starting-over.mdx
