# Run list filters: several states, backward paging and more filters for `listRuns`

Today `listRuns({ state, task?, limit?, cursor? })` lists the runs of exactly one state, of one task or of every task, and only pages forward. The question: what would it take to offer several states in one call up to "all runs", paging backward as well as forward, and more filters (several tasks, a time range, the failure reason, the attempt count, the runs of one event or one schedule, a sort order, search by run id or by text, totals). This note covers how the run indexes work today, what comparable systems offer and on which store, which Redis and Valkey commands are usable at the supported floor, and a costed list of options per capability. Sections 1 to 4 are facts, measurements and costed options. Section 5 lists the decisions left to the maintainer. Section 6 is a short recommendation, labelled as opinion.

## Versions read

Everything was read on 2026-11-10.

| Source                     | Version or commit                                               | Where                                                                                                                                                |
| -------------------------- | --------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| This repository            | `687befe` (Version 0.2.1)                                       | `src/`, `tests/`, `docs/`, linked relative to this file                                                                                              |
| BullMQ                     | tag `v6.7.3`, commit `6b7fd10`                                  | [taskforcesh/bullmq](https://github.com/taskforcesh/bullmq/tree/6b7fd10ce096d7b7b402c6996ff832df1ce38a96), source and `docs/gitbook`                 |
| Sidekiq                    | tag `v8.1.7`, commit `1bb4aa0`; wiki at `2ec8e00`               | [sidekiq/sidekiq](https://github.com/sidekiq/sidekiq/tree/1bb4aa06e5aa178a114a5e855f9f3d5c24f6c61b), [wiki](https://github.com/sidekiq/sidekiq/wiki) |
| Asynq                      | tag `v0.26.0`, commit `d704b68`                                 | [hibiken/asynq](https://github.com/hibiken/asynq/tree/d704b68a426d1d3a6c707f9661d29296e1350775)                                                      |
| RQ, Faktory, Flower        | `v3.0` (`05c7979`), `v1.10.0` (`b2d89a6`), `v2.2.0` (`4d4a9ac`) | rq/rq, contribsys/faktory, mher/flower                                                                                                               |
| Trigger.dev                | tag `v4.8.4`, commit `55a53ef`                                  | [triggerdotdev/trigger.dev](https://github.com/triggerdotdev/trigger.dev/tree/v4.8.4), source and `docs/`                                            |
| Inngest                    | tag `v1.52.0`, commit `065cbda`                                 | [inngest/inngest](https://github.com/inngest/inngest/tree/v1.52.0)                                                                                   |
| Temporal                   | docs at `735c37c`; server tag `v1.32.2`; API tag `v1.64.3`      | temporalio/documentation, temporalio/temporal, temporalio/api                                                                                        |
| pg-boss                    | tag `12.38.2`, commit `fc78d0d`                                 | [timgit/pg-boss](https://github.com/timgit/pg-boss/tree/12.38.2), core and `packages/dashboard`                                                      |
| River                      | tag `v0.51.2`, commit `8468ff2`                                 | [riverqueue/river](https://github.com/riverqueue/river/tree/v0.51.2)                                                                                 |
| Graphile Worker            | tag `v0.18.0`, commit `4cda192`                                 | [graphile/worker](https://github.com/graphile/worker/tree/v0.18.0)                                                                                   |
| Redis documentation        | redis/docs at `fa24ef7`                                         | `content/commands/`, `content/develop/`, published at redis.io                                                                                       |
| Valkey documentation       | valkey-io/valkey-doc at `5e6ab07`                               | `commands/`, `topics/`, published at valkey.io                                                                                                       |
| Redis source               | tags `7.4.0`, `8.0.0`, `8.10.3`                                 | redis/redis: `src/t_zset.c`, `src/script.c`, `src/commands/*.json`, release notes                                                                    |
| Valkey source              | tags `8.0.0`, `8.1.0`, `9.0.0`, `9.1.0`, `9.1.3`, `9.2.0-rc2`   | valkey-io/valkey: the same files                                                                                                                     |
| valkey-search              | tags `1.0.3`, `1.1.1`, `1.2.1`                                  | valkey-io/valkey-search: README, release notes, `docs/`                                                                                              |
| valkey-bundle, container   | `5601e9c`, `e3be9b5`                                            | valkey-io/valkey-bundle, valkey-io/valkey-container                                                                                                  |
| Servers in the experiments | Valkey 8.1.11, Redis 7.4.12                                     | docker images `valkey/valkey:8` and `redis:7.4`, already present locally                                                                             |

The sources of sections 2 and 3 were read from shallow clones at these tags. A sample of the quotes and line numbers was checked again against the clones before writing. Where a project's docs and source disagree, the note says so and follows the source.

## Summary

1. **The eight states have four score meanings, and that decides what "several states" can mean.** `pending` and `waitingForConcurrencySlot` are scored by a counter, `executing` by a start time, `delayed`, `suspended` and `backingOff` by a due time, `completed` and `failed` by a finish time. Scores compare inside a family and not across ([§1.2](#12-which-script-writes-each-index-and-with-which-score)).
2. **Several states of one family merge into one order with no new index.** It is the merge `listRuns` already does across tasks, given more indexes. All eight states then list as four blocks. Measured: 400 page scripts take 3 to 4 ms of server time ([§4.2](#42-several-states-up-to-all-states), S1 and S2).
3. **One timeline of all runs needs a new index.** Scored by `dispatchedAt` it is written twice in the life of a run, costs 139 to 161 bytes per run, and is the only list in which a run never moves. A state filter on it is read and discard ([§4.2](#42-several-states-up-to-all-states), S3).
4. **The real cost of any new index is its rollout, not its writes.** Runs that exist before the upgrade are in no new index, and an older release creates and prunes runs without it during a deploy overlap. It takes a backfill, a sweep of members whose run is gone, and possibly two releases ([§4.13](#413-rolling-out-a-new-index)).
5. **Paging backward needs no new index.** The mirror of the page script was written and tested on both floor servers, ties and a removed cursor run included. It costs the same as a forward page. The last page needs no cursor, and the offset of a page ("runs 151 to 200 of N") comes for free from ranks the script already computes ([§4.3](#43-paging-backward-and-what-a-position-means), [§7](#7-experiments)).
6. **A cursor is a position, and only a run that stays in its state is stable under it.** New runs enter ended and executing lists at the head, timed lists anywhere, queued lists at the head or at their old place. "First page" and "last page" are places, not snapshots ([§4.3](#43-paging-backward-and-what-a-position-means)).
7. **Several tasks, a time range on the state's own time, ascending order, the runs of one event or one schedule, and an exact run id need no new index.** They pick indexes, bound the score, or derive run ids. "Executing for more than 10 minutes" and "overdue" are score ranges. Queued runs have no time to filter on at all ([§4.4](#44-several-tasks) to [§4.10](#410-search-by-run-id)).
8. **`reason`, the attempt count and `dispatchedAt` within a state can be filtered by read and discard, inside the existing contract of short pages.** It costs about 2 to 2.5 microseconds per record examined in a script, gives no total, and degrades with rare values. An index per reason is the alternative, with three scripts changed ([§4.6](#46-the-failure-reason), [§4.7](#47-the-attempt-count)).
9. **Totals are free exactly where `ZCARD` and `ZCOUNT` answer:** a state, a task, a time range on the state's own time, any list on its own index. No read-and-discard filter has a total ([§4.12](#412-totals-and-the-filters-other-tools-offer)).
10. **Run id prefix search needs a scan of every index or a new lexicographic index; text search has no bounded option at the floor** ([§4.10](#410-search-by-run-id), [§4.11](#411-search-by-payload-or-error-text)).
11. **No Redis-backed system read here lists more than one state in one order.** BullMQ returns the requested states one after the other, with the offsets applied to each; Sidekiq and Asynq list one structure per call, by page number, and filter only by scanning ([§2.2](#22-bullmq), [§2.3](#23-sidekiq-asynq-and-other-redis-backed-engines)).
12. **Every system with rich filters lists from another store.** Trigger.dev reads ClickHouse then Postgres, Inngest SQL, Temporal a separate visibility store, pg-boss and River their Postgres table. Even there, backward paging is rare (Trigger.dev and the pg-boss dashboard), totals are mostly absent or approximate, and River and Temporal keep one time per group of states ([§2.1](#21-comparison)).
13. **A union of sorted sets costs the total size of its inputs, never the page.** Measured: 54 to 83 ms for 100,000 members and 0.6 to 0.8 s for 550,000, against 3 to 4 ms for the page scripts over the same data. No command merges several sorted sets with a limit ([§3.1](#31-sorted-set-commands), [§7](#7-experiments)).
14. **The sorted-set commands are the same on Redis and Valkey at the floor, and `LIMIT offset` is better than documented.** All 35 commands date from 7.0 or earlier. Both doc sets still warn that a large offset is O(N); since Redis 7.4 and Valkey 8.0 the source seeks it by rank ([§3.1](#31-sorted-set-commands), [§3.6](#36-where-redis-and-valkey-differ-at-the-floor)).
15. **Neither search module can be a dependency.** Redis has one built in from 8.0 only. Valkey's is a separate module, absent from the plain image, and the version shipped for Valkey 8 cannot index without a vector field ([§3.4](#34-search-modules)).

## 1. The current index design

Terms used below. A **sorted set** is the Redis and Valkey collection of unique **members**, each with a numeric **score**, kept in score order. A **run index** is one sorted set that lists the runs of one task in one state. The **record** of a run is the hash that holds its fields. The **stored run id** is the text `encodeURIComponent(task):runId` ([`src/engine/engine-keys.ts:15-20`](../src/engine/engine-keys.ts#L15-L20)); it is the member of every sorted set below.

### 1.1 Keys

Every key starts with the hash tag `{<prefix>}` ([ADR 0002](../docs/adr/0002-every-key-under-one-hash-tag.md), [`engine-keys.ts:83-108`](../src/engine/engine-keys.ts#L83-L108)). The Lua library builds the same key shapes from the prefix ([`src/engine/lua-library.ts:14-67`](../src/engine/lua-library.ts#L14-L67)).

| Key                               | Type       | Member        | Score                      | Who reads it                                                        |
| --------------------------------- | ---------- | ------------- | -------------------------- | ------------------------------------------------------------------- |
| `{p}:runs:<task>:<state>`         | sorted set | stored run id | depends on the state (1.2) | `listRuns`, `countRuns`, `prune`, `busyRunCounts`, `makeDue` (task) |
| `{p}:pending`                     | sorted set | stored run id | `order` (a counter)        | `fetch`, to take the next run                                       |
| `{p}:due`                         | sorted set | stored run id | due instant                | `promote`, to make due runs pending                                 |
| `{p}:locks`                       | sorted set | stored run id | `lockedUntil`              | `promote`, to find stalled runs                                     |
| `{p}:waiting:<task>[:<key>]`      | sorted set | stored run id | `order`                    | the wake of a parked run                                            |
| `{p}:slots:<task>[:<key>]`        | set        | stored run id | none                       | the concurrency limit                                               |
| `{p}:dedup:<task>`                | sorted set | stored run id | end of the dedup window    | `enqueue`                                                           |
| `{p}:tasks`                       | set        | task name     | none                       | `busyRunCounts`, the tick                                           |
| `{p}:order`                       | string     | none          | none                       | `INCR`, the source of `order`                                       |
| `{p}:run:<stored run id>`         | hash       | none          | none                       | the record (1.4)                                                    |
| `{p}:run:<stored run id>:payload` | string     | none          | none                       | the payload, as JSON text                                           |
| `{p}:run:<stored run id>:errors`  | list       | none          | none                       | one JSON error record per failed attempt                            |
| `{p}:run:<stored run id>:steps`   | hash       | none          | none                       | the saved steps                                                     |

`<key>` is the concurrency key. There are eight run indexes per task, one per state ([`engine-keys.ts:107`](../src/engine/engine-keys.ts#L107), [`lua-library.ts:65-67`](../src/engine/lua-library.ts#L65-L67)). The three global sorted sets `pending`, `due` and `locks` exist for the worker, not for the read API. Two of them happen to be a ready-made union over every task: `due` holds every delayed, suspended and backing-off run by due instant ([`lua-library.ts:183-187`](../src/engine/lua-library.ts#L183-L187)), and `pending` holds every pending run by `order` ([`lua-library.ts:175-181`](../src/engine/lua-library.ts#L175-L181)). `locks` is scored by lock expiry, which `renew` moves ([`src/engine/renew-script.ts:28-30`](../src/engine/renew-script.ts#L28-L30)), so it is not in the order of the `executing` index.

### 1.2 Which script writes each index, and with which score

One run sits in exactly one run index, the one of the `state` field of its record, with the score in the table. The integration test harness checks this after every test ([`tests/stored-key-violations.ts:26-35`](../tests/stored-key-violations.ts#L26-L35) names the score field of each state; lines 190-230 check the membership and the score). Every "now" is the server's `TIME` in milliseconds ([`lua-library.ts:69-72`](../src/engine/lua-library.ts#L69-L72)).

| State                       | Score (record field)             | Enters through                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | Leaves through                                                                                                                                                                                               |
| --------------------------- | -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `delayed`                   | due instant (`startsAt`)         | `createRun` with a delay, in `enqueue` ([`lua-library.ts:301-303`](../src/engine/lua-library.ts#L301-L303))                                                                                                                                                                                                                                                                                                                                                                                                                                         | `promote` once due ([`lua-library.ts:245-247`](../src/engine/lua-library.ts#L245-L247)); `makeDue` (testing)                                                                                                 |
| `pending`                   | `order`, a counter, not a time   | `enterPending` ([`lua-library.ts:175-181`](../src/engine/lua-library.ts#L175-L181)). With a fresh `order` (`INCR {p}:order`): `createRun` in `enqueue`, `fire`, `fireNow` and the failure notices of `settle`; `promote` of a due run; `replay`. With the `order` it already had: a woken run ([`lua-library.ts:268-271`](../src/engine/lua-library.ts#L268-L271)), a yield or a hand back ([`settle-script.ts:133-137`](../src/engine/settle-script.ts#L133-L137)), a stall ([`promote-script.ts:40-42`](../src/engine/promote-script.ts#L40-L42)) | `fetch` ([`fetch-script.ts:131`](../src/engine/fetch-script.ts#L131))                                                                                                                                        |
| `waitingForConcurrencySlot` | `order`, kept from `pending`     | `fetch` parks the run ([`fetch-script.ts:103-106`](../src/engine/fetch-script.ts#L103-L106))                                                                                                                                                                                                                                                                                                                                                                                                                                                        | a freed slot wakes it ([`lua-library.ts:268-271`](../src/engine/lua-library.ts#L268-L271))                                                                                                                   |
| `executing`                 | start of this lock (`startedAt`) | `fetch` locks the run ([`fetch-script.ts:89-97`](../src/engine/fetch-script.ts#L89-L97)). `renew` does not touch the index                                                                                                                                                                                                                                                                                                                                                                                                                          | `endLock` ([`lua-library.ts:275-281`](../src/engine/lua-library.ts#L275-L281)), called by `settle` and by `promote` for a stall                                                                              |
| `suspended`                 | due instant (`resumesAt`)        | `settle` kind `suspend` ([`settle-script.ts:123-126`](../src/engine/settle-script.ts#L123-L126))                                                                                                                                                                                                                                                                                                                                                                                                                                                    | `promote` once due; `makeDue`                                                                                                                                                                                |
| `backingOff`                | due instant (`nextAttemptAt`)    | `settle` kind `retry`, which also adds 1 to `attempt` ([`settle-script.ts:114-121`](../src/engine/settle-script.ts#L114-L121))                                                                                                                                                                                                                                                                                                                                                                                                                      | `promote` once due; `makeDue`                                                                                                                                                                                |
| `completed`                 | finish time (`finishedAt`)       | `settle` kind `complete` ([`settle-script.ts:86-90`](../src/engine/settle-script.ts#L86-L90), [`104-108`](../src/engine/settle-script.ts#L104-L108))                                                                                                                                                                                                                                                                                                                                                                                                | `prune` only ([`prune-script.ts:32-35`](../src/engine/prune-script.ts#L32-L35))                                                                                                                              |
| `failed`                    | finish time (`finishedAt`)       | `settle` kind `fail`, or a hand back 30 minutes after the first one ([`settle-script.ts:92-102`](../src/engine/settle-script.ts#L92-L102), [`128-131`](../src/engine/settle-script.ts#L128-L131))                                                                                                                                                                                                                                                                                                                                                   | `replay`, which makes it pending with a fresh `order`, resets `attempt` to 1 and deletes `finishedAt`, `reason` and the errors ([`replay-script.ts:32-37`](../src/engine/replay-script.ts#L32-L37)); `prune` |

Three details matter for the options in section 4:

- **The eight states fall into four score meanings.** `pending` and `waitingForConcurrencySlot` share the `order` counter. `executing` has a start time. `delayed`, `suspended` and `backingOff` share a due time, usually in the future. `completed` and `failed` share a finish time. This note calls each group a **score family**: queued, executing, timed and ended. Within a family the scores compare. Across families they do not: a counter value such as 48,213 and an epoch time such as 1,794,000,000,000 have no common order.
- **While a run stays in a state, its score does not change.** No script rewrites the score of a member that stays in its index. A run moves only by leaving one index and entering another.
- **A third stall stays `executing` until a worker settles it.** `promote` reports it and leaves the record and the index as they are ([`promote-script.ts:35-38`](../src/engine/promote-script.ts#L35-L38)); the worker then settles it as `fail` with the reason `stalled` ([`src/worker-loop.ts:397-412`](../src/worker-loop.ts#L397-L412)).

### 1.3 Retention

Only the `completed` and `failed` indexes are bounded. `prune` runs for one task per tick and per worker, and removes at most 200 runs per call ([`src/engine/engine.ts:47`](../src/engine/engine.ts#L47), [`docs/dashboard.md`](../docs/dashboard.md#what-reads-see-and-what-they-cost)). It removes, in this order: completed runs past their age, failed runs past their age, completed runs over the cap, failed runs over the cap ([`prune-script.ts:71-74`](../src/engine/prune-script.ts#L71-L74)).

- **Age is read from the index score.** A run is past its age when its score is at most `now - maxAge` ([`prune-script.ts:57-61`](../src/engine/prune-script.ts#L57-L61)). Over the cap, the member with the lowest score goes first ([`prune-script.ts:63-69`](../src/engine/prune-script.ts#L63-L69)). So the score of the two ended indexes must stay the finish time, or `prune` changes with it.
- The defaults are 24 hours and 1,000 runs per task for completed runs, 14 days and 10,000 runs per task for failed runs ([`src/worker-timing.ts:33-34`](../src/worker-timing.ts#L33-L34)).
- A pruned run loses its record, payload, errors, steps and index member in one call ([`prune-script.ts:32-35`](../src/engine/prune-script.ts#L32-L35)). `prune` passes over a record written by a newer release ([`prune-script.ts:43-45`](../src/engine/prune-script.ts#L43-L45)).
- `prune` removes the task from `{p}:tasks` when its dedup set and its eight run indexes are all empty ([`prune-script.ts:77-91`](../src/engine/prune-script.ts#L77-L91)). An extra per-task index would have to join that check.
- The other six indexes are as long as the backlog: nothing trims them.
- The load envelope the benchmark asserts is 50 tasks and 100,000 kept runs, 1,000 completed and 1,000 failed per task ([`docs/codebase/testing.md`](../docs/codebase/testing.md#benchmark)). With the failed cap reached on every task, the same 50 tasks keep 550,000 ended runs.

### 1.4 The record

| Field                                                 | Written by                                                                                                                                                       | Read by `listRuns` today |
| ----------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------ |
| `version`, `task`, `attempt`, `dispatchedAt`          | `createRun` ([`lua-library.ts:285-292`](../src/engine/lua-library.ts#L285-L292)); `attempt` also by a retry and a replay                                         | yes                      |
| `retries`, `replays`, `stalls`                        | `createRun`; `replays` by `replay`, `stalls` by `promote` and the step scripts                                                                                   | no                       |
| `concurrencyKey`                                      | `createRun`, when the run has one ([`lua-library.ts:293-295`](../src/engine/lua-library.ts#L293-L295))                                                           | no                       |
| `eventName`, `eventId`                                | `createRun`, for a run an emit created ([`lua-library.ts:296-298`](../src/engine/lua-library.ts#L296-L298))                                                      | no                       |
| `scheduledAt`                                         | `createFiringRun`, for a run a firing created ([`lua-library.ts:359`](../src/engine/lua-library.ts#L359))                                                        | no                       |
| `state`                                               | every transition                                                                                                                                                 | yes                      |
| `order`                                               | `enterPending` ([`lua-library.ts:177`](../src/engine/lua-library.ts#L177))                                                                                       | no                       |
| `startsAt`, `resumesAt`, `nextAttemptAt`              | `enterDueState` ([`lua-library.ts:184`](../src/engine/lua-library.ts#L184))                                                                                      | yes                      |
| `startedAt`, `lockToken`, `lockedUntil`, lock details | `fetch` ([`fetch-script.ts:89-95`](../src/engine/fetch-script.ts#L89-L95)); `startedAt` is overwritten at every lock, so it is the start of the last lock        | no                       |
| `endedLockToken`, `endedAs`                           | `endLock` ([`lua-library.ts:279`](../src/engine/lua-library.ts#L279))                                                                                            | no                       |
| `finishedAt`                                          | `settle` when the run ends ([`settle-script.ts:88`](../src/engine/settle-script.ts#L88)); deleted by `replay`                                                    | yes                      |
| `reason`                                              | `settle` when the run fails ([`settle-script.ts:94`](../src/engine/settle-script.ts#L94)); one of four values ([`src/run-states.ts:1`](../src/run-states.ts#L1)) | yes                      |
| `firstFoundUnknownAt`, `sleepsWokenAt`                | the hand back, `makeDue`                                                                                                                                         | no                       |

- `listRuns` reads ten fields of each listed run plus its last error, in one script per run ([`src/read-api.ts:153-164`](../src/read-api.ts#L153-L164), [`src/engine/run-read-scripts.ts:20-25`](../src/engine/run-read-scripts.ts#L20-L25)).
- So `reason`, `attempt`, `dispatchedAt` and `finishedAt` are already in hand for each listed run. `eventName`, `eventId`, `scheduledAt`, `concurrencyKey` and `replays` are in the record and cost one more `HMGET` field each.
- The record holds no time at which the run became pending, no first start, no duration, no parent run and no tag.
- The payload and the errors are outside the record, as JSON text. An error text is cut at 4,000 characters ([`engine.ts:51`](../src/engine/engine.ts#L51)).

### 1.5 How `listRuns` reads a page today

- **Round trip 1.** One page script per task the call lists, one task with `task`, every task of the instance without ([`read-api.ts:388-404`](../src/read-api.ts#L388-L404), [`438-439`](../src/read-api.ts#L438-L439)). The script returns `limit` entries, highest score first, after a position `(score, stored run id)`. It counts the higher scores with `ZCOUNT`, then finds the place among equal scores by a binary search by rank, comparing members byte by byte ([`src/engine/index-page-script.ts:31-49`](../src/engine/index-page-script.ts#L31-L49)).
- **Merge.** The client sorts the entries of every task by score, then by stored run id, both descending, and keeps the first `limit` ([`read-api.ts:342-347`](../src/read-api.ts#L342-L347), [`440-441`](../src/read-api.ts#L440-L441)). This is a **k-way merge**: several sorted lists read at their heads and interleaved into one.
- **Round trip 2.** One read script per kept entry: the ten record fields and the last error ([`read-api.ts:406-417`](../src/read-api.ts#L406-L417)). A run whose record is missing, is of a newer version, or no longer has the listed state is dropped, so a page can be shorter than `limit` ([`read-api.ts:325-340`](../src/read-api.ts#L325-L340)). [`docs/dashboard.md`](../docs/dashboard.md#page-through-failed-runs) states this: "A page may come back shorter than `limit`, even empty; only a missing `nextCursor` ends the walk."
- **Cursor.** Base64url of the JSON array `[2, state, listedTask, score, task, runId]`: a format version, the two filters, and the position of the last entry kept ([`src/run-list-cursor.ts:41-46`](../src/run-list-cursor.ts#L41-L46)). This is **keyset pagination**: the cursor is the sort key of the last row shown, not a row count. A cursor of another state or task is refused ([`run-list-cursor.ts:52-66`](../src/run-list-cursor.ts#L52-L66)).
- **Counts.** `countRuns` sends one `ZCARD` per task and state ([`read-api.ts:452-466`](../src/read-api.ts#L452-L466)).
- **Cost.** Round trip 1 returns up to `limit` entries per index, so up to tasks × `limit` entries cross the wire for one page: 2,500 entries with 50 tasks and the default limit. Measured in [section 7](#7-experiments): 50 page scripts take 1.5 to 1.7 ms of server time.

### 1.6 What a new index must respect

- **One hash tag.** A new key is `{<prefix>}:…` and is built in both `engine-keys.ts` and `lua-library.ts` ([ADR 0002](../docs/adr/0002-every-key-under-one-hash-tag.md)).
- **Supported servers.** Valkey 8.0 and Redis 7.4 are the floor ([`src/server-info.ts:102-105`](../src/server-info.ts#L102-L105)); CI runs `valkey/valkey:8`, `valkey/valkey:9`, `redis:7.4` and `redis:8` ([`.github/workflows/ci.yml:65`](../.github/workflows/ci.yml#L65)); [`docs/setup.md`](../docs/setup.md) says the same. Section 3 lists what that floor allows.
- **Scripts are isolated, not transactional.** A script that fails after a write keeps the write. Each script checks first and writes after; a new `ZADD` or `ZREM` goes with the writes. (Established in the earlier note `research/redis-valkey-engine-semantics.md`, section 1, on branch `research/redis-valkey-engine-semantics`.)
- **Two releases write at once during a deploy.** The record version is 1 ([`engine.ts:41`](../src/engine/engine.ts#L41)). An old release keeps creating, settling, replaying and pruning runs during the overlap ([`docs/deploys.md`](../docs/deploys.md), [`docs/worker.md`](../docs/worker.md#overlapping-releases)). It knows nothing of an index the new release adds. [Section 4.13](#413-rolling-out-a-new-index) works out what that does.
- **Runs that exist before the upgrade** are in no new index until something adds them.
- **Memory.** One more sorted-set entry per run costs 139 to 161 bytes with the member the indexes use today (56 bytes in the measurement), measured in [section 7](#7-experiments).
- **The harness.** `tests/stored-key-violations.ts` reports any sorted set under `runs:` that holds a run whose record does not match. A new index needs its own rule there, and [`docs/codebase/testing.md`](../docs/codebase/testing.md) changes with it.

## 2. What comparable systems offer

Link bases, each pinned: `bullmq:` is `https://github.com/taskforcesh/bullmq/blob/6b7fd10ce096d7b7b402c6996ff832df1ce38a96/`, `sidekiq:` is `https://github.com/sidekiq/sidekiq/blob/1bb4aa06e5aa178a114a5e855f9f3d5c24f6c61b/`, `asynq:` is `https://github.com/hibiken/asynq/blob/d704b68a426d1d3a6c707f9661d29296e1350775/`, `trigger:` is `https://github.com/triggerdotdev/trigger.dev/blob/v4.8.4/`, `inngest:` is `https://github.com/inngest/inngest/blob/v1.52.0/`, `temporal-docs:` is `https://github.com/temporalio/documentation/blob/735c37c81c06d7c08b959557b47cbef56eed5719/`, `temporal:` is `https://github.com/temporalio/temporal/blob/v1.32.2/`, `pg-boss:` is `https://github.com/timgit/pg-boss/blob/12.38.2/`, `river:` is `https://github.com/riverqueue/river/blob/v0.51.2/`, `graphile:` is `https://github.com/graphile/worker/blob/v0.18.0/`. The links below are full URLs.

### 2.1 Comparison

| System                 | States in one call                          | Other filters                                                                                   | Pagination                                    | Backward                   | Ordering                                                    | Counts                                  | Store behind the list                                                                        |
| ---------------------- | ------------------------------------------- | ----------------------------------------------------------------------------------------------- | --------------------------------------------- | -------------------------- | ----------------------------------------------------------- | --------------------------------------- | -------------------------------------------------------------------------------------------- |
| **This library today** | one                                         | one task                                                                                        | keyset cursor                                 | no                         | fixed per state, highest score first                        | per task and state, `ZCARD`             | Redis or Valkey sorted sets                                                                  |
| BullMQ 6.7             | several, returned one state after the other | none (Pro: a group)                                                                             | rank offsets `start`/`end`, applied per state | a lower offset; `asc` flag | per state; no order across states                           | per state, `LLEN`/`ZCARD`               | Redis lists and sorted sets                                                                  |
| Sidekiq 8.1            | one structure per call                      | substring of the job JSON by `ZSCAN` (full scan, not paged); a score range                      | page number, by rank                          | previous page number       | score, or list order                                        | `LLEN`/`ZCARD`, global counters         | Redis lists and sorted sets; no completed jobs kept                                          |
| Asynq 0.26             | one state and one queue per call            | none                                                                                            | page number, by rank                          | lower page number          | fixed per state                                             | per state, `LLEN`/`ZCARD`               | Redis lists and sorted sets                                                                  |
| Trigger.dev 4.8        | several                                     | task, version, tag, batch, schedule, error, test flag, queue, region, machine, created-at range | keyset cursor on `(created_at, run_id)`       | **yes**, `page[before]`    | fixed, created-at descending                                | none in the list                        | ClickHouse for the ids, Postgres for the rows                                                |
| Inngest 1.52           | several                                     | app, function, a time range on a chosen time field, a CEL expression over event data and output | keyset cursor                                 | no                         | caller picks queued, started or ended time, and a direction | GraphQL `totalCount`, by a second query | SQLite or Postgres (self-hosted); ClickHouse on Cloud (unverified beyond an incident report) |
| Temporal 1.32          | any boolean expression                      | any search attribute, with SQL-like operators                                                   | opaque next-page token                        | no                         | fixed on SQL and Cloud; `ORDER BY` on Elasticsearch only    | a separate count call, approximate      | a separate visibility store: Elasticsearch or SQL                                            |
| pg-boss 12.38 (core)   | a boolean "queued only"                     | id, singleton key, data containment; one queue per call                                         | keyset, `after` a job id                      | no                         | fixed, created ascending                                    | cached per-queue counts                 | Postgres, state is a column                                                                  |
| pg-boss dashboard      | one state, a "pending" group, or all        | id, queues, minimum retry count, data and output containment                                    | keyset cursor                                 | **yes**, `before`          | newest created, or newest finished for finished states      | cached; `null` for most filtered lists  | Postgres                                                                                     |
| River 0.51             | several, default all                        | ids, kinds, queues, priorities, metadata containment, tags, raw SQL                             | keyset cursor, `After`                        | no                         | caller picks id, time, scheduled-at or finalized-at         | none in the list                        | Postgres                                                                                     |
| Graphile Worker 0.18   | no list API                                 | the caller's SQL on a view                                                                      | none                                          | n/a                        | the caller's SQL                                            | the caller's SQL                        | Postgres view; successful jobs are deleted                                                   |

Two patterns stand out.

- **No Redis-backed system read here lists more than one state in one order.** Each lists one state per call, or several states one after the other, by rank offset, with no filter beyond a scan. This library's keyset cursor and its merge across tasks already go further.
- **Every system with rich filters lists from another store**: SQL, ClickHouse or Elasticsearch, fed from the engine. There the state is a column, so "several states", any sort and any filter are one query.

### 2.2 BullMQ

- `getJobs(types, start, end, asc)` takes a list of states. One Lua script applies the same `start` and `end` to each state's key in turn and returns one group per state: `for i = 5, #ARGV do … results[#results + 1] = collectJobs(stateKey, stateType) end` ([getJobs-1.lua#L145-L151](https://github.com/taskforcesh/bullmq/blob/6b7fd10ce096d7b7b402c6996ff832df1ce38a96/src/commands/getJobs-1.lua#L145-L151)). The client concatenates the groups in the order of `types` and drops repeated ids; there is no sort and no merge ([queue-getters.ts#L513-L538](https://github.com/taskforcesh/bullmq/blob/6b7fd10ce096d7b7b402c6996ff832df1ce38a96/src/classes/queue-getters.ts#L513-L538)). So a call for N states with a page of 100 can return up to N × 100 jobs.
- Pagination is by rank offset. The docs say only "It is also possible to retrieve the jobs with pagination style semantics" ([getters.md#L127-L138](https://github.com/taskforcesh/bullmq/blob/6b7fd10ce096d7b7b402c6996ff832df1ce38a96/docs/gitbook/guide/jobs/getters.md#L127-L138), published at <https://docs.bullmq.io/guide/jobs/getters>). No statement was found on offsets shifting while jobs move.
- Storage: `wait` and `active` are lists; `completed` and `failed` are sorted sets scored by the finish timestamp ([moveToFinished-14.lua#L169-L173](https://github.com/taskforcesh/bullmq/blob/6b7fd10ce096d7b7b402c6996ff832df1ce38a96/src/commands/moveToFinished-14.lua#L169-L173)); `delayed` and `prioritized` are sorted sets with packed scores ([getDelayedScore.lua](https://github.com/taskforcesh/bullmq/blob/6b7fd10ce096d7b7b402c6996ff832df1ce38a96/src/commands/includes/getDelayedScore.lua#L1-L25), [getPriorityScore.lua](https://github.com/taskforcesh/bullmq/blob/6b7fd10ce096d7b7b402c6996ff832df1ce38a96/src/commands/includes/getPriorityScore.lua#L5-L8)). This is the same model as this library's: one container per state, each with its own order.
- No filter by name, time, failure reason, attempts or data exists in the open-source getters ([queue-getters.ts#L503-L539](https://github.com/taskforcesh/bullmq/blob/6b7fd10ce096d7b7b402c6996ff832df1ce38a96/src/classes/queue-getters.ts#L503-L539)). BullMQ Pro documents getters per group, still by offset ([groups/getters.md](https://github.com/taskforcesh/bullmq/blob/6b7fd10ce096d7b7b402c6996ff832df1ce38a96/docs/gitbook/bullmq-pro/groups/getters.md#L1-L36)); its source is closed.
- Counts: one script, `LLEN` or `ZCARD` per state ([getCounts-1.lua#L16-L43](https://github.com/taskforcesh/bullmq/blob/6b7fd10ce096d7b7b402c6996ff832df1ce38a96/src/commands/getCounts-1.lua#L16-L43)).
- Retention trims the finished sets when a job finishes, by age and by count ([removeJobsByMaxAge.lua](https://github.com/taskforcesh/bullmq/blob/6b7fd10ce096d7b7b402c6996ff832df1ce38a96/src/commands/includes/removeJobsByMaxAge.lua#L9-L24), [removeJobsByMaxCount.lua](https://github.com/taskforcesh/bullmq/blob/6b7fd10ce096d7b7b402c6996ff832df1ce38a96/src/commands/includes/removeJobsByMaxCount.lua#L8-L15)).
- Docs and source disagree on `paused`: the docs list it as a status, the `JobType` union of 6.7 does not have it ([job-type.ts#L3-L11](https://github.com/taskforcesh/bullmq/blob/6b7fd10ce096d7b7b402c6996ff832df1ce38a96/src/types/job-type.ts#L3-L11)). The source wins.

### 2.3 Sidekiq, Asynq and other Redis-backed engines

- **Sidekiq.** A list per queue and three sorted sets, `schedule`, `retry` and `dead`, whose members are the whole job JSON ([client.rb#L284-L303](https://github.com/sidekiq/sidekiq/blob/1bb4aa06e5aa178a114a5e855f9f3d5c24f6c61b/lib/sidekiq/client.rb#L284-L303)). Completed jobs are not kept, only counted ([launcher.rb#L127-L132](https://github.com/sidekiq/sidekiq/blob/1bb4aa06e5aa178a114a5e855f9f3d5c24f6c61b/lib/sidekiq/launcher.rb#L127-L132)).
- The Web UI pages by page number and rank, `ZCARD` plus `ZRANGE` in one `MULTI` ([paginator.rb#L11-L59](https://github.com/sidekiq/sidekiq/blob/1bb4aa06e5aa178a114a5e855f9f3d5c24f6c61b/lib/sidekiq/paginator.rb#L11-L59)), with first, previous, next and last links.
- Its only filter is `scan(match)`: `conn.zscan(name, match: match, count: count)` against the raw member ([api.rb#L690-L705](https://github.com/sidekiq/sidekiq/blob/1bb4aa06e5aa178a114a5e855f9f3d5c24f6c61b/lib/sidekiq/api.rb#L690-L705)). The Web UI's filter box calls it and puts every match on one page ([helpers.rb#L142-L147](https://github.com/sidekiq/sidekiq/blob/1bb4aa06e5aa178a114a5e855f9f3d5c24f6c61b/lib/sidekiq/web/helpers.rb#L142-L147)). `find_job` is commented "This is a slow O(n) operation" ([api.rb#L818-L833](https://github.com/sidekiq/sidekiq/blob/1bb4aa06e5aa178a114a5e855f9f3d5c24f6c61b/lib/sidekiq/api.rb#L818-L833)). The wiki warns that the API "exposes some operations which are **not** scalable" (<https://github.com/sidekiq/sidekiq/wiki/API#notes>).
- `fetch(score)` reads a score range with `ZRANGE … BYSCORE` ([api.rb#L793-L816](https://github.com/sidekiq/sidekiq/blob/1bb4aa06e5aa178a114a5e855f9f3d5c24f6c61b/lib/sidekiq/api.rb#L793-L816)): a time range on the set's own score, as in [4.5](#45-a-time-range).
- Sidekiq Pro 8.1 adds a tag index, for batches only, and says it "requires Redis 7.4 or Valkey 9.0" (<https://github.com/sidekiq/sidekiq/wiki/Batches#tagging>). Its structure is closed source and unverified.
- **Asynq.** The Inspector has one method per state, for one queue, with `Page` and `PageSize` only ([inspector.go#L253-L526](https://github.com/hibiken/asynq/blob/d704b68a426d1d3a6c707f9661d29296e1350775/inspector.go#L253-L526)), turned into ranks ([inspect.go#L600-L616](https://github.com/hibiken/asynq/blob/d704b68a426d1d3a6c707f9661d29296e1350775/internal/rdb/inspect.go#L600-L616)). Its doc comments say archived and completed tasks come in descending order; the source reads them with a plain ascending `ZRANGE` ([inspect.go#L817-L877](https://github.com/hibiken/asynq/blob/d704b68a426d1d3a6c707f9661d29296e1350775/internal/rdb/inspect.go#L817-L877)). Not run, so the actual order is unverified.
- **RQ** and **Faktory** follow the same model: one sorted set per state read by rank ([rq registry.py#L167-L181](https://github.com/rq/rq/blob/05c797938e2e125f23363fd07a1d7b526408f43d/rq/registry.py#L167-L181), [faktory sorted_redis.go#L166-L191](https://github.com/contribsys/faktory/blob/b2d89a6088b489097a81f519d08b473bdb59b818/storage/sorted_redis.go#L166-L191)).
- **Flower**, the Celery dashboard, has filters by task name, worker, state and time, a sort key and a search. Its index is built in the dashboard process's memory from Celery events, not in Redis ([search.py#L312-L319](https://github.com/mher/flower/blob/4d4a9ac7b1b2f406610669a1b770ec8d15f7b776/flower/utils/search.py#L312-L319), [events.py#L109-L117](https://github.com/mher/flower/blob/4d4a9ac7b1b2f406610669a1b770ec8d15f7b776/flower/events.py#L109-L117)).
- None of these uses a search module.

### 2.4 Trigger.dev

- `GET /api/v1/runs` takes comma-separated lists for status, task, version, tag, queue, region and machine, plus batch, schedule, bulk action, error, test flag and a created-at range ([ApiRunListPresenter.server.ts#L30-L164](https://github.com/triggerdotdev/trigger.dev/blob/v4.8.4/apps/webapp/app/presenters/v3/ApiRunListPresenter.server.ts#L30-L164)). With no time filter the list covers the last 7 days ([v3-openapi.yaml#L2252](https://github.com/triggerdotdev/trigger.dev/blob/v4.8.4/docs/v3-openapi.yaml#L2252)).
- Pagination is a keyset cursor in both directions. The cursor is "an opaque URL-safe base64 token wrapping `{ c: createdAtMs, r: runId }`" ([runsCursor.server.ts#L1-L16](https://github.com/triggerdotdev/trigger.dev/blob/v4.8.4/apps/webapp/app/services/runsRepository/runsCursor.server.ts#L1-L16)). Forward is `(created_at, run_id) < (cursor)` ordered descending; backward flips the comparison and the order ([clickhouseRunsRepository.server.ts#L153-L175](https://github.com/triggerdotdev/trigger.dev/blob/v4.8.4/apps/webapp/app/services/runsRepository/clickhouseRunsRepository.server.ts#L153-L175)). This is the mirror of [4.3](#43-paging-backward-and-what-a-position-means), in SQL.
- On a forward page the previous cursor is set whenever a cursor came in, without checking that newer rows exist ([same file, L212](https://github.com/triggerdotdev/trigger.dev/blob/v4.8.4/apps/webapp/app/services/runsRepository/clickhouseRunsRepository.server.ts#L212)).
- The order is fixed, created-at descending. The response has `next` and `previous` and no total ([api.ts#L1589-L1595](https://github.com/triggerdotdev/trigger.dev/blob/v4.8.4/packages/core/src/v3/schemas/api.ts#L1589-L1595)).
- The list reads ClickHouse for the run ids, then Postgres for the rows ([taskRuns.ts#L440-L448](https://github.com/triggerdotdev/trigger.dev/blob/v4.8.4/internal-packages/clickhouse/src/taskRuns.ts#L440-L448), [clickhouseRunsRepository.server.ts#L317-L340](https://github.com/triggerdotdev/trigger.dev/blob/v4.8.4/apps/webapp/app/services/runsRepository/clickhouseRunsRepository.server.ts#L317-L340)). ClickHouse lags, so the code filters the status again in memory and a page can be short ([L342-L345](https://github.com/triggerdotdev/trigger.dev/blob/v4.8.4/apps/webapp/app/services/runsRepository/clickhouseRunsRepository.server.ts#L342-L345)). The run queue is Redis ([run-queue/index.ts#L2-L7](https://github.com/triggerdotdev/trigger.dev/blob/v4.8.4/internal-packages/run-engine/src/run-queue/index.ts#L2-L7)); the list does not read it.
- The dashboard adds a run id list, "root runs only" and the kind of task ([RunFilters.tsx#L129-L205](https://github.com/triggerdotdev/trigger.dev/blob/v4.8.4/apps/webapp/app/components/runs/v3/RunFilters.tsx#L129-L205)). There is no free-text search over the payload.
- Docs and source disagree on the cursor (the docs call it a run id) and on the minimum page size (docs 10, source 1). The source wins.
- Unverified: the row order of a `page[before]` page. Reading the code suggests oldest first; it was not run.

### 2.5 Inngest

- REST v1 has no "list all runs" route ([apiv1.go#L218-L224](https://github.com/inngest/inngest/blob/v1.52.0/pkg/api/apiv1/apiv1.go#L218-L224)). REST v2 has `GET /runs` with a cursor, a limit of 1 to 100, `from` and `until`, a `time_field` of queued, started or ended "used for filtering and ordering", several statuses, several apps and functions, an `order`, and `query`, a "CEL expression used to filter runs by event, output, or error data" ([service.proto#L4525-L4597](https://github.com/inngest/inngest/blob/v1.52.0/proto/api/v2/service.proto#L4525-L4597)). The response has `hasMore` and no total.
- The dashboard's GraphQL `runs(first, after, orderBy, filter)` has no `before` ([gql.query.graphql#L29-L34](https://github.com/inngest/inngest/blob/v1.52.0/pkg/coreapi/gql.query.graphql#L29-L34)). `from` is required ([gql.schema.graphql#L458-L470](https://github.com/inngest/inngest/blob/v1.52.0/pkg/coreapi/gql.schema.graphql#L458-L470)). `totalCount` runs the filter again as a count ([runs_v2.go#L313-L321](https://github.com/inngest/inngest/blob/v1.52.0/pkg/coreapi/graph/resolvers/runs_v2.go#L313-L321)).
- The cursor is a keyset on the ordered time fields plus the run id ([cqrs.go#L2624-L2648](https://github.com/inngest/inngest/blob/v1.52.0/pkg/cqrs/manager/cqrs.go#L2624-L2648)).
- The CEL filter is partly applied after the SQL query, in Go, and the SQL `LIMIT` is dropped when it is present ([cqrs.go#L2715-L2731](https://github.com/inngest/inngest/blob/v1.52.0/pkg/cqrs/manager/cqrs.go#L2715-L2731), [L2762-L2764](https://github.com/inngest/inngest/blob/v1.52.0/pkg/cqrs/manager/cqrs.go#L2762-L2764)). That is read and discard, on SQL.
- The list reads the `trace_runs` table, on Postgres when a Postgres URI is set and on SQLite otherwise ([cqrs.go#L2676-L2682](https://github.com/inngest/inngest/blob/v1.52.0/pkg/cqrs/manager/cqrs.go#L2676-L2682), [devserver.go#L238-L261](https://github.com/inngest/inngest/blob/v1.52.0/pkg/devserver/devserver.go#L238-L261)), with one composite index per sortable time field ([schema.sql#L508-L520](https://github.com/inngest/inngest/blob/v1.52.0/pkg/db/postgres/schema.sql#L508-L520)). The queue and the run state are in Redis ([devserver.go#L336-L349](https://github.com/inngest/inngest/blob/v1.52.0/pkg/devserver/devserver.go#L336-L349)); the list does not read them.
- Unverified: the store behind Inngest Cloud's run list. Its code is closed. An incident report of the company names ClickHouse for run history (<https://www.inngest.com/blog/2025-10-24-october-incident-report>); that page was read through a summarising fetch, so its wording is not quoted here.

### 2.6 Temporal

- Listing takes a **List Filter**, "an SQL-like string parameter", over **search attributes** such as `WorkflowType`, `ExecutionStatus`, `StartTime`, `CloseTime` and `ExecutionDuration` ([list-filter.mdx#L16-L33](https://github.com/temporalio/documentation/blob/735c37c81c06d7c08b959557b47cbef56eed5719/docs/encyclopedia/visibility/list-filter.mdx#L16-L33), [search-attributes.mdx#L66-L93](https://github.com/temporalio/documentation/blob/735c37c81c06d7c08b959557b47cbef56eed5719/docs/encyclopedia/visibility/search-attributes.mdx#L66-L93)).
- The order is fixed for most users: "The **ORDER BY** operator is currently not supported in Temporal Cloud" and the default is "`ClosedTime DESC NULL FIRST`, `StartTime DESC`" ([list-filter.mdx#L37-L41](https://github.com/temporalio/documentation/blob/735c37c81c06d7c08b959557b47cbef56eed5719/docs/encyclopedia/visibility/list-filter.mdx#L37-L41)). The SQL store refuses `ORDER BY` ([visibility_store.go#L970-L973](https://github.com/temporalio/temporal/blob/v1.32.2/common/persistence/visibility/store/sql/visibility_store.go#L970-L973)). So open runs come first, then closed runs by close time: a grouped order, as S2.
- Pagination is a next-page token, forward only ([request_response.proto#L1076-L1086](https://github.com/temporalio/api/blob/v1.64.3/temporal/api/workflowservice/v1/request_response.proto#L1076-L1086)). On SQL the token is a keyset of close time, start time and run id ([visibility_store.go#L436-L451](https://github.com/temporalio/temporal/blob/v1.32.2/common/persistence/visibility/store/sql/visibility_store.go#L436-L451)).
- Counts are a separate call and "The Count API returns approximate counts" ([visibility.mdx#L36-L48](https://github.com/temporalio/documentation/blob/735c37c81c06d7c08b959557b47cbef56eed5719/docs/encyclopedia/visibility/visibility.mdx#L36-L48)).
- The list reads a separate **visibility store**. Before server 1.20 the filtered list needed Elasticsearch; since 1.20 it also runs on MySQL 8.0.17+, PostgreSQL 12+ and SQLite 3.31+ ([visibility.mdx#L95-L97](https://github.com/temporalio/documentation/blob/735c37c81c06d7c08b959557b47cbef56eed5719/docs/encyclopedia/visibility/visibility.mdx#L95-L97), [self-hosted visibility#L28-L35](https://github.com/temporalio/documentation/blob/735c37c81c06d7c08b959557b47cbef56eed5719/docs/production-deployment/self-hosted-guide/visibility/index.mdx#L28-L35)).
- Why it is separate, in the docs' words: "Visibility is a search index that Temporal updates asynchronously … Treat Visibility as eventually consistent", and "Visibility is built for finding and filtering across many executions, not for reading the current state of one execution" ([visibility.mdx#L61-L67](https://github.com/temporalio/documentation/blob/735c37c81c06d7c08b959557b47cbef56eed5719/docs/encyclopedia/visibility/visibility.mdx#L61-L67)). The docs give no further rationale.

### 2.7 SQL-backed engines

- **pg-boss.** The state is an enum column of one `job` table ([plans.ts#L52-L59](https://github.com/timgit/pg-boss/blob/12.38.2/src/plans.ts#L52-L59)). The core `findJobs(name, options)` is per queue, filters by id, singleton key and top-level data, and pages forward with a keyset `(created_on, id)` ([plans.ts#L5034-L5073](https://github.com/timgit/pg-boss/blob/12.38.2/src/plans.ts#L5034-L5073)).
- Its dashboard package pages by keyset in both directions. A backward read reverses the `ORDER BY`, flips the comparison, reads `limit + 1` rows and reverses them back ([keyset.server.ts#L66-L140](https://github.com/timgit/pg-boss/blob/12.38.2/packages/dashboard/app/lib/keyset.server.ts#L66-L140)). The file states why: "a page costs the same however deep it is, and rows inserted or deleted between pages do not shift the next one" ([keyset.server.ts#L1-L5](https://github.com/timgit/pg-boss/blob/12.38.2/packages/dashboard/app/lib/keyset.server.ts#L1-L5)).
- It avoids `COUNT(*)`: counts come from cached per-queue statistics, and most filtered lists report `null` ([queries.server.ts#L698-L720](https://github.com/timgit/pg-boss/blob/12.38.2/packages/dashboard/app/lib/queries.server.ts#L698-L720)).
- Even on SQL, a list across queues is a per-queue read merged, because "No index orders the job table by created_on alone" ([queries.server.ts#L583-L634](https://github.com/timgit/pg-boss/blob/12.38.2/packages/dashboard/app/lib/queries.server.ts#L583-L634)). That is this library's merge across tasks.
- **River.** `JobList` takes several states (all eight by default), kinds, queues, priorities, metadata, tags, an order field with a direction, and `After(cursor)`; no `Before` and no total ([job_list_params.go#L112-L157](https://github.com/riverqueue/river/blob/v0.51.2/job_list_params.go#L112-L157), [L396-L575](https://github.com/riverqueue/river/blob/v0.51.2/job_list_params.go#L396-L575)).
- River's "order by time" picks the time column from the first listed state: scheduled-at for waiting states, attempted-at for running, finalized-at for finished ([job_list_params.go#L139-L156](https://github.com/riverqueue/river/blob/v0.51.2/job_list_params.go#L139-L156), [L590-L602](https://github.com/riverqueue/river/blob/v0.51.2/job_list_params.go#L590-L602)). So River, on SQL, also has one time per group of states, the same four meanings as this library's score families.
- **Graphile Worker** has no list API. It documents a `jobs` view to query with SQL, leaves the payload out of it "to avoid people from performing expensive filtering using it", and deletes a job when it completes ([jobs-view.md#L6-L65](https://github.com/graphile/worker/blob/v0.18.0/website/docs/jobs-view.md#L6-L65), [scaling.md#L28-L30](https://github.com/graphile/worker/blob/v0.18.0/website/docs/scaling.md#L28-L30)).

## 3. The primitives, on Redis 7.4 and Valkey 8.0 and later

Link bases, each pinned: `redis-docs:` is `https://github.com/redis/docs/blob/fa24ef7cd06f011fc0b58c4109fba77a6ad819fd/content/`, `valkey-doc:` is `https://github.com/valkey-io/valkey-doc/blob/5e6ab07802d0754e60dab951a73c2f51340fcaf9/`. Server sources are linked at a tag. Each command's `since` and complexity come from `src/commands/<command>.json` of Redis 8.10.3 and Valkey 9.1.3, lines 4 and 6.

### 3.1 Sorted-set commands

| Command                                                                     | Since                | Redis 7.4 and 8 | Valkey 8 and 9 | Documented complexity                                                 | In a read-only script |
| --------------------------------------------------------------------------- | -------------------- | --------------- | -------------- | --------------------------------------------------------------------- | --------------------- |
| `ZRANGE key a b [BYSCORE \| BYLEX] [REV] [LIMIT offset count] [WITHSCORES]` | 1.2.0; options 6.2.0 | yes             | yes            | O(log(N)+M)                                                           | yes                   |
| `ZRANK`, `ZREVRANK` (`WITHSCORE` since 7.2.0)                               | 2.0.0                | yes             | yes            | O(log(N))                                                             | yes                   |
| `ZCOUNT`, `ZLEXCOUNT`                                                       | 2.0.0, 2.8.9         | yes             | yes            | O(log(N))                                                             | yes                   |
| `ZCARD`, `ZSCORE`                                                           | 1.2.0                | yes             | yes            | O(1)                                                                  | yes                   |
| `ZMSCORE`                                                                   | 6.2.0                | yes             | yes            | O(N), N the members asked                                             | yes                   |
| `ZUNION`, `ZUNIONSTORE`                                                     | 6.2.0, 2.0.0         | yes             | yes            | O(N)+O(M log(M)), N the **sum of the input sizes**, M the result size | `ZUNION` only         |
| `ZINTER`, `ZINTERSTORE`                                                     | 6.2.0, 2.0.0         | yes             | yes            | O(N\*K)+O(M\*log(M)), N the smallest input, K the number of inputs    | `ZINTER` only         |
| `ZINTERCARD … [LIMIT n]`                                                    | 7.0.0                | yes             | yes            | O(N\*K)                                                               | yes                   |
| `ZRANGESTORE`                                                               | 6.2.0                | yes             | yes            | O(log(N)+M)                                                           | no, it writes         |
| `ZSCAN`                                                                     | 2.8.0                | yes             | yes            | O(1) per call, O(N) for a full iteration                              | yes                   |

([redis zunionstore.json](https://github.com/redis/redis/blob/8.10.3/src/commands/zunionstore.json#L4-L6), [valkey zunionstore.json](https://github.com/valkey-io/valkey/blob/9.1.3/src/commands/zunionstore.json#L4-L6); the other files sit beside them.)

- **The sorted-set command set is the same on both servers.** Redis 8.10.3 and Valkey 9.1.3 list the same 35 sorted-set commands, all from 7.0.0 or earlier (<https://redis.io/docs/latest/commands/?group=sorted-set>, <https://valkey.io/commands/#sorted-set>). Everything this note needs is at the floor.
- **No command merges several sorted sets with a limit.** `ZUNION`, `ZINTER`, `ZDIFF` and their `STORE` forms take no `LIMIT`. A union reads every member of every input: the code iterates all the inputs, then inserts every result ([redis t_zset.c#L3083-L3131](https://github.com/redis/redis/blob/8.10.3/src/t_zset.c#L3083-L3131), [valkey t_zset.c#L2787-L2841](https://github.com/valkey-io/valkey/blob/9.1.3/src/t_zset.c#L2787-L2841)). Measured in [section 7](#7-experiments).
- **A stored union is a write.** `ZUNIONSTORE` and `ZRANGESTORE` are write commands. A script that calls one cannot run as a read-only script or on a replica ("Write commands are not allowed from read-only scripts", [redis script.c#L426-L431](https://github.com/redis/redis/blob/8.10.3/src/script.c#L426-L431)), its writes go to the replicas and the append-only file ([eval-intro.md#L277](https://github.com/redis/docs/blob/fa24ef7cd06f011fc0b58c4109fba77a6ad819fd/content/develop/programmability/eval-intro.md#L277)), and it is refused over `maxmemory`. `UNLINK` frees a large key in another thread; `DEL` blocks ([unlink.md](https://github.com/redis/docs/blob/fa24ef7cd06f011fc0b58c4109fba77a6ad819fd/content/commands/unlink.md)).
- **One slot.** "Commands performing complex multi-key operations like set unions and intersections are implemented for cases where all of the keys involved in the operation hash to the same slot" ([cluster-spec.md#L38-L40](https://github.com/redis/docs/blob/fa24ef7cd06f011fc0b58c4109fba77a6ad819fd/content/operate/oss_and_stack/reference/cluster-spec.md#L38-L40), [Valkey's](https://github.com/valkey-io/valkey-doc/blob/5e6ab07802d0754e60dab951a73c2f51340fcaf9/topics/cluster-spec.md#L23-L26)). The one hash tag of ADR 0002 satisfies it.
- **`LIMIT offset`: the docs and the source disagree, and the source is better.** Both doc sets say "if `<offset>` is large, the sorted set needs to be traversed for `<offset>` elements before getting to the elements to return, which can add up to O(N) time complexity" ([redis zrange.md#L197](https://github.com/redis/docs/blob/fa24ef7cd06f011fc0b58c4109fba77a6ad819fd/content/commands/zrange.md#L197), [valkey zrange.md#L14](https://github.com/valkey-io/valkey-doc/blob/5e6ab07802d0754e60dab951a73c2f51340fcaf9/commands/zrange.md#L14)). Since Redis 7.4.0 and Valkey 8.0.0, exactly the floor, the code jumps to the offset by rank: `zslNthInRange` ([redis 7.4.0 t_zset.c#L333-L379](https://github.com/redis/redis/blob/7.4.0/src/t_zset.c#L333-L379)); the release notes say "`ZRANGE ... LIMIT`: improved performance" ([Redis 7.4.0 notes, L113](https://github.com/redis/redis/blob/7.4.0/00-RELEASENOTES#L113)) and "Optimize `ZRANGE` offset location from linear search to skip list jump" ([Valkey 8.0.0 notes, L215](https://github.com/valkey-io/valkey/blob/8.0.0/00-RELEASENOTES#L215)). The small listpack encoding (128 entries at most by default) still walks.
- **Order among equal scores.** "Elements with the same score are ordered lexicographically", and with `REV` "score ties are resolved by reverse lexicographical ordering" ([redis zrange.md#L192-L194](https://github.com/redis/docs/blob/fa24ef7cd06f011fc0b58c4109fba77a6ad819fd/content/commands/zrange.md#L192-L194), [valkey zrange.md#L9-L11](https://github.com/valkey-io/valkey-doc/blob/5e6ab07802d0754e60dab951a73c2f51340fcaf9/commands/zrange.md#L9-L11)). The comparison is by bytes, `memcmp`, the longer string greater on a shared prefix ([redis indexes page, L194-L203](https://github.com/redis/docs/blob/fa24ef7cd06f011fc0b58c4109fba77a6ad819fd/content/develop/clients/patterns/indexes/index.md#L194-L203)). This is what the page script's byte comparison relies on.
- **Scores are doubles**, exact for integers up to 2^53 ([redis zadd.md#L199](https://github.com/redis/docs/blob/fa24ef7cd06f011fc0b58c4109fba77a6ad819fd/content/commands/zadd.md#L199), [valkey zadd.md#L31](https://github.com/valkey-io/valkey-doc/blob/5e6ab07802d0754e60dab951a73c2f51340fcaf9/commands/zadd.md#L31)). Epoch milliseconds and the `order` counter both fit.
- **Memory.** Neither project documents a byte figure per sorted-set entry. The layout changes between versions: Valkey 8.1 replaced the dictionary of sorted sets ([8.1.0 notes, L109](https://github.com/valkey-io/valkey/blob/8.1.0/00-RELEASENOTES#L109)), Valkey 9.1 embeds the element in the skiplist node ([9.1.0 notes, L92](https://github.com/valkey-io/valkey/blob/9.1.0/00-RELEASENOTES#L92)), and the Valkey 9.2 release candidate replaces the skiplist of large sorted sets with a B+ tree ([9.2.0-rc2 notes, L205](https://github.com/valkey-io/valkey/blob/9.2.0-rc2/00-RELEASENOTES#L205)). So the figures of [section 7](#7-experiments) hold for the two versions measured only.

### 3.2 Keyset pagination with equal scores

- No single command reads "after `(score, member)`" in a sorted set with mixed scores. `BYSCORE` takes scores only. `BYLEX` is for equal scores: "If the elements in the sorted set have different scores, the returned elements are unspecified" ([redis zrangebylex.md#L73](https://github.com/redis/docs/blob/fa24ef7cd06f011fc0b58c4109fba77a6ad819fd/content/commands/zrangebylex.md#L73), [valkey zrangebylex.md#L3](https://github.com/valkey-io/valkey-doc/blob/5e6ab07802d0754e60dab951a73c2f51340fcaf9/commands/zrangebylex.md#L3)).
- The exact seek is built from three `O(log N)` commands: `ZCOUNT` to count the higher scores, a read by rank, and `ZRANK` or `ZREVRANK` when the member still exists. The current script uses the first two and a binary search among the equal scores.
- Neither project documents a pagination recipe beyond the `LIMIT` paragraph above. The forward and backward forms in this note are derived from the documented order, and tested in [section 7](#7-experiments).
- A trap in the backward form: a stop rank of `-1` means "the last element", so a page that ends before rank 0 must return nothing instead of calling `ZRANGE key 0 -1`.
- Both directions are read-only, so one script covers the seek and the read at one instant.

### 3.3 Secondary indexes in the official documentation

The page is "Secondary indexing" at <https://redis.io/docs/latest/develop/clients/patterns/indexes/> ([source](https://github.com/redis/docs/blob/fa24ef7cd06f011fc0b58c4109fba77a6ad819fd/content/develop/clients/patterns/indexes/index.md)); Valkey keeps the same text at <https://valkey.io/topics/indexing/> ([source](https://github.com/valkey-io/valkey-doc/blob/5e6ab07802d0754e60dab951a73c2f51340fcaf9/topics/indexing.md)), without the search section.

- **A numeric index** is a sorted set with the field as the score and the object id as the member: "it is possible to store just the ID of the object", and then "objects can change without touching the index, as long as we don't change the indexed field" (L104-L127). Every run index of this library is this pattern.
- **Updating** it: "re-adding back an element with a different score and the same value will simply update the score" (L141-L152).
- **A lexicographic index** puts every score at 0 and reads a prefix with `ZRANGE myindex "[bit" "[bit\xff" BYLEX` (L209-L274). This is option I3.
- **A composite index** joins several fields into the member, padded so that byte order is numeric order: "Here the fields are `room:price:product_id`" (L408-L458). Its limit: "Its effectiveness depends on the order of the fields and the queries I want to run" (L466-L471). A member such as `<reason>:<padded time>:<run id>` would serve one query shape and no other.
- When the indexed value is inside the member, an update must remove the old member first, which needs a place to remember it (L480-L497).
- **Inconsistency is expected**: "in the course of months or years it is possible that inconsistencies are added because of software bugs, network partitions or other events", and "the `SCAN` family of commands can be used in order to verify, update or rebuild the index from scratch, incrementally" (L755-L765). That is the backfill and the sweep of [4.13](#413-rolling-out-a-new-index).
- The page describes no index bucketed by time. It does not tell readers to prefer the query engine; it lists Redis Search as one option (L25-L53).

### 3.4 Search modules

**A library that must run on plain Redis and on plain Valkey cannot depend on either search module.**

- **Redis.** The query engine (`FT.CREATE`, `FT.SEARCH`, `FT.AGGREGATE`) is part of Redis from 8.0: "Stand alone RediSearch, RedisJSON, RedisTimeSeries, and RedisBloom are no longer needed as they are now part of Redis" ([Redis 8.0.0 notes, L8-L30](https://github.com/redis/redis/blob/8.0.0/00-RELEASENOTES#L8-L30)). Redis 7.4, the floor, has it only as a separately loaded module. A build of Redis 8 from source without the modules cloned is "a core-only build" ([README.md#L304-L320](https://github.com/redis/redis/blob/8.10.3/README.md#L304-L320)).
- It indexes hashes and JSON documents with `TEXT`, `TAG`, `NUMERIC`, `GEO`, `VECTOR` and `GEOSHAPE` fields ([indexes page, L37-L53](https://github.com/redis/docs/blob/fa24ef7cd06f011fc0b58c4109fba77a6ad819fd/content/develop/clients/patterns/indexes/index.md#L37-L53)). Existing documents are indexed in the background ([indexing page, L51](https://github.com/redis/docs/blob/fa24ef7cd06f011fc0b58c4109fba77a6ad819fd/content/develop/ai/search-and-query/indexing/_index.md#L51)).
- **Valkey.** Search is a separate module, `valkey-search`, loaded with `--loadmodule` ([README.md#L169-L173](https://github.com/valkey-io/valkey-search/blob/1.2.1/README.md#L169-L173)). The plain `valkey/valkey` image builds no module ([Dockerfile.template](https://github.com/valkey-io/valkey-container/blob/e3be9b56fb26039e4aaf5788cf419740fbdaca54/Dockerfile.template)); the separate `valkey-bundle` image carries it (<https://valkey.io/topics/valkey-bundle/>).
- What it indexes, by release ([release notes of 1.2.1](https://github.com/valkey-io/valkey-search/blob/1.2.1/00-RELEASENOTES), [of 1.1.1](https://github.com/valkey-io/valkey-search/blob/1.1.1/00-RELEASENOTES#L20-L33), [of 1.0.3](https://github.com/valkey-io/valkey-search/blob/1.0.3/00-RELEASENOTES#L39-L88)):

| valkey-search | Needs Valkey | Indexes                                              | Notes                                                                               |
| ------------- | ------------ | ---------------------------------------------------- | ----------------------------------------------------------------------------------- |
| 1.0           | 8.1.1        | vectors, with numeric and tag fields beside a vector | a vector field is required                                                          |
| 1.1           | 9.0.1        | vector, numeric, tag                                 | "Indexes and query operations no longer require a vector component"; `FT.AGGREGATE` |
| 1.2 (latest)  | 9.0.1        | vector, numeric, tag, text                           | `SORTBY` on `FT.SEARCH`                                                             |

- The bundle for Valkey 8.1 and 9.0 ships valkey-search 1.0.3, the bundle for 9.1 ships 1.2.1 ([versions.json](https://github.com/valkey-io/valkey-bundle/blob/5601e9ca68e5c450987d1b696873cf91ca26b706/versions.json)). So on Valkey 8, the floor, no released search module can index runs without a vector field.
- Its index is updated "by background threads" ([search.md#L44-L58](https://github.com/valkey-io/valkey-search/blob/1.2.1/docs/topics/search.md#L44-L58)): a read just after a write may not see it.
- The two dialects differ. valkey-search 1.2 says only that some parameters are accepted to be "more interoperable with RediSearch" ([ft.create.md#L50](https://github.com/valkey-io/valkey-search/blob/1.2.1/docs/commands/ft.create.md#L50)).
- Unverified: whether managed services (ElastiCache, MemoryDB and others) expose either module. No official page could be read.

### 3.5 `SCAN` and `SORT`

- `SCAN`, `HSCAN`, `SSCAN` and `ZSCAN` cost "O(1) for every call. O(N) for a complete iteration". `MATCH` filters after the fact: "the **MATCH** filter is applied after elements are retrieved from the collection, just before returning data to the client. This means that if the pattern matches very little elements inside the collection, `SCAN` will likely return no elements in most iterations" ([redis scan.md#L202](https://github.com/redis/docs/blob/fa24ef7cd06f011fc0b58c4109fba77a6ad819fd/content/commands/scan.md#L202), [valkey scan.md#L113](https://github.com/valkey-io/valkey-doc/blob/5e6ab07802d0754e60dab951a73c2f51340fcaf9/commands/scan.md#L113)).
- `COUNT` is "just a hint". "A given element may be returned multiple times." Elements that come or go during the iteration "may be returned or not: it is undefined" (same pages, L158-L178 and L66-L86).
- So a scan cannot give an ordered page: no order, no bound on the calls per match, possible repeats. It can find the matches of a rare pattern in a bounded index, at the cost of visiting all of it.
- `SORT key BY pattern->field` sorts a list, set or sorted set by a field of other hashes. It reads every element: "O(N+M\*log(M))". Since Redis 7.4 and Valkey 8.0 it works in cluster mode when the pattern has a hash tag ([redis sort.md#L277-L279](https://github.com/redis/docs/blob/fa24ef7cd06f011fc0b58c4109fba77a6ad819fd/content/commands/sort.md#L277-L279), [valkey sort.md#L108-L110](https://github.com/valkey-io/valkey-doc/blob/5e6ab07802d0754e60dab951a73c2f51340fcaf9/commands/sort.md#L108-L110)). `SORT` counts as a write; `SORT_RO` (7.0) is the read-only form. It could sort a whole index by `dispatchedAt`, at the cost of the whole index for each page.

### 3.6 Where Redis and Valkey differ at the floor

| Feature                                         | Redis           | Valkey                        | Usable by this library |
| ----------------------------------------------- | --------------- | ----------------------------- | ---------------------- |
| The 35 sorted-set commands                      | 7.0 and earlier | 7.0 and earlier               | yes                    |
| `ZRANGE … LIMIT` offset found by rank           | 7.4             | 8.0                           | yes                    |
| `SORT BY`/`GET` with a hash tag in cluster mode | 7.4             | 8.0                           | yes                    |
| `HSCAN … NOVALUES`                              | 7.4             | 8.0                           | yes                    |
| `ZSCAN … NOSCORES`                              | none            | 8.0                           | no                     |
| Hash field expiry (`HEXPIRE` and the like)      | 7.4             | 9.0                           | no                     |
| `AGGREGATE COUNT` on unions and intersections   | 8.8             | none                          | no                     |
| A built-in search engine                        | 8.0             | none (a module, see 3.4)      | no                     |
| `OBJECT ENCODING` of a large sorted set         | `skiplist`      | `skiplist`; `btree` in 9.2 RC | do not assert it       |

(Sources: the command tables and release notes of the tags above; [hscan.json at Redis 7.4.0](https://github.com/redis/redis/blob/7.4.0/src/commands/hscan.json#L61), [Valkey 8.0.0 notes, L129-L130](https://github.com/valkey-io/valkey/blob/8.0.0/00-RELEASENOTES#L129-L130).)

## 4. Options for this library

Every cost below is for one page read or one run written. "Server time" is the time the server spent executing the commands, read from `SLOWLOG` in the experiments of [section 7](#7-experiments), with 50 tasks.

### 4.1 Four ways to apply a filter

- **Pick indexes.** Choose which of the existing run indexes to read. A filter on the task or on the state is this. It costs nothing new.
- **Range on the score.** Start or stop the read at a score. It is free on the score the index already has, and impossible on any other field.
- **Read and discard.** Read the record of each candidate and drop those that do not match. The cost follows the number of candidates examined, not the number of runs returned. A page can come back short or empty while the walk is not over, which the `listRuns` contract already allows ([section 1.5](#15-how-listruns-reads-a-page-today)). No total is known without reading everything.
- **A new index.** One more sorted set, written by the engine scripts. It costs a write per run, memory per run, and a rollout over the runs that already exist ([section 4.13](#413-rolling-out-a-new-index)).

| Capability                                                                     | Way                                 | New index | Where                                                 |
| ------------------------------------------------------------------------------ | ----------------------------------- | --------- | ----------------------------------------------------- |
| Several states of one score family, in one order                               | pick indexes                        | no        | [4.2](#42-several-states-up-to-all-states), S1        |
| Several or all states, grouped by score family                                 | pick indexes                        | no        | [4.2](#42-several-states-up-to-all-states), S2        |
| All runs in one order, by dispatch time                                        | new index                           | **yes**   | [4.2](#42-several-states-up-to-all-states), S3        |
| Previous page, last page, "runs 151 to 200 of N"                               | the page script, mirrored           | no        | [4.3](#43-paging-backward-and-what-a-position-means)  |
| Ascending order                                                                | the page script, mirrored           | no        | [4.9](#49-sort-order)                                 |
| Several tasks                                                                  | pick indexes                        | no        | [4.4](#44-several-tasks)                              |
| Time range on the state's own time (`finishedAt`, start of the lock, due time) | range on the score                  | no        | [4.5](#45-a-time-range)                               |
| "Executing for more than 10 minutes", "overdue"                                | range on the score                  | no        | [4.5](#45-a-time-range)                               |
| Time range on `dispatchedAt` within a state                                    | read and discard                    | no        | [4.5](#45-a-time-range)                               |
| Time range on `dispatchedAt` at a bounded cost                                 | range on the score of the S3 index  | **yes**   | [4.5](#45-a-time-range)                               |
| Failure `reason`                                                               | read and discard, or a new index    | optional  | [4.6](#46-the-failure-reason)                         |
| Attempt count, replayed runs                                                   | read and discard                    | no        | [4.7](#47-the-attempt-count)                          |
| Runs of one emit                                                               | exists: `getEvent`                  | no        | [4.8](#48-the-runs-of-one-event-or-one-schedule)      |
| Runs of one event name                                                         | pick indexes, then discard          | no        | [4.8](#48-the-runs-of-one-event-or-one-schedule)      |
| Runs of one schedule, the last N firings                                       | pick indexes, or derive the run ids | no        | [4.8](#48-the-runs-of-one-event-or-one-schedule)      |
| Sort by a field that is not the state's score                                  | new index, one per sort key         | **yes**   | [4.9](#49-sort-order)                                 |
| A run by its exact id, task unknown                                            | one read per task                   | no        | [4.10](#410-search-by-run-id)                         |
| Runs by run id prefix                                                          | a scan, or a new index              | optional  | [4.10](#410-search-by-run-id)                         |
| Payload or error text                                                          | read and discard only               | no        | [4.11](#411-search-by-payload-or-error-text)          |
| Totals for a state, a task, a time range on the state's own time               | `ZCARD`, `ZCOUNT`                   | no        | [4.12](#412-totals-and-the-filters-other-tools-offer) |
| Totals for any read-and-discard filter                                         | not available                       | needs one | [4.12](#412-totals-and-the-filters-other-tools-offer) |

### 4.2 Several states, up to all states

The difficulty is in [section 1.2](#12-which-script-writes-each-index-and-with-which-score): the score means four different things. Six options follow.

**S1. Merge the states of one score family. No new index.**

- How: give the existing merge more indexes, tasks × states instead of tasks. It is exact when the listed states share a score meaning: `completed` + `failed` (most recently finished first), `delayed` + `suspended` + `backingOff` (latest due first), `pending` + `waitingForConcurrencySlot` (most recently queued first). A run is in one index at a time, so `(score, stored run id)` stays a total order.
- Page read: two round trips, as today. The first sends tasks × states page scripts. Measured: 100 scripts (2 states) take 2.6 to 3.2 ms of server time and 5 to 6 ms on localhost, 400 scripts take 2.9 to 4.3 ms. Up to tasks × states × `limit` entries cross the wire.
- A shortcut exists for every task at once: the global `{p}:due` already is the timed family merged, and `{p}:pending` is the pending runs merged ([section 1.1](#11-keys)). One `ZRANGE` would replace 150 page scripts. They also hold the runs of tasks the release does not define, which the reader would have to drop.
- Write path, memory, existing runs, deploy overlap: nothing changes.
- Cursor: the `state` field becomes the list of states. `listedRunOf` checks the record's state against the list instead of one state ([`read-api.ts:332`](../src/read-api.ts#L332)).
- It cannot put two families in one order.

**S2. Several families, one after the other. No new index.**

- How: walk the families in a fixed order, each merged as in S1. "All runs" is then a list in four blocks, for example executing, queued, timed, ended. Each run carries its `state`, so a dashboard can draw the block headers.
- Page read: as S1 inside a block. At a block boundary, either the page comes back short and the next call starts the next block, or the first round trip reads the head of every block at once (400 scripts, measured above).
- Cursor: one more field, the family being walked.
- This is what the Redis-backed systems do: BullMQ returns the requested states one after the other, and Sidekiq and Asynq list one structure per call ([section 2](#2-what-comparable-systems-offer)).
- It cannot give one timeline. A run that completes during the walk can be skipped or seen twice, as it can today across two calls.

**S3. One "all runs" index by dispatch time. A new index.**

- How: one more sorted set, scored by `dispatchedAt`, holding every run whatever its state. Per task (`tasks` page scripts per page, as a single-state list today, and the task filter stays "pick indexes"), or one for the whole prefix (one `ZRANGE` per page, the task filter becomes read and discard, and runs of tasks the release does not define have to be dropped).
- Write path: `createRun` adds the member ([`lua-library.ts:283-307`](../src/engine/lua-library.ts#L283-L307); it serves `enqueue`, `fire`, `fireNow` and the failure notices of `settle`), and `removeRunWhole` in `prune` removes it ([`prune-script.ts:32-35`](../src/engine/prune-script.ts#L32-L35)). Two writes in the life of a run. No other script changes: `dispatchedAt` never changes, and `replay` keeps it.
- Memory: 139 to 161 bytes per run, so 14 to 16 MB for the envelope's 100,000 runs (measured).
- Position: a run never moves in this list. New runs enter at the head, and retention removes runs anywhere. A forward walk from the head shows each run at most once and misses none that stays. No other option gives that.
- A state filter on this list is read and discard: the state is in the record, not in the index.
- A time range on `dispatchedAt` is a range on its score, and `ZCOUNT` gives the total.
- Existing runs and deploy overlap: [section 4.13](#413-rolling-out-a-new-index). This is the main cost.
- Cursor: one more field for the kind of list.
- It cannot order by latest activity, and it cannot filter by state at a bounded cost.

**S4. One index by time of the last state change. A new index, written at every transition.**

- How: as S3, but the score is the instant the run entered its current state.
- Write path: one more `ZADD` in `enterPending`, `enterDueState`, the lock in `fetch`, the park in `fetch` and `endRun` in `settle`, so in every transition of every run, `fetch` included.
- Position: a run jumps to the head at each transition, so a walk can show it twice.
- Rescoring the existing indexes instead is not open: `prune` reads the ended scores as finish times, `makeDue` reads the timed scores as due times, and an old release would keep writing the old scores during an overlap.

**S5. `ZUNIONSTORE` into a temporary key. Not viable for a page read.**

- The cost of a union follows the total size of its inputs, not the page ([section 3.1](#31-sorted-set-commands)). Measured: 54 ms (Valkey 8.1) and 83 ms (Redis 7.4) for 100 indexes holding 100,000 members; 625 ms and 796 ms for 550,000 members. The server serves nobody else meanwhile.
- The temporary key took 14 to 16 MB, and 78 to 91 MB at 550,000 members.
- It turns a read into a write, which a replica refuses.
- It does not make the scores of two families comparable either.
- By contrast, the page scripts over the same 550,000 members took 3.6 to 3.8 ms.

**S6. A search module.** `FT.SEARCH` over the record hashes would answer every filter in this note. It is not available on the supported floor ([section 3.4](#34-search-modules)).

### 4.3 Paging backward, and what a position means

**The mirror of the page script. No new index.**

- How: the forward script finds the rank of the first entry after a position. The mirror finds the rank of the first entry at or after the position, with the same two `ZCOUNT` and the same binary search, the comparison turned around, and reads the `limit` ranks before it. Across several indexes, the client merges and keeps the last `limit` entries instead of the first.
- Tested in [section 7](#7-experiments) on Valkey 8.1 and Redis 7.4: on a sorted set of 500 members with 14 distinct scores, the backward walk returned exactly the entries of the forward walk; and pages before and after a position whose member had been removed landed on the right ranks.
- Page read: the same as forward. Measured: 0.35 to 2.6 ms of server time for 50 to 400 indexes.
- **Last page.** The mirror with no position returns the lowest `limit` entries. So "first page" and "last page" both need no cursor.
- **Is there a previous page, and where am I.** The forward script already computes, per index, how many entries come before the position. Returned and summed over the indexes, that is the offset of the page in the merged list, exact at the time of the read: a dashboard can show "runs 151 to 200 of 12,340" and disable "previous" with no extra command. The total is the sum of `ZCARD`.
- **Jumping to page N** is the reverse question, an offset to a position. On one index it is a read by rank. Over several indexes it needs a search for the score at which the summed ranks reach the offset. That is possible in a script and was not measured.
- Write path, memory, existing runs, deploy overlap: nothing changes.
- Cursor: a cursor is a position, and a position serves both directions. If the direction is an option of the call (as Trigger.dev's `page[after]` and `page[before]`), the format does not change. If `previousCursor` and `nextCursor` each carry their direction, it takes one more field.

**What a position means while runs move.** Facts from [section 1.2](#12-which-script-writes-each-index-and-with-which-score):

- A cursor is a value, `(score, stored run id)`, not a rank and not a member that must exist. It stays valid after its run leaves the index. The pg-boss dashboard makes the same choice for the same reason: "the cursor carries the boundary row's key values, so paging on past a row that has since been deleted still works" ([keyset.server.ts#L1-L5](https://github.com/timgit/pg-boss/blob/12.38.2/packages/dashboard/app/lib/keyset.server.ts#L1-L5)).
- A run that stays in the listed state never moves. A walk shows it once.
- Where new entries arrive depends on the family. Ended and executing: at the head, since the score is "now" (a server clock stepping back is the exception). Timed: anywhere, since due times are arbitrary. Queued: at the head for a fresh `order`; a yielded, handed-back, stalled or woken run re-enters at its old place.
- So in an ended list, "the first page" changes at every finished run. A forward walk from a cursor neither repeats nor skips a run that stays. A backward walk from the same cursor shows what finished since.
- "The last page" of an ended list is where retention removes runs, so it drains while it is read.
- A run that leaves the state and comes back further along the walk is listed again; one that leaves is skipped. This is today's documented behaviour ([`docs/dashboard.md`](../docs/dashboard.md#page-through-failed-runs)), and neither direction changes it.
- Only the S3 list never moves a run.

### 4.4 Several tasks

- How: pick the indexes of the listed tasks. Fewer page scripts than "every task".
- Cost, write path, memory, existing runs: nothing new.
- Cursor: `listedTask` becomes a list of names, or `null` for every task. With many long names the cursor grows; a hash of the list is shorter but ends the error messages that name the task ([`run-list-cursor.ts:60-64`](../src/run-list-cursor.ts#L60-L64)).
- Types: the listed runs are typed by the union of the tasks.

### 4.5 A time range

**On the state's own time. No new index.**

- How: `ZCOUNT key (until +inf` gives the first rank for an upper bound, and the walk ends at the first entry below the lower bound. Two more `O(log N)` commands per index at most.
- What the range means per family: `finishedAt` for ended runs; the start of the current lock for executing runs; the due time for timed runs.
- It gives "executing for more than 10 minutes" (upper bound at now minus 10 minutes) and "overdue" (timed runs with a due time before now).
- It does not exist for queued runs. Their score is a counter, and the record holds no time at which the run became pending ([section 1.4](#14-the-record)).
- Total: one `ZCOUNT` per index. Measured: 100 `ZCOUNT` take 0.31 ms of server time.
- Cursor: the bounds are filters. Either they go into the cursor, to refuse a cursor used with other bounds, or they are free to change between pages.

**On `dispatchedAt`.**

- Within a state list: read and discard. The field is already read for each listed run. The walk cannot stop early on the upper bound. On the lower bound it can for ended runs, since a run finishes after it is dispatched.
- At a bounded cost: the S3 index, whose score is `dispatchedAt`.

### 4.6 The failure reason

`reason` has four values and exists on failed runs only ([`src/run-states.ts:1`](../src/run-states.ts#L1)).

**R1. Read and discard. No new index.**

- How, client side: `listRuns` already reads `reason` for each listed run. The call drops the others and returns a short page with a cursor.
- How, in a script: one script walks the failed index, reads `reason` from each record, and stops at `limit` matches or at a fixed budget of records examined. One round trip instead of many.
- Measured, one task with 1,000 failed runs, 1 % of them `stalled` and 9 % `non-retriable`: 1.9 to 2.5 ms of server time to examine 1,000 records and find the 10 `stalled`; 0.8 to 1.0 ms to examine 550 and find 50 `non-retriable`; 0.1 ms when most runs match. About 2 to 2.5 microseconds per record examined.
- From that rate, a full failed index at the 10,000 cap is 20 to 25 ms for one task, and 50 tasks are 1 to 1.3 s. This is computed, not measured. A budget per call keeps each script short and spreads the walk over several calls.
- Client side, 1,000 pipelined `HMGET` took 0.7 to 1.2 ms of server time and 5 to 10 ms on localhost.
- Cursor: the position in the failed index, as today, plus the filter.
- It gives no total per reason, and a rare reason means many records examined per run shown.

**R2. One index per task and reason. A new index.**

- How: four more sorted sets per task, scored by `finishedAt`.
- Write path: `failRun` in `settle` adds the member ([`settle-script.ts:92-95`](../src/engine/settle-script.ts#L92-L95)). `replay` and `prune` remove it, and each must read `reason` before it deletes it ([`replay-script.ts:32-34`](../src/engine/replay-script.ts#L32-L34), [`prune-script.ts:32-35`](../src/engine/prune-script.ts#L32-L35)). Three scripts change.
- Memory: 139 to 161 bytes per failed run, at most 1.6 MB per task at the 10,000 cap. A small index stays in the compact encoding: 10 members took 960 to 984 bytes.
- Page read: as any single-state list. Total per reason: `ZCARD`.
- Existing runs and deploy overlap: [section 4.13](#413-rolling-out-a-new-index).

### 4.7 The attempt count

- `attempt` is in the record and already read for each listed run. A filter such as "attempt above 1" is read and discard, with the costs of R1.
- An index by attempt would be rewritten at every retry ([`settle-script.ts:117`](../src/engine/settle-script.ts#L117)).
- `replay` resets `attempt` to 1 ([`replay-script.ts:35`](../src/engine/replay-script.ts#L35)); `replays` counts the replays and is one more `HMGET` field.
- The `backingOff` list already is "the runs retrying now".
- Among the systems of section 2, only the pg-boss dashboard has such a filter, a minimum retry count, as a SQL condition ([queries.server.ts#L538](https://github.com/timgit/pg-boss/blob/12.38.2/packages/dashboard/app/lib/queries.server.ts#L538)).

### 4.8 The runs of one event or one schedule

- **One emit.** `getEvent(event, eventId)` reads every listener run of one emit in one round trip, because the run ids are derived from the event id ([`read-api.ts:468-486`](../src/read-api.ts#L468-L486)).
- **One event name.** A listener is a task subscribed to one event, so the runs of an event are the runs of its listener tasks: [4.4](#44-several-tasks), with the listener names the instance already knows ([`read-api.ts:140`](../src/read-api.ts#L140)). One exception: a listener can also be dispatched directly, and such a run has no `eventName` ([`docs/tasks-and-events.md`](../docs/tasks-and-events.md#dispatch-a-task)). Leaving those out is read and discard on `eventName`, one more `HMGET` field.
- **One schedule.** A schedule belongs to one task, so its runs are that task's runs: the `task` filter. A run started by `dispatch` is among them; a firing's run has `scheduledAt` ([`lua-library.ts:359`](../src/engine/lua-library.ts#L359)). Keeping the firings only is read and discard on `scheduledAt`.
- **The last N firings, with no index at all.** The run id of a firing is derived from the task and the planned instant ([`src/ids.ts:38-40`](../src/ids.ts#L38-L40)), which is how `listSchedules` names `previousFiring.runId` ([`read-api.ts:503-509`](../src/read-api.ts#L503-L509)). The N planned instants before now come from the cron (`cron-parser` has `prev()`), so N run ids, so N record reads in one round trip. A firing whose run retention removed, or that never fired, reads as `undefined`. It follows the current cron only: firings under an earlier cron are not found this way.

### 4.9 Sort order

- **Ascending instead of descending** on the same index is the mirror of [4.3](#43-paging-backward-and-what-a-position-means) read the other way. No new index. It gives "pending, next to run first" and "delayed, soonest due first".
- **Another sort key** (dispatch time, attempt, duration) needs an index scored by that key, or reading every run and sorting. S3 is that index for dispatch time. Duration is not stored.

### 4.10 Search by run id

A run id is a random UUID, or any text the app passes to `dispatch` ([`docs/tasks-and-events.md`](../docs/tasks-and-events.md#run-once-per-business-fact)). It is scoped to its task.

**I1. An exact id, task unknown. No new index.** The record key is built from the task and the run id ([`engine-keys.ts:85`](../src/engine/engine-keys.ts#L85)). One read per listed task, in one round trip, finds the run under every task that has it. This is `getRun` without its task argument.

**I2. A prefix, by scanning. No new index.**

- `ZRANGE … BYLEX` does not apply: it is specified only when every score is equal ([section 3.2](#32-keyset-pagination-with-equal-scores)).
- `ZSCAN key MATCH <task>:<prefix>*` visits every member. Measured: 0.43 to 0.55 ms for one index of 1,000 members. From that rate, the 550,000 ended runs of 50 tasks at their caps are about 0.25 s of server time, spread over many calls (computed, not measured). The backlog indexes have no bound.
- `SCAN` over the record keys visits every key of the database.
- The result has no order, and a member can come twice ([section 3.5](#35-scan-and-sort)).

**I3. A lexicographic index. A new index.**

- How: one sorted set per task, every score 0, the member the run id. `ZRANGE key [prefix [prefix\xff BYLEX LIMIT 0 n` reads the matches in `O(log N + n)`. Measured: 0.01 ms.
- Write path: `createRun` adds, `prune` removes.
- Memory: 111 to 126 bytes per run for 36-byte ids.
- The result comes in id order, not in time order.
- Existing runs and deploy overlap: [section 4.13](#413-rolling-out-a-new-index).

### 4.11 Search by payload or error text

- The payload is a string key of JSON text, the errors a list of JSON records ([section 1.4](#14-the-record)). No command of the floor searches inside a string or a list.
- Read and discard: fetch the payload or the errors of each candidate and match in the script or in the client. The cost follows the bytes read, and a payload has no size limit that this note found. A budget per call applies as in R1.
- `listRuns` already returns `lastErrorMessage` for each listed run, so a dashboard can filter the page it holds.
- An inverted index built by hand (a set of run ids per word) is the pattern the Redis documentation describes ([section 3.3](#33-secondary-indexes-in-the-official-documentation)). It means one write per word of each payload.
- Sidekiq's substring filter works because the member of its sorted sets is the whole job JSON, which `ZSCAN MATCH` scans in full ([section 2](#2-what-comparable-systems-offer)). Here the members are ids.
- The systems that offer text or expression search do it on SQL or ClickHouse, or with a search module ([section 2](#2-what-comparable-systems-offer)).

### 4.12 Totals, and the filters other tools offer

| Total of                             | How                                | Cost                                                        |
| ------------------------------------ | ---------------------------------- | ----------------------------------------------------------- |
| One state, one task or several       | `ZCARD` per index (`countRuns`)    | 400 `ZCARD`: 0.05 to 0.07 ms                                |
| Several states                       | sum of `ZCARD`                     | the same                                                    |
| A time range on the state's own time | `ZCOUNT` per index                 | 100 `ZCOUNT`: 0.31 ms                                       |
| The offset of the current page       | the ranks the page script computes | none ([4.3](#43-paging-backward-and-what-a-position-means)) |
| A list on a new index (S3, R2, I3)   | `ZCARD` or `ZCOUNT` of that index  | the same                                                    |
| Any read-and-discard filter          | not available short of reading all | the full walk                                               |

`countRuns` counts the runs a newer release wrote, which `listRuns` leaves out ([`docs/dashboard.md`](../docs/dashboard.md#count-the-backlog)); a total next to a list inherits that.

Filters the systems of [section 2](#2-what-comparable-systems-offer) offer and the request did not list, and what each would be here:

| Filter elsewhere                                                            | Here                                                                                                                                                                        |
| --------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| A group of states under one name (pg-boss dashboard "pending"; Trigger.dev) | The score families of S1 and S2                                                                                                                                             |
| The kind of run: standard, scheduled, from an event (Trigger.dev dashboard) | Pick indexes: `taskDefinitions` already says which tasks are listeners and which have a schedule. Exact per run by read and discard on `eventName` and `scheduledAt`        |
| A choice of time field: queued, started, ended (Inngest, River)             | The state decides the time. River does the same: one time column per group of states                                                                                        |
| A default time window (Trigger.dev 7 days; Inngest requires `from`)         | Retention already bounds the ended lists: 24 hours and 14 days                                                                                                              |
| The error, by fingerprint (Trigger.dev)                                     | No fingerprint is stored. The error name and message are in the last error, which `listRuns` already reads: read and discard                                                |
| Tags (Trigger.dev, River; Sidekiq Pro for batches)                          | Runs have no tags. It would be a new `dispatch` option plus one index per tag: a feature, not a filter                                                                      |
| Data containment (pg-boss, River metadata, Inngest's CEL expression)        | [4.11](#411-search-by-payload-or-error-text)                                                                                                                                |
| Root runs only, or the runs a run started (Trigger.dev dashboard)           | The record holds no parent. A handler's sends are in the parent's saved steps, which `getRun` returns                                                                       |
| Queue, priority, machine, region, version, batch, test flag                 | No such notion in this library                                                                                                                                              |
| A concurrency key (no system read here lists by it)                         | Already indexed for two states: `{p}:waiting:<task>:<key>` holds the runs waiting for a slot of one key, `{p}:slots:<task>:<key>` those executing ([section 1.1](#11-keys)) |

### 4.13 Rolling out a new index

This applies to S3, S4, R2 and I3 alike. Call N the first release that writes the index.

**Runs that exist before N** are in no new index. Three ways to add them:

- A backfill by the worker: a bounded batch per tick, as `prune` works, walking the eight state indexes of each task and adding each member with the score read from its record. At 200 runs per tick and per worker, the envelope's 100,000 runs take about 8 minutes with one worker. The walk needs a place to keep its progress, and the list is incomplete until it ends.
- Lazily: every script of N that touches a run adds it. That puts a write on every transition, which S3 otherwise avoids, and a run that nothing touches (a delayed run, a kept failed run) is never added.
- Not at all: the list is complete once every run older than N is gone. That is 24 hours for completed runs and 14 days for failed runs, and has no bound for a delayed or suspended run.

**During a deploy overlap**, and after a rollback, an older release writes without the index:

| The older release | S3 and I3 (written at create, removed at prune) | R2 (written at fail, removed at replay and prune) |
| ----------------- | ----------------------------------------------- | ------------------------------------------------- |
| creates a run     | the run is missing from the index               | no effect                                         |
| fails a run       | no effect                                       | the run is missing from the index                 |
| replays a run     | no effect                                       | a member stays for a run that is no longer failed |
| prunes a run      | a member stays for a run that no longer exists  | the same                                          |

- A member whose record is gone or does not match is already dropped by the reader ([`read-api.ts:325-340`](../src/read-api.ts#L325-L340)), so it shortens a page and inflates `ZCARD`, and nothing else. Nothing removes it, though: `prune` of N would need a bounded sweep of the new index for members with no record.
- A missing run needs the backfill above to run again after the overlap, or to keep running.
- Raising the record version does not solve this. An old worker passes over the records N writes ([`fetch-script.ts:121-126`](../src/engine/fetch-script.ts#L121-L126), [`prune-script.ts:43-45`](../src/engine/prune-script.ts#L43-L45)), which keeps old scripts off new runs. It does nothing for the runs the old release creates itself during the overlap, and it stops old workers from executing new runs for the length of the overlap.
- Two releases is the other way: release N writes and sweeps the index, release N+1 exposes the read. The read is complete if every instance ran N or later since the backfill ended.

**A cursor across releases.** A cursor of a new format reaching an older instance fails with `Invalid listRuns cursor: pass back a nextCursor that listRuns returned`, and so does an old cursor reaching a new instance, unless the new release keeps reading format 2 ([`run-list-cursor.ts:21-35`](../src/run-list-cursor.ts#L21-L35)). A cursor lives as long as a dashboard page stays open.

## 5. Open questions

Each is for the maintainer to decide. The costs are those of section 4.

1. **What does "all runs" mean in the API?**
   - (a) Four blocks, one per score family, each in its own order (S2). No new index, no rollout. Not one timeline.
   - (b) One timeline by dispatch time (S3). One new index, two writes per run, 139 to 161 bytes per run, a backfill and a sweep, and a state filter on it that is read and discard.
   - (c) Both: (a) for the lists by state, (b) as a separate "all runs" list.
2. **If a new index is added, how is it rolled out over existing runs and a deploy overlap?** A backfill in the worker tick; lazily on every transition; or nothing, with the list documented as incomplete for up to 14 days and without bound for delayed and suspended runs. And: in one release, or written in one release and read in the next ([4.13](#413-rolling-out-a-new-index)).
3. **Is the "all runs" index per task or one for the prefix?** Per task keeps the task filter free and costs one page script per task. One for the prefix costs one `ZRANGE` and makes the task filter read and discard.
4. **Are read-and-discard filters acceptable in the API?** They fit the contract of short pages. They mean a page that can be empty while the walk goes on, a cost that follows the runs examined, and no total. If yes: done in the client with the fields already read, or in a script with a budget per call (how many records: 1,000 cost about 2 ms)?
5. **Does `reason` get its own index (R2) or stay read and discard (R1)?** R2 changes three scripts and adds a rollout, and gives totals per reason and full pages. R1 changes none.
6. **What is the shape of backward paging?** A `direction` option with one cursor type, or `previousCursor` and `nextCursor` that each carry their direction. Is `previousCursor` returned on a first page, where it means "what arrived since"? Is the offset ("runs 151 to 200 of N") returned?
7. **Is jumping to page N wanted?** It is the one paging feature that is not nearly free across several indexes, and the Redis-backed systems that have it accept pages that shift.
8. **Does the default order of the queued and timed lists stay highest first?** With ascending available, "next to run first" and "soonest due first" become possible defaults; changing a default is a breaking change.
9. **What goes into the cursor?** The full filter set, which keeps the precise error messages and makes the cursor grow with the task list; or a hash of it, which keeps it short and ends those messages; or the position only, which lets a filter change between pages.
10. **Is run id prefix search wanted, or is an exact id without the task enough?** The exact lookup (I1) needs no index. A prefix needs a scan of every index (I2) or a new index of 111 to 126 bytes per run (I3).
11. **Is text search out of scope?** At the supported floor it can only be read and discard over payloads of unbounded size. The alternative is to document that a search module or the app's own database is the place for it.

## 6. Recommendation (the author's opinion)

This section is opinion. Sections 1 to 4 do not depend on it.

- **Ship everything that needs no new index first, in one cursor format change**: several states by score family (S1) and all states in blocks (S2), several tasks, backward paging with the offset, ascending order, a time range on the state's own time, and totals from `ZCARD` and `ZCOUNT`. It changes no engine script, adds no memory and has no rollout. It already exceeds what any Redis-backed system of section 2 offers.
- **Add an exact run id lookup without the task** (I1). It is one round trip and covers the common "find this run" case.
- **Offer `reason` as read and discard, done in the client**, since the field is already read. Revisit R2 only if a dashboard needs totals per reason.
- **Defer the "all runs by dispatch time" index** until a dashboard is shown to need one timeline. If it is built: per task, scored by `dispatchedAt`, written in one release and read in the next, with a backfill and a sweep in the tick.
- **Leave text search out.** No option at the floor has a bounded cost.

## 7. Experiments

Run on 2026-11-10 against `valkey/valkey:8` (Valkey 8.1.11) and `redis:7.4` (Redis 7.4.12), local Docker containers with default settings, from Node 24 with `@redis/client` 6.3.1 over the loopback interface. The scripts are throwaway and are not in the repository. Server time is the sum of the command durations in `SLOWLOG` with `slowlog-log-slower-than 0`; "elapsed" is measured in the client and includes the round trip. One run each, so read the figures as orders of magnitude.

Data set A, the envelope: 50 tasks, each with 1,000 completed and 1,000 failed runs, a record per run with the fields a real ended run has, members of 56 bytes (`task-name-number-07:<uuid>`). Data set B, the retention caps: 50 tasks, each with 1,000 completed and 10,000 failed index members, 550,000 in all, indexes only. The page script is `INDEX_PAGE_SCRIPT` copied from `src/engine/index-page-script.ts`.

| Measurement                                                                     | Valkey 8.1.11                 | Redis 7.4.12           |
| ------------------------------------------------------------------------------- | ----------------------------- | ---------------------- |
| Memory of a run index, 1,000 members (`MEMORY USAGE … SAMPLES 0`)               | 143 B per entry               | 159 B per entry        |
| Memory of a per-task "all runs" index, 50 × 2,000 members (`used_memory` delta) | 144 B per run                 | 159 B per run          |
| Memory of one "all runs" index, 100,000 members                                 | 139 to 140 B per run          | 160 to 161 B per run   |
| Memory of a run id index, score 0, 36-byte members                              | 111 B per run                 | 126 B per run          |
| A sorted set of 10 members (compact encoding)                                   | 960 B                         | 984 B                  |
| 50 page scripts, 1 state, from the head (A)                                     | 1.6 ms server, 3.3 ms elapsed | 1.6 ms, 4.2 ms         |
| 50 page scripts, 1 state, after a cursor (A)                                    | 1.5 ms, 3.1 ms                | 1.7 ms, 3.4 ms         |
| 50 backward page scripts, 1 state, before a cursor (A)                          | 0.35 ms, 1.0 ms               | 0.58 ms, 1.2 ms        |
| 100 page scripts, completed + failed, after a cursor (A)                        | 2.6 ms, 5.3 ms                | 3.2 ms, 5.8 ms         |
| 100 backward page scripts (A)                                                   | 1.5 ms, 3.0 ms                | 1.7 ms, 3.3 ms         |
| 400 page scripts, 8 states, after a cursor (A, six states empty)                | 3.3 ms, 5.9 ms                | 4.3 ms, 6.8 ms         |
| 400 backward page scripts (A)                                                   | 2.3 ms, 4.0 ms                | 2.6 ms, 4.1 ms         |
| One script that reads 100 indexes and sorts 5,000 entries itself (A)            | 7.0 ms                        | 7.3 ms                 |
| 100 page scripts after a cursor (B, 550,000 members)                            | 3.8 ms, 6.2 ms                | 3.6 ms, 6.4 ms         |
| `ZUNIONSTORE` of 100 indexes, 100,000 members (A)                               | 54 ms; key of 13.9 MB         | 83 ms; key of 16.0 MB  |
| `ZUNION … WITHSCORES` of the same, reply read whole (A)                         | 55 ms, 98 ms elapsed          | 92 ms, 131 ms elapsed  |
| `ZUNIONSTORE` of 100 indexes, 550,000 members (B)                               | 625 ms; key of 78 MB          | 796 ms; key of 91 MB   |
| `UNLINK` of the temporary key                                                   | 0.03 ms                       | 0.04 ms                |
| Script: walk a failed index, read `reason`, 1,000 records examined, 10 matches  | 1.9 ms                        | 2.5 ms                 |
| Same, 550 records examined, 50 matches                                          | 0.8 ms                        | 1.0 ms                 |
| Same, 52 records examined, 50 matches                                           | 0.1 ms                        | 0.1 ms                 |
| 1,000 pipelined `HMGET` of 10 fields                                            | 0.7 ms, 4.7 ms elapsed        | 1.2 ms, 9.9 ms elapsed |
| `ZSCAN … MATCH` over one index of 1,000 members, full iteration                 | 0.43 ms                       | 0.55 ms                |
| `ZRANGE … BYLEX … LIMIT 0 50` on the run id index                               | 0.01 ms                       | 0.01 ms                |
| 400 `ZCARD` (B)                                                                 | 0.05 ms                       | 0.07 ms                |
| 100 `ZCOUNT` on a one-hour score range (B)                                      | 0.31 ms                       | 0.31 ms                |

The backward page script that was tested, the mirror of the forward one. It returns the `limit` entries before a position, highest score first, or the last `limit` entries of the index when no position is given:

```lua
local indexKey = KEYS[1]
local limit = tonumber(ARGV[1])
local function isBytewiseBefore(left, right)
  local sharedLength = math.min(#left, #right)
  for position = 1, sharedLength do
    local leftByte, rightByte = string.byte(left, position), string.byte(right, position)
    if leftByte ~= rightByte then
      return leftByte < rightByte
    end
  end
  return #left < #right
end
local endRank = redis.call("ZCARD", indexKey)
if #ARGV == 3 then
  local score, runId = ARGV[2], ARGV[3]
  local higherScoredCount = redis.call("ZCOUNT", indexKey, "(" .. score, "+inf")
  local sameScoredCount = redis.call("ZCOUNT", indexKey, score, score)
  local lowRank, highRank = higherScoredCount, higherScoredCount + sameScoredCount
  while lowRank < highRank do
    local middleRank = math.floor((lowRank + highRank) / 2)
    local middleRunId = redis.call("ZRANGE", indexKey, middleRank, middleRank, "REV")[1]
    if isBytewiseBefore(runId, middleRunId) then
      lowRank = middleRank + 1
    else
      highRank = middleRank
    end
  end
  endRank = lowRank
end
if endRank == 0 then
  return {}
end
return redis.call("ZRANGE", indexKey, math.max(0, endRank - limit), endRank - 1, "REV", "WITHSCORES")
```

The test of both directions, on both servers: a sorted set of 500 members with 14 distinct scores, so with long runs of equal scores, and pages of 7.

- The forward walk, page after page, equals `ZRANGE key 0 -1 REV WITHSCORES`: true.
- The backward walk from the end, page before page, equals the same list: true.
- After removing the member at rank 250, the backward page from its old position is ranks 243 to 249 of the original list and the forward page is ranks 251 to 257: true.

Not measured: a merge across indexes that pulls entries lazily inside one script; jumping to an offset across several indexes; a read-and-discard walk over a full 10,000-run index; anything under concurrent load.

## 8. Not verified

- **Asynq:** the real order of `ListArchivedTasks` and `ListCompletedTasks`. The comments say descending, the source reads ascending; it was not run.
- **Trigger.dev:** the row order of a `page[before]` page (the code suggests oldest first; not run), and the version that removed the Postgres implementation of the run list.
- **Inngest:** the store behind Inngest Cloud's run list, and the exact wording of its docs and blog pages, which were read through a summarising fetch. The open-source code is the source for everything stated in [§2.5](#25-inngest).
- **Temporal:** the default and maximum page size, and how the Elasticsearch page token is built.
- **BullMQ Pro and Sidekiq Pro:** closed source. Only their public docs were read. The Redis structure of Sidekiq Pro's batch tag index is unknown.
- **River:** the encoding of its cursor, and whether it has a separate count call.
- **Managed services:** whether any hosted Redis or Valkey exposes a search module.
- **Command flags at the floor tags:** the read-only and write flags were read at Redis 8.10.3 and Valkey 9.1.3. Their presence at 7.4.0 and 8.0.0 is inferred from each command's `since`.
- **Web pages:** most command pages were read as their source files in redis/docs and valkey-io/valkey-doc, not as rendered pages.
- **Payload size:** this note found no limit on the size of a payload in this library; it did not search for one exhaustively.
- **Costs marked "computed":** the full-index read-and-discard walk and the `ZSCAN` of 550,000 members are extrapolated from the measured rate.
