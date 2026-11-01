# Park-and-wake spike

Throwaway spike for issue #12 (concurrency keys and flow control in v1), Q8: does the park-and-wake mechanism hold on OSS BullMQ? Never merged.

Mechanism tested: a slot counter Lua script (sorted set per concurrency key, holder = worker lock token, expiry = `lockDuration`, renewed with the lock); park = add the job to the key's waiting list, `job.moveToDelayed(now + safetyDelay, token)`, `throw new DelayedError()`; wake = on release, `promote()` the oldest waiting job that still exists; re-check = right after parking, if a slot is free, wake the oldest waiting job. One BullMQ queue.

## Verdict

Park and wake holds. Across every scenario the limit was never exceeded and no run got stuck, except under a blocked event loop, which loses the lock in plain BullMQ too. The re-check after parking is required: without it, a delayed park loses every wake, and recovery after a crashed holder falls through to the safety delay.

## Setup

- BullMQ 6.5.3, ioredis 5.11.1, Valkey 9.1.3 (Docker, `noeviction`), Node 24.16.0.
- 4 worker processes, `concurrency` 10 each.
- Shortened timings: `lockDuration` 5 s, `stalledInterval` 5 s, `maxStalledCount` 3, safety delay 15 s (longer than the lock, so a fallback to it stands out).
- Violations are computed from per-execution start/end times (a killed execution ends at its kill time).
- Run: `npm install && npx tsx run.ts [scenario ids…]`, then `npx tsx summary.ts`. About 10 minutes for all scenarios. Raw results in `results/`.

## Results

| Scenario | Result |
| --- | --- |
| S1: 1 key, limit 1, 1,000 runs, 5–20 ms handler | Max 1 executing, 1,000/1,000 complete, 0 stuck, 16.5 s. Release → next start p50/p95/p99/max: 3/5/6/193 ms |
| S2: same, limit 3 | Max 3 executing, all complete, 5.6 s. Release → next start: 2/4/4/188 ms |
| S3: key A 1,000 runs, then key B 1 run | B starts 157 ms after dispatch (behind 999 parks, not 999 executions). With 3,000 keyless runs between: B after 1,160 ms; A's slot idle once for 1,031 ms (no round-robin) |
| S4: lost wakes | Idle Valkey without re-check: 0/1,000 lost; 0/500 (500 keys × 2). With a 30 ms pause before `moveToDelayed`: 100/100 lost without re-check (each waits 15.0 s); 0/100 with re-check (max gap 99 ms). Re-check cost: about one failed `promote()` per park |
| S5: SIGKILL chaos | Limit 3 (7 kills), limit 1 short runs (6 kills), limit 1 with 2 s runs (66 kills, 13 holders killed mid-execution): 0 violations, all complete, 0 stuck. Key blocked after a killed holder: up to 8.4 s with re-check (≤ `lockDuration` + `stalledInterval`); up to 17.6 s without (p95 14 s, safety delay) |
| S5b: blocked event loop (12 s, lock 5 s) | 10 windows with 2 executing at once, 0.2–6.2 s each; BullMQ re-runs the job elsewhere, fails after 3 stalls. Errors: `could not renew lock`, `Missing lock … moveToFinished`, `Lock mismatch … moveToFinished from active` |
| S6: retry with 2 s backoff | Slot freed during backoff; 5 other runs went first; the retried run parked at the back and completed last. `attemptsMade` 2, `attemptsStarted` 3 |

## BullMQ behaviour observed

- `promote()` on a waiting, active or completed job throws the same `Job <id> is not in the delayed state. promote`, so a wake cannot tell "still parking" from "already woken".
- `promote()` after `moveToDelayed` but before `DelayedError` is thrown works; the job reruns correctly.
- Returning normally instead of throwing `DelayedError` after `moveToDelayed` gives `Missing lock … moveToFinished`.
- Parking keeps `attemptsMade` at 0; `attemptsStarted` grows on every pickup.

## Design consequences

1. The re-check after parking is part of the mechanism. With the production 2 min lock, a crashed holder blocks its key for about 2.5 minutes.
2. Start order is loose: 266 of 1,000 runs in S1 started before an older run of the same key, overtaking at most 34 places, because the waiting list is ordered by pickup and 40 slots pick up at once. A waiting list scored by dispatch time would tighten it (untested).
3. The safety delay is polling: each parked run is picked up again once per safety delay. With 2 min, 1,000 waiting runs on a key cost about 8 pickups per second.
4. The wrapper must always throw `DelayedError` after `moveToDelayed`, and must never set `maxStartedAttempts`.
