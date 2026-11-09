import { expect } from "vitest";

import {
  DAY_IN_MILLISECONDS,
  endRuns,
  MINUTE_IN_MILLISECONDS,
  runIdsOf,
  SEND_EMAIL_PRUNE_REQUEST,
} from "../../tests/ended-runs.js";
import { backdateEndedRun, engineTest, fetchedLockedRunsOf } from "../../tests/engine-test.js";
import type { EngineTestContext } from "../../tests/engine-test.js";
import { invariantViolationsOf } from "../../tests/invariant-violations.js";
import { seededRandomnessOfThisRun } from "../../tests/seeded-random.js";
import type { SeededRandomness } from "../../tests/seeded-random.js";
import { serverInstantInMillisecondsOf } from "../../tests/server-instant.js";
import type { RunId } from "../ids.js";
import { EXPIRED_DEDUP_ENTRIES_TRIMMED_PER_PRUNE, MOST_RUNS_PRUNED_PER_PRUNE, RECORD_VERSION } from "./engine.js";
import type { PruneRequest, SettleOutcome } from "./engine.js";
import { NEWER_VERSION_RECORDS_PASSED_OVER_PER_PRUNE } from "./prune-script.js";

const PRUNE_REQUEST_WITH_NO_COMPLETED_RUN_KEPT: PruneRequest = {
  ...SEND_EMAIL_PRUNE_REQUEST,
  completedRetention: { ...SEND_EMAIL_PRUNE_REQUEST.completedRetention, maxRunCount: 0 },
};

async function endRunsOfANewerRecordVersionBefore(
  context: EngineTestContext,
  newerVersionRunIds: readonly RunId[],
  runIds: readonly RunId[],
): Promise<void> {
  const { engine, redis, keys } = context;
  await endRuns(engine, "send-email", [...newerVersionRunIds, ...runIds], "complete");
  for (const runId of newerVersionRunIds) {
    await redis.hset(keys.runRecord(runId), "version", RECORD_VERSION + 1);
  }
}

async function storedKeyCountOf({ redis, keys }: EngineTestContext, runId: RunId): Promise<number> {
  return redis.exists(keys.runRecord(runId), keys.runPayload(runId), keys.runErrors(runId), keys.runSteps(runId));
}

async function completedRunIdsOf({ redis, keys }: EngineTestContext): Promise<string[]> {
  return redis.zrange(keys.runIndex("send-email", "completed"), 0, -1);
}

engineTest(
  "1,200 completed runs of a task end at 1,000 after prunes, the oldest removed first with every key they had",
  async (context) => {
    const { engine } = context;
    const runIds = runIdsOf("run", 1200);
    await endRuns(engine, "send-email", runIds, "complete");

    const prunedRunCounts = [(await engine.prune(SEND_EMAIL_PRUNE_REQUEST)).prunedRunCount];
    prunedRunCounts.push((await engine.prune(SEND_EMAIL_PRUNE_REQUEST)).prunedRunCount);

    expect(prunedRunCounts).toEqual([MOST_RUNS_PRUNED_PER_PRUNE, 0]);
    expect(await completedRunIdsOf(context)).toEqual(runIds.slice(200));
    for (const runId of runIds.slice(0, 200)) {
      expect(await storedKeyCountOf(context, runId), `the keys of ${runId}`).toBe(0);
    }
  },
);

engineTest(
  "a failed run past its age leaves whole: record, payload, errors, steps and index entry",
  async (context) => {
    const { engine, redis, keys } = context;
    await engine.enqueue({ runs: [{ runId: "run-1", task: "send-email", payload: "{}", retries: 0 }] });
    const [lockedRun] = await fetchedLockedRunsOf(context, 1);
    await engine.saveStep({ ...lockedRun!, stepId: "charge", step: { kind: "value", value: '["paid"]' } });
    await engine.settle({
      ...lockedRun!,
      outcome: {
        kind: "fail",
        reason: "non-retriable",
        error: { attempt: 1, name: "Error", message: "boom", stack: "", failedAt: "" },
        failureNotice: undefined,
      },
    });
    const storedKeyCountBefore = await storedKeyCountOf(context, "run-1");
    await backdateEndedRun(context, "run-1", 14 * DAY_IN_MILLISECONDS);

    const reply = await engine.prune(SEND_EMAIL_PRUNE_REQUEST);

    expect(storedKeyCountBefore).toBe(4);
    expect(reply).toEqual({ kind: "pruned", prunedRunCount: 1 });
    expect(await storedKeyCountOf(context, "run-1")).toBe(0);
    expect(await redis.zscore(keys.runIndex("send-email", "failed"), "run-1")).toBeNull();
  },
);

engineTest(
  "on a task with 1,001 completed runs, a repeat 23 h 59 after its run ended is dropped, and one at 24 h 01 starts a new run",
  async (context) => {
    const { engine, redis, keys } = context;
    await endRuns(engine, "send-email", ["run-ended-24h01-ago", "run-ended-23h59-ago"], "complete");
    await endRuns(engine, "send-email", runIdsOf("newer-run", 1000), "complete");
    await backdateEndedRun(context, "run-ended-24h01-ago", DAY_IN_MILLISECONDS + MINUTE_IN_MILLISECONDS);
    await backdateEndedRun(context, "run-ended-23h59-ago", DAY_IN_MILLISECONDS - MINUTE_IN_MILLISECONDS);

    const pruneReply = await engine.prune(SEND_EMAIL_PRUNE_REQUEST);
    const repeatReply = await engine.enqueue({
      runs: ["run-ended-24h01-ago", "run-ended-23h59-ago"].map((runId) => ({
        runId,
        task: "send-email",
        payload: "{}",
        retries: 0,
      })),
    });

    expect(pruneReply).toEqual({ kind: "pruned", prunedRunCount: 2 });
    expect(repeatReply).toEqual({
      kind: "enqueued",
      createdRunIds: ["run-ended-24h01-ago"],
      skippedRunIds: ["run-ended-23h59-ago"],
    });
    expect(await redis.exists(keys.runRecord("run-ended-23h59-ago"))).toBe(0);
  },
);

engineTest(
  `a prune trims at most ${String(EXPIRED_DEDUP_ENTRIES_TRIMMED_PER_PRUNE)} expired dedup entries of its task, and none that is live`,
  async ({ engine, redis, keys }) => {
    const dedupKey = keys.runDedup("send-email");
    const expiredEntryCount = EXPIRED_DEDUP_ENTRIES_TRIMMED_PER_PRUNE + 50;
    const liveExpiresAt = (await serverInstantInMillisecondsOf(redis)) + DAY_IN_MILLISECONDS;
    await redis.zadd(dedupKey, ...runIdsOf("expired-run", expiredEntryCount).flatMap((runId) => [1, runId]));
    await redis.zadd(dedupKey, liveExpiresAt, "live-run");
    await redis.zadd(keys.runDedup("resize-image"), 1, "other-task-run");

    await engine.prune(SEND_EMAIL_PRUNE_REQUEST);
    const entryCountAfterTheFirstPrune = await redis.zcard(dedupKey);
    await engine.prune(SEND_EMAIL_PRUNE_REQUEST);

    expect(entryCountAfterTheFirstPrune).toBe(expiredEntryCount + 1 - EXPIRED_DEDUP_ENTRIES_TRIMMED_PER_PRUNE);
    expect(await redis.zrange(dedupKey, 0, -1)).toEqual(["live-run"]);
    expect(await redis.zcard(keys.runDedup("resize-image"))).toBe(1);
  },
);

engineTest(
  "a prune passes over aged runs of a newer record version, leaving them whole, and removes the rest",
  async (context) => {
    const { engine, redis, keys } = context;
    const newerVersionRunIds = runIdsOf("newer-version-run", 3);
    const runIds = runIdsOf("run", 5);
    await endRunsOfANewerRecordVersionBefore(context, newerVersionRunIds, runIds);
    for (const runId of [...newerVersionRunIds, ...runIds]) {
      await backdateEndedRun(context, runId, 2 * DAY_IN_MILLISECONDS);
    }

    const reply = await engine.prune(SEND_EMAIL_PRUNE_REQUEST);

    expect(reply).toEqual({ kind: "pruned", prunedRunCount: 5 });
    expect(await completedRunIdsOf(context)).toEqual(newerVersionRunIds);
    for (const runId of newerVersionRunIds) {
      expect(await redis.exists(keys.runRecord(runId), keys.runPayload(runId)), runId).toBe(2);
    }
  },
);

engineTest(
  `a prune stops after passing over ${String(NEWER_VERSION_RECORDS_PASSED_OVER_PER_PRUNE)} runs of a newer record version`,
  async (context) => {
    const { engine } = context;
    const newerVersionRunIds = runIdsOf("newer-version-run", NEWER_VERSION_RECORDS_PASSED_OVER_PER_PRUNE);
    await endRunsOfANewerRecordVersionBefore(context, newerVersionRunIds, ["run-1"]);

    const reply = await engine.prune(PRUNE_REQUEST_WITH_NO_COMPLETED_RUN_KEPT);

    expect(reply).toEqual({ kind: "pruned", prunedRunCount: 0 });
    expect(await completedRunIdsOf(context)).toEqual([...newerVersionRunIds, "run-1"]);
  },
);

const LOAD_TASKS = ["send-email", "resize-image", "sync-crm"];

const LOAD_ROUND_COUNT = 60;

const LOAD_ROUNDS_BETWEEN_SAMPLES = 5;

async function fetchAndEndSomeRuns(context: EngineTestContext, { random, pick }: SeededRandomness): Promise<void> {
  const { engine } = context;
  const lockedRuns = await fetchedLockedRunsOf(context, 1 + Math.floor(random() * 6));
  await Promise.all(
    lockedRuns.map((lockedRun) =>
      engine.settle({
        ...lockedRun,
        outcome: pick<SettleOutcome>([
          { kind: "complete" },
          {
            kind: "fail",
            reason: "non-retriable",
            error: { attempt: 1, name: "Error", message: "boom", stack: "", failedAt: "" },
            failureNotice: undefined,
          },
        ]),
      }),
    ),
  );
}

function loadPruneRequestOf({ random, pick }: SeededRandomness): PruneRequest {
  const retentionLimitOf = () => ({
    maxAgeInMilliseconds: pick([0, DAY_IN_MILLISECONDS]),
    maxRunCount: Math.floor(random() * 4),
  });
  return { task: pick(LOAD_TASKS), completedRetention: retentionLimitOf(), failedRetention: retentionLimitOf() };
}

engineTest(
  "the invariant checker passes across prunes under concurrent enqueues, fetches and settles",
  async (context) => {
    const { engine, redis, keyPrefix } = context;
    const randomness = seededRandomnessOfThisRun();
    const { random, pick, seedMessage } = randomness;
    const enqueuedRunIds: RunId[] = [];
    const prunedRunCounts: number[] = [];

    for (let round = 1; round <= LOAD_ROUND_COUNT; round += 1) {
      const runs = Array.from({ length: 1 + Math.floor(random() * 8) }, (_, position) => ({
        runId:
          enqueuedRunIds.length > 0 && random() < 0.2
            ? pick(enqueuedRunIds)
            : `run-${String(round)}-${String(position)}`,
        task: pick(LOAD_TASKS),
        payload: "{}",
        retries: 0,
      }));
      enqueuedRunIds.push(...runs.map(({ runId }) => runId));
      const [, , pruneReply] = await Promise.all([
        engine.enqueue({ runs }),
        fetchAndEndSomeRuns(context, randomness),
        engine.prune(loadPruneRequestOf(randomness)),
        fetchAndEndSomeRuns(context, randomness),
      ]);
      prunedRunCounts.push(pruneReply.prunedRunCount);
      if (round % LOAD_ROUNDS_BETWEEN_SAMPLES === 0) {
        expect(await invariantViolationsOf(redis, keyPrefix), `after round ${String(round)}, ${seedMessage}`).toEqual(
          [],
        );
      }
    }

    expect(
      prunedRunCounts.some((prunedRunCount) => prunedRunCount > 0),
      `some prune removed a run, ${seedMessage}`,
    ).toBe(true);
  },
);
