import { expect } from "vitest";

import {
  DAY_IN_MILLISECONDS,
  endRuns,
  MINUTE_IN_MILLISECONDS,
  runIdsOf,
  SEND_EMAIL_PRUNE_REQUEST,
  sequentialRoundTripCountOfEndRuns,
} from "../../tests/ended-runs.js";
import { backdateEndedRun, engineTest, fetchedLockedRunsOf } from "../../tests/engine-test.js";
import {
  SEQUENTIAL_ROUND_TRIP_BOUND_IN_MILLISECONDS,
  TEST_TIMEOUT_PAST_ITS_WAITS_IN_MILLISECONDS,
} from "../../tests/integration-test.js";
import type { EngineTestContext } from "../../tests/engine-test.js";
import { storedKeyViolationsOf } from "../../tests/stored-key-violations.js";
import { seededRandomnessOfThisRun } from "../../tests/seeded-random.js";
import type { SeededRandomness } from "../../tests/seeded-random.js";
import { serverInstantInMillisecondsOf } from "../../tests/server-instant.js";
import type { RunId } from "../ids.js";
import { storedRunIdOf } from "./engine-keys.js";
import { EXPIRED_DEDUP_ENTRIES_TRIMMED_PER_PRUNE, MOST_RUNS_PRUNED_PER_PRUNE, RECORD_VERSION } from "./engine.js";
import type { PruneRequest, SettleOutcome } from "./engine.js";
import { NEWER_VERSION_RECORDS_PASSED_OVER_PER_PRUNE } from "./prune-script.js";

const PRUNE_REQUEST_WITH_NO_COMPLETED_RUN_KEPT: PruneRequest = {
  ...SEND_EMAIL_PRUNE_REQUEST,
  completedRetention: { ...SEND_EMAIL_PRUNE_REQUEST.completedRetention, maxRunCount: 0 },
};

const OVER_CAP_ENDED_RUN_COUNT = 1200;

const OVER_CAP_PRUNE_CALL_COUNT = 2;

const COMPLETED_INDEX_READ_COUNT = 1;

const OVER_CAP_PRUNE_TEST_SEQUENTIAL_ROUND_TRIP_COUNT =
  sequentialRoundTripCountOfEndRuns(OVER_CAP_ENDED_RUN_COUNT) +
  OVER_CAP_PRUNE_CALL_COUNT +
  COMPLETED_INDEX_READ_COUNT +
  MOST_RUNS_PRUNED_PER_PRUNE;

const OVER_CAP_PRUNE_TEST_TIMEOUT_IN_MILLISECONDS =
  OVER_CAP_PRUNE_TEST_SEQUENTIAL_ROUND_TRIP_COUNT * SEQUENTIAL_ROUND_TRIP_BOUND_IN_MILLISECONDS +
  TEST_TIMEOUT_PAST_ITS_WAITS_IN_MILLISECONDS;

async function endRunsOfANewerRecordVersionBefore(
  context: EngineTestContext,
  newerVersionRunIds: readonly RunId[],
  runIds: readonly RunId[],
): Promise<void> {
  const { engine, redis, keys } = context;
  await endRuns(engine, "send-email", [...newerVersionRunIds, ...runIds], "complete");
  for (const runId of newerVersionRunIds) {
    await redis.hSet(keys.runRecord("send-email", runId), "version", RECORD_VERSION + 1);
  }
}

async function storedKeyCountOf({ redis, keys }: EngineTestContext, runId: RunId): Promise<number> {
  return redis.exists([
    keys.runRecord("send-email", runId),
    keys.runPayload("send-email", runId),
    keys.runErrors("send-email", runId),
    keys.runSteps("send-email", runId),
  ]);
}

async function completedRunIdsOf({ redis, keys }: EngineTestContext): Promise<string[]> {
  return redis.zRange(keys.runIndex("send-email", "completed"), 0, -1);
}

engineTest(
  "1,200 completed runs of a task end at 1,000 after prunes, the oldest removed first with every key they had",
  async (context) => {
    const { engine } = context;
    const runIds = runIdsOf("run", OVER_CAP_ENDED_RUN_COUNT);
    await endRuns(engine, "send-email", runIds, "complete");

    const prunedRunCounts = [(await engine.prune(SEND_EMAIL_PRUNE_REQUEST)).prunedRunCount];
    prunedRunCounts.push((await engine.prune(SEND_EMAIL_PRUNE_REQUEST)).prunedRunCount);

    expect(prunedRunCounts).toEqual([MOST_RUNS_PRUNED_PER_PRUNE, 0]);
    expect(await completedRunIdsOf(context)).toEqual(
      runIds.slice(MOST_RUNS_PRUNED_PER_PRUNE).map((runId) => storedRunIdOf("send-email", runId)),
    );
    for (const runId of runIds.slice(0, MOST_RUNS_PRUNED_PER_PRUNE)) {
      expect(await storedKeyCountOf(context, runId), `the keys of ${runId}`).toBe(0);
    }
  },
  { timeout: OVER_CAP_PRUNE_TEST_TIMEOUT_IN_MILLISECONDS },
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
        error: { attempt: 1, name: "Error", message: "boom", stack: "" },
        failureNotice: undefined,
      },
    });
    const storedKeyCountBefore = await storedKeyCountOf(context, "run-1");
    await backdateEndedRun(context, "send-email", "run-1", 14 * DAY_IN_MILLISECONDS);

    const reply = await engine.prune(SEND_EMAIL_PRUNE_REQUEST);

    expect(storedKeyCountBefore).toBe(4);
    expect(reply).toEqual({ kind: "pruned", prunedRunCount: 1 });
    expect(await storedKeyCountOf(context, "run-1")).toBe(0);
    expect(await redis.zScore(keys.runIndex("send-email", "failed"), storedRunIdOf("send-email", "run-1"))).toBeNull();
  },
);

engineTest(
  "on a task with 1,001 completed runs, a repeat 23 h 59 after its run ended is dropped, and one at 24 h 01 starts a new run",
  async (context) => {
    const { engine, redis, keys } = context;
    await endRuns(engine, "send-email", ["run-ended-24h01-ago", "run-ended-23h59-ago"], "complete");
    await endRuns(engine, "send-email", runIdsOf("newer-run", 1000), "complete");
    await backdateEndedRun(context, "send-email", "run-ended-24h01-ago", DAY_IN_MILLISECONDS + MINUTE_IN_MILLISECONDS);
    await backdateEndedRun(context, "send-email", "run-ended-23h59-ago", DAY_IN_MILLISECONDS - MINUTE_IN_MILLISECONDS);

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
      createdRuns: [{ task: "send-email", runId: "run-ended-24h01-ago" }],
      skippedRuns: [{ task: "send-email", runId: "run-ended-23h59-ago" }],
    });
    expect(await redis.exists(keys.runRecord("send-email", "run-ended-23h59-ago"))).toBe(0);
  },
);

engineTest(
  `a prune trims at most ${String(EXPIRED_DEDUP_ENTRIES_TRIMMED_PER_PRUNE)} expired dedup entries of its task, and none that is live`,
  async ({ engine, redis, keys }) => {
    const dedupKey = keys.runDedup("send-email");
    const expiredEntryCount = EXPIRED_DEDUP_ENTRIES_TRIMMED_PER_PRUNE + 50;
    const liveExpiresAt = (await serverInstantInMillisecondsOf(redis)) + DAY_IN_MILLISECONDS;
    await redis.zAdd(
      dedupKey,
      runIdsOf("expired-run", expiredEntryCount).map((runId) => ({
        score: 1,
        value: storedRunIdOf("send-email", runId),
      })),
    );
    await redis.zAdd(dedupKey, { score: liveExpiresAt, value: storedRunIdOf("send-email", "live-run") });
    await redis.zAdd(keys.runDedup("resize-image"), {
      score: 1,
      value: storedRunIdOf("resize-image", "other-task-run"),
    });

    await engine.prune(SEND_EMAIL_PRUNE_REQUEST);
    const entryCountAfterTheFirstPrune = await redis.zCard(dedupKey);
    await engine.prune(SEND_EMAIL_PRUNE_REQUEST);

    expect(entryCountAfterTheFirstPrune).toBe(expiredEntryCount + 1 - EXPIRED_DEDUP_ENTRIES_TRIMMED_PER_PRUNE);
    expect(await redis.zRange(dedupKey, 0, -1)).toEqual([storedRunIdOf("send-email", "live-run")]);
    expect(await redis.zCard(keys.runDedup("resize-image"))).toBe(1);
  },
);

engineTest(
  "a prune passes over aged runs of a newer record version, leaving them whole and their task in the tasks set, and removes the rest",
  async (context) => {
    const { engine, redis, keys } = context;
    const newerVersionRunIds = runIdsOf("newer-version-run", 3);
    const runIds = runIdsOf("run", 5);
    await endRunsOfANewerRecordVersionBefore(context, newerVersionRunIds, runIds);
    for (const runId of [...newerVersionRunIds, ...runIds]) {
      await backdateEndedRun(context, "send-email", runId, 2 * DAY_IN_MILLISECONDS);
    }

    const reply = await engine.prune(SEND_EMAIL_PRUNE_REQUEST);

    expect(reply).toEqual({ kind: "pruned", prunedRunCount: 5 });
    expect(await completedRunIdsOf(context)).toEqual(
      newerVersionRunIds.map((runId) => storedRunIdOf("send-email", runId)),
    );
    for (const runId of newerVersionRunIds) {
      expect(
        await redis.exists([keys.runRecord("send-email", runId), keys.runPayload("send-email", runId)]),
        runId,
      ).toBe(2);
    }
    expect(await redis.zCard(keys.runDedup("send-email"))).toBe(0);
    expect(await redis.sMembers(keys.tasks)).toEqual(["send-email"]);
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
    expect(await completedRunIdsOf(context)).toEqual(
      [...newerVersionRunIds, "run-1"].map((runId) => storedRunIdOf("send-email", runId)),
    );
  },
);

engineTest(
  `a prune that removed its ${String(MOST_RUNS_PRUNED_PER_PRUNE)} runs with aged runs left leaves the task in the tasks set, and the prune that removes the last ones takes it out`,
  async (context) => {
    const { engine, redis, keys } = context;
    const runIds = runIdsOf("run", MOST_RUNS_PRUNED_PER_PRUNE + 50);
    await endRuns(engine, "send-email", runIds, "complete");
    for (const runId of runIds) {
      await backdateEndedRun(context, "send-email", runId, 2 * DAY_IN_MILLISECONDS);
    }

    const firstReply = await engine.prune(SEND_EMAIL_PRUNE_REQUEST);
    const taskNamesAfterTheFirstPrune = await redis.sMembers(keys.tasks);
    const secondReply = await engine.prune(SEND_EMAIL_PRUNE_REQUEST);

    expect(firstReply).toEqual({ kind: "pruned", prunedRunCount: MOST_RUNS_PRUNED_PER_PRUNE });
    expect(taskNamesAfterTheFirstPrune).toEqual(["send-email"]);
    expect(secondReply).toEqual({ kind: "pruned", prunedRunCount: 50 });
    expect(await redis.sMembers(keys.tasks)).toEqual([]);
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
            error: { attempt: 1, name: "Error", message: "boom", stack: "" },
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
        expect(await storedKeyViolationsOf(redis, keyPrefix), `after round ${String(round)}, ${seedMessage}`).toEqual(
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
