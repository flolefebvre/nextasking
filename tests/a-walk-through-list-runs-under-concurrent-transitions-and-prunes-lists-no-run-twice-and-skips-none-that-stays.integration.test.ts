import { expect } from "vitest";
import { z } from "zod";

import { storedRunIdOf } from "../src/engine/engine-keys.js";
import { createNextasking, defineTask } from "../src/index.js";
import type { RunState } from "../src/run-states.js";
import { DEFAULT_WORKER_TIMING } from "../src/worker-timing.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { endRuns, runIdsOf } from "./ended-runs.js";
import { engineTestContextOf, fetchedLockedRunsOf } from "./engine-test.js";
import type { EngineTestContext } from "./engine-test.js";
import { integrationTest } from "./integration-test.js";
import { seededRandomnessOfThisRun } from "./seeded-random.js";
import type { SeededRandomness } from "./seeded-random.js";
import { storedKeyViolationsOf } from "./stored-key-violations.js";
import { withRedis } from "./with-redis.js";

const TASK_NAMES = ["archive-order", "bill-customer", "notify-courier", "restock-shelf"] as const;

type TaskName = (typeof TASK_NAMES)[number];

const PENDING_RUNS_PER_TASK = 600;

const COMPLETED_RUNS_PER_TASK = 200;

const MOST_RUNS_PER_MOVE = 20;

const LEAST_CHANGES_DURING_A_WALK = 1_000;

const MUTATIONS_PER_SAMPLE = 20;

const MOST_PAGES_PER_WALK = 300;

type Walk = { state: RunState; task?: TaskName; limit: number; changesBetweenTwoPages: number };

const WALKS: readonly Walk[] = [
  { state: "pending", limit: 20, changesBetweenTwoPages: 25 },
  { state: "pending", task: "bill-customer", limit: 5, changesBetweenTwoPages: 50 },
  { state: "completed", limit: 8, changesBetweenTwoPages: 40 },
  { state: "completed", task: "restock-shelf", limit: 2, changesBetweenTwoPages: 50 },
];

type ListedRun = { task: string; runId: string; dispatchedAt: Date; finishedAt?: Date };

type WalkOutcome = {
  walk: Walk;
  listedRuns: ListedRun[];
  storedIdsBefore: Set<string>;
  storedIdsAfter: Set<string>;
  changesDuringTheWalk: number;
  isWalkFinished: boolean;
};

function createWalkedNextasking(redisUrl: string, keyPrefix: string) {
  return createNextasking({
    connection: { url: redisUrl },
    prefix: keyPrefix,
    tasks: TASK_NAMES.map((taskName) => defineTask(taskName, { schema: z.object({}), handler: async () => undefined })),
  });
}

type WalkedNextasking = ReturnType<typeof createWalkedNextasking>;

function createMutator(context: EngineTestContext, { random, pick }: SeededRandomness) {
  let changeCount = 0;
  let isMutating = true;
  const waiters: { changeCount: number; resolve: () => void }[] = [];
  const sampledViolations: string[] = [];
  let sampleCount = 0;
  const enqueuedRunCountByTask = new Map<string, number>(TASK_NAMES.map((taskName) => [taskName, 0]));

  const noteChanges = (count: number) => {
    changeCount += count;
    for (const waiter of waiters.filter((candidate) => candidate.changeCount <= changeCount)) {
      waiters.splice(waiters.indexOf(waiter), 1);
      waiter.resolve();
    }
  };

  const enqueueRuns = async (task: TaskName, count: number) => {
    const firstPosition = enqueuedRunCountByTask.get(task)!;
    enqueuedRunCountByTask.set(task, firstPosition + count);
    const runIds = runIdsOf(`${task}-pending`, firstPosition + count).slice(firstPosition);
    await context.engine.enqueue({ runs: runIds.map((runId) => ({ runId, task, payload: "{}", retries: 0 })) });
  };

  const completeTheOldestPendingRuns = async (count: number) => {
    const lockedRuns = await fetchedLockedRunsOf(context, count);
    await Promise.all(
      lockedRuns.map((lockedRun) => context.engine.settle({ ...lockedRun, outcome: { kind: "complete" } })),
    );
    noteChanges(lockedRuns.length);
  };

  const pruneTheOldestCompletedRuns = async (task: TaskName, count: number) => {
    const completedCount = await context.redis.zCard(context.keys.runIndex(task, "completed"));
    const { prunedRunCount } = await context.engine.prune({
      task,
      completedRetention: {
        maxAgeInMilliseconds: DEFAULT_WORKER_TIMING.completedRunRetention.maxAgeInMilliseconds,
        maxRunCount: Math.max(completedCount - count, 0),
      },
      failedRetention: DEFAULT_WORKER_TIMING.failedRunRetention,
    });
    noteChanges(prunedRunCount);
  };

  const mutateOnce = async () => {
    const choice = random();
    const count = 1 + Math.floor(random() * MOST_RUNS_PER_MOVE);
    if (choice < 0.6) {
      await completeTheOldestPendingRuns(count);
    } else if (choice < 0.8) {
      await enqueueRuns(pick(TASK_NAMES), count);
    } else {
      await pruneTheOldestCompletedRuns(pick(TASK_NAMES), Math.ceil(count / 2));
    }
  };

  const mutating = (async () => {
    for (let mutation = 1; isMutating; mutation += 1) {
      await mutateOnce();
      if (mutation % MUTATIONS_PER_SAMPLE === 0) {
        sampledViolations.push(...(await storedKeyViolationsOf(context.redis, context.keyPrefix)));
        sampleCount += 1;
      }
    }
  })();

  return {
    changeCount: () => changeCount,
    changesReached: (target: number) =>
      target <= changeCount
        ? Promise.resolve()
        : new Promise<void>((resolve) => {
            waiters.push({ changeCount: target, resolve });
          }),
    stop: async () => {
      isMutating = false;
      await mutating;
      return { sampledViolations, sampleCount };
    },
  };
}

type Mutator = ReturnType<typeof createMutator>;

async function storedIdsOf(context: EngineTestContext, { state, task }: Walk): Promise<Set<string>> {
  const storedIds = new Set<string>();
  for (const taskName of task === undefined ? TASK_NAMES : [task]) {
    for (const storedId of await context.redis.zRange(context.keys.runIndex(taskName, state), 0, -1)) {
      storedIds.add(storedId);
    }
  }
  return storedIds;
}

async function walkedWhileMutating(
  context: EngineTestContext,
  nextasking: WalkedNextasking,
  mutator: Mutator,
  walk: Walk,
): Promise<WalkOutcome> {
  const storedIdsBefore = await storedIdsOf(context, walk);
  const changeCountAtTheFirstPage = mutator.changeCount();
  const listedRuns: ListedRun[] = [];
  let cursor: string | undefined;
  let pageCount = 0;
  for (; pageCount === 0 || (cursor !== undefined && pageCount < MOST_PAGES_PER_WALK); pageCount += 1) {
    await mutator.changesReached(changeCountAtTheFirstPage + pageCount * walk.changesBetweenTwoPages);
    const { runs, nextCursor } = await nextasking.listRuns({
      state: walk.state,
      limit: walk.limit,
      ...(walk.task === undefined ? {} : { task: walk.task }),
      ...(cursor === undefined ? {} : { cursor }),
    });
    listedRuns.push(...(runs as ListedRun[]));
    cursor = nextCursor;
  }
  const changesDuringTheWalk = mutator.changeCount() - changeCountAtTheFirstPage;
  const storedIdsAfter = await storedIdsOf(context, walk);
  return {
    walk,
    listedRuns,
    storedIdsBefore,
    storedIdsAfter,
    changesDuringTheWalk,
    isWalkFinished: cursor === undefined,
  };
}

function promisedInstantOf({ walk }: WalkOutcome, listedRun: ListedRun): number {
  return (walk.state === "completed" ? listedRun.finishedAt! : listedRun.dispatchedAt).getTime();
}

function expectNoRunTwiceAndNoneThatStayedSkipped(outcome: WalkOutcome, seedMessage: string): void {
  const message = `${JSON.stringify(outcome.walk)}: ${seedMessage}`;
  const listedIds = outcome.listedRuns.map(({ task, runId }) => storedRunIdOf(task, runId));
  const stayedIds = [...outcome.storedIdsBefore].filter((storedId) => outcome.storedIdsAfter.has(storedId));
  const listedIdSet = new Set<string>(listedIds);
  const promisedInstants = outcome.listedRuns.map((listedRun) => promisedInstantOf(outcome, listedRun));
  expect(outcome.isWalkFinished, `no walk end within ${String(MOST_PAGES_PER_WALK)} pages: ${message}`).toBe(true);
  expect(
    listedIds.filter((storedId, position) => listedIds.indexOf(storedId) !== position),
    message,
  ).toEqual([]);
  expect(
    stayedIds.filter((storedId) => !listedIdSet.has(storedId)),
    message,
  ).toEqual([]);
  expect(
    outcome.listedRuns.filter(({ task }) => outcome.walk.task !== undefined && task !== outcome.walk.task),
    message,
  ).toEqual([]);
  expect(
    promisedInstants.filter((instant, position) => position > 0 && instant > promisedInstants[position - 1]!),
    message,
  ).toEqual([]);
  expect(outcome.changesDuringTheWalk, message).toBeGreaterThanOrEqual(LEAST_CHANGES_DURING_A_WALK);
}

integrationTest(
  "a walk through listRuns, with or without a task, under concurrent transitions and prunes lists no run twice, skips none that stays in the state and keeps the promised order",
  { timeout: 60_000 },
  async ({ redisUrl, keyPrefix }) => {
    const randomness = seededRandomnessOfThisRun();
    const { seedMessage } = randomness;
    const nextasking = createWalkedNextasking(redisUrl, keyPrefix);
    try {
      await withRedis(redisUrl, async (redis, client) => {
        const context = engineTestContextOf(redis, client, keyPrefix);
        for (const taskName of TASK_NAMES) {
          await endRuns(
            context.engine,
            taskName,
            runIdsOf(`${taskName}-completed`, COMPLETED_RUNS_PER_TASK),
            "complete",
          );
        }
        for (const taskName of TASK_NAMES) {
          await context.engine.enqueue({
            runs: runIdsOf(`${taskName}-initial`, PENDING_RUNS_PER_TASK).map((runId) => ({
              runId,
              task: taskName,
              payload: "{}",
              retries: 0,
            })),
          });
        }
        const mutator = createMutator(context, randomness);
        let mutatorReport: Awaited<ReturnType<Mutator["stop"]>> | undefined;
        let outcomes: WalkOutcome[] = [];
        try {
          outcomes = await Promise.all(WALKS.map((walk) => walkedWhileMutating(context, nextasking, mutator, walk)));
        } finally {
          mutatorReport = await mutator.stop();
        }

        for (const outcome of outcomes) {
          expectNoRunTwiceAndNoneThatStayedSkipped(outcome, seedMessage);
        }
        expect(mutatorReport.sampleCount, seedMessage).toBeGreaterThan(0);
        expect(mutatorReport.sampledViolations, seedMessage).toEqual([]);
      });
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);
