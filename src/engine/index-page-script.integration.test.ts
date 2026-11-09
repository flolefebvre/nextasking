import { expect, vi } from "vitest";

import { engineTest, fetchedLockedRunsOf } from "../../tests/engine-test.js";
import type { EngineTestContext } from "../../tests/engine-test.js";
import { serverInstantInMillisecondsOf } from "../../tests/server-instant.js";
import type { EpochMilliseconds } from "../durations.js";
import type { RunId } from "../ids.js";
import { storedRunIdOf } from "./engine-keys.js";
import type { StoredRunId } from "./engine-keys.js";
import type { LockedRun } from "./engine.js";
import { indexPageCommandOf, indexPageOf } from "./index-page-script.js";
import type { IndexPosition } from "./index-page-script.js";

const EARLIER_RUN_IDS_IN_INDEX_ORDER: readonly RunId[] = [
  "\u{1F600}-astral",
  "～-fullwidth",
  "épée",
  "run-1",
  "run",
  "apple",
  "Banana",
  "Apple",
];

const LATER_RUN_IDS_IN_INDEX_ORDER: readonly RunId[] = ["later-b", "later-a"];

function storedRunIdsOf(runIds: readonly RunId[]): StoredRunId[] {
  return runIds.map((runId) => storedRunIdOf("send-email", runId));
}

type StartedGroup = { startedAt: EpochMilliseconds; lockedRuns: LockedRun[] };

type TwoStartedGroups = { earlier: StartedGroup; later: StartedGroup; indexKey: string };

async function runsStartedTogether(context: EngineTestContext, runIds: readonly RunId[]): Promise<StartedGroup> {
  await context.engine.enqueue({
    runs: runIds.map((runId) => ({ runId, task: "send-email", payload: "", retries: 0 })),
  });
  const lockedRuns = await fetchedLockedRunsOf(context, runIds.length);
  const startedAt = lockedRuns[0]!.startedAt;
  expect(lockedRuns.map((lockedRun) => lockedRun.startedAt)).toEqual(runIds.map(() => startedAt));
  return { startedAt, lockedRuns };
}

async function twoGroupsOfExecutingRunsOf(context: EngineTestContext): Promise<TwoStartedGroups> {
  const earlier = await runsStartedTogether(context, [...EARLIER_RUN_IDS_IN_INDEX_ORDER].reverse());
  await vi.waitUntil(async () => (await serverInstantInMillisecondsOf(context.redis)) > earlier.startedAt, {
    timeout: 1000,
    interval: 1,
  });
  const later = await runsStartedTogether(context, LATER_RUN_IDS_IN_INDEX_ORDER);
  return { earlier, later, indexKey: context.keys.runIndex("send-email", "executing") };
}

async function indexPageAfter(
  { client }: EngineTestContext,
  indexKey: string,
  limit: number,
  after: IndexPosition | undefined,
): Promise<IndexPosition[]> {
  return indexPageOf(await client.send(indexPageCommandOf({ indexKey, limit, after })));
}

async function storedRunIdsAfter(
  context: EngineTestContext,
  indexKey: string,
  limit: number,
  after: IndexPosition | undefined,
): Promise<string[]> {
  return (await indexPageAfter(context, indexKey, limit, after)).map(({ storedRunId }) => storedRunId);
}

async function walkedStoredRunIdsOf(context: EngineTestContext, indexKey: string, limit: number): Promise<string[]> {
  const walkedStoredRunIds: string[] = [];
  let after: IndexPosition | undefined;
  for (;;) {
    const indexPage = await indexPageAfter(context, indexKey, limit, after);
    walkedStoredRunIds.push(...indexPage.map(({ storedRunId }) => storedRunId));
    if (indexPage.length < limit) {
      return walkedStoredRunIds;
    }
    after = indexPage.at(-1);
  }
}

engineTest("an index page lists the index most recent score first, then run ids in byte order", async (context) => {
  const { earlier, later, indexKey } = await twoGroupsOfExecutingRunsOf(context);

  const indexPage = await indexPageAfter(context, indexKey, 20, undefined);

  expect(indexPage).toEqual([
    ...storedRunIdsOf(LATER_RUN_IDS_IN_INDEX_ORDER).map((storedRunId) => ({ storedRunId, score: later.startedAt })),
    ...storedRunIdsOf(EARLIER_RUN_IDS_IN_INDEX_ORDER).map((storedRunId) => ({ storedRunId, score: earlier.startedAt })),
  ]);
});

for (const limit of [1, 2, 3]) {
  engineTest(
    `a walk in pages of ${String(limit)} lists every run of the index once, in the index's order`,
    async (context) => {
      const { indexKey } = await twoGroupsOfExecutingRunsOf(context);

      const walkedStoredRunIds = await walkedStoredRunIdsOf(context, indexKey, limit);

      expect(walkedStoredRunIds).toEqual(
        storedRunIdsOf([...LATER_RUN_IDS_IN_INDEX_ORDER, ...EARLIER_RUN_IDS_IN_INDEX_ORDER]),
      );
      expect(walkedStoredRunIds).toEqual(await context.redis.zRange(indexKey, 0, -1, { REV: true }));
    },
  );
}

engineTest("a page after the first run of a score starts at the second run of that score", async (context) => {
  const { earlier, indexKey } = await twoGroupsOfExecutingRunsOf(context);

  const storedRunIds = await storedRunIdsAfter(context, indexKey, 2, {
    score: earlier.startedAt,
    storedRunId: storedRunIdOf("send-email", EARLIER_RUN_IDS_IN_INDEX_ORDER[0]!),
  });

  expect(storedRunIds).toEqual(storedRunIdsOf(EARLIER_RUN_IDS_IN_INDEX_ORDER.slice(1, 3)));
});

engineTest("a page after the last run of a score starts at the next lower score", async (context) => {
  const { later, indexKey } = await twoGroupsOfExecutingRunsOf(context);

  const storedRunIds = await storedRunIdsAfter(context, indexKey, 2, {
    score: later.startedAt,
    storedRunId: storedRunIdOf("send-email", LATER_RUN_IDS_IN_INDEX_ORDER.at(-1)!),
  });

  expect(storedRunIds).toEqual(storedRunIdsOf(EARLIER_RUN_IDS_IN_INDEX_ORDER.slice(0, 2)));
});

engineTest("a page after the last run of the index is empty", async (context) => {
  const { earlier, indexKey } = await twoGroupsOfExecutingRunsOf(context);

  const storedRunIds = await storedRunIdsAfter(context, indexKey, 2, {
    score: earlier.startedAt,
    storedRunId: storedRunIdOf("send-email", EARLIER_RUN_IDS_IN_INDEX_ORDER.at(-1)!),
  });

  expect(storedRunIds).toEqual([]);
});

engineTest("a page after a score no run has starts at the next lower score", async (context) => {
  const { earlier, later, indexKey } = await twoGroupsOfExecutingRunsOf(context);

  const storedRunIdsBetweenTheScores = await storedRunIdsAfter(context, indexKey, 1, {
    score: earlier.startedAt + 0.5,
    storedRunId: storedRunIdOf("send-email", "z"),
  });
  const storedRunIdsAboveEveryScore = await storedRunIdsAfter(context, indexKey, 1, {
    score: later.startedAt + 1,
    storedRunId: storedRunIdOf("send-email", "a"),
  });

  expect(storedRunIdsBetweenTheScores).toEqual(storedRunIdsOf(EARLIER_RUN_IDS_IN_INDEX_ORDER.slice(0, 1)));
  expect(storedRunIdsAboveEveryScore).toEqual(storedRunIdsOf(LATER_RUN_IDS_IN_INDEX_ORDER.slice(0, 1)));
});

engineTest("a page after a run that left the index starts at the run that followed it", async (context) => {
  const { engine } = context;
  const { earlier, indexKey } = await twoGroupsOfExecutingRunsOf(context);
  const leavingRun = earlier.lockedRuns.find((lockedRun) => lockedRun.runId === "run")!;
  await engine.settle({ ...leavingRun, outcome: { kind: "complete" } });

  const storedRunIds = await storedRunIdsAfter(context, indexKey, 2, {
    score: earlier.startedAt,
    storedRunId: storedRunIdOf("send-email", "run"),
  });

  expect(storedRunIds).toEqual(storedRunIdsOf(["apple", "Banana"]));
});

engineTest("a page after a run id that is a prefix of the next one starts after the longer id", async (context) => {
  const { earlier, indexKey } = await twoGroupsOfExecutingRunsOf(context);

  const storedRunIdsAfterTheLongerId = await storedRunIdsAfter(context, indexKey, 1, {
    score: earlier.startedAt,
    storedRunId: storedRunIdOf("send-email", "run-1"),
  });
  const storedRunIdsAfterThePrefix = await storedRunIdsAfter(context, indexKey, 1, {
    score: earlier.startedAt,
    storedRunId: storedRunIdOf("send-email", "run"),
  });

  expect(storedRunIdsAfterTheLongerId).toEqual(storedRunIdsOf(["run"]));
  expect(storedRunIdsAfterThePrefix).toEqual(storedRunIdsOf(["apple"]));
});
