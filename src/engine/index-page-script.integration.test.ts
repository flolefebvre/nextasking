import { expect, vi } from "vitest";

import { endRuns } from "../../tests/ended-runs.js";
import { engineTest, fetchedLockedRunsOf } from "../../tests/engine-test.js";
import type { EngineTestContext } from "../../tests/engine-test.js";
import { serverInstantInMillisecondsOf } from "../../tests/server-instant.js";
import type { EpochMilliseconds } from "../durations.js";
import type { RunId } from "../ids.js";
import { storedRunIdOf } from "./engine-keys.js";
import type { StoredRunId } from "./engine-keys.js";
import type { LockedRun } from "./engine.js";
import { indexPagesCommandOf, indexPagesOf } from "./index-page-script.js";
import type {
  IndexInList,
  IndexPage,
  IndexPageBoundary,
  IndexPlace,
  ListPlace,
  PagingDirection,
} from "./index-page-script.js";

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
  expect(later.startedAt).toBeGreaterThan(earlier.startedAt);
  return { earlier, later, indexKey: context.keys.runIndex("send-email", "executing") };
}

async function indexPagesInListAt(
  { client }: EngineTestContext,
  indexesInList: readonly IndexInList[],
  limit: number,
  boundary: IndexPageBoundary,
): Promise<IndexPage[]> {
  return indexPagesOf(await client.send(indexPagesCommandOf({ indexesInList, limit, boundary })), indexesInList.length);
}

const ONE_ORDER_GROUP_NUMBER = 1;

async function indexPagesAt(
  context: EngineTestContext,
  indexKeys: readonly string[],
  limit: number,
  { direction, place }: { direction: PagingDirection; place: IndexPlace | undefined },
): Promise<IndexPage[]> {
  return indexPagesInListAt(
    context,
    indexKeys.map((indexKey) => ({ indexKey, orderGroupNumber: ONE_ORDER_GROUP_NUMBER })),
    limit,
    { direction, place: place === undefined ? undefined : { ...place, orderGroupNumber: ONE_ORDER_GROUP_NUMBER } },
  );
}

async function indexPagesAfter(
  context: EngineTestContext,
  indexKeys: readonly string[],
  limit: number,
  after: IndexPlace | undefined,
): Promise<IndexPage[]> {
  return indexPagesAt(context, indexKeys, limit, { direction: "forward", place: after });
}

async function indexPageBefore(
  context: EngineTestContext,
  indexKey: string,
  limit: number,
  before: IndexPlace | undefined,
): Promise<IndexPage> {
  const [indexPage] = await indexPagesAt(context, [indexKey], limit, { direction: "backward", place: before });
  return indexPage!;
}

async function storedRunIdsBefore(
  context: EngineTestContext,
  indexKey: string,
  limit: number,
  before: IndexPlace | undefined,
): Promise<string[]> {
  return (await indexPageBefore(context, indexKey, limit, before)).places.map(({ storedRunId }) => storedRunId);
}

async function indexPageAfter(
  context: EngineTestContext,
  indexKey: string,
  limit: number,
  after: IndexPlace | undefined,
): Promise<IndexPlace[]> {
  const [indexPage] = await indexPagesAfter(context, [indexKey], limit, after);
  return indexPage!.places;
}

async function storedRunIdsAfter(
  context: EngineTestContext,
  indexKey: string,
  limit: number,
  after: IndexPlace | undefined,
): Promise<string[]> {
  return (await indexPageAfter(context, indexKey, limit, after)).map(({ storedRunId }) => storedRunId);
}

async function walkedStoredRunIdsOf(context: EngineTestContext, indexKey: string, limit: number): Promise<string[]> {
  const walkedStoredRunIds: string[] = [];
  let after: IndexPlace | undefined;
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

async function placeOfARunThatLeftTheIndexOf(
  context: EngineTestContext,
): Promise<{ indexKey: string; placeLeft: IndexPlace }> {
  const { earlier, indexKey } = await twoGroupsOfExecutingRunsOf(context);
  const leavingRun = earlier.lockedRuns.find((lockedRun) => lockedRun.runId === "run")!;
  await context.engine.settle({ ...leavingRun, outcome: { kind: "complete" } });
  return { indexKey, placeLeft: { score: earlier.startedAt, storedRunId: storedRunIdOf("send-email", "run") } };
}

engineTest("a page after a run that left the index starts at the run that followed it", async (context) => {
  const { indexKey, placeLeft } = await placeOfARunThatLeftTheIndexOf(context);

  const storedRunIds = await storedRunIdsAfter(context, indexKey, 2, placeLeft);

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

engineTest(
  "an index page counts the entries of its index and those before its first entry, among runs sharing a score",
  async (context) => {
    const { earlier, indexKey } = await twoGroupsOfExecutingRunsOf(context);

    const [indexPage] = await indexPagesAfter(context, [indexKey], 2, {
      score: earlier.startedAt,
      storedRunId: storedRunIdOf("send-email", "épée"),
    });

    expect(indexPage).toEqual({
      entryCount: 10,
      entryCountBeforeBoundary: 5,
      firstEntryRank: 5,
      places: storedRunIdsOf(["run-1", "run"]).map((storedRunId) => ({ storedRunId, score: earlier.startedAt })),
    });
  },
);

engineTest(
  "an index page after a run that left the index counts the entries before the place it left",
  async (context) => {
    const { indexKey, placeLeft } = await placeOfARunThatLeftTheIndexOf(context);

    const [indexPage] = await indexPagesAfter(context, [indexKey], 1, placeLeft);

    expect(indexPage).toMatchObject({ entryCount: 9, entryCountBeforeBoundary: 6, firstEntryRank: 6 });
  },
);

engineTest("one call reads a page of each index it is given, in the order of its keys", async (context) => {
  const { later, indexKey } = await twoGroupsOfExecutingRunsOf(context);
  const emptyIndexKey = context.keys.runIndex("build-report", "executing");

  const indexPages = await indexPagesAfter(context, [emptyIndexKey, indexKey], 1, undefined);

  expect(indexPages).toEqual([
    { entryCount: 0, entryCountBeforeBoundary: 0, firstEntryRank: 0, places: [] },
    {
      entryCount: 10,
      entryCountBeforeBoundary: 0,
      firstEntryRank: 0,
      places: [{ storedRunId: storedRunIdOf("send-email", LATER_RUN_IDS_IN_INDEX_ORDER[0]!), score: later.startedAt }],
    },
  ]);
});

const RUN_IDS_IN_INDEX_ORDER: readonly RunId[] = [...LATER_RUN_IDS_IN_INDEX_ORDER, ...EARLIER_RUN_IDS_IN_INDEX_ORDER];

function placesBeforeTheBoundaryOf(indexPage: IndexPage): IndexPlace[] {
  return indexPage.places.slice(0, indexPage.entryCountBeforeBoundary - indexPage.firstEntryRank);
}

async function storedRunIdsWalkedBackwardOf(
  context: EngineTestContext,
  indexKey: string,
  limit: number,
): Promise<string[]> {
  let walkedStoredRunIds: string[] = [];
  let before: IndexPlace | undefined;
  for (;;) {
    const indexPage = await indexPageBefore(context, indexKey, limit, before);
    const placesBefore = placesBeforeTheBoundaryOf(indexPage);
    walkedStoredRunIds = [...placesBefore.map(({ storedRunId }) => storedRunId), ...walkedStoredRunIds];
    if (indexPage.firstEntryRank === 0) {
      return walkedStoredRunIds;
    }
    before = placesBefore[0];
  }
}

for (const limit of [1, 2, 3]) {
  engineTest(
    `a walk backward from the end in pages of ${String(limit)} lists every run of the index once, in the index's order`,
    async (context) => {
      const { indexKey } = await twoGroupsOfExecutingRunsOf(context);

      const walkedStoredRunIds = await storedRunIdsWalkedBackwardOf(context, indexKey, limit);

      expect(walkedStoredRunIds).toEqual(storedRunIdsOf(RUN_IDS_IN_INDEX_ORDER));
    },
  );
}

engineTest("a page before the end of the index holds its last runs and counts every entry before", async (context) => {
  const { earlier, indexKey } = await twoGroupsOfExecutingRunsOf(context);

  const indexPage = await indexPageBefore(context, indexKey, 3, undefined);

  expect(indexPage).toEqual({
    entryCount: 10,
    entryCountBeforeBoundary: 10,
    firstEntryRank: 7,
    places: storedRunIdsOf(["apple", "Banana", "Apple"]).map((storedRunId) => ({
      storedRunId,
      score: earlier.startedAt,
    })),
  });
});

engineTest("a page before a run ends at the run before it, among runs sharing a score", async (context) => {
  const { earlier, indexKey } = await twoGroupsOfExecutingRunsOf(context);

  const indexPage = await indexPageBefore(context, indexKey, 2, {
    score: earlier.startedAt,
    storedRunId: storedRunIdOf("send-email", "épée"),
  });

  expect(indexPage).toEqual({
    entryCount: 10,
    entryCountBeforeBoundary: 4,
    firstEntryRank: 2,
    places: storedRunIdsOf(["\u{1F600}-astral", "～-fullwidth"]).map((storedRunId) => ({
      storedRunId,
      score: earlier.startedAt,
    })),
  });
});

engineTest("a page before the first run of a score ends at the last run of the higher score", async (context) => {
  const { earlier, indexKey } = await twoGroupsOfExecutingRunsOf(context);

  const storedRunIds = await storedRunIdsBefore(context, indexKey, 1, {
    score: earlier.startedAt,
    storedRunId: storedRunIdOf("send-email", EARLIER_RUN_IDS_IN_INDEX_ORDER[0]!),
  });

  expect(storedRunIds).toEqual(storedRunIdsOf(LATER_RUN_IDS_IN_INDEX_ORDER.slice(1)));
});

engineTest("a page before a run that left the index ends at the run that came before it", async (context) => {
  const { indexKey, placeLeft } = await placeOfARunThatLeftTheIndexOf(context);

  const indexPage = await indexPageBefore(context, indexKey, 2, placeLeft);

  expect(indexPage).toMatchObject({ entryCount: 9, entryCountBeforeBoundary: 6, firstEntryRank: 4 });
  expect(indexPage.places.map(({ storedRunId }) => storedRunId)).toEqual(storedRunIdsOf(["épée", "run-1"]));
});

engineTest("a page before a run id that is a prefix of the one before ends at the longer id", async (context) => {
  const { earlier, indexKey } = await twoGroupsOfExecutingRunsOf(context);

  const storedRunIdsBeforeThePrefix = await storedRunIdsBefore(context, indexKey, 1, {
    score: earlier.startedAt,
    storedRunId: storedRunIdOf("send-email", "run"),
  });
  const storedRunIdsBeforeTheLongerId = await storedRunIdsBefore(context, indexKey, 1, {
    score: earlier.startedAt,
    storedRunId: storedRunIdOf("send-email", "run-1"),
  });

  expect(storedRunIdsBeforeThePrefix).toEqual(storedRunIdsOf(["run-1"]));
  expect(storedRunIdsBeforeTheLongerId).toEqual(storedRunIdsOf(["épée"]));
});

engineTest(
  "a page before a place with fewer runs before it than the limit holds the first runs of the index",
  async (context) => {
    const { earlier, indexKey } = await twoGroupsOfExecutingRunsOf(context);

    const indexPage = await indexPageBefore(context, indexKey, 6, {
      score: earlier.startedAt,
      storedRunId: storedRunIdOf("send-email", "épée"),
    });

    expect(indexPage).toMatchObject({ entryCount: 10, entryCountBeforeBoundary: 4, firstEntryRank: 0 });
    expect(indexPage.places.map(({ storedRunId }) => storedRunId)).toEqual(
      storedRunIdsOf(RUN_IDS_IN_INDEX_ORDER.slice(0, 6)),
    );
  },
);

type IndexesOfThreeGroups = {
  executing: IndexInList;
  pending: IndexInList;
  completed: IndexInList;
  indexesInList: IndexInList[];
};

async function indexesOfThreeGroupsOf(context: EngineTestContext): Promise<IndexesOfThreeGroups> {
  await endRuns(context.engine, "send-email", ["ended-1", "ended-2", "ended-3"], "complete");
  await context.engine.enqueue({
    runs: ["queued-1", "queued-2", "queued-3", "queued-4"].map((runId) => ({
      runId,
      task: "send-email",
      payload: "",
      retries: 0,
    })),
  });
  await fetchedLockedRunsOf(context, 2);
  const [executing, pending, completed] = [
    { indexKey: context.keys.runIndex("send-email", "executing"), orderGroupNumber: 1 },
    { indexKey: context.keys.runIndex("send-email", "pending"), orderGroupNumber: 2 },
    { indexKey: context.keys.runIndex("send-email", "completed"), orderGroupNumber: 4 },
  ];
  return { executing, pending, completed, indexesInList: [executing, pending, completed] };
}

async function placesAtRanksOf(
  { redis }: EngineTestContext,
  { indexKey }: IndexInList,
  firstRank: number,
  lastRank: number,
): Promise<IndexPlace[]> {
  const entries = await redis.zRangeWithScores(indexKey, firstRank, lastRank, { REV: true });
  return entries.map(({ value, score }) => ({ storedRunId: value, score }));
}

async function listPlaceAtRankOf(
  context: EngineTestContext,
  indexInList: IndexInList,
  rank: number,
): Promise<ListPlace> {
  const [place] = await placesAtRanksOf(context, indexInList, rank, rank);
  return { ...place!, orderGroupNumber: indexInList.orderGroupNumber };
}

engineTest(
  "a page forward from a place counts the indexes of the groups before its group wholly before it, those after wholly after, and reads only what the page can hold",
  async (context) => {
    const { pending, completed, indexesInList } = await indexesOfThreeGroupsOf(context);
    const place = await listPlaceAtRankOf(context, pending, 0);

    const indexPages = await indexPagesInListAt(context, indexesInList, 2, { direction: "forward", place });

    expect(indexPages).toEqual([
      { entryCount: 2, entryCountBeforeBoundary: 2, firstEntryRank: 2, places: [] },
      {
        entryCount: 2,
        entryCountBeforeBoundary: 1,
        firstEntryRank: 1,
        places: await placesAtRanksOf(context, pending, 1, 1),
      },
      {
        entryCount: 3,
        entryCountBeforeBoundary: 0,
        firstEntryRank: 0,
        places: await placesAtRanksOf(context, completed, 0, 0),
      },
    ]);
  },
);

engineTest("a page forward that the first groups fill reads no entry of the groups after them", async (context) => {
  const { executing, indexesInList } = await indexesOfThreeGroupsOf(context);

  const indexPages = await indexPagesInListAt(context, indexesInList, 2, { direction: "forward", place: undefined });

  expect(indexPages).toEqual([
    {
      entryCount: 2,
      entryCountBeforeBoundary: 0,
      firstEntryRank: 0,
      places: await placesAtRanksOf(context, executing, 0, 1),
    },
    { entryCount: 2, entryCountBeforeBoundary: 0, firstEntryRank: 0, places: [] },
    { entryCount: 3, entryCountBeforeBoundary: 0, firstEntryRank: 0, places: [] },
  ]);
});

engineTest(
  "a page backward from a place reads the groups before its group from the last, only what the page can hold",
  async (context) => {
    const { pending, completed, indexesInList } = await indexesOfThreeGroupsOf(context);
    const place = await listPlaceAtRankOf(context, completed, 1);

    const indexPages = await indexPagesInListAt(context, indexesInList, 2, { direction: "backward", place });

    expect(indexPages).toEqual([
      { entryCount: 2, entryCountBeforeBoundary: 2, firstEntryRank: 2, places: [] },
      {
        entryCount: 2,
        entryCountBeforeBoundary: 2,
        firstEntryRank: 1,
        places: await placesAtRanksOf(context, pending, 1, 1),
      },
      {
        entryCount: 3,
        entryCountBeforeBoundary: 1,
        firstEntryRank: 0,
        places: await placesAtRanksOf(context, completed, 0, 0),
      },
    ]);
  },
);

engineTest(
  "a page backward from a place with fewer entries before it than the limit, across every group, reads the head of the list",
  async (context) => {
    const { executing, pending, completed, indexesInList } = await indexesOfThreeGroupsOf(context);
    const place = await listPlaceAtRankOf(context, completed, 1);

    const indexPages = await indexPagesInListAt(context, indexesInList, 6, { direction: "backward", place });

    expect(indexPages).toEqual([
      {
        entryCount: 2,
        entryCountBeforeBoundary: 2,
        firstEntryRank: 0,
        places: await placesAtRanksOf(context, executing, 0, 1),
      },
      {
        entryCount: 2,
        entryCountBeforeBoundary: 2,
        firstEntryRank: 0,
        places: await placesAtRanksOf(context, pending, 0, 1),
      },
      {
        entryCount: 3,
        entryCountBeforeBoundary: 1,
        firstEntryRank: 0,
        places: await placesAtRanksOf(context, completed, 0, 1),
      },
    ]);
  },
);
