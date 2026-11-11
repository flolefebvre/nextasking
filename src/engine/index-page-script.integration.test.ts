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
  ListOrder,
  ListPlace,
  PagingDirection,
  ScoreRange,
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

type IndexListing = { order: ListOrder; scoreRange: ScoreRange };

const EVERY_SCORE_IN_DESCENDING_ORDER: IndexListing = {
  order: "descending",
  scoreRange: { lowestScore: undefined, highestScore: undefined },
};

async function indexPagesInListAt(
  { client }: EngineTestContext,
  indexesInList: readonly IndexInList[],
  limit: number,
  boundary: IndexPageBoundary,
  listing: IndexListing = EVERY_SCORE_IN_DESCENDING_ORDER,
): Promise<IndexPage[]> {
  return indexPagesOf(
    await client.send(indexPagesCommandOf({ indexesInList, limit, boundary, ...listing })),
    indexesInList.length,
  );
}

const ONE_ORDER_GROUP_NUMBER = 1;

async function indexPagesAt(
  context: EngineTestContext,
  indexKeys: readonly string[],
  limit: number,
  { direction, place }: { direction: PagingDirection; place: IndexPlace | undefined },
  listing: IndexListing = EVERY_SCORE_IN_DESCENDING_ORDER,
): Promise<IndexPage[]> {
  return indexPagesInListAt(
    context,
    indexKeys.map((indexKey) => ({ indexKey, orderGroupNumber: ONE_ORDER_GROUP_NUMBER })),
    limit,
    { direction, place: place === undefined ? undefined : { ...place, orderGroupNumber: ONE_ORDER_GROUP_NUMBER } },
    listing,
  );
}

async function indexPagesAfter(
  context: EngineTestContext,
  indexKeys: readonly string[],
  limit: number,
  after: IndexPlace | undefined,
  listing: IndexListing = EVERY_SCORE_IN_DESCENDING_ORDER,
): Promise<IndexPage[]> {
  return indexPagesAt(context, indexKeys, limit, { direction: "forward", place: after }, listing);
}

async function indexPageBefore(
  context: EngineTestContext,
  indexKey: string,
  limit: number,
  before: IndexPlace | undefined,
  listing: IndexListing = EVERY_SCORE_IN_DESCENDING_ORDER,
): Promise<IndexPage> {
  const [indexPage] = await indexPagesAt(context, [indexKey], limit, { direction: "backward", place: before }, listing);
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
  listing: IndexListing = EVERY_SCORE_IN_DESCENDING_ORDER,
): Promise<IndexPlace[]> {
  const [indexPage] = await indexPagesAfter(context, [indexKey], limit, after, listing);
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

async function walkedStoredRunIdsOf(
  context: EngineTestContext,
  indexKey: string,
  limit: number,
  listing: IndexListing = EVERY_SCORE_IN_DESCENDING_ORDER,
): Promise<string[]> {
  const walkedStoredRunIds: string[] = [];
  let after: IndexPlace | undefined;
  for (;;) {
    const indexPage = await indexPageAfter(context, indexKey, limit, after, listing);
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
  listing: IndexListing = EVERY_SCORE_IN_DESCENDING_ORDER,
): Promise<string[]> {
  let walkedStoredRunIds: string[] = [];
  let before: IndexPlace | undefined;
  for (;;) {
    const indexPage = await indexPageBefore(context, indexKey, limit, before, listing);
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

function placesOf(runIds: readonly RunId[], score: number): IndexPlace[] {
  return storedRunIdsOf(runIds).map((storedRunId) => ({ storedRunId, score }));
}

function listingOf(order: ListOrder, lowestScore?: number, highestScore?: number): IndexListing {
  return { order, scoreRange: { lowestScore, highestScore } };
}

function reversed<Element>(elements: readonly Element[]): Element[] {
  return [...elements].reverse();
}

const LISTINGS_WITH_THEIR_RUN_IDS: readonly [string, (groups: TwoStartedGroups) => IndexListing, readonly RunId[]][] = [
  ["in ascending order", () => listingOf("ascending"), reversed(RUN_IDS_IN_INDEX_ORDER)],
  [
    "in a score range, in descending order",
    ({ earlier }) => listingOf("descending", earlier.startedAt, earlier.startedAt),
    EARLIER_RUN_IDS_IN_INDEX_ORDER,
  ],
  [
    "in a score range, in ascending order",
    ({ earlier }) => listingOf("ascending", earlier.startedAt, earlier.startedAt),
    reversed(EARLIER_RUN_IDS_IN_INDEX_ORDER),
  ],
  [
    "from a lowest score alone, in ascending order",
    ({ later }) => listingOf("ascending", later.startedAt),
    reversed(LATER_RUN_IDS_IN_INDEX_ORDER),
  ],
  [
    "up to a highest score alone, in descending order",
    ({ earlier }) => listingOf("descending", undefined, earlier.startedAt),
    EARLIER_RUN_IDS_IN_INDEX_ORDER,
  ],
];

for (const [description, listingOfTheGroups, expectedRunIds] of LISTINGS_WITH_THEIR_RUN_IDS) {
  for (const limit of [1, 3]) {
    engineTest(
      `a walk ${description} in pages of ${String(limit)} lists the entries inside the range once each, forward and backward alike`,
      async (context) => {
        const groups = await twoGroupsOfExecutingRunsOf(context);
        const listing = listingOfTheGroups(groups);

        const walkedForward = await walkedStoredRunIdsOf(context, groups.indexKey, limit, listing);
        const walkedBackward = await storedRunIdsWalkedBackwardOf(context, groups.indexKey, limit, listing);

        expect(walkedForward).toEqual(storedRunIdsOf(expectedRunIds));
        expect(walkedBackward).toEqual(storedRunIdsOf(expectedRunIds));
      },
    );
  }
}

engineTest(
  "an index page in a score range counts and ranks only the entries inside it, in each order",
  async (context) => {
    const { earlier, later, indexKey } = await twoGroupsOfExecutingRunsOf(context);
    const earlierOnly = (order: ListOrder) => listingOf(order, earlier.startedAt, earlier.startedAt);
    const placeOfÉpée = { score: earlier.startedAt, storedRunId: storedRunIdOf("send-email", "épée") };

    const pages = {
      descendingAfterÉpée: await indexPagesAfter(context, [indexKey], 2, placeOfÉpée, earlierOnly("descending")),
      descendingAtTheEnd: await indexPageBefore(context, indexKey, 3, undefined, earlierOnly("descending")),
      ascendingAfterÉpée: await indexPagesAfter(context, [indexKey], 2, placeOfÉpée, earlierOnly("ascending")),
      ascendingOfTheLaterScore: await indexPagesAfter(
        context,
        [indexKey],
        1,
        undefined,
        listingOf("ascending", later.startedAt, later.startedAt),
      ),
    };

    expect(pages.descendingAfterÉpée).toEqual([
      {
        entryCount: 8,
        entryCountBeforeBoundary: 3,
        firstEntryRank: 3,
        places: placesOf(["run-1", "run"], earlier.startedAt),
      },
    ]);
    expect(pages.descendingAtTheEnd).toEqual({
      entryCount: 8,
      entryCountBeforeBoundary: 8,
      firstEntryRank: 5,
      places: placesOf(["apple", "Banana", "Apple"], earlier.startedAt),
    });
    expect(pages.ascendingAfterÉpée).toEqual([
      {
        entryCount: 8,
        entryCountBeforeBoundary: 6,
        firstEntryRank: 6,
        places: placesOf(["～-fullwidth", "\u{1F600}-astral"], earlier.startedAt),
      },
    ]);
    expect(pages.ascendingOfTheLaterScore).toEqual([
      { entryCount: 2, entryCountBeforeBoundary: 0, firstEntryRank: 0, places: placesOf(["later-a"], later.startedAt) },
    ]);
  },
);

type PlaceSide = "before the range" | "after the range";

for (const order of ["descending", "ascending"] as const) {
  for (const placeSide of ["before the range", "after the range"] as const satisfies readonly PlaceSide[]) {
    for (const direction of ["forward", "backward"] as const) {
      engineTest(
        `a place ${placeSide} in ${order} order counts as the range's edge when paging ${direction}`,
        async (context) => {
          const { earlier, later, indexKey } = await twoGroupsOfExecutingRunsOf(context);
          const listing = listingOf(order, earlier.startedAt, earlier.startedAt);
          const [placeAbove, placeBelow] = [
            { score: later.startedAt, storedRunId: storedRunIdOf("send-email", "later-a") },
            { score: earlier.startedAt - 1, storedRunId: storedRunIdOf("send-email", "z") },
          ];
          const isAboveBeforeInList = order === "descending";
          const place = (placeSide === "before the range") === isAboveBeforeInList ? placeAbove : placeBelow;
          const runIdsInListOrder =
            order === "descending" ? EARLIER_RUN_IDS_IN_INDEX_ORDER : reversed(EARLIER_RUN_IDS_IN_INDEX_ORDER);

          const [indexPage] = await indexPagesAt(context, [indexKey], 2, { direction, place }, listing);

          const entryCountBeforeBoundary = placeSide === "before the range" ? 0 : 8;
          const expectedPage = {
            "before the range forward": { firstEntryRank: 0, runIds: runIdsInListOrder.slice(0, 2) },
            "before the range backward": { firstEntryRank: 0, runIds: runIdsInListOrder.slice(0, 2) },
            "after the range forward": { firstEntryRank: 8, runIds: [] },
            "after the range backward": { firstEntryRank: 6, runIds: runIdsInListOrder.slice(6) },
          }[`${placeSide} ${direction}`];
          expect(indexPage).toEqual({
            entryCount: 8,
            entryCountBeforeBoundary,
            firstEntryRank: expectedPage.firstEntryRank,
            places: placesOf(expectedPage.runIds, earlier.startedAt),
          });
        },
      );
    }
  }
}

for (const order of ["descending", "ascending"] as const) {
  engineTest(`an index wholly outside the score range reads as empty, in ${order} order`, async (context) => {
    const { earlier, later, indexKey } = await twoGroupsOfExecutingRunsOf(context);
    const [aboveEveryScore, belowEveryScore] = [
      listingOf(order, later.startedAt + 1, later.startedAt + 1000),
      listingOf(order, earlier.startedAt - 1000, earlier.startedAt - 1),
    ];
    const anyPlace = { score: earlier.startedAt, storedRunId: storedRunIdOf("send-email", "run") };

    const indexPages = [
      ...(await indexPagesAfter(context, [indexKey], 2, undefined, aboveEveryScore)),
      ...(await indexPagesAfter(context, [indexKey], 2, anyPlace, belowEveryScore)),
      await indexPageBefore(context, indexKey, 2, undefined, aboveEveryScore),
      await indexPageBefore(context, indexKey, 2, anyPlace, belowEveryScore),
    ];

    for (const indexPage of indexPages) {
      expect(indexPage).toEqual({ entryCount: 0, entryCountBeforeBoundary: 0, firstEntryRank: 0, places: [] });
    }
  });
}

for (const [description, listing, message] of [
  ["an unknown order", listingOf("sideways" as ListOrder), "order must be descending or ascending, got sideways"],
  ["a lowest score that is not a number", listingOf("descending", Number.NaN), "the lowest score must be a finite"],
  [
    "a highest score that is not finite",
    listingOf("descending", undefined, Number.POSITIVE_INFINITY),
    "the highest score must be a finite",
  ],
  ["a lowest score above the highest", listingOf("ascending", 2, 1), "the lowest score must not be above the highest"],
] as const) {
  engineTest(`the index page script refuses ${description}`, async (context) => {
    const indexKey = context.keys.runIndex("send-email", "executing");

    const indexPages = indexPagesAfter(context, [indexKey], 1, undefined, listing);

    await expect(indexPages).rejects.toThrow(`invalid argument: ${message}`);
  });
}
