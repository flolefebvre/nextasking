import { expect } from "vitest";

import { engineTest, storedValuesByKeyOf } from "../../tests/engine-test.js";
import type { EngineTestContext } from "../../tests/engine-test.js";
import { freshLockTokensOf } from "../../tests/lock-tokens.js";
import { serverInstantInMillisecondsOf } from "../../tests/server-instant.js";
import type { EngineKeys } from "./engine-keys.js";
import type { FetchedReply, FetchReply, FetchRequest, LockedRun, RunToCreate } from "./engine.js";
import { MOST_LOCK_TOKENS_PER_FETCH, MOST_RUNS_PARKED_PER_FETCH } from "./fetch-script.js";

const LOCK_DURATION_IN_MILLISECONDS = 120_000;

function sendEmailRunsOf(
  count: number,
  { runIdPrefix = "run", concurrencyKey }: { runIdPrefix?: string; concurrencyKey?: string } = {},
): RunToCreate[] {
  return Array.from({ length: count }, (_, position) => ({
    runId: `${runIdPrefix}-${String(position + 1)}`,
    task: "send-email",
    payload: `{"position":${String(position + 1)}}`,
    retries: 3,
    ...(concurrencyKey === undefined ? {} : { concurrencyKey }),
  }));
}

function fetchRequestOf(lockTokenCount: number, concurrencyLimitsByTask: Record<string, number> = {}): FetchRequest {
  return {
    lockTokens: freshLockTokensOf(lockTokenCount),
    concurrencyLimitsByTask,
    lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS,
  };
}

function fetchedReplyOf(reply: FetchReply): FetchedReply {
  if (reply.kind !== "fetched") {
    throw new Error(`expected a fetched reply, got ${JSON.stringify(reply)}`);
  }
  return reply;
}

async function markRecordsNewer(context: EngineTestContext, runIds: readonly string[]): Promise<void> {
  for (const runId of runIds) {
    await context.redis.hset(context.keys.runRecord(runId), "version", 2);
  }
}

async function lockedRunIdsOf(fetch: Promise<FetchReply>): Promise<string[]> {
  return fetchedReplyOf(await fetch).lockedRuns.map(({ runId }) => runId);
}

function sortedSetMembersOf(context: EngineTestContext, sortedSetKey: (keys: EngineKeys) => string) {
  return context.redis.zrange(sortedSetKey(context.keys), 0, -1);
}

engineTest(
  "a fetch of more tokens than pending runs locks every run in dispatch order, each executing with its slot held",
  async (context) => {
    const { engine, redis, keys } = context;
    await engine.enqueue({ runs: sendEmailRunsOf(2) });
    const request = fetchRequestOf(3);
    const instantBefore = await serverInstantInMillisecondsOf(redis);

    const reply = fetchedReplyOf(await engine.fetch(request));

    const instantAfter = await serverInstantInMillisecondsOf(redis);
    expect(reply.lockedRuns.map(({ runId, lockToken }) => [runId, lockToken])).toEqual([
      ["run-1", request.lockTokens[0]],
      ["run-2", request.lockTokens[1]],
    ]);
    expect(reply.parkedRuns).toEqual([]);
    expect(reply.passedOverRecordVersions).toEqual([]);
    for (const { runId, lockToken, startedAt, lockedUntil } of reply.lockedRuns) {
      expect(startedAt).toBeGreaterThanOrEqual(instantBefore);
      expect(startedAt).toBeLessThanOrEqual(instantAfter);
      expect(lockedUntil).toBe(startedAt + LOCK_DURATION_IN_MILLISECONDS);
      expect(await redis.hmget(keys.runRecord(runId), "state", "lockToken", "lockedUntil", "startedAt")).toEqual([
        "executing",
        lockToken,
        String(lockedUntil),
        String(startedAt),
      ]);
      expect(await redis.get(keys.lock(lockToken))).toBe(runId);
      expect(await redis.pttl(keys.lock(lockToken))).toBeGreaterThan(0);
      expect(await redis.pttl(keys.lock(lockToken))).toBeLessThanOrEqual(LOCK_DURATION_IN_MILLISECONDS);
    }
    expect((await redis.smembers(keys.slotSet("send-email", undefined))).sort()).toEqual(["run-1", "run-2"]);
    expect(await sortedSetMembersOf(context, ({ pending }) => pending)).toEqual([]);
  },
);

engineTest("a fetch of fewer tokens than pending runs locks one run per token", async (context) => {
  const { engine } = context;
  await engine.enqueue({ runs: sendEmailRunsOf(3) });

  const lockedRunIds = await lockedRunIdsOf(engine.fetch(fetchRequestOf(2)));

  expect(lockedRunIds).toEqual(["run-1", "run-2"]);
  expect(await sortedSetMembersOf(context, ({ pending }) => pending)).toEqual(["run-3"]);
});

engineTest("a locked run carries what its attempt needs to execute", async ({ engine }) => {
  await engine.enqueue({
    runs: [
      {
        runId: "crm",
        task: "sync-crm",
        payload: '{"userId":"u-1"}',
        retries: 2,
        concurrencyKey: "account-7",
        sourceEvent: { eventName: "user.signed-up", eventId: "event-1" },
      },
    ],
  });
  const request = fetchRequestOf(1);

  const [lockedRun] = fetchedReplyOf(await engine.fetch(request)).lockedRuns;

  expect(lockedRun).toEqual({
    lockToken: request.lockTokens[0],
    runId: "crm",
    task: "sync-crm",
    payload: '{"userId":"u-1"}',
    attempt: 1,
    retries: 2,
    replays: 0,
    isResume: false,
    dispatchedAt: expect.any(Number),
    startedAt: expect.any(Number),
    lockedUntil: expect.any(Number),
    sourceEvent: { eventName: "user.signed-up", eventId: "event-1" },
    concurrencyKey: "account-7",
  });
});

engineTest(
  "a run of a key at its limit is parked on its waiting list in the same fetch, using no token",
  async (context) => {
    const { engine, redis, keys } = context;
    await engine.enqueue({
      runs: [
        ...sendEmailRunsOf(3, { runIdPrefix: "account-7", concurrencyKey: "account-7" }),
        ...sendEmailRunsOf(1, { runIdPrefix: "account-8", concurrencyKey: "account-8" }),
      ],
    });

    const reply = fetchedReplyOf(await engine.fetch(fetchRequestOf(2, { "send-email": 1 })));

    expect(reply.lockedRuns.map(({ runId }) => runId)).toEqual(["account-7-1", "account-8-1"]);
    expect(reply.parkedRuns).toEqual([
      { runId: "account-7-2", task: "send-email", attempt: 1 },
      { runId: "account-7-3", task: "send-email", attempt: 1 },
    ]);
    expect(await redis.hget(keys.runRecord("account-7-2"), "state")).toBe("waitingForConcurrencySlot");
    expect(await sortedSetMembersOf(context, ({ waitingList }) => waitingList("send-email", "account-7"))).toEqual([
      "account-7-2",
      "account-7-3",
    ]);
    expect(await redis.smembers(keys.slotSet("send-email", "account-7"))).toEqual(["account-7-1"]);
  },
);

engineTest(
  "a parked run carries the source event of its emit and the planned instant of its firing",
  async ({ engine, redis }) => {
    const limitsByTask = { "build-report": 1, "sync-crm": 1 };
    await engine.enqueue({
      runs: [
        { runId: "report-running", task: "build-report", payload: "", retries: 0 },
        { runId: "crm-running", task: "sync-crm", payload: "{}", retries: 0 },
      ],
    });
    await engine.fetch(fetchRequestOf(2, limitsByTask));
    const plannedInstant = (await serverInstantInMillisecondsOf(redis)) - 60_000;
    await engine.syncSchedules({
      schedulesToSync: [{ task: "build-report", cron: "0 * * * *", timezone: "UTC", nextFiringAt: plannedInstant }],
    });
    await engine.fire({
      task: "build-report",
      instant: plannedInstant,
      instantAfter: plannedInstant + 3_600_000,
      runId: "report-fired",
      retries: 0,
    });
    await engine.enqueue({
      runs: [
        {
          runId: "crm-listener",
          task: "sync-crm",
          payload: "{}",
          retries: 0,
          sourceEvent: { eventName: "user.signed-up", eventId: "event-1" },
        },
      ],
    });

    const reply = fetchedReplyOf(await engine.fetch(fetchRequestOf(2, limitsByTask)));

    expect(reply.parkedRuns).toEqual([
      { runId: "report-fired", task: "build-report", attempt: 1, scheduledAt: plannedInstant },
      {
        runId: "crm-listener",
        task: "sync-crm",
        attempt: 1,
        sourceEvent: { eventName: "user.signed-up", eventId: "event-1" },
      },
    ]);
  },
);

engineTest("a run of a task without a limit is never parked", async ({ engine, redis, keys }) => {
  await engine.enqueue({ runs: sendEmailRunsOf(3) });

  const reply = fetchedReplyOf(await engine.fetch(fetchRequestOf(3, { "other-task": 1 })));

  expect(reply.lockedRuns).toHaveLength(3);
  expect(reply.parkedRuns).toEqual([]);
  expect(await redis.scard(keys.slotSet("send-email", undefined))).toBe(3);
});

engineTest(
  `a fetch parks at most ${String(MOST_RUNS_PARKED_PER_FETCH)} runs and leaves the rest pending with the wake-up marker armed`,
  async (context) => {
    const { engine, redis, keys } = context;
    await engine.enqueue({ runs: sendEmailRunsOf(MOST_RUNS_PARKED_PER_FETCH + 3) });
    await redis.del(keys.wakeUpMarker);

    const reply = fetchedReplyOf(await engine.fetch(fetchRequestOf(2, { "send-email": 1 })));

    expect(reply.lockedRuns.map(({ runId }) => runId)).toEqual(["run-1"]);
    expect(reply.parkedRuns).toHaveLength(MOST_RUNS_PARKED_PER_FETCH);
    expect(await redis.zcard(keys.pending)).toBe(2);
    expect(await redis.zcard(keys.wakeUpMarker)).toBe(1);
  },
);

engineTest("a resent fetch with the same tokens returns the same locks and locks nothing new", async (context) => {
  const { engine, redis, keyPrefix } = context;
  await engine.enqueue({ runs: sendEmailRunsOf(3) });
  const request = fetchRequestOf(2);
  const firstReply = fetchedReplyOf(await engine.fetch(request));
  const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

  const resentReply = fetchedReplyOf(await engine.fetch(request));

  expect(resentReply.lockedRuns).toEqual(firstReply.lockedRuns);
  expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
});

engineTest("a resent batch locks new runs only with the tokens the first fetch did not use", async (context) => {
  const { engine } = context;
  await engine.enqueue({ runs: sendEmailRunsOf(1) });
  const request = fetchRequestOf(2);
  await engine.fetch(request);
  await engine.enqueue({ runs: sendEmailRunsOf(1, { runIdPrefix: "later" }) });

  const resentReply = fetchedReplyOf(await engine.fetch(request));

  expect(resentReply.lockedRuns.map(({ runId, lockToken }) => [runId, lockToken])).toEqual([
    ["run-1", request.lockTokens[0]],
    ["later-1", request.lockTokens[1]],
  ]);
});

async function resumedRunFetchedTwiceOf(
  { engine, redis, keys }: EngineTestContext,
  resumeFields: Record<string, number>,
): Promise<{ lockedRun: LockedRun | undefined; resentLockedRun: LockedRun | undefined }> {
  await engine.enqueue({ runs: sendEmailRunsOf(1) });
  await redis.hset(keys.runRecord("run-1"), resumeFields);
  const request = fetchRequestOf(1);
  const [lockedRun] = fetchedReplyOf(await engine.fetch(request)).lockedRuns;
  const [resentLockedRun] = fetchedReplyOf(await engine.fetch(request)).lockedRuns;
  return { lockedRun, resentLockedRun };
}

engineTest(
  "a run with a resume instant is fetched as a resume and loses the instant, and a resent fetch reports no resume",
  async (context) => {
    const { redis, keys } = context;

    const { lockedRun, resentLockedRun } = await resumedRunFetchedTwiceOf(context, { resumesAt: 1000 });

    expect(lockedRun?.isResume).toBe(true);
    expect(await redis.hexists(keys.runRecord("run-1"), "resumesAt")).toBe(0);
    expect(resentLockedRun).toEqual({ ...lockedRun, isResume: false });
  },
);

engineTest(
  "a run whose sleeps were woken carries sleepsWokenAt on its fresh lock only, and loses it from the record",
  async (context) => {
    const { redis, keys } = context;

    const { lockedRun, resentLockedRun } = await resumedRunFetchedTwiceOf(context, {
      resumesAt: 1000,
      sleepsWokenAt: 1000,
    });

    expect(lockedRun?.sleepsWokenAt).toBe(1000);
    expect(await redis.hexists(keys.runRecord("run-1"), "sleepsWokenAt")).toBe(0);
    expect(resentLockedRun).not.toHaveProperty("sleepsWokenAt");
    expect(resentLockedRun).toEqual({ ...lockedRun, isResume: false, sleepsWokenAt: undefined });
  },
);

engineTest(
  "a fetch meeting 10 newer-version records at the head replies unknownVersion, takes no lock and leaves the indexes untouched",
  async (context) => {
    const { engine, redis, keyPrefix } = context;
    const runs = sendEmailRunsOf(11);
    await engine.enqueue({ runs });
    await markRecordsNewer(
      context,
      runs.slice(0, 10).map(({ runId }) => runId),
    );
    const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

    const reply = await engine.fetch(fetchRequestOf(1));

    expect(reply).toEqual({ kind: "unknownVersion", recordVersion: 2 });
    expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
  },
);

engineTest(
  "a fetch passes over newer-version records, leaves them pending and locks the runs behind them",
  async (context) => {
    const { engine } = context;
    await engine.enqueue({ runs: sendEmailRunsOf(5) });
    await markRecordsNewer(context, ["run-1", "run-2", "run-4"]);

    const reply = fetchedReplyOf(await engine.fetch(fetchRequestOf(2)));

    expect(reply.lockedRuns.map(({ runId }) => runId)).toEqual(["run-3", "run-5"]);
    expect(reply.passedOverRecordVersions).toEqual([2, 2, 2]);
    expect(await sortedSetMembersOf(context, ({ pending }) => pending)).toEqual(["run-1", "run-2", "run-4"]);
  },
);

engineTest(
  "a fetch that parks a run before meeting 10 newer-version records replies with the parked run",
  async (context) => {
    const { engine } = context;
    const limits = { "send-email": 1 };
    await engine.enqueue({ runs: sendEmailRunsOf(1, { runIdPrefix: "holder" }) });
    await engine.fetch(fetchRequestOf(1, limits));
    const runs = sendEmailRunsOf(11);
    await engine.enqueue({ runs });
    await markRecordsNewer(
      context,
      runs.slice(1).map(({ runId }) => runId),
    );

    const reply = fetchedReplyOf(await engine.fetch(fetchRequestOf(1, limits)));

    expect(reply.lockedRuns).toEqual([]);
    expect(reply.parkedRuns.map(({ runId }) => runId)).toEqual(["run-1"]);
    expect(reply.passedOverRecordVersions).toHaveLength(10);
  },
);

engineTest(
  "a fetch re-arms the wake-up marker only while runs it did not pass over remain pending",
  async (context) => {
    const { engine, redis, keys } = context;
    await engine.enqueue({ runs: sendEmailRunsOf(3) });
    await markRecordsNewer(context, ["run-1"]);
    await redis.del(keys.wakeUpMarker);

    await engine.fetch(fetchRequestOf(1));
    const isArmedWithRunsLeft = (await redis.zcard(keys.wakeUpMarker)) === 1;
    await redis.del(keys.wakeUpMarker);
    await engine.fetch(fetchRequestOf(1));
    const isArmedWithOnlyNewerRecordsLeft = (await redis.zcard(keys.wakeUpMarker)) === 1;

    expect(isArmedWithRunsLeft).toBe(true);
    expect(isArmedWithOnlyNewerRecordsLeft).toBe(false);
  },
);

const INVALID_FETCHES = [
  { name: "no token", request: fetchRequestOf(0), message: "a fetch carries at least one lock token" },
  {
    name: `more than ${String(MOST_LOCK_TOKENS_PER_FETCH)} tokens`,
    request: fetchRequestOf(MOST_LOCK_TOKENS_PER_FETCH + 1),
    message: `a fetch carries at most ${String(MOST_LOCK_TOKENS_PER_FETCH)} lock tokens`,
  },
  {
    name: "a token twice",
    request: { ...fetchRequestOf(1), lockTokens: ["same-token", "same-token"] },
    message: "lock tokens must be distinct",
  },
  {
    name: "an empty token",
    request: { ...fetchRequestOf(1), lockTokens: [""] },
    message: "lockToken must not be empty",
  },
  {
    name: "a limit of zero",
    request: fetchRequestOf(1, { "send-email": 0 }),
    message: "concurrencyLimit must be a positive integer",
  },
  {
    name: "a lock duration of zero",
    request: { ...fetchRequestOf(1), lockDurationInMilliseconds: 0 },
    message: "lockDurationInMilliseconds must be a positive integer",
  },
];

for (const { name, request, message } of INVALID_FETCHES) {
  engineTest(`a fetch with ${name} is rejected and writes nothing`, async ({ engine, redis, keyPrefix }) => {
    await engine.enqueue({ runs: sendEmailRunsOf(1) });
    const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

    const fetch = engine.fetch(request);

    await expect(fetch).rejects.toThrow(`invalid argument: ${message}`);
    expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
  });
}
