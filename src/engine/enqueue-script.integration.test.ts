import { expect, vi } from "vitest";

import { engineTest, fetchedLockedRunsOf, storedValuesByKeyOf } from "../../tests/engine-test.js";
import type { EngineTestContext } from "../../tests/engine-test.js";
import { freshLockTokensOf } from "../../tests/lock-tokens.js";
import { serverInstantInMillisecondsOf } from "../../tests/server-instant.js";
import type { EpochMilliseconds } from "../durations.js";
import { storedRunIdOf } from "./engine-keys.js";
import { DEDUP_WINDOW_IN_MILLISECONDS, RECORD_VERSION } from "./engine.js";
import type { DispatchOrEmitRequest, EnqueueReply, ParentFence, RunToCreate } from "./engine.js";

async function enqueueTimedByTheServer(
  { engine, redis }: EngineTestContext,
  request: DispatchOrEmitRequest,
): Promise<{ reply: EnqueueReply; instantBefore: EpochMilliseconds; instantAfter: EpochMilliseconds }> {
  const instantBefore = await serverInstantInMillisecondsOf(redis);
  const reply = await engine.enqueue(request);
  return { reply, instantBefore, instantAfter: await serverInstantInMillisecondsOf(redis) };
}

const SEND_EMAIL_RUN: RunToCreate = {
  runId: "run-1",
  task: "send-email",
  payload: '{"to":"ada@example.com"}',
  retries: 3,
};

const USER_SIGNED_UP = { eventName: "user.signed-up", eventId: "event-1" };

const LISTENER_RUN_IDENTITIES = [
  { task: "send-welcome-email", runId: "welcome" },
  { task: "sync-crm", runId: "crm" },
  { task: "write-audit-log", runId: "audit" },
];

function listenerRunsOf(sourceEvent: { eventName: string; eventId: string }, runIdSuffix = ""): RunToCreate[] {
  return [
    {
      runId: `welcome${runIdSuffix}`,
      task: "send-welcome-email",
      payload: '{"userId":"u-1"}',
      retries: 5,
      sourceEvent,
    },
    {
      runId: `crm${runIdSuffix}`,
      task: "sync-crm",
      payload: '{"userId":"u-1"}',
      retries: 2,
      concurrencyKey: "account-7",
      sourceEvent,
    },
    { runId: `audit${runIdSuffix}`, task: "write-audit-log", payload: '{"userId":"u-1"}', retries: 0, sourceEvent },
  ];
}

engineTest(
  "a dispatch stores one pending run with its record, payload, indexes and wake-up marker",
  async (context) => {
    const { redis, keys } = context;

    const { reply, instantBefore, instantAfter } = await enqueueTimedByTheServer(context, { runs: [SEND_EMAIL_RUN] });

    expect(reply).toEqual({
      kind: "enqueued",
      createdRuns: [{ task: SEND_EMAIL_RUN.task, runId: SEND_EMAIL_RUN.runId }],
      skippedRuns: [],
    });
    const record = await redis.hgetall(keys.runRecord(SEND_EMAIL_RUN.task, SEND_EMAIL_RUN.runId));
    expect(record).toEqual({
      version: String(RECORD_VERSION),
      state: "pending",
      task: "send-email",
      attempt: "1",
      retries: "3",
      replays: "0",
      stalls: "0",
      order: expect.any(String),
      dispatchedAt: expect.any(String),
    });
    expect(Number(record["dispatchedAt"])).toBeGreaterThanOrEqual(instantBefore);
    expect(Number(record["dispatchedAt"])).toBeLessThanOrEqual(instantAfter);
    expect(await redis.get(keys.runPayload("send-email", "run-1"))).toBe('{"to":"ada@example.com"}');
    expect(await redis.zrange(keys.pending, 0, -1)).toEqual([storedRunIdOf("send-email", "run-1")]);
    expect(await redis.zrange(keys.runIndex("send-email", "pending"), 0, -1)).toEqual([
      storedRunIdOf("send-email", "run-1"),
    ]);
    expect(await redis.smembers(keys.tasks)).toEqual(["send-email"]);
    expect(await redis.zcard(keys.wakeUpMarker)).toBe(1);
  },
);

engineTest("runs dispatched one after another are pending in that order", async ({ engine, redis, keys }) => {
  await engine.enqueue({
    runs: [
      { ...SEND_EMAIL_RUN, runId: "run-1" },
      { ...SEND_EMAIL_RUN, runId: "run-2" },
    ],
  });

  await engine.enqueue({ runs: [{ ...SEND_EMAIL_RUN, runId: "run-3" }] });

  expect(await redis.zrange(keys.pending, 0, -1)).toEqual([
    storedRunIdOf("send-email", "run-1"),
    storedRunIdOf("send-email", "run-2"),
    storedRunIdOf("send-email", "run-3"),
  ]);
});

engineTest("a run created with a delay sits in delayed, due at the server's TIME plus the delay", async (context) => {
  const { redis, keys } = context;
  const delayInMilliseconds = 90_000;
  vi.useFakeTimers({ toFake: ["Date"], now: new Date("2000-01-01T00:00:00Z") });
  try {
    const { instantBefore, instantAfter } = await enqueueTimedByTheServer(context, {
      runs: [{ ...SEND_EMAIL_RUN, delayInMilliseconds }],
    });

    const startsAt = Number(await redis.hget(keys.runRecord("send-email", "run-1"), "startsAt"));
    expect(startsAt).toBeGreaterThanOrEqual(instantBefore + delayInMilliseconds);
    expect(startsAt).toBeLessThanOrEqual(instantAfter + delayInMilliseconds);
    expect(await redis.hget(keys.runRecord("send-email", "run-1"), "state")).toBe("delayed");
    expect(Number(await redis.zscore(keys.due, storedRunIdOf("send-email", "run-1")))).toBe(startsAt);
    expect(
      Number(await redis.zscore(keys.runIndex("send-email", "delayed"), storedRunIdOf("send-email", "run-1"))),
    ).toBe(startsAt);
    expect(await redis.zcard(keys.pending)).toBe(0);
    expect(await redis.zcard(keys.wakeUpMarker)).toBe(0);
  } finally {
    vi.useRealTimers();
  }
});

engineTest(
  "an emit stores every listener run with its source event, and the event's dedup entry for 24 hours",
  async (context) => {
    const { redis, keys } = context;

    const { reply, instantBefore, instantAfter } = await enqueueTimedByTheServer(context, {
      runs: listenerRunsOf(USER_SIGNED_UP),
      eventDedup: USER_SIGNED_UP,
    });

    expect(reply).toEqual({ kind: "enqueued", createdRuns: LISTENER_RUN_IDENTITIES, skippedRuns: [] });
    expect(
      await redis.hmget(keys.runRecord("sync-crm", "crm"), "eventName", "eventId", "concurrencyKey", "retries"),
    ).toEqual(["user.signed-up", "event-1", "account-7", "2"]);
    expect(await redis.hget(keys.runRecord("send-welcome-email", "welcome"), "concurrencyKey")).toBeNull();
    expect(await redis.zcard(keys.pending)).toBe(3);
    const eventDedupExpiresAt = Number(await redis.zscore(keys.eventDedup("user.signed-up"), "event-1"));
    expect(eventDedupExpiresAt).toBeGreaterThanOrEqual(instantBefore + DEDUP_WINDOW_IN_MILLISECONDS);
    expect(eventDedupExpiresAt).toBeLessThanOrEqual(instantAfter + DEDUP_WINDOW_IN_MILLISECONDS);
  },
);

engineTest(
  "a repeat of an event id within its window replies duplicate and writes nothing",
  async ({ engine, redis, keyPrefix }) => {
    await engine.enqueue({ runs: listenerRunsOf(USER_SIGNED_UP), eventDedup: USER_SIGNED_UP });
    const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

    const reply = await engine.enqueue({
      runs: listenerRunsOf(USER_SIGNED_UP, "-repeat"),
      eventDedup: USER_SIGNED_UP,
    });

    expect(reply).toEqual({ kind: "duplicate" });
    expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
  },
);

engineTest("an emit with one run the script rejects writes none of the runs", async ({ engine, redis, keyPrefix }) => {
  const [welcomeRun, crmRun, auditRun] = listenerRunsOf(USER_SIGNED_UP);

  const enqueue = engine.enqueue({
    runs: [welcomeRun!, crmRun!, { ...auditRun!, delayInMilliseconds: -1 }],
    eventDedup: USER_SIGNED_UP,
  });

  await expect(enqueue).rejects.toThrow("invalid argument: delayInMilliseconds must be a non-negative integer");
  expect(await redis.keys(`{${keyPrefix}}:*`)).toEqual([]);
});

engineTest("an enqueue of no run is rejected and writes nothing", async ({ engine, redis, keyPrefix }) => {
  const enqueue = engine.enqueue({ runs: [], eventDedup: USER_SIGNED_UP });

  await expect(enqueue).rejects.toThrow("invalid argument: an enqueue creates at least one run");
  expect(await redis.keys(`{${keyPrefix}}:*`)).toEqual([]);
});

engineTest(
  "a repeat of an event id after its window creates only the listener runs that are gone",
  async ({ engine, redis, keys }) => {
    const [welcomeRun] = listenerRunsOf(USER_SIGNED_UP);
    await engine.enqueue({ runs: [welcomeRun!] });
    await redis.zadd(keys.eventDedup("user.signed-up"), 1, "event-1");

    const reply = await engine.enqueue({ runs: listenerRunsOf(USER_SIGNED_UP), eventDedup: USER_SIGNED_UP });

    expect(reply).toEqual({
      kind: "enqueued",
      createdRuns: [
        { task: "sync-crm", runId: "crm" },
        { task: "write-audit-log", runId: "audit" },
      ],
      skippedRuns: [{ task: "send-welcome-email", runId: "welcome" }],
    });
    expect(Number(await redis.zscore(keys.eventDedup("user.signed-up"), "event-1"))).toBeGreaterThan(
      await serverInstantInMillisecondsOf(redis),
    );
  },
);

engineTest(
  "a repeat of a run id whose record exists is skipped and reported, its record untouched",
  async ({ engine, redis, keys }) => {
    await engine.enqueue({ runs: [SEND_EMAIL_RUN] });
    const recordBefore = await redis.hgetall(keys.runRecord("send-email", "run-1"));

    const reply = await engine.enqueue({ runs: [{ ...SEND_EMAIL_RUN, payload: '{"to":"bob@example.com"}' }] });

    expect(reply).toEqual({ kind: "enqueued", createdRuns: [], skippedRuns: [{ task: "send-email", runId: "run-1" }] });
    expect(await redis.hgetall(keys.runRecord("send-email", "run-1"))).toEqual(recordBefore);
    expect(await redis.get(keys.runPayload("send-email", "run-1"))).toBe('{"to":"ada@example.com"}');
  },
);

engineTest(
  "a repeat of a run id whose dedup entry is live is skipped and reported",
  async ({ engine, redis, keys }) => {
    await redis.zadd(
      keys.runDedup("send-email"),
      (await serverInstantInMillisecondsOf(redis)) + 60_000,
      storedRunIdOf("send-email", "run-1"),
    );

    const reply = await engine.enqueue({ runs: [SEND_EMAIL_RUN] });

    expect(reply).toEqual({ kind: "enqueued", createdRuns: [], skippedRuns: [{ task: "send-email", runId: "run-1" }] });
    expect(await redis.exists(keys.runRecord("send-email", "run-1"))).toBe(0);
  },
);

engineTest(
  "a run id whose dedup entry has expired starts a new run, and the expired entry is trimmed",
  async ({ engine, redis, keys }) => {
    await redis.zadd(
      keys.runDedup("send-email"),
      1,
      storedRunIdOf("send-email", "run-1"),
      2,
      storedRunIdOf("send-email", "older-run"),
    );

    const reply = await engine.enqueue({ runs: [SEND_EMAIL_RUN] });

    expect(reply).toEqual({ kind: "enqueued", createdRuns: [{ task: "send-email", runId: "run-1" }], skippedRuns: [] });
    expect(await redis.zcard(keys.runDedup("send-email"))).toBe(0);
  },
);

async function parentFenceOf(context: EngineTestContext, stepValue: string): Promise<ParentFence> {
  await context.engine.enqueue({ runs: [{ ...SEND_EMAIL_RUN, runId: "parent-run", task: "sign-up" }] });
  const [lockedRun] = await fetchedLockedRunsOf(context, 1);
  await context.redis.hset(context.keys.runRecord("sign-up", "parent-run"), "stalls", 2);
  return {
    parentTask: "sign-up",
    parentRunId: "parent-run",
    lockToken: lockedRun!.lockToken,
    stepId: "send-1",
    stepValue,
  };
}

engineTest(
  "a handler dispatch with the parent's current token creates the child and saves the parent's step, resetting stalls",
  async (context) => {
    const { engine, redis, keys } = context;
    const parentFence = await parentFenceOf(context, '"run-1"');

    const reply = await engine.enqueue({ runs: [SEND_EMAIL_RUN], parentFence });

    expect(reply).toEqual({ kind: "enqueued", createdRuns: [{ task: "send-email", runId: "run-1" }], skippedRuns: [] });
    expect(await redis.hget(keys.runRecord("send-email", "run-1"), "state")).toBe("pending");
    expect(await redis.hgetall(keys.runSteps("sign-up", "parent-run"))).toEqual({ "send-1": '"run-1"' });
    expect(await redis.hget(keys.runRecord("sign-up", "parent-run"), "stalls")).toBe("0");
  },
);

engineTest(
  "a handler emit with the parent's current token creates the listener runs and saves the event id as the step",
  async (context) => {
    const { engine, redis, keys } = context;
    const parentFence = await parentFenceOf(context, '"event-1"');

    const reply = await engine.enqueue({
      runs: listenerRunsOf(USER_SIGNED_UP),
      eventDedup: USER_SIGNED_UP,
      parentFence,
    });

    expect(reply).toEqual({ kind: "enqueued", createdRuns: LISTENER_RUN_IDENTITIES, skippedRuns: [] });
    expect(await redis.hgetall(keys.runSteps("sign-up", "parent-run"))).toEqual({ "send-1": '"event-1"' });
  },
);

engineTest("a handler send whose child already exists still saves the parent's step", async (context) => {
  const { engine, redis, keys } = context;
  const parentFence = await parentFenceOf(context, '"run-1"');
  await engine.enqueue({ runs: [SEND_EMAIL_RUN] });

  const reply = await engine.enqueue({ runs: [SEND_EMAIL_RUN], parentFence });

  expect(reply).toEqual({ kind: "enqueued", createdRuns: [], skippedRuns: [{ task: "send-email", runId: "run-1" }] });
  expect(await redis.hgetall(keys.runSteps("sign-up", "parent-run"))).toEqual({ "send-1": '"run-1"' });
});

engineTest("a re-entered handler send replies alreadySent with the saved value and writes nothing", async (context) => {
  const { engine, redis, keyPrefix } = context;
  const parentFence = await parentFenceOf(context, '"event-1"');
  await engine.enqueue({ runs: listenerRunsOf(USER_SIGNED_UP), eventDedup: USER_SIGNED_UP, parentFence });
  const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

  const reply = await engine.enqueue({
    runs: listenerRunsOf({ ...USER_SIGNED_UP, eventId: "event-2" }, "-diverging"),
    eventDedup: { ...USER_SIGNED_UP, eventId: "event-2" },
    parentFence: { ...parentFence, stepValue: '"event-2"' },
  });

  expect(reply).toEqual({ kind: "alreadySent", savedValue: '"event-1"' });
  expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
});

engineTest("a resent handler emit hears alreadySent rather than duplicate", async (context) => {
  const { engine } = context;
  const parentFence = await parentFenceOf(context, '"event-1"');
  const handlerEmit = { runs: listenerRunsOf(USER_SIGNED_UP), eventDedup: USER_SIGNED_UP, parentFence };
  await engine.enqueue(handlerEmit);

  const reply = await engine.enqueue(handlerEmit);

  expect(reply).toEqual({ kind: "alreadySent", savedValue: '"event-1"' });
});

for (const staleness of ["another token", "an ended lock", "a missing parent"] as const) {
  engineTest(`a handler send with ${staleness} creates nothing and replies stale`, async (context) => {
    const { engine, redis, keyPrefix } = context;
    const parentFence = await parentFenceOf(context, '"run-1"');
    if (staleness === "an ended lock") {
      await engine.settle({
        task: "sign-up",
        runId: "parent-run",
        lockToken: parentFence.lockToken,
        outcome: { kind: "yield" },
      });
    }
    const staleFence: ParentFence = {
      ...parentFence,
      ...(staleness === "another token" ? { lockToken: freshLockTokensOf(1)[0]! } : {}),
      ...(staleness === "a missing parent" ? { parentRunId: "missing-run" } : {}),
    };
    const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

    const reply = await engine.enqueue({ runs: [SEND_EMAIL_RUN], parentFence: staleFence });

    expect(reply).toEqual({ kind: "stale" });
    expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
  });
}

engineTest(
  "a handler send whose parent record is of a newer version replies unknownVersion and writes nothing",
  async (context) => {
    const { engine, redis, keys, keyPrefix } = context;
    const parentFence = await parentFenceOf(context, '"run-1"');
    await redis.hset(keys.runRecord("sign-up", "parent-run"), "version", RECORD_VERSION + 1);
    const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

    const reply = await engine.enqueue({ runs: [SEND_EMAIL_RUN], parentFence });

    expect(reply).toEqual({ kind: "unknownVersion", recordVersion: RECORD_VERSION + 1 });
    expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
  },
);
