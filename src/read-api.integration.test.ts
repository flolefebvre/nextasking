import { expect } from "vitest";

import { DAY_IN_MILLISECONDS, endRuns, runIdsOf, SEND_EMAIL_PRUNE_REQUEST } from "../tests/ended-runs.js";
import { backdateEndedRun, engineTest, engineTestContextOf, fetchedLockedRunsOf } from "../tests/engine-test.js";
import type { EngineTestContext } from "../tests/engine-test.js";
import type { EpochMilliseconds } from "./durations.js";
import { integrationTest } from "../tests/integration-test.js";
import { freshLockTokensOf } from "../tests/lock-tokens.js";
import { readTrafficOf } from "../tests/read-traffic.js";
import type { ReadTraffic } from "../tests/read-traffic.js";
import { storedKeyViolationsOf } from "../tests/stored-key-violations.js";
import { serverInstantInMillisecondsOf } from "../tests/server-instant.js";
import { withRedis } from "../tests/with-redis.js";
import { defineScheduledTask } from "./definitions.js";
import type { AnyTaskDefinition } from "./definitions.js";
import { runIdentityOf, storedRunIdOf } from "./engine/engine-keys.js";
import type { RunIdentity } from "./engine/engine-keys.js";
import { RECORD_VERSION } from "./engine/engine.js";
import type { LockedRun, RunToCreate, SettleOutcome } from "./engine/engine.js";
import { createReadApi } from "./read-api.js";
import type { ReadApi } from "./read-api.js";
import type { RedisClient } from "./redis-client.js";
import { UnknownEventError, UnknownTaskError } from "./errors.js";
import { deriveFiringRunId, deriveListenerRunId } from "./ids.js";
import type { RunId } from "./ids.js";
import { cursorOfFields } from "./run-list-cursor.js";
import type { AnyListedRun, ListRunsOptionsOf, RunPageOf } from "./read-api.js";
import { RUN_STATES } from "./run-states.js";
import type { RunState } from "./run-states.js";
import type { ScheduleSummary } from "./engine/engine.js";
import { DEFAULT_WORKER_TIMING } from "./worker-timing.js";

const TASK_NAMES = ["send-email", "build-report", "resize-image"] as const;

const LISTENER_NAMES = ["record-payment", "email-receipt"] as const;

const DECLARED_SCHEDULES: readonly ScheduleSummary[] = [
  { task: "build-report", cron: "0 * * * *", timezone: "UTC" },
  { task: "send-email", cron: "*/5 * * * *", timezone: "Europe/Paris" },
];

const READ_COMMAND_NAMES = new Set(["hmget", "get", "lrange", "lindex", "hgetall", "zcard", "eval", "evalsha"]);

const ENDED_ORDER_GROUP_NUMBER = 4;

const ATTEMPT_ERROR = { attempt: 1, name: "Error", message: "boom", stack: "Error: boom" };

function taskDefinitionOf(taskName: string): AnyTaskDefinition {
  return defineScheduledTask(taskName, { schedule: { cron: "0 * * * *" }, handler: async () => undefined });
}

function readerOf({ client, keyPrefix }: EngineTestContext, supportedRecordVersion = RECORD_VERSION): ReadApi {
  const taskNames = [...TASK_NAMES, ...LISTENER_NAMES];
  return createReadApi({
    client,
    prefix: keyPrefix,
    tasksByName: new Map(taskNames.map((taskName) => [taskName, taskDefinitionOf(taskName)])),
    listenerNamesByEventName: new Map([["invoice.paid", LISTENER_NAMES]]),
    declaredSchedules: DECLARED_SCHEDULES,
    supportedRecordVersion,
  });
}

function expectOnlyBoundedReads({ sentCommands }: ReadTraffic): void {
  expect(sentCommands.map(({ name }) => name).filter((name) => !READ_COMMAND_NAMES.has(name))).toEqual([]);
}

function runOf(runId: RunId, task: string, extraFields: Partial<RunToCreate> = {}): RunToCreate {
  return { runId, task, payload: `{"runId":"${runId}"}`, retries: 1, ...extraFields };
}

async function enqueueRuns(context: EngineTestContext, runs: readonly RunToCreate[]): Promise<void> {
  await context.engine.enqueue({ runs });
}

async function settleEach(
  context: EngineTestContext,
  lockedRuns: readonly LockedRun[],
  outcome: SettleOutcome,
): Promise<void> {
  for (const lockedRun of lockedRuns) {
    await context.engine.settle({ ...lockedRun, outcome });
  }
}

async function recordFieldOf(context: EngineTestContext, task: string, runId: RunId, field: string): Promise<Date> {
  return new Date(Number(await context.redis.hGet(context.keys.runRecord(task, runId), field)));
}

engineTest(
  "getRun reads a running run with its payload, attempt, errors and saved values, sleeps left out",
  async (context) => {
    const { engine } = context;
    await enqueueRuns(context, [runOf("run-1", "send-email")]);
    const [firstAttempt] = await fetchedLockedRunsOf(context, 1);
    await engine.saveStep({ ...firstAttempt!, stepId: "charge", step: { kind: "value", value: '["paid"]' } });
    await engine.saveStep({ ...firstAttempt!, stepId: "pause", step: { kind: "sleep", durationInMilliseconds: 0 } });
    await engine.settle({
      ...firstAttempt!,
      outcome: { kind: "retry", error: ATTEMPT_ERROR, backoffInMilliseconds: 0 },
    });
    await engine.promote();
    const [secondAttempt] = await fetchedLockedRunsOf(context, 1);

    const run = await readerOf(context).getRun("send-email", "run-1");

    expect(run).toEqual({
      task: "send-email",
      runId: "run-1",
      state: "executing",
      attempt: 2,
      dispatchedAt: new Date(secondAttempt!.dispatchedAt),
      payload: { runId: "run-1" },
      errors: [{ ...ATTEMPT_ERROR, failedAt: expect.any(Date) }],
      steps: [{ id: "charge", result: "paid" }],
    });
    expect(run?.errors[0]?.failedAt.getTime()).toBeLessThanOrEqual(secondAttempt!.startedAt);
  },
);

engineTest(
  "getRun reads every saved value, including those whose step ids are integer-like or __proto__",
  async (context) => {
    const { engine } = context;
    await enqueueRuns(context, [runOf("run-1", "send-email")]);
    const [attempt] = await fetchedLockedRunsOf(context, 1);
    for (const [stepId, value] of [
      ["charge", '["paid"]'],
      ["__proto__", '["polluted"]'],
      ["10", '["ten"]'],
      ["2", '["two"]'],
    ]) {
      await engine.saveStep({ ...attempt!, stepId: stepId!, step: { kind: "value", value: value! } });
    }

    const run = await readerOf(context).getRun("send-email", "run-1");

    expect(run?.steps).toEqual(
      expect.arrayContaining([
        { id: "2", result: "two" },
        { id: "10", result: "ten" },
        { id: "charge", result: "paid" },
        { id: "__proto__", result: "polluted" },
      ]),
    );
    expect(run?.steps).toHaveLength(4);
  },
);

engineTest("getRun reads each state with its own field and attempt", async (context) => {
  const { engine } = context;
  await enqueueRuns(context, [
    runOf("backing-off-run", "send-email"),
    runOf("suspended-run", "send-email"),
    runOf("failed-run", "send-email"),
    runOf("completed-run", "send-email"),
    runOf("slot-holding-run", "resize-image"),
  ]);
  const [backingOff, suspended, failed, completed] = await fetchedLockedRunsOf(context, 5, { "resize-image": 1 });
  const resumesAt = (await serverInstantInMillisecondsOf(context.redis)) + 60_000;
  await engine.settle({
    ...backingOff!,
    outcome: { kind: "retry", error: ATTEMPT_ERROR, backoffInMilliseconds: 60_000 },
  });
  await engine.settle({ ...suspended!, outcome: { kind: "suspend", resumesAt } });
  await engine.settle({
    ...failed!,
    outcome: { kind: "fail", reason: "non-retriable", error: ATTEMPT_ERROR, failureNotice: undefined },
  });
  await engine.settle({ ...completed!, outcome: { kind: "complete" } });
  await enqueueRuns(context, [runOf("parked-run", "resize-image")]);
  await fetchedLockedRunsOf(context, 1, { "resize-image": 1 });
  await enqueueRuns(context, [
    runOf("pending-run", "send-email"),
    runOf("delayed-run", "send-email", { delayInMilliseconds: 60_000 }),
  ]);
  const reader = readerOf(context);

  const runsByRunId = Object.fromEntries(
    await Promise.all(
      [
        ["backing-off-run", "send-email"],
        ["suspended-run", "send-email"],
        ["failed-run", "send-email"],
        ["completed-run", "send-email"],
        ["parked-run", "resize-image"],
        ["pending-run", "send-email"],
        ["delayed-run", "send-email"],
      ].map(async ([runId, task]) => [runId, await reader.getRun(task!, runId!)]),
    ),
  );

  expect(runsByRunId["backing-off-run"]).toMatchObject({
    state: "backingOff",
    attempt: 2,
    nextAttemptAt: await recordFieldOf(context, "send-email", "backing-off-run", "nextAttemptAt"),
  });
  expect(runsByRunId["suspended-run"]).toMatchObject({
    state: "suspended",
    attempt: 1,
    resumesAt: new Date(resumesAt),
  });
  expect(runsByRunId["failed-run"]).toMatchObject({
    state: "failed",
    attempt: 1,
    reason: "non-retriable",
    finishedAt: await recordFieldOf(context, "send-email", "failed-run", "finishedAt"),
  });
  expect(runsByRunId["completed-run"]).toMatchObject({
    state: "completed",
    attempt: 1,
    finishedAt: await recordFieldOf(context, "send-email", "completed-run", "finishedAt"),
    steps: [],
  });
  expect(runsByRunId["parked-run"]).toMatchObject({ state: "waitingForConcurrencySlot", attempt: 1 });
  expect(runsByRunId["pending-run"]).toMatchObject({ state: "pending", attempt: 1 });
  expect(runsByRunId["delayed-run"]).toMatchObject({
    state: "delayed",
    attempt: 1,
    startsAt: await recordFieldOf(context, "send-email", "delayed-run", "startsAt"),
  });
  expect(runsByRunId["pending-run"]).not.toHaveProperty("finishedAt");
});

engineTest("getRun reads a run stored without a payload with an undefined payload", async (context) => {
  await enqueueRuns(context, [runOf("run-1", "build-report", { payload: "" })]);

  const run = await readerOf(context).getRun("build-report", "run-1");

  expect(run).toMatchObject({ runId: "run-1", payload: undefined });
});

engineTest(
  "getRun reads as undefined a run never dispatched, an empty run id, a run of another task and a run of a newer record version",
  async (context) => {
    const { redis, keys } = context;
    await enqueueRuns(context, [runOf("other-task-run", "build-report"), runOf("newer-run", "send-email")]);
    await redis.hSet(keys.runRecord("send-email", "newer-run"), "version", RECORD_VERSION + 1);
    const reader = readerOf(context);

    const runs = [
      await reader.getRun("send-email", "never-dispatched-run"),
      await reader.getRun("send-email", ""),
      await reader.getRun("send-email", "other-task-run"),
      await reader.getRun("send-email", "newer-run"),
    ];

    expect(runs).toEqual([undefined, undefined, undefined, undefined]);
  },
);

engineTest("getRun reads a run of an older record version as a run", async (context) => {
  const { redis, keys } = context;
  await enqueueRuns(context, [runOf("older-run", "send-email")]);
  await redis.hSet(keys.runRecord("send-email", "older-run"), "version", RECORD_VERSION - 1);

  const run = await readerOf(context).getRun("send-email", "older-run");

  expect(run).toMatchObject({ runId: "older-run", task: "send-email", state: "pending" });
});

engineTest("getRun throws UnknownTaskError for a task the release does not define", async (context) => {
  const getRun = readerOf(context).getRun("removed-task", "run-1");

  await expect(getRun).rejects.toThrow(UnknownTaskError);
});

engineTest(
  "countRuns equals the index sizes, counting a run of a newer record version that getRun and listRuns leave out",
  async (context) => {
    const { redis, keys } = context;
    await enqueueRuns(context, [
      runOf("pending-run", "send-email"),
      runOf("newer-run", "send-email"),
      runOf("delayed-run", "build-report", { delayInMilliseconds: 60_000 }),
      runOf("removed-task-run", "removed-task"),
    ]);
    await redis.hSet(keys.runRecord("send-email", "newer-run"), "version", RECORD_VERSION + 1);
    const reader = readerOf(context);

    const runCounts = await reader.countRuns();

    expect(runCounts["send-email"]).toMatchObject({ pending: 2, delayed: 0, completed: 0 });
    expect(runCounts["build-report"]).toMatchObject({ pending: 0, delayed: 1 });
    expect(runCounts).not.toHaveProperty("removed-task");
    for (const taskName of [...TASK_NAMES, ...LISTENER_NAMES]) {
      for (const state of RUN_STATES) {
        expect(runCounts[taskName]?.[state], `${taskName} ${state}`).toBe(
          await redis.zCard(keys.runIndex(taskName, state)),
        );
      }
    }
    expect(await reader.getRun("send-email", "newer-run")).toBeUndefined();
    expect((await reader.listRuns({ state: "pending" })).runs.map(({ runId }) => runId)).toEqual(["pending-run"]);
  },
);

engineTest(
  "getEvent reads every listener as undefined for an event never emitted, and its listener runs once emitted",
  async (context) => {
    const reader = readerOf(context);
    const runsBeforeTheEmit = (await reader.getEvent("invoice.paid", "event-1")).runsByListener;
    await context.engine.enqueue({
      runs: LISTENER_NAMES.map((listenerName) => runOf(deriveListenerRunId("event-1", listenerName), listenerName)),
      eventDedup: { eventName: "invoice.paid", eventId: "event-1" },
    });

    const event = await reader.getEvent("invoice.paid", "event-1");

    expect(runsBeforeTheEmit).toEqual({ "record-payment": undefined, "email-receipt": undefined });
    expect(event).toEqual({
      event: "invoice.paid",
      eventId: "event-1",
      runsByListener: Object.fromEntries(
        LISTENER_NAMES.map((listenerName) => [
          listenerName,
          expect.objectContaining({
            task: listenerName,
            runId: deriveListenerRunId("event-1", listenerName),
            state: "pending",
          }),
        ]),
      ),
    });
    expect(event).not.toHaveProperty("fanOut");
  },
);

engineTest("getEvent reads every listener as undefined once retention removed its runs", async (context) => {
  const { engine } = context;
  await engine.enqueue({
    runs: LISTENER_NAMES.map((listenerName) => runOf(deriveListenerRunId("event-1", listenerName), listenerName)),
    eventDedup: { eventName: "invoice.paid", eventId: "event-1" },
  });
  await settleEach(context, await fetchedLockedRunsOf(context, LISTENER_NAMES.length), { kind: "complete" });
  for (const listenerName of LISTENER_NAMES) {
    await backdateEndedRun(
      context,
      listenerName,
      deriveListenerRunId("event-1", listenerName),
      2 * DAY_IN_MILLISECONDS,
    );
    await engine.prune({
      task: listenerName,
      completedRetention: DEFAULT_WORKER_TIMING.completedRunRetention,
      failedRetention: DEFAULT_WORKER_TIMING.failedRunRetention,
    });
  }

  const event = await readerOf(context).getEvent("invoice.paid", "event-1");

  expect(event.runsByListener).toEqual({ "record-payment": undefined, "email-receipt": undefined });
});

engineTest("getEvent throws UnknownEventError for an event the release does not define", async (context) => {
  const getEvent = readerOf(context).getEvent("invoice.refunded", "event-1");

  await expect(getEvent).rejects.toThrow(UnknownEventError);
});

engineTest(
  "listSchedules lists each declared schedule in order, without firings before a worker synced it",
  async (context) => {
    const schedules = await readerOf(context).listSchedules();

    expect(schedules).toEqual([
      { task: "build-report", cron: "0 * * * *", timezone: "UTC" },
      { task: "send-email", cron: "*/5 * * * *", timezone: "Europe/Paris" },
    ]);
  },
);

engineTest("listSchedules shows the previous firing kept across a cron change", async (context) => {
  const { engine, redis } = context;
  const firedAt = (await serverInstantInMillisecondsOf(redis)) - 1000;
  const nextFiringAt = firedAt + 3_600_000;
  await engine.syncSchedules({
    schedulesToSync: [{ task: "build-report", cron: "0 * * * *", timezone: "UTC", nextFiringAt: firedAt, retries: 0 }],
  });
  await engine.fire({
    task: "build-report",
    instant: firedAt,
    instantAfter: nextFiringAt,
    runId: deriveFiringRunId("build-report", firedAt),
    retries: 0,
  });
  await engine.syncSchedules({
    schedulesToSync: [
      { task: "build-report", cron: "30 * * * *", timezone: "UTC", nextFiringAt: nextFiringAt + 1, retries: 0 },
    ],
  });

  const [buildReport] = await readerOf(context).listSchedules();

  expect(buildReport).toEqual({
    task: "build-report",
    cron: "0 * * * *",
    timezone: "UTC",
    nextFiringAt: new Date(nextFiringAt + 1),
    previousFiring: { scheduledAt: new Date(firedAt), runId: deriveFiringRunId("build-report", firedAt) },
  });
});

async function nextFiringAtOfAScheduleStoredAtVersion(
  { engine, redis, keys }: EngineTestContext,
  recordVersion: number,
): Promise<EpochMilliseconds> {
  const nextFiringAt = (await serverInstantInMillisecondsOf(redis)) + 3_600_000;
  await engine.syncSchedules({
    schedulesToSync: [{ task: "build-report", cron: "0 * * * *", timezone: "UTC", nextFiringAt, retries: 0 }],
  });
  await redis.hSet(keys.schedule("build-report"), "version", recordVersion);
  return nextFiringAt;
}

engineTest("listSchedules reads a schedule record of a newer version as no record", async (context) => {
  const nextFiringAt = await nextFiringAtOfAScheduleStoredAtVersion(context, RECORD_VERSION + 1);
  await context.redis.hSet(context.keys.schedule("build-report"), "previousFiringAt", nextFiringAt - 3_600_000);

  const [buildReport] = await readerOf(context).listSchedules();

  expect(buildReport).toEqual({ task: "build-report", cron: "0 * * * *", timezone: "UTC" });
});

engineTest(
  "a reader of an older release reads a run and a schedule record of the current version as no record",
  async (context) => {
    await nextFiringAtOfAScheduleStoredAtVersion(context, RECORD_VERSION);
    await enqueueRuns(context, [runOf("current-run", "send-email")]);
    const olderReader = readerOf(context, RECORD_VERSION - 1);

    const run = await olderReader.getRun("send-email", "current-run");
    const [buildReport] = await olderReader.listSchedules();

    expect(run).toBeUndefined();
    expect(buildReport).toEqual({ task: "build-report", cron: "0 * * * *", timezone: "UTC" });
  },
);

engineTest("listSchedules reads a schedule record of an older version as its record", async (context) => {
  const nextFiringAt = await nextFiringAtOfAScheduleStoredAtVersion(context, RECORD_VERSION - 1);

  const [buildReport] = await readerOf(context).listSchedules();

  expect(buildReport).toEqual({
    task: "build-report",
    cron: "0 * * * *",
    timezone: "UTC",
    nextFiringAt: new Date(nextFiringAt),
  });
});

engineTest(
  "each read but listRuns is one round trip of bounded reads, and listSchedules never reads the schedules set",
  async (context) => {
    const { client, keys } = context;
    await enqueueRuns(context, [runOf("run-1", "send-email")]);
    const reader = readerOf(context);

    const trafficByRead = {
      getRun: await readTrafficOf(client, () => reader.getRun("send-email", "run-1")),
      countRuns: await readTrafficOf(client, () => reader.countRuns()),
      getEvent: await readTrafficOf(client, () => reader.getEvent("invoice.paid", "event-1")),
      listSchedules: await readTrafficOf(client, () => reader.listSchedules()),
    };

    for (const [read, traffic] of Object.entries(trafficByRead)) {
      expect(traffic.roundTripCount, read).toBe(1);
      expectOnlyBoundedReads(traffic);
    }
    expect(trafficByRead.listSchedules.sentCommands.flatMap(({ args }) => args)).not.toContain(keys.schedules);
  },
);

engineTest(
  "getRun and getEvent read each run, and listRuns each listed run, in one script call holding every key it reads",
  async (context) => {
    const { client, keys } = context;
    const listenerRunIds = LISTENER_NAMES.map((listenerName) => deriveListenerRunId("event-1", listenerName));
    await endRuns(context.engine, "build-report", ["run-2"], "fail");
    await enqueueRuns(context, [
      runOf("run-1", "send-email"),
      ...LISTENER_NAMES.map((listenerName, position) => runOf(listenerRunIds[position]!, listenerName)),
    ]);
    const reader = readerOf(context);
    const runKeysOf = (task: string, runId: RunId) => [
      keys.runRecord(task, runId),
      keys.runPayload(task, runId),
      keys.runErrors(task, runId),
      keys.runSteps(task, runId),
    ];
    const runKeysOfEachCommandOf = ({ sentCommands }: ReadTraffic) => sentCommands.map(({ args }) => args.slice(0, 4));

    const trafficByRead = {
      getRun: await readTrafficOf(client, () => reader.getRun("send-email", "run-1")),
      getEvent: await readTrafficOf(client, () => reader.getEvent("invoice.paid", "event-1")),
      listRuns: await readTrafficOf(client, () => reader.listRuns({ state: "failed", task: "build-report" })),
    };

    expect(trafficByRead.getRun.sentCommands.map(({ name }) => name)).toEqual(["evalsha"]);
    expect(runKeysOfEachCommandOf(trafficByRead.getRun)).toEqual([runKeysOf("send-email", "run-1")]);
    expect(trafficByRead.getEvent.sentCommands.map(({ name }) => name)).toEqual(["evalsha", "evalsha"]);
    expect(runKeysOfEachCommandOf(trafficByRead.getEvent)).toEqual(
      LISTENER_NAMES.map((listenerName, position) => runKeysOf(listenerName, listenerRunIds[position]!)),
    );
    expect(trafficByRead.listRuns.sentCommands.map(({ name }) => name)).toEqual(["evalsha", "evalsha"]);
    expect(trafficByRead.listRuns.sentCommands[1]?.args.slice(0, 2)).toEqual([
      keys.runRecord("build-report", "run-2"),
      keys.runErrors("build-report", "run-2"),
    ]);
  },
);

type ListedIndexesOptions = Omit<ListRunsOptionsOf<string>, "limit" | "cursor">;

const MOST_PAGES_WALKED = 1000;

async function walkedRunPagesOf(
  reader: ReadApi,
  options: ListedIndexesOptions,
  limits: readonly number[],
  firstCursor?: string,
  followedCursor: "nextCursor" | "previousCursor" = "nextCursor",
): Promise<RunPageOf<AnyListedRun>[]> {
  const pages: RunPageOf<AnyListedRun>[] = [];
  let cursor = firstCursor;
  for (let pageCount = 0; pageCount < MOST_PAGES_WALKED; pageCount += 1) {
    const limit = limits[pageCount % limits.length]!;
    const page = await reader.listRuns({ ...options, limit, ...(cursor === undefined ? {} : { cursor }) });
    expect(page.runs.length).toBeLessThanOrEqual(limit);
    if (followedCursor === "nextCursor") {
      pages.push(page);
    } else {
      pages.unshift(page);
    }
    if (page[followedCursor] === undefined) {
      return pages;
    }
    expect(page[followedCursor]).not.toBe(cursor);
    cursor = page[followedCursor];
  }
  throw new Error(`the walk did not end within ${String(MOST_PAGES_WALKED)} pages`);
}

async function walkedPagesOf(
  reader: ReadApi,
  options: ListedIndexesOptions,
  limits: readonly number[],
  firstCursor?: string,
): Promise<AnyListedRun[][]> {
  return (await walkedRunPagesOf(reader, options, limits, firstCursor)).map(({ runs }) => runs);
}

function pageFiguresOf({ runs, totalRunCount, firstRunPosition, nextCursor }: RunPageOf<AnyListedRun>) {
  return { runCount: runs.length, totalRunCount, firstRunPosition, hasNextCursor: nextCursor !== undefined };
}

function runIdsOfPages(pages: readonly AnyListedRun[][]): RunId[] {
  return pages.flat().map(({ runId }) => runId);
}

async function runIdsInPromisedOrderOf(
  { redis, keys }: EngineTestContext,
  taskNames: readonly string[],
  states: RunState | readonly RunState[],
): Promise<RunId[]> {
  const indexEntries: { storedRunId: string; score: number }[] = [];
  for (const state of typeof states === "string" ? [states] : states) {
    for (const taskName of taskNames) {
      for (const { value, score } of await redis.zRangeWithScores(keys.runIndex(taskName, state), 0, -1)) {
        indexEntries.push({ storedRunId: value, score });
      }
    }
  }
  const compareAsciiStoredRunIds = (left: string, right: string) => (left < right ? -1 : Number(left > right));
  return indexEntries
    .sort((left, right) => right.score - left.score || compareAsciiStoredRunIds(right.storedRunId, left.storedRunId))
    .map(({ storedRunId }) => runIdentityOf(storedRunId)!.runId);
}

const STATES_BY_ORDER_GROUP_IN_BLOCK_SEQUENCE: readonly (readonly RunState[])[] = [
  ["executing"],
  ["pending", "waitingForConcurrencySlot"],
  ["delayed", "suspended", "backingOff"],
  ["completed", "failed"],
];

async function runIdsInBlocksOf(
  context: EngineTestContext,
  taskNames: readonly string[],
  listedStates: readonly RunState[] = RUN_STATES,
): Promise<RunId[]> {
  const runIds: RunId[] = [];
  for (const groupStates of STATES_BY_ORDER_GROUP_IN_BLOCK_SEQUENCE) {
    runIds.push(
      ...(await runIdsInPromisedOrderOf(
        context,
        taskNames,
        groupStates.filter((state) => listedStates.includes(state)),
      )),
    );
  }
  return runIds;
}

async function runsFinishedAtOneInstant(context: EngineTestContext, runs: readonly RunIdentity[]): Promise<void> {
  const finishedAts = await Promise.all(
    runs.map(async ({ task, runId }) => (await recordFieldOf(context, task, runId, "finishedAt")).getTime()),
  );
  const oneInstant = Math.min(...finishedAts) - 1000;
  for (const [runNumber, { task, runId }] of runs.entries()) {
    await backdateEndedRun(context, task, runId, finishedAts[runNumber]! - oneInstant);
  }
}

async function readerOfRunsInFourOrderGroupsOverTwoTasks(context: EngineTestContext): Promise<ReadApi> {
  await endRuns(context.engine, "send-email", ["ended-1", "ended-2"], "complete");
  await endRuns(context.engine, "build-report", ["ended-3"], "fail");
  await endRuns(context.engine, "build-report", ["ended-4"], "complete");
  await runsSuspendedUntilTheSameInstant(context, [runOf("asleep-1", "send-email"), runOf("asleep-2", "build-report")]);
  await enqueueRuns(context, [runOf("started-1", "send-email"), runOf("started-2", "build-report")]);
  await fetchedLockedRunsOf(context, 2);
  await enqueueRuns(context, [
    runOf("queued-1", "send-email"),
    runOf("queued-2", "build-report"),
    runOf("queued-3", "send-email"),
  ]);
  return readerOf(context);
}

const RUN_COUNT_IN_FOUR_ORDER_GROUPS = 11;

async function endRunsOfEachTask(context: EngineTestContext, runCountPerTask: number): Promise<RunId[]> {
  const endedRunIds: RunId[] = [];
  for (const taskName of TASK_NAMES) {
    const runIds = runIdsOf(`${taskName}-run`, runCountPerTask);
    await endRuns(context.engine, taskName, runIds, "complete");
    endedRunIds.push(...runIds);
  }
  return endedRunIds;
}

async function runsSuspendedUntilTheSameInstant(context: EngineTestContext, runs: readonly RunToCreate[]) {
  await enqueueRuns(context, runs);
  const resumesAt = (await serverInstantInMillisecondsOf(context.redis)) + 60_000;
  await settleEach(context, await fetchedLockedRunsOf(context, runs.length), { kind: "suspend", resumesAt });
}

async function readerOfTenRunsSuspendedUntilTheSameInstantOverTwoTasks(context: EngineTestContext): Promise<ReadApi> {
  await runsSuspendedUntilTheSameInstant(context, [
    ...runIdsOf("send-email-run", 7).map((runId) => runOf(runId, "send-email")),
    ...runIdsOf("build-report-run", 3).map((runId) => runOf(runId, "build-report")),
  ]);
  return readerOf(context);
}

engineTest(
  "listRuns without a task pages through every task, listing every run once in the promised order",
  async (context) => {
    const endedRunIds = await endRunsOfEachTask(context, 5);

    const pages = await walkedPagesOf(readerOf(context), { state: "completed" }, [4]);

    expect(runIdsOfPages(pages)).toEqual(await runIdsInPromisedOrderOf(context, TASK_NAMES, "completed"));
    expect([...runIdsOfPages(pages)].sort()).toEqual([...endedRunIds].sort());
    const finishedAts = pages.flat().map((run) => run.finishedAt!.getTime());
    expect(finishedAts).toEqual([...finishedAts].sort((left, right) => right - left));
  },
);

engineTest(
  "listRuns hands a next cursor when the tasks' pages, each under the limit, add up to more than it",
  async (context) => {
    await enqueueRuns(
      context,
      TASK_NAMES.flatMap((taskName) => runIdsOf(`${taskName}-run`, 5).map((runId) => runOf(runId, taskName))),
    );
    const reader = readerOf(context);

    const firstPage = await reader.listRuns({ state: "pending", limit: 6 });
    const pages = await walkedPagesOf(reader, { state: "pending" }, [6]);

    expect(firstPage.runs).toHaveLength(6);
    expect(firstPage.nextCursor).toBeDefined();
    expect(runIdsOfPages(pages)).toEqual(await runIdsInPromisedOrderOf(context, TASK_NAMES, "pending"));
    expect(runIdsOfPages(pages)).toHaveLength(15);
  },
);

engineTest(
  "listRuns lists every run once when more runs than the limit share one score, in one task and across tasks",
  async (context) => {
    const reader = await readerOfTenRunsSuspendedUntilTheSameInstantOverTwoTasks(context);

    const pagesOfEveryTask = await walkedPagesOf(reader, { state: "suspended" }, [2]);
    const pagesOfOneTask = await walkedPagesOf(reader, { state: "suspended", task: "send-email" }, [3]);

    expect(runIdsOfPages(pagesOfEveryTask)).toEqual(await runIdsInPromisedOrderOf(context, TASK_NAMES, "suspended"));
    expect(runIdsOfPages(pagesOfEveryTask)).toHaveLength(10);
    expect(runIdsOfPages(pagesOfOneTask)).toEqual([...runIdsOf("send-email-run", 7)].reverse());
  },
);

engineTest("listRuns hands no next cursor on a last page that is exactly full", async (context) => {
  await endRunsOfEachTask(context, 2);
  const reader = readerOf(context);

  const pagesOfEveryTask = await walkedRunPagesOf(reader, { state: "completed" }, [3]);
  const pagesOfOneTask = await walkedRunPagesOf(reader, { state: "completed", task: "send-email" }, [1]);

  expect(pagesOfEveryTask.map(({ runs, nextCursor }) => [runs.length, nextCursor !== undefined])).toEqual([
    [3, true],
    [3, false],
  ]);
  expect(pagesOfOneTask.map(({ runs, nextCursor }) => [runs.length, nextCursor !== undefined])).toEqual([
    [1, true],
    [1, false],
  ]);
});

engineTest(
  "a page of listRuns gives the total of the listed indexes and the position of its first run, across tasks sharing a score",
  async (context) => {
    const reader = await readerOfTenRunsSuspendedUntilTheSameInstantOverTwoTasks(context);

    const pagesOfEveryTask = await walkedRunPagesOf(reader, { state: "suspended" }, [3]);
    const pagesOfOneTask = await walkedRunPagesOf(reader, { state: "suspended", task: "send-email" }, [4]);

    expect(pagesOfEveryTask.map(pageFiguresOf)).toEqual([
      { runCount: 3, totalRunCount: 10, firstRunPosition: 1, hasNextCursor: true },
      { runCount: 3, totalRunCount: 10, firstRunPosition: 4, hasNextCursor: true },
      { runCount: 3, totalRunCount: 10, firstRunPosition: 7, hasNextCursor: true },
      { runCount: 1, totalRunCount: 10, firstRunPosition: 10, hasNextCursor: false },
    ]);
    expect(pagesOfOneTask.map(pageFiguresOf)).toEqual([
      { runCount: 4, totalRunCount: 7, firstRunPosition: 1, hasNextCursor: true },
      { runCount: 3, totalRunCount: 7, firstRunPosition: 5, hasNextCursor: false },
    ]);
  },
);

engineTest("an empty list gives a total of zero, no position and no next cursor", async (context) => {
  const page = await readerOf(context).listRuns({ state: "failed" });

  expect(page).toEqual({ runs: [], totalRunCount: 0 });
});

engineTest("listRuns lists every run once when the limit changes between pages", async (context) => {
  await endRunsOfEachTask(context, 5);

  const pages = await walkedPagesOf(readerOf(context), { state: "completed" }, [3, 1, 4, 2]);

  expect(runIdsOfPages(pages)).toEqual(await runIdsInPromisedOrderOf(context, TASK_NAMES, "completed"));
});

engineTest("listRuns lists pending runs newest dispatched first, then the handed-back runs", async (context) => {
  await enqueueRuns(context, [runOf("run-1", "send-email")]);
  const [handedBackRun] = await fetchedLockedRunsOf(context, 1);
  await enqueueRuns(context, [runOf("run-2", "send-email"), runOf("run-3", "build-report")]);
  await context.engine.settle({
    ...handedBackRun!,
    outcome: { kind: "handBack", error: ATTEMPT_ERROR, failureNotice: undefined },
  });

  const { runs } = await readerOf(context).listRuns({ state: "pending" });

  expect(runs.map(({ runId }) => runId)).toEqual(["run-3", "run-2", "run-1"]);
});

engineTest("a listed run carries the message of its last error, when it has one", async (context) => {
  await enqueueRuns(context, [runOf("retried-run", "send-email")]);
  await context.engine.settle({
    ...(await fetchedLockedRunsOf(context, 1))[0]!,
    outcome: { kind: "retry", error: { ...ATTEMPT_ERROR, message: "upstream down" }, backoffInMilliseconds: 0 },
  });
  await context.engine.promote();
  await enqueueRuns(context, [runOf("fresh-run", "send-email")]);

  const { runs } = await readerOf(context).listRuns({ state: "pending", task: "send-email" });

  expect(runs.map(({ runId }) => runId)).toEqual(["fresh-run", "retried-run"]);
  expect(runs[0]).not.toHaveProperty("lastErrorMessage");
  expect(runs[1]).toMatchObject({ attempt: 2, lastErrorMessage: "upstream down" });
});

engineTest("a run that leaves the state between two pages is skipped, not listed twice", async (context) => {
  const runIds = runIdsOf("send-email-run", 6);
  await enqueueRuns(
    context,
    runIds.map((runId) => runOf(runId, "send-email")),
  );
  const reader = readerOf(context);
  const firstPage = await reader.listRuns({ state: "pending", limit: 2 });
  await settleEach(context, await fetchedLockedRunsOf(context, 1), { kind: "complete" });

  const nextPages = await walkedPagesOf(reader, { state: "pending" }, [2], firstPage.nextCursor);

  expect(firstPage.runs.map(({ runId }) => runId)).toEqual([runIds[5], runIds[4]]);
  expect(runIdsOfPages(nextPages)).toEqual([runIds[3], runIds[2], runIds[1]]);
});

engineTest("a walk goes on past a cursor whose run was pruned between two pages", async (context) => {
  const runIds = runIdsOf("send-email-run", 6);
  await endRuns(context.engine, "send-email", runIds, "complete");
  const reader = readerOf(context);
  const firstPage = await reader.listRuns({ state: "completed", limit: 2 });
  const cursorRunId = firstPage.runs.at(-1)!.runId;
  await backdateEndedRun(context, "send-email", cursorRunId, 2 * DAY_IN_MILLISECONDS);
  await context.engine.prune(SEND_EMAIL_PRUNE_REQUEST);

  const nextPages = await walkedPagesOf(reader, { state: "completed" }, [2], firstPage.nextCursor);

  expect(await context.redis.exists(context.keys.runRecord("send-email", cursorRunId))).toBe(0);
  expect([...firstPage.runs.map(({ runId }) => runId), ...runIdsOfPages(nextPages)].sort()).toEqual(runIds);
  expect(runIdsOfPages(nextPages)).toEqual(
    (await runIdsInPromisedOrderOf(context, ["send-email"], "completed")).slice(1),
  );
});

engineTest("a walk goes on past a cursor whose run was replayed between two pages", async (context) => {
  const runIds = runIdsOf("send-email-run", 6);
  await endRuns(context.engine, "send-email", runIds, "fail");
  const reader = readerOf(context);
  const firstPage = await reader.listRuns({ state: "failed", limit: 2 });
  const remainingRunIds = (await runIdsInPromisedOrderOf(context, ["send-email"], "failed")).slice(2);
  await context.engine.replay({ task: "send-email", runId: firstPage.runs.at(-1)!.runId });

  const nextPages = await walkedPagesOf(reader, { state: "failed" }, [2], firstPage.nextCursor);

  expect(runIdsOfPages(nextPages)).toEqual(remainingRunIds);
  expect(nextPages.flat().every((run) => run.lastErrorMessage === "boom")).toBe(true);
});

engineTest(
  "a walk goes on past a cursor whose run was settled between two pages, among runs started together",
  async (context) => {
    const runIds = runIdsOf("send-email-run", 6);
    await enqueueRuns(
      context,
      runIds.map((runId) => runOf(runId, "send-email")),
    );
    const lockedRuns = await fetchedLockedRunsOf(context, 6);
    const reader = readerOf(context);
    const firstPage = await reader.listRuns({ state: "executing", limit: 2 });
    const cursorRunId = firstPage.runs.at(-1)!.runId;
    await settleEach(
      context,
      lockedRuns.filter(({ runId }) => runId === cursorRunId),
      { kind: "complete" },
    );

    const nextPages = await walkedPagesOf(reader, { state: "executing" }, [2], firstPage.nextCursor);

    expect(firstPage.runs.map(({ runId }) => runId)).toEqual([runIds[5], runIds[4]]);
    expect(runIdsOfPages(nextPages)).toEqual(runIds.slice(0, 4).reverse());
  },
);

engineTest(
  "a cursor of the format version 2, from a page opened before a deploy, still pages forward",
  async (context) => {
    const reader = await readerOfTenRunsSuspendedUntilTheSameInstantOverTwoTasks(context);
    const firstPage = await reader.listRuns({ state: "suspended", limit: 3 });
    const { task, runId } = firstPage.runs.at(-1)!;
    const score = await context.redis.zScore(context.keys.runIndex(task, "suspended"), storedRunIdOf(task, runId));
    const versionTwoCursor = cursorOfFields([2, "suspended", null, score, task, runId]);

    const pageAfterTheVersionTwoCursor = await reader.listRuns({
      state: "suspended",
      limit: 3,
      cursor: versionTwoCursor,
    });
    const pageAfterTheNextCursor = await reader.listRuns({
      state: "suspended",
      limit: 3,
      cursor: firstPage.nextCursor!,
    });

    expect(pageAfterTheVersionTwoCursor).toEqual(pageAfterTheNextCursor);
    expect(pageAfterTheVersionTwoCursor.firstRunPosition).toBe(4);
  },
);

function runIdsOfRunPages(pages: readonly RunPageOf<AnyListedRun>[]): RunId[] {
  return runIdsOfPages(pages.map(({ runs }) => runs));
}

for (const [options, limit, readerOfTheRuns] of [
  [{ state: "suspended" }, 3, readerOfTenRunsSuspendedUntilTheSameInstantOverTwoTasks],
  [{ state: "suspended" }, 4, readerOfTenRunsSuspendedUntilTheSameInstantOverTwoTasks],
  [{ state: "suspended", task: "send-email" }, 2, readerOfTenRunsSuspendedUntilTheSameInstantOverTwoTasks],
  [{}, 3, readerOfRunsInFourOrderGroupsOverTwoTasks],
  [{}, 4, readerOfRunsInFourOrderGroupsOverTwoTasks],
  [{ state: ["executing", "failed"] }, 1, readerOfRunsInFourOrderGroupsOverTwoTasks],
] as const) {
  engineTest(
    `a walk backward from the last page lists the runs of a walk forward in the same order, ${JSON.stringify(options)} in pages of ${String(limit)}`,
    async (context) => {
      const reader = await readerOfTheRuns(context);
      const forwardPages = await walkedRunPagesOf(reader, options, [limit]);
      const lastPage = forwardPages.at(-1)!;

      const backwardPages = await walkedRunPagesOf(
        reader,
        options,
        [limit],
        lastPage.previousCursor!,
        "previousCursor",
      );

      expect(runIdsOfRunPages([...backwardPages, lastPage])).toEqual(runIdsOfRunPages(forwardPages));
      expect([...backwardPages, lastPage].map(pageFiguresOf)).toEqual(forwardPages.map(pageFiguresOf));
    },
  );
}

engineTest(
  "a first page hands no previous cursor, and a page read backward hands both when runs lie on both sides",
  async (context) => {
    const reader = await readerOfTenRunsSuspendedUntilTheSameInstantOverTwoTasks(context);
    const [firstPage, , thirdPage] = await walkedRunPagesOf(reader, { state: "suspended" }, [3]);

    const pageReadBackward = await reader.listRuns({
      state: "suspended",
      limit: 3,
      cursor: thirdPage!.previousCursor!,
    });

    expect(firstPage!.previousCursor).toBeUndefined();
    expect(pageReadBackward).toMatchObject({
      totalRunCount: 10,
      firstRunPosition: 4,
      previousCursor: expect.any(String),
      nextCursor: expect.any(String),
    });
    expect(pageReadBackward.runs).toHaveLength(3);
  },
);

engineTest("a next page then its previous page are the page left, and back again", async (context) => {
  const reader = await readerOfTenRunsSuspendedUntilTheSameInstantOverTwoTasks(context);
  const [firstPage, secondPage, thirdPage] = await walkedRunPagesOf(reader, { state: "suspended" }, [3]);

  const pageBeforeTheSecond = await reader.listRuns({
    state: "suspended",
    limit: 3,
    cursor: secondPage!.previousCursor!,
  });
  const pageBeforeTheThird = await reader.listRuns({
    state: "suspended",
    limit: 3,
    cursor: thirdPage!.previousCursor!,
  });

  expect(pageBeforeTheSecond).toEqual(firstPage);
  expect(pageBeforeTheThird).toEqual(secondPage);
});

engineTest("a page read backward that reaches the head of the list is the first page, full", async (context) => {
  const reader = await readerOfTenRunsSuspendedUntilTheSameInstantOverTwoTasks(context);
  const pageOfTwo = await reader.listRuns({ state: "suspended", limit: 2 });
  const pageAfterIt = await reader.listRuns({ state: "suspended", limit: 3, cursor: pageOfTwo.nextCursor! });

  const pageReachingTheHead = await reader.listRuns({
    state: "suspended",
    limit: 3,
    cursor: pageAfterIt.previousCursor!,
  });

  expect(pageAfterIt.firstRunPosition).toBe(3);
  expect(pageReachingTheHead).toEqual(await reader.listRuns({ state: "suspended", limit: 3 }));
  expect(pageReachingTheHead.runs).toHaveLength(3);
  expect(pageReachingTheHead).not.toHaveProperty("previousCursor");
});

engineTest(
  "after the runs a page's cursors name leave the list, the page before and the page after start at the right runs",
  async (context) => {
    for (const taskName of TASK_NAMES) {
      await endRuns(context.engine, taskName, runIdsOf(`${taskName}-run`, 3), "fail");
    }
    const reader = readerOf(context);
    const runIdsInOrder = await runIdsInPromisedOrderOf(context, TASK_NAMES, "failed");
    const firstPage = await reader.listRuns({ state: "failed", limit: 2 });
    const secondPage = await reader.listRuns({ state: "failed", limit: 2, cursor: firstPage.nextCursor! });
    for (const { task, runId } of secondPage.runs) {
      await context.engine.replay({ task, runId });
    }

    const pageBefore = await reader.listRuns({ state: "failed", limit: 2, cursor: secondPage.previousCursor! });
    const pageAfter = await reader.listRuns({ state: "failed", limit: 2, cursor: secondPage.nextCursor! });

    expect(secondPage.runs.map(({ runId }) => runId)).toEqual(runIdsInOrder.slice(2, 4));
    expect(pageBefore.runs.map(({ runId }) => runId)).toEqual(runIdsInOrder.slice(0, 2));
    expect(pageBefore).toMatchObject({ totalRunCount: 7, firstRunPosition: 1 });
    expect(pageBefore).not.toHaveProperty("previousCursor");
    expect(pageAfter.runs.map(({ runId }) => runId)).toEqual(runIdsInOrder.slice(4, 6));
    expect(pageAfter).toMatchObject({ totalRunCount: 7, firstRunPosition: 3, previousCursor: expect.any(String) });
  },
);

engineTest(
  "a page past the end of the list hands the end of the list as its previous page, the run of its cursor included",
  async (context) => {
    await endRuns(context.engine, "send-email", runIdsOf("send-email-run", 5), "fail");
    const reader = readerOf(context);
    const runIdsInOrder = await runIdsInPromisedOrderOf(context, ["send-email"], "failed");
    const firstPage = await reader.listRuns({ state: "failed", limit: 4 });
    await context.engine.replay({ task: "send-email", runId: runIdsInOrder[4]! });

    const pagePastTheEnd = await reader.listRuns({ state: "failed", limit: 4, cursor: firstPage.nextCursor! });
    const lastPage = await reader.listRuns({ state: "failed", limit: 2, cursor: pagePastTheEnd.previousCursor! });

    expect(pagePastTheEnd).toEqual({ runs: [], totalRunCount: 4, previousCursor: expect.any(String) });
    expect(lastPage.runs.map(({ runId }) => runId)).toEqual(runIdsInOrder.slice(2, 4));
    expect(lastPage).toMatchObject({ totalRunCount: 4, firstRunPosition: 3, previousCursor: expect.any(String) });
    expect(lastPage).not.toHaveProperty("nextCursor");
  },
);

engineTest("listRuns refuses a cursor from another state or another task", async (context) => {
  await endRunsOfEachTask(context, 3);
  const reader = readerOf(context);
  const cursorOfEveryTask = (await reader.listRuns({ state: "completed", limit: 1 })).nextCursor;
  const cursorOfOneTask = (await reader.listRuns({ state: "completed", task: "send-email", limit: 1 })).nextCursor;

  const listings = [
    reader.listRuns({ state: "failed", cursor: cursorOfEveryTask! }),
    reader.listRuns({ state: "completed", task: "send-email", cursor: cursorOfEveryTask! }),
    reader.listRuns({ state: "completed", cursor: cursorOfOneTask! }),
    reader.listRuns({ state: "completed", task: "build-report", cursor: cursorOfOneTask! }),
  ];

  await expect(listings[0]).rejects.toThrow(
    'Invalid listRuns cursor: it pages through state "completed", not "failed"',
  );
  await expect(listings[1]).rejects.toThrow(
    'Invalid listRuns cursor: it pages through every task, not task "send-email"',
  );
  await expect(listings[2]).rejects.toThrow(
    'Invalid listRuns cursor: it pages through task "send-email", not every task',
  );
  await expect(listings[3]).rejects.toThrow(
    'Invalid listRuns cursor: it pages through task "send-email", not task "build-report"',
  );
});

engineTest(
  "listRuns takes the same states listed in another order as the same list, and refuses a cursor from another set of states",
  async (context) => {
    await endRunsOfEachTask(context, 2);
    const reader = readerOf(context);
    const cursorOfTwoStates = (await reader.listRuns({ state: ["failed", "completed"], limit: 1 })).nextCursor!;
    const cursorOfEveryState = (await reader.listRuns({ limit: 1 })).nextCursor!;

    const pageOfTheStatesInAnotherOrder = await reader.listRuns({
      state: ["completed", "failed"],
      limit: 1,
      cursor: cursorOfTwoStates,
    });
    const listings = [
      reader.listRuns({ state: "completed", cursor: cursorOfTwoStates }),
      reader.listRuns({ cursor: cursorOfTwoStates }),
      reader.listRuns({ state: ["completed", "failed"], cursor: cursorOfEveryState }),
    ];

    expect(pageOfTheStatesInAnotherOrder.firstRunPosition).toBe(2);
    await expect(listings[0]).rejects.toThrow(
      'Invalid listRuns cursor: it pages through states "completed" and "failed", not "completed"',
    );
    await expect(listings[1]).rejects.toThrow(
      'Invalid listRuns cursor: it pages through states "completed" and "failed", not every state',
    );
    await expect(listings[2]).rejects.toThrow(
      'Invalid listRuns cursor: it pages through every state, not "completed" and "failed"',
    );
  },
);

engineTest(
  "listRuns of two tasks out of three lists their runs in one order, and neither lists nor counts nor reads the third",
  async (context) => {
    const { client, keys } = context;
    await endRunsOfEachTask(context, 3);
    const reader = readerOf(context);

    const pages = await walkedRunPagesOf(reader, { state: "completed", task: ["send-email", "resize-image"] }, [2]);
    const traffic = await readTrafficOf(client, () =>
      reader.listRuns({ state: "completed", task: ["send-email", "resize-image"] }),
    );

    expect(runIdsOfRunPages(pages)).toEqual(
      await runIdsInPromisedOrderOf(context, ["send-email", "resize-image"], "completed"),
    );
    expect(pages.map(pageFiguresOf)).toEqual([
      { runCount: 2, totalRunCount: 6, firstRunPosition: 1, hasNextCursor: true },
      { runCount: 2, totalRunCount: 6, firstRunPosition: 3, hasNextCursor: true },
      { runCount: 2, totalRunCount: 6, firstRunPosition: 5, hasNextCursor: false },
    ]);
    expect(traffic.roundTripCount).toBe(2);
    expect(traffic.sentCommands[0]?.args.slice(0, 2)).toEqual([
      keys.runIndex("send-email", "completed"),
      keys.runIndex("resize-image", "completed"),
    ]);
    expect(traffic.sentCommands.flatMap(({ args }) => args)).not.toContain(keys.runIndex("build-report", "completed"));
  },
);

engineTest(
  "listRuns takes the same tasks listed in another order as the same list, and refuses a cursor from another set of tasks",
  async (context) => {
    await endRunsOfEachTask(context, 2);
    const reader = readerOf(context);
    const cursorOfTwoTasks = (
      await reader.listRuns({ state: "completed", task: ["resize-image", "send-email"], limit: 1 })
    ).nextCursor!;

    const pageOfTheTasksInAnotherOrder = await reader.listRuns({
      state: "completed",
      task: ["send-email", "resize-image"],
      limit: 1,
      cursor: cursorOfTwoTasks,
    });
    const listings = [
      reader.listRuns({ state: "completed", task: "send-email", cursor: cursorOfTwoTasks }),
      reader.listRuns({ state: "completed", task: ["send-email", "build-report"], cursor: cursorOfTwoTasks }),
      reader.listRuns({ state: "completed", cursor: cursorOfTwoTasks }),
    ];

    expect(pageOfTheTasksInAnotherOrder.firstRunPosition).toBe(2);
    await expect(listings[0]).rejects.toThrow(
      'Invalid listRuns cursor: it pages through tasks "send-email" and "resize-image", not task "send-email"',
    );
    await expect(listings[1]).rejects.toThrow(
      'Invalid listRuns cursor: it pages through tasks "send-email" and "resize-image", not tasks "send-email" and "build-report"',
    );
    await expect(listings[2]).rejects.toThrow(
      'Invalid listRuns cursor: it pages through tasks "send-email" and "resize-image", not every task',
    );
  },
);

engineTest(
  "listRuns of completed and failed runs of several tasks lists them in one order by finish time, equal finish times included",
  async (context) => {
    const endedRuns: RunIdentity[] = [];
    for (const taskName of TASK_NAMES) {
      const [completedRunIds, failedRunIds] = [runIdsOf(`${taskName}-done`, 2), runIdsOf(`${taskName}-broken`, 2)];
      await endRuns(context.engine, taskName, completedRunIds, "complete");
      await endRuns(context.engine, taskName, failedRunIds, "fail");
      endedRuns.push(...[...completedRunIds, ...failedRunIds].map((runId) => ({ task: taskName, runId })));
    }
    await runsFinishedAtOneInstant(context, endedRuns.slice(2, 9));

    const pages = await walkedPagesOf(readerOf(context), { state: ["failed", "completed"] }, [5]);

    expect(runIdsOfPages(pages)).toEqual(await runIdsInPromisedOrderOf(context, TASK_NAMES, ["completed", "failed"]));
    expect(runIdsOfPages(pages)).toHaveLength(12);
    const finishedAts = pages.flat().map((run) => run.finishedAt!.getTime());
    expect(finishedAts).toEqual([...finishedAts].sort((left, right) => right - left));
    expect(new Set(pages.flat().map(({ state }) => state))).toEqual(new Set(["completed", "failed"]));
  },
);

engineTest(
  "listRuns of states of several order groups lists them as blocks in the fixed sequence, a page filling across a boundary",
  async (context) => {
    const reader = await readerOfRunsInFourOrderGroupsOverTwoTasks(context);

    const pages = await walkedPagesOf(reader, { state: ["completed", "suspended", "pending", "executing"] }, [3]);

    expect(runIdsOfPages(pages)).toEqual(
      await runIdsInBlocksOf(context, TASK_NAMES, ["completed", "suspended", "pending", "executing"]),
    );
    expect(pages.map((runs) => runs.map(({ state }) => state))).toEqual([
      ["executing", "executing", "pending"],
      ["pending", "pending", "suspended"],
      ["suspended", "completed", "completed"],
      ["completed"],
    ]);
  },
);

engineTest(
  "listRuns without a state lists every run of the instance once, block after block, and places each page in the whole list",
  async (context) => {
    const reader = await readerOfRunsInFourOrderGroupsOverTwoTasks(context);

    const pages = await walkedRunPagesOf(reader, {}, [2]);

    expect(runIdsOfRunPages(pages)).toEqual(await runIdsInBlocksOf(context, TASK_NAMES));
    expect(pages.map(pageFiguresOf)).toEqual(
      [2, 2, 2, 2, 2, 1].map((runCount, pageNumber) => ({
        runCount,
        totalRunCount: RUN_COUNT_IN_FOUR_ORDER_GROUPS,
        firstRunPosition: pageNumber * 2 + 1,
        hasNextCursor: pageNumber < 5,
      })),
    );
    expect(pages[1]!.runs.map(({ state }) => state)).toEqual(["pending", "pending"]);
  },
);

engineTest("a page read backward across a block boundary is the forward page it comes after", async (context) => {
  const reader = await readerOfRunsInFourOrderGroupsOverTwoTasks(context);
  const [, secondPage, thirdPage] = await walkedRunPagesOf(reader, {}, [3]);

  const pageBeforeTheThird = await reader.listRuns({ limit: 3, cursor: thirdPage!.previousCursor! });

  expect(secondPage!.runs.map(({ state }) => state)).toEqual(["pending", "pending", "suspended"]);
  expect(pageBeforeTheThird).toEqual(secondPage);
});

engineTest(
  "a page read backward from a place in a later block reaches the head of the whole list exactly when fewer runs than the limit come before it",
  async (context) => {
    const reader = await readerOfRunsInFourOrderGroupsOverTwoTasks(context);
    const [, pageStartingTheQueuedBlock] = await walkedRunPagesOf(reader, {}, [2]);
    const [, pageStartingAfterTheFirstQueuedRun] = await walkedRunPagesOf(reader, {}, [3, 3]);
    const [cursorWithTwoRunsBefore, cursorWithThreeRunsBefore] = [
      pageStartingTheQueuedBlock!.previousCursor!,
      pageStartingAfterTheFirstQueuedRun!.previousCursor!,
    ];

    const pages = {
      fewerThanTheLimitBefore: await reader.listRuns({ limit: 3, cursor: cursorWithTwoRunsBefore }),
      asManyAsTheLimitBefore: await reader.listRuns({ limit: 2, cursor: cursorWithTwoRunsBefore }),
      moreThanTheLimitBefore: await reader.listRuns({ limit: 2, cursor: cursorWithThreeRunsBefore }),
    };

    const runIdsInBlocks = await runIdsInBlocksOf(context, TASK_NAMES);
    expect(pageStartingTheQueuedBlock!.runs[0]!.state).toBe("pending");
    expect(pageStartingAfterTheFirstQueuedRun!.firstRunPosition).toBe(4);
    expect(pages.fewerThanTheLimitBefore).toEqual(await reader.listRuns({ limit: 3 }));
    expect(pages.fewerThanTheLimitBefore).toMatchObject({ firstRunPosition: 1 });
    expect(pages.fewerThanTheLimitBefore).not.toHaveProperty("previousCursor");
    expect(pages.asManyAsTheLimitBefore.runs.map(({ runId }) => runId)).toEqual(runIdsInBlocks.slice(0, 2));
    expect(pages.asManyAsTheLimitBefore).toMatchObject({ firstRunPosition: 1 });
    expect(pages.asManyAsTheLimitBefore).not.toHaveProperty("previousCursor");
    expect(pages.moreThanTheLimitBefore.runs.map(({ runId }) => runId)).toEqual(runIdsInBlocks.slice(1, 3));
    expect(pages.moreThanTheLimitBefore).toMatchObject({
      firstRunPosition: 2,
      previousCursor: expect.any(String),
    });
  },
);

engineTest(
  "a run that moves between two listed states of one order group between the two round trips is left out of the page",
  async (context) => {
    const { client, engine } = context;
    await enqueueRuns(context, [runOf("started-run", "send-email")]);
    await fetchedLockedRunsOf(context, 1);
    await enqueueRuns(context, [runOf("moving-run", "send-email")]);
    let isRunMoved = false;
    const clientMovingTheRunBeforeTheRunReads: RedisClient = {
      send: (command) => client.send(command),
      sendBatch: async (commands) => {
        if (!isRunMoved) {
          isRunMoved = true;
          await engine.fetch({
            lockTokens: freshLockTokensOf(1),
            concurrencyLimitsByTask: { "send-email": 1 },
            lockDurationInMilliseconds: 120_000,
          });
        }
        return client.sendBatch(commands);
      },
    };
    const reader = readerOf({ ...context, client: clientMovingTheRunBeforeTheRunReads });
    const queuedStates = ["pending", "waitingForConcurrencySlot"] as const;

    const pageReadAcrossTheMove = await reader.listRuns({ state: queuedStates });
    const pageReadAfterTheMove = await reader.listRuns({ state: queuedStates });

    expect(pageReadAcrossTheMove).toEqual({ runs: [], totalRunCount: 1, firstRunPosition: 1 });
    expect(pageReadAfterTheMove.runs.map(({ runId, state }) => ({ runId, state }))).toEqual([
      { runId: "moving-run", state: "waitingForConcurrencySlot" },
    ]);
  },
);

engineTest(
  "listRuns refuses a 0.1.x cursor, a forward cursor from no place, an invalid state or limit, and a task it does not define",
  async (context) => {
    const reader = readerOf(context);

    const listings = {
      oldCursor: reader.listRuns({ state: "failed", cursor: cursorOfFields([1, "failed", 0, 50]) }),
      forwardFromNoPlace: reader.listRuns({
        state: "failed",
        cursor: cursorOfFields([3, "forward", null, "failed", null]),
      }),
      invalidState: reader.listRuns({ state: "sleeping" as RunState }),
      invalidLimit: reader.listRuns({ state: "failed", limit: 0 }),
      unknownTask: reader.listRuns({ state: "failed", task: "removed-task" }),
    };

    for (const refusedCursorListing of [listings.oldCursor, listings.forwardFromNoPlace]) {
      await expect(refusedCursorListing).rejects.toThrow(
        "Invalid listRuns cursor: pass back a nextCursor or previousCursor that listRuns returned",
      );
    }
    await expect(listings.invalidState).rejects.toThrow('Invalid listRuns state: "sleeping" is not one of');
    await expect(listings.invalidLimit).rejects.toThrow("Invalid listRuns limit: 0 is not a positive integer");
    await expect(listings.unknownTask).rejects.toThrow(UnknownTaskError);
  },
);

engineTest(
  "listRuns is two round trips of bounded reads, never asking the index of a task the release does not define",
  async (context) => {
    const { client, keys } = context;
    await endRunsOfEachTask(context, 3);
    await endRuns(context.engine, "removed-task", ["removed-task-run"], "complete");
    const reader = readerOf(context);
    const { nextCursor } = await reader.listRuns({ state: "completed", limit: 2 });
    const secondPage = await reader.listRuns({ state: "completed", limit: 2, cursor: nextCursor! });
    const thirdPage = await reader.listRuns({ state: "completed", limit: 2, cursor: secondPage.nextCursor! });
    const everyStatePage = await reader.listRuns({ limit: 2 });

    const trafficByListing = {
      firstPage: await readTrafficOf(client, () => reader.listRuns({ state: "completed", limit: 2 })),
      nextPage: await readTrafficOf(client, () =>
        reader.listRuns({ state: "completed", limit: 2, cursor: nextCursor! }),
      ),
      previousPage: await readTrafficOf(client, () =>
        reader.listRuns({ state: "completed", limit: 2, cursor: thirdPage.previousCursor! }),
      ),
      previousPageReachingTheHead: await readTrafficOf(client, () =>
        reader.listRuns({ state: "completed", limit: 3, cursor: secondPage.previousCursor! }),
      ),
      oneTask: await readTrafficOf(client, () => reader.listRuns({ state: "completed", task: "send-email" })),
      emptyState: await readTrafficOf(client, () => reader.listRuns({ state: "failed" })),
      everyState: await readTrafficOf(client, () => reader.listRuns({ limit: 2 })),
      everyStateNextPage: await readTrafficOf(client, () =>
        reader.listRuns({ limit: 2, cursor: everyStatePage.nextCursor! }),
      ),
      twoStates: await readTrafficOf(client, () => reader.listRuns({ state: ["completed", "failed"], limit: 2 })),
    };

    expect(trafficByListing.firstPage.roundTripCount).toBe(2);
    expect(trafficByListing.nextPage.roundTripCount).toBe(2);
    expect(trafficByListing.previousPage.roundTripCount).toBe(2);
    expect(trafficByListing.previousPageReachingTheHead.roundTripCount).toBe(2);
    expect(trafficByListing.oneTask.roundTripCount).toBe(2);
    expect(trafficByListing.emptyState.roundTripCount).toBe(1);
    expect(trafficByListing.everyState.roundTripCount).toBe(2);
    expect(trafficByListing.everyStateNextPage.roundTripCount).toBe(2);
    expect(trafficByListing.twoStates.roundTripCount).toBe(2);
    expect(trafficByListing.everyState.sentCommands[0]?.args).toHaveLength(
      (TASK_NAMES.length + LISTENER_NAMES.length) * RUN_STATES.length * 2 + 2,
    );
    expect(trafficByListing.firstPage.sentCommands[0]).toEqual({
      name: "evalsha",
      args: [
        ...[...TASK_NAMES, ...LISTENER_NAMES].map((taskName) => keys.runIndex(taskName, "completed")),
        2,
        "forward",
        ...[...TASK_NAMES, ...LISTENER_NAMES].map(() => ENDED_ORDER_GROUP_NUMBER),
      ],
    });
    for (const traffic of Object.values(trafficByListing)) {
      expectOnlyBoundedReads(traffic);
      expect(traffic.sentCommands.flatMap(({ args }) => args)).not.toContain(
        keys.runIndex("removed-task", "completed"),
      );
    }
  },
);

integrationTest(
  "listRuns silently drops an index entry without its record, of a newer record version or of another state",
  async ({ redisUrl, keyPrefix, expectInvariantViolation }) => {
    await withRedis(redisUrl, async (redis, client) => {
      const context = engineTestContextOf(redis, client, keyPrefix);
      const { keys } = context;
      const keptRunIds = runIdsOf("kept-run", 3);
      await endRuns(context.engine, "send-email", [...keptRunIds, "newer-run"], "complete");
      await redis.hSet(keys.runRecord("send-email", "newer-run"), "version", RECORD_VERSION + 1);
      await enqueueRuns(context, [runOf("pending-run", "send-email")]);
      const latestScore = (await serverInstantInMillisecondsOf(redis)) + 60_000;
      const completedIndexKey = keys.runIndex("send-email", "completed");
      const [ghostStoredRunId, pendingStoredRunId] = [
        storedRunIdOf("send-email", "ghost-run"),
        storedRunIdOf("send-email", "pending-run"),
      ];
      await redis.zAdd(completedIndexKey, [
        { score: latestScore, value: ghostStoredRunId },
        { score: latestScore, value: pendingStoredRunId },
      ]);
      const corruptions = [
        `run "${ghostStoredRunId}" sits in ${completedIndexKey} but has no record with a known state`,
        `run "${pendingStoredRunId}" sits in ${completedIndexKey} though its record is pending`,
      ];
      for (const corruption of corruptions) {
        expectInvariantViolation("stored keys", corruption);
      }

      const pages = await walkedPagesOf(readerOf(context), { state: "completed" }, [2]);

      expect(pages[0]).toEqual([]);
      expect([...runIdsOfPages(pages)].sort()).toEqual(keptRunIds);
      expect([...(await storedKeyViolationsOf(redis, keyPrefix))].sort()).toEqual([...corruptions].sort());
    });
  },
);
