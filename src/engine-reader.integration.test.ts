import type { Redis } from "ioredis";
import { expect, vi } from "vitest";

import { DAY_IN_MILLISECONDS, endRuns, runIdsOf, SEND_EMAIL_PRUNE_REQUEST } from "../tests/ended-runs.js";
import { backdateEndedRun, engineTest, fetchedLockedRunsOf } from "../tests/engine-test.js";
import type { EngineTestContext } from "../tests/engine-test.js";
import { integrationTest } from "../tests/integration-test.js";
import { invariantViolationsOf } from "../tests/invariant-violations.js";
import { serverInstantInMillisecondsOf } from "../tests/server-instant.js";
import { withRedis } from "../tests/with-redis.js";
import { defineScheduledTask } from "./definitions.js";
import type { AnyTaskDefinition } from "./definitions.js";
import { engineKeysOf, runIdentityOf, storedRunIdOf } from "./engine/engine-keys.js";
import { createEngine, RECORD_VERSION } from "./engine/engine.js";
import type { LockedRun, RunToCreate, SettleOutcome } from "./engine/engine.js";
import { createEngineReader } from "./engine-reader.js";
import type { EngineReader } from "./engine-reader.js";
import { UnknownEventError, UnknownTaskError } from "./errors.js";
import { deriveFiringRunId, deriveListenerRunId } from "./ids.js";
import type { RunId } from "./ids.js";
import { cursorOfFields } from "./read-api.js";
import type { AnyListedRun, ListRunsOptionsOf } from "./read-api.js";
import { RUN_STATES } from "./run-states.js";
import type { RunState } from "./run-states.js";
import type { DeclaredSchedule } from "./schedules.js";
import { DEFAULT_WORKER_TIMING } from "./worker-timing.js";

const TASK_NAMES = ["send-email", "build-report", "resize-image"] as const;

const LISTENER_NAMES = ["record-payment", "email-receipt"] as const;

const DECLARED_SCHEDULES: readonly DeclaredSchedule[] = [
  { taskName: "build-report", cron: "0 * * * *", timezone: "UTC" },
  { taskName: "send-email", cron: "*/5 * * * *", timezone: "Europe/Paris" },
];

const READ_COMMAND_NAMES = new Set(["hmget", "get", "lrange", "lindex", "hgetall", "zcard", "eval", "evalsha"]);

const ATTEMPT_ERROR = { attempt: 1, name: "Error", message: "boom", stack: "Error: boom", failedAt: "" };

type SentCommand = { name: string; args: unknown[] };

type ReadTraffic = { roundTripCount: number; sentCommands: SentCommand[] };

function taskDefinitionOf(taskName: string): AnyTaskDefinition {
  return defineScheduledTask(taskName, { schedule: { cron: "0 * * * *" }, handler: async () => undefined });
}

function readerOf({ redis, keyPrefix }: EngineTestContext): EngineReader {
  const taskNames = [...TASK_NAMES, ...LISTENER_NAMES];
  return createEngineReader({
    client: redis,
    prefix: keyPrefix,
    tasksByName: new Map(taskNames.map((taskName) => [taskName, taskDefinitionOf(taskName)])),
    listenerNamesByEventName: new Map([["invoice.paid", LISTENER_NAMES]]),
    declaredSchedules: DECLARED_SCHEDULES,
  });
}

async function readTrafficOf(redis: Redis, read: () => Promise<unknown>): Promise<ReadTraffic> {
  const streamWrite = vi.spyOn(redis.stream, "write");
  const sendCommand = vi.spyOn(redis, "sendCommand");
  try {
    await read();
    return {
      roundTripCount: streamWrite.mock.calls.length,
      sentCommands: sendCommand.mock.calls.map(([command]) => ({ name: command.name, args: command.args })),
    };
  } finally {
    streamWrite.mockRestore();
    sendCommand.mockRestore();
  }
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
  return new Date(Number(await context.redis.hget(context.keys.runRecord(task, runId), field)));
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
  "getRun reads as undefined a run never dispatched, a run of another task and a run of a newer record version",
  async (context) => {
    const { redis, keys } = context;
    await enqueueRuns(context, [runOf("other-task-run", "build-report"), runOf("newer-run", "send-email")]);
    await redis.hset(keys.runRecord("send-email", "newer-run"), "version", RECORD_VERSION + 1);
    const reader = readerOf(context);

    const runs = [
      await reader.getRun("send-email", "never-dispatched-run"),
      await reader.getRun("send-email", "other-task-run"),
      await reader.getRun("send-email", "newer-run"),
    ];

    expect(runs).toEqual([undefined, undefined, undefined]);
  },
);

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
    await redis.hset(keys.runRecord("send-email", "newer-run"), "version", RECORD_VERSION + 1);
    const reader = readerOf(context);

    const runCounts = await reader.countRuns();

    expect(runCounts["send-email"]).toMatchObject({ pending: 2, delayed: 0, completed: 0 });
    expect(runCounts["build-report"]).toMatchObject({ pending: 0, delayed: 1 });
    expect(runCounts).not.toHaveProperty("removed-task");
    for (const taskName of [...TASK_NAMES, ...LISTENER_NAMES]) {
      for (const state of RUN_STATES) {
        expect(runCounts[taskName]?.[state], `${taskName} ${state}`).toBe(
          await redis.zcard(keys.runIndex(taskName, state)),
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

engineTest("listSchedules reads a schedule record of a newer version as no record", async (context) => {
  const { engine, redis, keys } = context;
  const nextFiringAt = (await serverInstantInMillisecondsOf(redis)) + 3_600_000;
  await engine.syncSchedules({
    schedulesToSync: [{ task: "build-report", cron: "0 * * * *", timezone: "UTC", nextFiringAt, retries: 0 }],
  });
  await redis.hset(keys.schedule("build-report"), "version", RECORD_VERSION + 1);

  const [buildReport] = await readerOf(context).listSchedules();

  expect(buildReport).toEqual({ task: "build-report", cron: "0 * * * *", timezone: "UTC" });
});

engineTest(
  "each read but listRuns is one round trip of bounded reads, and listSchedules never reads the schedules set",
  async (context) => {
    const { redis, keys } = context;
    await enqueueRuns(context, [runOf("run-1", "send-email")]);
    const reader = readerOf(context);

    const trafficByRead = {
      getRun: await readTrafficOf(redis, () => reader.getRun("send-email", "run-1")),
      countRuns: await readTrafficOf(redis, () => reader.countRuns()),
      getEvent: await readTrafficOf(redis, () => reader.getEvent("invoice.paid", "event-1")),
      listSchedules: await readTrafficOf(redis, () => reader.listSchedules()),
    };

    for (const [read, traffic] of Object.entries(trafficByRead)) {
      expect(traffic.roundTripCount, read).toBe(1);
      expectOnlyBoundedReads(traffic);
    }
    expect(trafficByRead.listSchedules.sentCommands.flatMap(({ args }) => args)).not.toContain(keys.schedules);
  },
);

type ListedIndexesOptions = Omit<ListRunsOptionsOf<string>, "limit" | "cursor">;

const MOST_PAGES_WALKED = 1000;

async function walkedPagesOf(
  reader: EngineReader,
  options: ListedIndexesOptions,
  limits: readonly number[],
  firstCursor?: string,
): Promise<AnyListedRun[][]> {
  const pages: AnyListedRun[][] = [];
  let cursor = firstCursor;
  for (let pageCount = 0; pageCount < MOST_PAGES_WALKED; pageCount += 1) {
    const limit = limits[pageCount % limits.length]!;
    const page = await reader.listRuns({ ...options, limit, ...(cursor === undefined ? {} : { cursor }) });
    expect(page.runs.length).toBeLessThanOrEqual(limit);
    pages.push(page.runs);
    if (page.nextCursor === undefined) {
      return pages;
    }
    expect(page.nextCursor).not.toBe(cursor);
    cursor = page.nextCursor;
  }
  throw new Error(`the walk did not end within ${String(MOST_PAGES_WALKED)} pages`);
}

function runIdsOfPages(pages: readonly AnyListedRun[][]): RunId[] {
  return pages.flat().map(({ runId }) => runId);
}

async function runIdsInPromisedOrderOf(
  { redis, keys }: EngineTestContext,
  taskNames: readonly string[],
  state: RunState,
): Promise<RunId[]> {
  const indexEntries: { storedRunId: string; score: number }[] = [];
  for (const taskName of taskNames) {
    const membersWithScores = await redis.zrange(keys.runIndex(taskName, state), 0, -1, "WITHSCORES");
    for (let position = 0; position < membersWithScores.length; position += 2) {
      indexEntries.push({ storedRunId: membersWithScores[position]!, score: Number(membersWithScores[position + 1]) });
    }
  }
  const compareAsciiStoredRunIds = (left: string, right: string) => (left < right ? -1 : Number(left > right));
  return indexEntries
    .sort((left, right) => right.score - left.score || compareAsciiStoredRunIds(right.storedRunId, left.storedRunId))
    .map(({ storedRunId }) => runIdentityOf(storedRunId)!.runId);
}

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
    await runsSuspendedUntilTheSameInstant(context, [
      ...runIdsOf("send-email-run", 7).map((runId) => runOf(runId, "send-email")),
      ...runIdsOf("build-report-run", 3).map((runId) => runOf(runId, "build-report")),
    ]);
    const reader = readerOf(context);

    const pagesOfEveryTask = await walkedPagesOf(reader, { state: "suspended" }, [2]);
    const pagesOfOneTask = await walkedPagesOf(reader, { state: "suspended", task: "send-email" }, [3]);

    expect(runIdsOfPages(pagesOfEveryTask)).toEqual(await runIdsInPromisedOrderOf(context, TASK_NAMES, "suspended"));
    expect(runIdsOfPages(pagesOfEveryTask)).toHaveLength(10);
    expect(runIdsOfPages(pagesOfOneTask)).toEqual([...runIdsOf("send-email-run", 7)].reverse());
  },
);

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
  "listRuns refuses a 0.1.x cursor, an invalid state or limit, and a task it does not define",
  async (context) => {
    const reader = readerOf(context);

    const listings = {
      oldCursor: reader.listRuns({ state: "failed", cursor: cursorOfFields([1, "failed", 0, 50]) }),
      invalidState: reader.listRuns({ state: "sleeping" as RunState }),
      invalidLimit: reader.listRuns({ state: "failed", limit: 0 }),
      unknownTask: reader.listRuns({ state: "failed", task: "removed-task" }),
    };

    await expect(listings.oldCursor).rejects.toThrow(
      "Invalid listRuns cursor: pass back a nextCursor that listRuns returned",
    );
    await expect(listings.invalidState).rejects.toThrow('Invalid listRuns state: "sleeping" is not one of');
    await expect(listings.invalidLimit).rejects.toThrow("Invalid listRuns limit: 0 is not a positive integer");
    await expect(listings.unknownTask).rejects.toThrow(UnknownTaskError);
  },
);

engineTest(
  "listRuns is two round trips of bounded reads, never asking the index of a task the release does not define",
  async (context) => {
    const { redis, keys } = context;
    await endRunsOfEachTask(context, 3);
    await endRuns(context.engine, "removed-task", ["removed-task-run"], "complete");
    const reader = readerOf(context);
    const { nextCursor } = await reader.listRuns({ state: "completed", limit: 2 });

    const trafficByListing = {
      firstPage: await readTrafficOf(redis, () => reader.listRuns({ state: "completed", limit: 2 })),
      nextPage: await readTrafficOf(redis, () =>
        reader.listRuns({ state: "completed", limit: 2, cursor: nextCursor! }),
      ),
      oneTask: await readTrafficOf(redis, () => reader.listRuns({ state: "completed", task: "send-email" })),
      emptyState: await readTrafficOf(redis, () => reader.listRuns({ state: "failed" })),
    };

    expect(trafficByListing.firstPage.roundTripCount).toBe(2);
    expect(trafficByListing.nextPage.roundTripCount).toBe(2);
    expect(trafficByListing.oneTask.roundTripCount).toBe(2);
    expect(trafficByListing.emptyState.roundTripCount).toBe(1);
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
  async ({ redisUrl, keyPrefix }) => {
    await withRedis(redisUrl, async (redis) => {
      const keys = engineKeysOf(keyPrefix);
      const context: EngineTestContext = { engine: createEngine(redis, keyPrefix), redis, keys, keyPrefix };
      const keptRunIds = runIdsOf("kept-run", 3);
      await endRuns(context.engine, "send-email", [...keptRunIds, "newer-run"], "complete");
      await redis.hset(keys.runRecord("send-email", "newer-run"), "version", RECORD_VERSION + 1);
      await enqueueRuns(context, [runOf("pending-run", "send-email")]);
      const latestScore = (await serverInstantInMillisecondsOf(redis)) + 60_000;
      const completedIndexKey = keys.runIndex("send-email", "completed");
      const [ghostStoredRunId, pendingStoredRunId] = [
        storedRunIdOf("send-email", "ghost-run"),
        storedRunIdOf("send-email", "pending-run"),
      ];
      await redis.zadd(completedIndexKey, latestScore, ghostStoredRunId, latestScore, pendingStoredRunId);

      const pages = await walkedPagesOf(readerOf(context), { state: "completed" }, [2]);

      expect(pages[0]).toEqual([]);
      expect([...runIdsOfPages(pages)].sort()).toEqual(keptRunIds);
      expect(await invariantViolationsOf(redis, keyPrefix)).toEqual(
        expect.arrayContaining([
          `run "${ghostStoredRunId}" sits in ${completedIndexKey} but has no record with a known state`,
          `run "${pendingStoredRunId}" sits in ${completedIndexKey} though its record is pending`,
        ]),
      );
      expect(await invariantViolationsOf(redis, keyPrefix)).toHaveLength(2);
    });
  },
);
