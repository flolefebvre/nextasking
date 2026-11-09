import * as v from "valibot";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { runFailed } from "./built-in-events.js";
import { defineListener, defineScheduledTask, defineTask } from "./definitions.js";
import type { AnyTaskDefinition } from "./definitions.js";
import type { FetchRequest, FireRequest, RunToCreate, SettleRequest, TickRequest } from "./engine/engine.js";
import { syncDeclaredSchedules } from "./engine-schedules.js";
import { deriveFailureNoticeEventId, deriveFiringRunId, deriveListenerRunId } from "./ids.js";
import type { EpochMilliseconds } from "./durations.js";
import type { Schedule } from "./schedules.js";
import type { HandlerSender } from "./step-engine.js";
import { startWorkerLoop } from "./worker-loop.js";
import type { WaitForWakeUp, WorkerLoop, WorkerLoopEngine } from "./worker-loop.js";
import { DEFAULT_WORKER_TIMING } from "./worker-timing.js";
import type { WorkerTiming } from "./worker-timing.js";
import { createInMemoryEngine } from "../tests/in-memory-engine.js";
import type { HeldCalls, InMemoryEngine } from "../tests/in-memory-engine.js";
import { freshLockTokensOf } from "../tests/lock-tokens.js";
import { createRecordingLogger } from "../tests/recording-logger.js";
import {
  createSimulatedRedisOutage,
  engineAwayDuringOutage,
  OUT_OF_MEMORY_REPLY,
  UNANSWERED_COMMAND_ERROR,
} from "../tests/simulated-redis-outage.js";
import type { AwayEngineCalls, SimulatedRedisOutage } from "../tests/simulated-redis-outage.js";
import type { RecordedLogLine } from "../tests/recording-logger.js";
import { brokenKeyRunFailedListener, workerDefinitionsOf } from "../tests/worker-definitions.js";

const NO_SENDER: HandlerSender = {
  dispatch: () => Promise.reject(new Error("this scenario sends nothing")),
  emit: () => Promise.reject(new Error("this scenario sends nothing")),
};

const MINUTE_IN_MILLISECONDS = 60_000;

type HandlerGate = {
  enter: () => Promise<void>;
  release: (count: number) => void;
  inFlightCount: () => number;
  mostInFlightCount: () => number;
};

function createHandlerGate(): HandlerGate {
  const waitingReleases: (() => void)[] = [];
  let inFlightCount = 0;
  let mostInFlightCount = 0;
  return {
    enter: async () => {
      inFlightCount += 1;
      mostInFlightCount = Math.max(mostInFlightCount, inFlightCount);
      await new Promise<void>((resolve) => waitingReleases.push(resolve));
      inFlightCount -= 1;
    },
    release: (count) => {
      for (const release of waitingReleases.splice(0, count)) {
        release();
      }
    },
    inFlightCount: () => inFlightCount,
    mostInFlightCount: () => mostInFlightCount,
  };
}

type LoopScenario = {
  engine: InMemoryEngine;
  lines: RecordedLogLine[];
  loop: WorkerLoop;
  simulatedRedisOutage: SimulatedRedisOutage;
};

const startedLoops: WorkerLoop[] = [];

function startedLoopOf(
  tasks: readonly AnyTaskDefinition[],
  {
    concurrency = 50,
    engine = createInMemoryEngine(),
    loopEngine = engine,
    waitForWakeUp = engine.waitForWakeUp,
    timing = DEFAULT_WORKER_TIMING,
    recordingLogger = createRecordingLogger(),
    simulatedRedisOutage = createSimulatedRedisOutage(recordingLogger.logger),
  }: {
    concurrency?: number;
    engine?: InMemoryEngine;
    loopEngine?: WorkerLoopEngine;
    waitForWakeUp?: WaitForWakeUp;
    timing?: WorkerTiming;
    recordingLogger?: ReturnType<typeof createRecordingLogger>;
    simulatedRedisOutage?: SimulatedRedisOutage;
  } = {},
): LoopScenario {
  const { logger, lines } = recordingLogger;
  const loop = startWorkerLoop({
    engine: loopEngine,
    definitions: workerDefinitionsOf(tasks),
    timing,
    logger,
    concurrency,
    sender: NO_SENDER,
    waitForWakeUp,
    redisOutage: simulatedRedisOutage.redisOutage,
  });
  startedLoops.push(loop);
  return { engine, lines, loop, simulatedRedisOutage };
}

function runsOf(task: string, count: number, extraFields: Partial<RunToCreate> = {}): RunToCreate[] {
  return Array.from({ length: count }, (_, position) => ({
    runId: `${task}-${String(position + 1)}`,
    task,
    payload: "{}",
    retries: 3,
    ...extraFields,
  }));
}

function requestsOf<Request>(engine: InMemoryEngine, method: "fetch" | "settle" | "tick" | "fire"): Request[] {
  return engine.calls.filter((call) => call.method === method).map(({ request }) => request as Request);
}

function linesWithMessage(lines: readonly RecordedLogLine[], message: string): RecordedLogLine[] {
  return lines.filter((line) => line.message === message);
}

async function scenarioOfALockTakenOverWhileGated(
  sendEmail: AnyTaskDefinition,
  handlerGate: HandlerGate,
): Promise<LoopScenario> {
  const engine = createInMemoryEngine();
  await engine.enqueue({ runs: runsOf("send-email", 1) });
  const scenario = startedLoopOf([sendEmail], { engine, concurrency: 1 });
  await vi.advanceTimersByTimeAsync(0);
  engine.expireLock("send-email-1");
  await engine.promote();
  handlerGate.release(1);
  await vi.advanceTimersByTimeAsync(0);
  return scenario;
}

beforeEach(() => {
  vi.useFakeTimers({ now: Date.UTC(2030, 0, 1) });
});

afterEach(async () => {
  for (const loop of startedLoops.splice(0)) {
    await loop.stopFetching();
    await loop.stopTicking();
  }
  vi.useRealTimers();
});

test("with capacity 50 and 120 pending runs the loop fetches 50, then as many as it freed, never holding more than 50", async () => {
  const handlerGate = createHandlerGate();
  const sendEmail = defineTask("send-email", { schema: v.object({}), handler: handlerGate.enter });
  const engine = createInMemoryEngine();
  await engine.enqueue({ runs: runsOf("send-email", 120) });
  startedLoopOf([sendEmail] as AnyTaskDefinition[], { engine, concurrency: 50 });
  await vi.advanceTimersByTimeAsync(0);
  const firstFetchTokenCounts = requestsOf<FetchRequest>(engine, "fetch").map(({ lockTokens }) => lockTokens.length);

  handlerGate.release(7);
  await vi.advanceTimersByTimeAsync(0);
  const fetchesAfterRelease = requestsOf<FetchRequest>(engine, "fetch").slice(firstFetchTokenCounts.length);
  const inFlightAfterRelease = handlerGate.inFlightCount();
  for (let releasedCount = 0; releasedCount < 120; releasedCount += 10) {
    handlerGate.release(10);
    await vi.advanceTimersByTimeAsync(0);
  }

  expect(firstFetchTokenCounts).toEqual([50]);
  expect(fetchesAfterRelease.reduce((tokenCount, { lockTokens }) => tokenCount + lockTokens.length, 0)).toBe(7);
  expect(inFlightAfterRelease).toBe(50);
  expect(handlerGate.mostInFlightCount()).toBe(50);
  expect(engine.runIdsInState("completed")).toHaveLength(120);
});

test("a run enqueued while the loop waits is fetched on the wake-up, before the next tick", async () => {
  const sendEmail = defineTask("send-email", { schema: v.object({}), handler: async () => undefined });
  const { engine } = startedLoopOf([sendEmail] as AnyTaskDefinition[]);
  await vi.advanceTimersByTimeAsync(100);

  await engine.enqueue({ runs: runsOf("send-email", 1) });
  await vi.advanceTimersByTimeAsync(0);

  expect(requestsOf(engine, "tick")).toHaveLength(0);
  expect(engine.storedRunOf("send-email-1")?.state).toBe("completed");
});

test("a completed, retried, failed and suspended attempt each settle with their kind, the failing one with its notice", async () => {
  const alertOnCall = defineListener("alert-on-call", { on: runFailed, handler: async () => undefined });
  const tasks = [
    defineTask("completes", { schema: v.object({}), handler: async () => undefined }),
    defineTask("retries", {
      schema: v.object({}),
      handler: async () => {
        throw new Error("provider down");
      },
    }),
    defineTask("fails", {
      schema: v.object({}),
      retries: 0,
      handler: async () => {
        throw new Error("provider down");
      },
    }),
    defineTask("suspends", {
      schema: v.object({}),
      handler: async ({ step }) => {
        await step.sleep("wait", "1h");
      },
    }),
    alertOnCall,
  ] as AnyTaskDefinition[];
  const engine = createInMemoryEngine();
  await engine.enqueue({
    runs: [
      ...runsOf("completes", 1),
      ...runsOf("retries", 1),
      ...runsOf("fails", 1, { retries: 0 }),
      ...runsOf("suspends", 1),
    ],
  });

  startedLoopOf(tasks, { engine });
  await vi.advanceTimersByTimeAsync(0);

  const outcomesByRunId = Object.fromEntries(
    requestsOf<SettleRequest>(engine, "settle").map(({ runId, outcome }) => [runId, outcome]),
  );
  expect(["completes-1", "retries-1", "fails-1", "suspends-1"].map((runId) => outcomesByRunId[runId]?.kind)).toEqual([
    "complete",
    "retry",
    "fail",
    "suspend",
  ]);
  const failOutcome = outcomesByRunId["fails-1"];
  expect(failOutcome?.kind === "fail" ? failOutcome.failureNotice?.runs.map(({ runId }) => runId) : []).toEqual([
    deriveListenerRunId(deriveFailureNoticeEventId("fails", "fails-1", 0), "alert-on-call"),
  ]);
  expect(engine.storedRunOf("fails-1")?.state).toBe("failed");
  expect(engine.storedRunOf("retries-1")?.state).toBe("backingOff");
  expect(engine.storedRunOf("suspends-1")?.state).toBe("suspended");
});

test("a run of a listener of runFailed that fails carries an empty notice", async () => {
  const failingListener = defineListener("failing-listener", {
    on: runFailed,
    retries: 0,
    handler: async () => {
      throw new Error("pager down");
    },
  });
  const engine = createInMemoryEngine();
  await engine.enqueue({
    runs: runsOf("failing-listener", 1, {
      retries: 0,
      sourceEvent: { eventName: runFailed.name, eventId: "notice-1" },
    }),
  });

  startedLoopOf([failingListener] as AnyTaskDefinition[], { engine });
  await vi.advanceTimersByTimeAsync(0);

  const [settleRequest] = requestsOf<SettleRequest>(engine, "settle");
  expect(settleRequest?.outcome).toMatchObject({ kind: "fail", failureNotice: { runs: [] } });
});

test("a listener whose key throws costs its own notice run and writes one worker error, and the run still fails", async () => {
  const alertOnCall = defineListener("alert-on-call", { on: runFailed, handler: async () => undefined });
  const fails = defineTask("fails", {
    schema: v.object({}),
    retries: 0,
    handler: async () => {
      throw new Error("provider down");
    },
  });
  const engine = createInMemoryEngine();
  await engine.enqueue({ runs: runsOf("fails", 1, { retries: 0 }) });

  const { lines } = startedLoopOf([fails, brokenKeyRunFailedListener, alertOnCall] as AnyTaskDefinition[], { engine });
  await vi.advanceTimersByTimeAsync(0);

  const [settleRequest] = requestsOf<SettleRequest>(engine, "settle");
  const outcome = settleRequest?.outcome;
  expect(outcome?.kind === "fail" ? outcome.failureNotice?.runs.map(({ task }) => task) : []).toEqual([
    "alert-on-call",
  ]);
  expect(engine.storedRunOf("fails-1")?.state).toBe("failed");
  expect(linesWithMessage(lines, "worker error")).toEqual([
    { level: "error", message: "worker error", fields: { err: expect.any(Error) } },
  ]);
});

test("a settle answered stale writes attempt outcome discarded: lock lost with the kind as outcome", async () => {
  const handlerGate = createHandlerGate();
  const sendEmail = defineTask("send-email", { schema: v.object({}), handler: handlerGate.enter });

  const { lines } = await scenarioOfALockTakenOverWhileGated(sendEmail as AnyTaskDefinition, handlerGate);

  expect(linesWithMessage(lines, "attempt outcome discarded: lock lost")).toEqual([
    {
      level: "warn",
      message: "attempt outcome discarded: lock lost",
      fields: { task: "send-email", runId: "send-email-1", attempt: 1, outcome: "complete" },
    },
  ]);
  expect(linesWithMessage(lines, "run completed")).toEqual([]);
});

test("a settle answered already writes the normal completion line", async () => {
  const sendEmail = defineTask("send-email", { schema: v.object({}), handler: async () => undefined });
  const engine = createInMemoryEngine();
  await engine.enqueue({ runs: runsOf("send-email", 1) });
  const settleWhoseFirstReplyIsLost: WorkerLoopEngine["settle"] = async (request) => {
    await engine.settle(request);
    return engine.settle(request);
  };

  const { lines } = startedLoopOf([sendEmail] as AnyTaskDefinition[], {
    engine,
    loopEngine: { ...engine, settle: settleWhoseFirstReplyIsLost },
  });
  await vi.advanceTimersByTimeAsync(0);

  expect(linesWithMessage(lines, "run completed")).toEqual([
    {
      level: "info",
      message: "run completed",
      fields: {
        task: "send-email",
        runId: "send-email-1",
        attempt: 1,
        runDurationInMilliseconds: 0,
        executionDurationInMilliseconds: 0,
      },
    },
  ]);
  expect(linesWithMessage(lines, "attempt outcome discarded: lock lost")).toEqual([]);
});

function abortableTaskOf(name: string, abortedSignals: AbortSignal[] = []): AnyTaskDefinition {
  return defineTask(name, {
    schema: v.object({}),
    handler: ({ abortSignal }) =>
      new Promise<void>((_resolve, reject) => {
        abortSignal.addEventListener("abort", () => {
          abortedSignals.push(abortSignal);
          reject(abortSignal.reason as Error);
        });
      }),
  }) as AnyTaskDefinition;
}

const LOCK_LOST_ABORT_LINE = {
  level: "debug",
  message: "attempt aborted: lock lost",
  fields: { task: "send-email", runId: "send-email-1", attempt: 1 },
};

test("an attempt whose renewals never answer is aborted with lock lost at 2 minutes by the worker's clock, and sends no settle", async () => {
  const abortedSignals: AbortSignal[] = [];
  const engine = createInMemoryEngine();
  await engine.enqueue({ runs: runsOf("send-email", 1) });
  const heldTicks = engine.holdCalls("tick");
  const { lines } = startedLoopOf([abortableTaskOf("send-email", abortedSignals)], { engine });

  await vi.advanceTimersByTimeAsync(2 * MINUTE_IN_MILLISECONDS - 1);
  const linesBeforeTheLockExpires = linesWithMessage(lines, "attempt aborted: lock lost");
  await vi.advanceTimersByTimeAsync(1);
  heldTicks.release();
  await vi.advanceTimersByTimeAsync(0);

  expect(linesBeforeTheLockExpires).toEqual([]);
  expect(linesWithMessage(lines, "attempt aborted: lock lost")).toEqual([LOCK_LOST_ABORT_LINE]);
  expect(abortedSignals.map(({ reason }) => (reason as Error).name)).toEqual(["AttemptAbortedError"]);
  expect(requestsOf(engine, "settle")).toEqual([]);
});

test("an attempt whose renewal replies stale is aborted with lock lost and sends no settle", async () => {
  const engine = createInMemoryEngine();
  await engine.enqueue({ runs: runsOf("send-email", 1) });
  const tickRenewingStale: WorkerLoopEngine["tick"] = async (request) => ({
    ...(await engine.tick(request)),
    renewals: { kind: "replied", reply: request.heldLocks.map(() => ({ kind: "stale" })) },
  });
  const { lines } = startedLoopOf([abortableTaskOf("send-email")], {
    engine,
    loopEngine: { ...engine, tick: tickRenewingStale },
  });

  await vi.advanceTimersByTimeAsync(MINUTE_IN_MILLISECONDS + 1_000);

  expect(linesWithMessage(lines, "attempt aborted: lock lost")).toEqual([LOCK_LOST_ABORT_LINE]);
  expect(requestsOf(engine, "settle")).toEqual([]);
});

test("a step refused for a lost lock aborts the attempt with lock lost, and it sends no settle", async () => {
  const handlerGate = createHandlerGate();
  const sendEmail = defineTask("send-email", {
    schema: v.object({}),
    handler: async ({ step }) => {
      await handlerGate.enter();
      await step.run("charge", () => "paid");
    },
  }) as AnyTaskDefinition;

  const { engine, lines } = await scenarioOfALockTakenOverWhileGated(sendEmail, handlerGate);

  expect(linesWithMessage(lines, "attempt aborted: lock lost")).toEqual([LOCK_LOST_ABORT_LINE]);
  expect(requestsOf(engine, "settle")).toEqual([]);
});

test("a settle answered unknownVersion writes one worker error and no lost-lock line", async () => {
  const sendEmail = defineTask("send-email", { schema: v.object({}), handler: async () => undefined });
  const engine = createInMemoryEngine();
  await engine.enqueue({ runs: runsOf("send-email", 1) });

  const { lines } = startedLoopOf([sendEmail] as AnyTaskDefinition[], {
    engine,
    loopEngine: { ...engine, settle: async () => ({ kind: "unknownVersion", recordVersion: 2 }) },
  });
  await vi.advanceTimersByTimeAsync(0);

  expect(lines).toContainEqual({
    level: "error",
    message: "worker error",
    fields: { err: expect.objectContaining({ message: expect.stringContaining("version 2") }) },
  });
  expect(linesWithMessage(lines, "worker error")).toHaveLength(1);
  expect(lines.filter(({ message }) => message.includes("lock lost"))).toEqual([]);
});

test("every tick is one round trip to the engine, and a tick still running when the next is due is skipped", async () => {
  const { engine } = startedLoopOf([]);
  await vi.advanceTimersByTimeAsync(3_000);
  const tickCountBeforeHold = requestsOf(engine, "tick").length;
  const heldTicks = engine.holdCalls("tick");

  await vi.advanceTimersByTimeAsync(5_000);
  const tickCountWhileHeld = requestsOf(engine, "tick").length;
  heldTicks.release();
  await vi.advanceTimersByTimeAsync(1_000);

  expect(tickCountBeforeHold).toBe(3);
  expect(engine.calls.filter(({ method }) => method === "promote" || method === "renew")).toEqual([]);
  expect(tickCountWhileHeld).toBe(4);
  expect(requestsOf(engine, "tick")).toHaveLength(5);
});

test("a tick renews only the locks with less than a minute left", async () => {
  const handlerGate = createHandlerGate();
  const sendEmail = defineTask("send-email", { schema: v.object({}), handler: handlerGate.enter });
  const engine = createInMemoryEngine();
  await engine.enqueue({ runs: runsOf("send-email", 1, { runId: "early-run" }) });
  startedLoopOf([sendEmail] as AnyTaskDefinition[], { engine });
  await vi.advanceTimersByTimeAsync(30_000);
  await engine.enqueue({ runs: runsOf("send-email", 1, { runId: "late-run" }) });
  await vi.advanceTimersByTimeAsync(30_000);
  const ticksBeforeTheMinute = requestsOf<TickRequest>(engine, "tick");

  await vi.advanceTimersByTimeAsync(2_000);

  const ticksAroundTheMinute = requestsOf<TickRequest>(engine, "tick").slice(ticksBeforeTheMinute.length);
  expect(ticksBeforeTheMinute.flatMap(({ heldLocks }) => heldLocks)).toEqual([]);
  expect(ticksAroundTheMinute.map(({ heldLocks }) => heldLocks.map(({ runId }) => runId))).toEqual([["early-run"], []]);
  expect(engine.storedRunOf("early-run")?.lockedUntil).toBe(Date.UTC(2030, 0, 1) + 61_000 + 2 * MINUTE_IN_MILLISECONDS);
});

test("a tick prunes one task per tick, round robin over the task names", async () => {
  const engine = createInMemoryEngine();
  await engine.enqueue({ runs: [...runsOf("resize-image", 1), ...runsOf("send-email", 1)] });
  startedLoopOf([], { engine });

  await vi.advanceTimersByTimeAsync(4_000);

  expect(requestsOf<TickRequest>(engine, "tick").map(({ prune }) => prune?.task)).toEqual([
    undefined,
    "resize-image",
    "send-email",
    "resize-image",
  ]);
  expect(requestsOf<TickRequest>(engine, "tick")[1]?.prune).toEqual({
    task: "resize-image",
    completedRetention: DEFAULT_WORKER_TIMING.completedRunRetention,
    failedRetention: DEFAULT_WORKER_TIMING.failedRunRetention,
  });
});

test("a tick whose prune fails still applies its renewals, writes one worker error, and the next tick prunes the next task", async () => {
  const handlerGate = createHandlerGate();
  const sendEmail = defineTask("send-email", { schema: v.object({}), handler: handlerGate.enter });
  const engine = createInMemoryEngine();
  await engine.enqueue({
    runs: [
      ...runsOf("send-email", 1),
      ...runsOf("resize-image", 1, { delayInMilliseconds: 60 * MINUTE_IN_MILLISECONDS }),
    ],
  });
  const tickWithAFailedPruneWhenItRenews: WorkerLoopEngine["tick"] = async (request) => {
    const reply = await engine.tick(request);
    return request.heldLocks.length > 0 && request.prune !== undefined
      ? { ...reply, prune: { kind: "failed", error: new Error("WRONGTYPE Operation against a key") } }
      : reply;
  };
  const { lines } = startedLoopOf([sendEmail] as AnyTaskDefinition[], {
    engine,
    loopEngine: { ...engine, tick: tickWithAFailedPruneWhenItRenews },
  });

  await vi.advanceTimersByTimeAsync(63_000);

  const ticks = requestsOf<TickRequest>(engine, "tick");
  const renewingTickPosition = ticks.findIndex(({ heldLocks }) => heldLocks.length > 0);
  const [renewingTick, nextTick] = ticks.slice(renewingTickPosition);
  expect(ticks.filter(({ heldLocks }) => heldLocks.length > 0)).toHaveLength(1);
  expect(nextTick?.prune?.task).not.toBe(renewingTick?.prune?.task);
  expect(linesWithMessage(lines, "worker error")).toEqual([
    {
      level: "error",
      message: "worker error",
      fields: { err: expect.objectContaining({ message: expect.stringContaining("WRONGTYPE") }) },
    },
  ]);
  expect(engine.storedRunOf("send-email-1")?.state).toBe("executing");
});

const SIGNED_UP = { eventName: "user.signed-up", eventId: "event-1" };

async function expireTheLockOfAFetchedRun(engine: InMemoryEngine, stallsBefore: number): Promise<void> {
  for (let stallCount = 0; stallCount <= stallsBefore; stallCount += 1) {
    if (stallCount > 0) {
      await engine.promote();
    }
    await engine.fetch({
      lockTokens: freshLockTokensOf(1),
      concurrencyLimitsByTask: {},
      lockDurationInMilliseconds: DEFAULT_WORKER_TIMING.lockDurationInMilliseconds,
    });
    engine.expireLock("send-email-1");
  }
}

test("an expired lock found by the tick writes attempt stalled with its stalls and its run's source event", async () => {
  const handlerGate = createHandlerGate();
  const sendEmail = defineTask("send-email", { schema: v.object({}), handler: handlerGate.enter });
  const engine = createInMemoryEngine();
  await engine.enqueue({ runs: runsOf("send-email", 1, { sourceEvent: SIGNED_UP }) });
  await expireTheLockOfAFetchedRun(engine, 0);

  const { lines } = startedLoopOf([sendEmail] as AnyTaskDefinition[], { engine });
  await vi.advanceTimersByTimeAsync(1_000);

  expect(linesWithMessage(lines, "attempt stalled")).toEqual([
    {
      level: "warn",
      message: "attempt stalled",
      fields: { task: "send-email", runId: "send-email-1", attempt: 1, ...SIGNED_UP, stalls: 1 },
    },
  ]);
});

test("two loops ticking on the same reported third stall end with one failed run, one notice and one run failed line", async () => {
  const alertOnCall = defineListener("alert-on-call", { on: runFailed, handler: async () => undefined });
  const sendEmail = defineTask("send-email", { schema: v.object({}), handler: async () => undefined });
  const tasks = [sendEmail, alertOnCall, brokenKeyRunFailedListener] as AnyTaskDefinition[];
  const engine = createInMemoryEngine();
  await engine.enqueue({ runs: runsOf("send-email", 1, { sourceEvent: SIGNED_UP }) });
  await expireTheLockOfAFetchedRun(engine, 2);
  const heldSettles = engine.holdCalls("settle");
  const firstLoop = startedLoopOf(tasks, { engine });
  const secondLoop = startedLoopOf(tasks, { engine });
  await vi.advanceTimersByTimeAsync(1_000);

  heldSettles.release();
  await vi.advanceTimersByTimeAsync(0);

  const noticeRunId = deriveListenerRunId(deriveFailureNoticeEventId("send-email", "send-email-1", 0), "alert-on-call");
  const lines = [...firstLoop.lines, ...secondLoop.lines];
  expect(requestsOf<SettleRequest>(engine, "settle").filter(({ runId }) => runId === "send-email-1")).toHaveLength(2);
  expect(engine.storedRunOf("send-email-1")).toMatchObject({
    state: "failed",
    reason: "stalled",
    errors: [expect.any(String)],
  });
  expect(engine.storedRunOf(noticeRunId)?.state).toBe("completed");
  expect(linesWithMessage(lines, "run failed")).toEqual([
    {
      level: "error",
      message: "run failed",
      fields: {
        task: "send-email",
        runId: "send-email-1",
        attempt: 1,
        ...SIGNED_UP,
        err: expect.objectContaining({ name: "StalledRunError" }),
        reason: "stalled",
        runDurationInMilliseconds: expect.any(Number),
        executionDurationInMilliseconds: expect.any(Number),
      },
    },
  ]);
  expect(linesWithMessage(lines, "worker error")).toEqual([
    {
      level: "error",
      message: "worker error",
      fields: {
        err: expect.objectContaining({ cause: expect.objectContaining({ message: "no tenant in this payload" }) }),
      },
    },
  ]);
});

test("a run of an unknown task is held 10 seconds against the concurrency, then handed back to the front of pending with its order", async () => {
  const sendEmail = defineTask("send-email", { schema: v.object({}), handler: async () => undefined });
  const engine = createInMemoryEngine();
  await engine.enqueue({ runs: [...runsOf("resize-image", 1), ...runsOf("send-email", 1)] });
  const orderBefore = engine.storedRunOf("resize-image-1")?.order;
  const { lines } = startedLoopOf([sendEmail] as AnyTaskDefinition[], { engine, concurrency: 1 });

  await vi.advanceTimersByTimeAsync(10_000 - 1);
  const settlesDuringTheHold = requestsOf<SettleRequest>(engine, "settle");
  const sendEmailStateDuringTheHold = engine.storedRunOf("send-email-1")?.state;
  await vi.advanceTimersByTimeAsync(1);

  expect(settlesDuringTheHold).toEqual([]);
  expect(sendEmailStateDuringTheHold).toBe("pending");
  expect(requestsOf<SettleRequest>(engine, "settle").map(({ runId, outcome }) => [runId, outcome.kind])).toEqual([
    ["resize-image-1", "handBack"],
  ]);
  expect(engine.storedRunOf("resize-image-1")).toMatchObject({ state: "executing", order: orderBefore, attempt: 1 });
  expect(engine.storedRunOf("send-email-1")?.state).toBe("pending");
  expect(lines).toEqual([
    {
      level: "debug",
      message: "run handed back: unknown task",
      fields: { task: "resize-image", runId: "resize-image-1", attempt: 1 },
    },
  ]);
});

test("a run of an unknown task first found 30 minutes ago fails unknown task with its notice", async () => {
  const alertOnCall = defineListener("alert-on-call", { on: runFailed, handler: async () => undefined });
  const engine = createInMemoryEngine();
  await engine.enqueue({ runs: runsOf("resize-image", 1) });
  const { lines } = startedLoopOf([alertOnCall] as AnyTaskDefinition[], { engine });

  await vi.advanceTimersByTimeAsync(30 * MINUTE_IN_MILLISECONDS + 10_000);

  const noticeRunId = deriveListenerRunId(
    deriveFailureNoticeEventId("resize-image", "resize-image-1", 0),
    "alert-on-call",
  );
  expect(engine.storedRunOf("resize-image-1")).toMatchObject({ state: "failed", reason: "unknown task" });
  expect(engine.storedRunOf(noticeRunId)?.state).toBe("completed");
  expect(linesWithMessage(lines, "run failed")).toEqual([
    {
      level: "error",
      message: "run failed",
      fields: {
        task: "resize-image",
        runId: "resize-image-1",
        attempt: 1,
        err: expect.objectContaining({ name: "UnknownTaskRunError" }),
        reason: "unknown task",
        runDurationInMilliseconds: 30 * MINUTE_IN_MILLISECONDS + 10_000,
        executionDurationInMilliseconds: 10_000,
      },
    },
  ]);
  expect(linesWithMessage(lines, "attempt started").filter(({ fields }) => fields.task === "resize-image")).toEqual([]);
});

test("an unknown task's hold stopped by the local stop sends no hand back", async () => {
  const engine = createInMemoryEngine();
  await engine.enqueue({ runs: runsOf("resize-image", 1) });
  const heldTicks = engine.holdCalls("tick");
  const { lines } = startedLoopOf([], {
    engine,
    timing: { ...DEFAULT_WORKER_TIMING, unknownNameHandBackDelayInMilliseconds: 3 * MINUTE_IN_MILLISECONDS },
  });

  await vi.advanceTimersByTimeAsync(3 * MINUTE_IN_MILLISECONDS);
  heldTicks.release();
  await vi.advanceTimersByTimeAsync(0);

  expect(linesWithMessage(lines, "attempt aborted: lock lost")).toEqual([
    {
      level: "debug",
      message: "attempt aborted: lock lost",
      fields: { task: "resize-image", runId: "resize-image-1", attempt: 1 },
    },
  ]);
  expect(requestsOf(engine, "settle")).toEqual([]);
});

const PASS_OVER_LINE = {
  level: "debug",
  message: "runs passed over: newer record version",
  fields: { recordVersion: 2, supportedVersion: 1 },
};

test("a fetch answered unknownVersion writes one pass-over line and no fetch happens for 10 seconds", async () => {
  const sendEmail = defineTask("send-email", { schema: v.object({}), handler: async () => undefined });
  const engine = createInMemoryEngine();
  await engine.enqueue({ runs: runsOf("send-email", 1) });
  let fetchCount = 0;
  const fetchAnsweredUnknownVersionFirst: WorkerLoopEngine["fetch"] = async (request) => {
    fetchCount += 1;
    return fetchCount === 1 ? { kind: "unknownVersion", recordVersion: 2 } : engine.fetch(request);
  };
  const { lines } = startedLoopOf([sendEmail] as AnyTaskDefinition[], {
    engine,
    loopEngine: { ...engine, fetch: fetchAnsweredUnknownVersionFirst },
  });

  await vi.advanceTimersByTimeAsync(10_000 - 1);
  const fetchCountDuringTheWait = fetchCount;
  await vi.advanceTimersByTimeAsync(1);

  expect(fetchCountDuringTheWait).toBe(1);
  expect(engine.storedRunOf("send-email-1")?.state).toBe("completed");
  expect(linesWithMessage(lines, "runs passed over: newer record version")).toEqual([PASS_OVER_LINE]);
});

test("a promote answered unknownVersion leaves the promote out of the ticks for 10 seconds, which still renew, and the line is written once per version", async () => {
  const engine = createInMemoryEngine();
  const tickWithAPromoteAnsweredUnknownVersion: WorkerLoopEngine["tick"] = async (request) => {
    const reply = await engine.tick(request);
    return reply.promote === undefined
      ? reply
      : { ...reply, promote: { kind: "replied", reply: { kind: "unknownVersion", recordVersion: 2 } } };
  };
  const { lines } = startedLoopOf([], {
    engine,
    loopEngine: { ...engine, tick: tickWithAPromoteAnsweredUnknownVersion },
  });

  await vi.advanceTimersByTimeAsync(25_000);

  const ticks = requestsOf<TickRequest>(engine, "tick");
  expect(ticks).toHaveLength(25);
  expect(
    ticks.flatMap(({ isPromoteIncluded }, position) => (isPromoteIncluded === true ? [position + 1] : [])),
  ).toEqual([1, 11, 21]);
  expect(linesWithMessage(lines, "runs passed over: newer record version")).toEqual([PASS_OVER_LINE]);
});

test("a fetched reply that passed over records writes the pass-over line once per version and does not wait", async () => {
  const sendEmail = defineTask("send-email", { schema: v.object({}), handler: async () => undefined });
  const engine = createInMemoryEngine();
  const fetchPassingOverVersion2: WorkerLoopEngine["fetch"] = async (request) => {
    const reply = await engine.fetch(request);
    return reply.kind === "fetched" ? { ...reply, passedOverRecordVersions: [2, 2] } : reply;
  };
  const { lines } = startedLoopOf([sendEmail] as AnyTaskDefinition[], {
    engine,
    loopEngine: { ...engine, fetch: fetchPassingOverVersion2 },
  });
  await vi.advanceTimersByTimeAsync(1_000);

  await engine.enqueue({ runs: runsOf("send-email", 1) });
  await vi.advanceTimersByTimeAsync(0);

  expect(engine.storedRunOf("send-email-1")?.state).toBe("completed");
  expect(linesWithMessage(lines, "runs passed over: newer record version")).toEqual([PASS_OVER_LINE]);
});

test("a pickup after a suspend writes run resumed, and a pickup after a stall writes attempt started with the same attempt", async () => {
  const handlerGate = createHandlerGate();
  const tasks = [
    defineTask("sleeps", {
      schema: v.object({}),
      handler: async ({ step }) => {
        await step.sleep("wait", "1m");
      },
    }),
    defineTask("stalls", { schema: v.object({}), handler: handlerGate.enter }),
  ] as AnyTaskDefinition[];
  const engine = createInMemoryEngine();
  await engine.enqueue({ runs: [...runsOf("sleeps", 1), ...runsOf("stalls", 1)] });
  const { lines } = startedLoopOf(tasks, { engine });
  await vi.advanceTimersByTimeAsync(0);
  engine.expireLock("stalls-1");

  await vi.advanceTimersByTimeAsync(MINUTE_IN_MILLISECONDS + 1_000);

  const pickupLines = lines
    .filter(({ message }) => message === "attempt started" || message === "run resumed")
    .map(({ message, fields }) => [fields.runId, message, fields.attempt]);
  expect(pickupLines).toEqual([
    ["sleeps-1", "attempt started", 1],
    ["stalls-1", "attempt started", 1],
    ["stalls-1", "attempt started", 1],
    ["sleeps-1", "run resumed", 1],
  ]);
});

test("a parked run writes run waiting for a concurrency slot", async () => {
  const handlerGate = createHandlerGate();
  const sendEmail = defineTask("send-email", {
    schema: v.object({}),
    concurrency: { limit: 1 },
    handler: handlerGate.enter,
  });
  const engine = createInMemoryEngine();
  await engine.enqueue({ runs: runsOf("send-email", 2) });

  const { lines } = startedLoopOf([sendEmail] as AnyTaskDefinition[], { engine });
  await vi.advanceTimersByTimeAsync(0);

  expect(linesWithMessage(lines, "run waiting for a concurrency slot")).toEqual([
    {
      level: "debug",
      message: "run waiting for a concurrency slot",
      fields: { task: "send-email", runId: "send-email-2", attempt: 1 },
    },
  ]);
});

test("the loop survives a rejected fetch and keeps fetching", async () => {
  const sendEmail = defineTask("send-email", { schema: v.object({}), handler: async () => undefined });
  const engine = createInMemoryEngine();
  let rejectedFetchCount = 0;
  const fetchRejectedOnce: WorkerLoopEngine["fetch"] = async (request) => {
    if (rejectedFetchCount === 0) {
      rejectedFetchCount += 1;
      throw new Error("connection reset");
    }
    return engine.fetch(request);
  };
  await engine.enqueue({ runs: runsOf("send-email", 1) });

  startedLoopOf([sendEmail] as AnyTaskDefinition[], { engine, loopEngine: { ...engine, fetch: fetchRejectedOnce } });
  await vi.advanceTimersByTimeAsync(1_000);

  expect(rejectedFetchCount).toBe(1);
  expect(engine.storedRunOf("send-email-1")?.state).toBe("completed");
});

async function unhandledRejectionsDuring(body: () => Promise<void>): Promise<unknown[]> {
  const unhandledRejections: unknown[] = [];
  const recordUnhandledRejection = (reason: unknown) => {
    unhandledRejections.push(reason);
  };
  process.on("unhandledRejection", recordUnhandledRejection);
  try {
    await body();
    await vi.advanceTimersByTimeAsync(0);
  } finally {
    process.off("unhandledRejection", recordUnhandledRejection);
  }
  return unhandledRejections;
}

test("the loop survives a rejected wake-up wait without spinning, and a run enqueued afterwards still completes", async () => {
  const unhandledRejections = await unhandledRejectionsDuring(async () => {
    const sendEmail = defineTask("send-email", { schema: v.object({}), handler: async () => undefined });
    const engine = createInMemoryEngine();
    let waitCount = 0;
    const waitRejectedFirst: WaitForWakeUp = async (timeoutInMilliseconds) => {
      waitCount += 1;
      if (waitCount === 1) {
        throw new Error("LOADING Redis is loading the dataset in memory");
      }
      return engine.waitForWakeUp(timeoutInMilliseconds);
    };
    startedLoopOf([sendEmail] as AnyTaskDefinition[], { engine, waitForWakeUp: waitRejectedFirst });
    await vi.advanceTimersByTimeAsync(100);

    await engine.enqueue({ runs: runsOf("send-email", 1) });
    await vi.advanceTimersByTimeAsync(2_000);

    expect(engine.storedRunOf("send-email-1")?.state).toBe("completed");
    expect(waitCount).toBeLessThan(10);
  });

  expect(unhandledRejections).toEqual([]);
});

test("a wake-up landing while a fetch is in flight is followed by another fetch, with no tick in between", async () => {
  const handlerGate = createHandlerGate();
  const sendEmail = defineTask("send-email", { schema: v.object({}), handler: handlerGate.enter });
  const engine = createInMemoryEngine();
  let isNextFetchReplyHeld = false;
  let releaseFetchReply = () => undefined as void;
  const fetchWithAReplyHeldOnce: WorkerLoopEngine["fetch"] = async (request) => {
    const reply = await engine.fetch(request);
    if (isNextFetchReplyHeld) {
      isNextFetchReplyHeld = false;
      await new Promise<void>((resolve) => {
        releaseFetchReply = resolve;
      });
    }
    return reply;
  };
  await engine.enqueue({ runs: runsOf("send-email", 1, { runId: "first-run" }) });
  startedLoopOf([sendEmail] as AnyTaskDefinition[], {
    engine,
    concurrency: 2,
    loopEngine: { ...engine, fetch: fetchWithAReplyHeldOnce },
  });
  await vi.advanceTimersByTimeAsync(0);
  isNextFetchReplyHeld = true;
  handlerGate.release(1);
  await vi.advanceTimersByTimeAsync(0);
  await engine.enqueue({ runs: runsOf("send-email", 1, { runId: "second-run" }) });
  await vi.advanceTimersByTimeAsync(0);

  releaseFetchReply();
  await vi.advanceTimersByTimeAsync(0);

  expect(requestsOf(engine, "tick")).toEqual([]);
  expect(engine.storedRunOf("second-run")?.state).toBe("executing");
});

function messagesOf(lines: readonly RecordedLogLine[]): string[] {
  return lines.map(({ message }) => message);
}

type OutageScenario = LoopScenario & { handlerGate: HandlerGate };

async function scenarioOfAnOutageWhileAStepRuns(awayEngineCalls: AwayEngineCalls): Promise<OutageScenario> {
  const handlerGate = createHandlerGate();
  const sendEmail = defineTask("send-email", {
    schema: v.object({}),
    handler: async ({ step }) => {
      await step.run("look-up", () => "found");
      await handlerGate.enter();
      await step.run("charge", () => "paid");
    },
  });
  const engine = createInMemoryEngine();
  await engine.enqueue({ runs: runsOf("send-email", 1) });
  const recordingLogger = createRecordingLogger();
  const simulatedRedisOutage = createSimulatedRedisOutage(recordingLogger.logger);
  const scenario = startedLoopOf([sendEmail] as AnyTaskDefinition[], {
    engine,
    loopEngine: engineAwayDuringOutage(engine, simulatedRedisOutage, awayEngineCalls),
    recordingLogger,
    simulatedRedisOutage,
  });
  await vi.advanceTimersByTimeAsync(0);
  simulatedRedisOutage.startOutage();
  handlerGate.release(1);
  return { ...scenario, handlerGate };
}

test("during an outage no attempt fails, nothing is handed back, and no line but the connection pair is written", async () => {
  const { engine, lines, simulatedRedisOutage } = await scenarioOfAnOutageWhileAStepRuns("rejected");
  await vi.advanceTimersByTimeAsync(30_000);
  const linesDuringTheOutage = [...lines];

  simulatedRedisOutage.endOutage();
  await vi.advanceTimersByTimeAsync(1_000);

  expect(messagesOf(linesDuringTheOutage)).toEqual(["attempt started", "connection error"]);
  expect(messagesOf(lines)).toEqual(["attempt started", "connection error", "connection restored", "run completed"]);
  expect(requestsOf<SettleRequest>(engine, "settle").map(({ outcome }) => outcome.kind)).toEqual(["complete"]);
});

test("a handler step waiting on an outage is aborted by the local stop, sends no settle, and leaves no rejection unhandled", async () => {
  let scenario: OutageScenario | undefined;
  let linesBeforeTheOutageEnds: string[] = [];
  const unhandledRejections = await unhandledRejectionsDuring(async () => {
    scenario = await scenarioOfAnOutageWhileAStepRuns("pending until the outage ends");
    await vi.advanceTimersByTimeAsync(2 * MINUTE_IN_MILLISECONDS);
    linesBeforeTheOutageEnds = messagesOf(scenario.lines);
    scenario.simulatedRedisOutage.endOutage();
    await vi.advanceTimersByTimeAsync(0);
  });

  expect(linesBeforeTheOutageEnds).toEqual(["attempt started", "connection error", "attempt aborted: lock lost"]);
  expect(requestsOf(scenario!.engine, "settle")).toEqual([]);
  expect(unhandledRejections).toEqual([]);
});

test("an out-of-memory reply lasting several ticks, with answered reads in between, writes one connection pair", async () => {
  const engine = createInMemoryEngine();
  let isOutOfMemory = true;
  const outOfMemory = { kind: "failed", error: OUT_OF_MEMORY_REPLY } as const;
  const tickRefusedWhileOutOfMemory: WorkerLoopEngine["tick"] = async (request) =>
    isOutOfMemory
      ? {
          promote: outOfMemory,
          renewals: outOfMemory,
          taskNames: { kind: "replied", reply: [] },
          prune: undefined,
          dueSchedules: undefined,
        }
      : engine.tick(request);
  const fetchRefusedWhileOutOfMemory: WorkerLoopEngine["fetch"] = async (request) =>
    isOutOfMemory ? Promise.reject(OUT_OF_MEMORY_REPLY) : engine.fetch(request);
  const { lines } = startedLoopOf([], {
    engine,
    loopEngine: { ...engine, tick: tickRefusedWhileOutOfMemory, fetch: fetchRefusedWhileOutOfMemory },
  });
  await vi.advanceTimersByTimeAsync(5_000);

  isOutOfMemory = false;
  await vi.advanceTimersByTimeAsync(3_000);

  expect(messagesOf(lines)).toEqual(["connection error", "connection restored"]);
  expect(lines[0]?.fields).toEqual({ err: OUT_OF_MEMORY_REPLY });
});

type StoppedLoopParts = { stoppedParts: () => string[] };

function stoppedPartsOf(loop: WorkerLoop): StoppedLoopParts {
  const stoppedParts: string[] = [];
  void loop.stopFetching().then(() => stoppedParts.push("fetching"));
  void loop.stopTicking().then(() => stoppedParts.push("ticking"));
  return { stoppedParts: () => [...stoppedParts].sort() };
}

async function scenarioOfALoopStartedDuringAnOutage(awayEngineCalls: AwayEngineCalls): Promise<LoopScenario> {
  const engine = createInMemoryEngine();
  const recordingLogger = createRecordingLogger();
  const simulatedRedisOutage = createSimulatedRedisOutage(recordingLogger.logger);
  simulatedRedisOutage.startOutage();
  const scenario = startedLoopOf([], {
    engine,
    loopEngine: engineAwayDuringOutage(engine, simulatedRedisOutage, awayEngineCalls),
    recordingLogger,
    simulatedRedisOutage,
  });
  await vi.advanceTimersByTimeAsync(3_000);
  return scenario;
}

test("stopFetching and stopTicking end their wait for Redis at the stop when their call was rejected as away", async () => {
  const { loop, lines, engine, simulatedRedisOutage } = await scenarioOfALoopStartedDuringAnOutage("rejected");

  const { stoppedParts } = stoppedPartsOf(loop);
  await vi.advanceTimersByTimeAsync(0);
  const stoppedPartsAtTheStop = stoppedParts();
  simulatedRedisOutage.endOutage();
  await vi.advanceTimersByTimeAsync(5_000);

  expect(stoppedPartsAtTheStop).toEqual(["fetching", "ticking"]);
  expect(engine.calls).toEqual([]);
  expect(messagesOf(lines)).toEqual(["connection error", "connection restored"]);
});

test("stopFetching and stopTicking wait for a call the client holds unanswered, and end without a resend or a line once it is rejected", async () => {
  const { loop, lines, engine, simulatedRedisOutage } = await scenarioOfALoopStartedDuringAnOutage(
    "pending until the outage ends",
  );

  const { stoppedParts } = stoppedPartsOf(loop);
  await vi.advanceTimersByTimeAsync(0);
  const stoppedPartsBeforeTheRejection = stoppedParts();
  simulatedRedisOutage.rejectUnansweredCalls();
  await vi.advanceTimersByTimeAsync(0);
  const stoppedPartsAfterTheRejection = stoppedParts();
  simulatedRedisOutage.endOutage();
  await vi.advanceTimersByTimeAsync(5_000);

  expect(stoppedPartsBeforeTheRejection).toEqual([]);
  expect(stoppedPartsAfterTheRejection).toEqual(["fetching", "ticking"]);
  expect(engine.calls).toEqual([]);
  expect(messagesOf(lines)).toEqual(["connection error", "connection restored"]);
});

test("stopFetching with a fetch in flight resolves once the fetch answers, and the batch's attempt runs", async () => {
  const sendEmail = defineTask("send-email", { schema: v.object({}), handler: async () => undefined });
  const engine = createInMemoryEngine();
  await engine.enqueue({ runs: runsOf("send-email", 1) });
  const heldFetches = engine.holdCalls("fetch");
  const { loop } = startedLoopOf([sendEmail] as AnyTaskDefinition[], { engine });
  await vi.advanceTimersByTimeAsync(0);
  let isStopped = false;

  void loop.stopFetching().then(() => {
    isStopped = true;
  });
  await vi.advanceTimersByTimeAsync(0);
  const isStoppedBeforeTheAnswer = isStopped;
  heldFetches.release();
  await vi.advanceTimersByTimeAsync(0);
  await loop.waitForAttemptsInFlight();

  expect(isStoppedBeforeTheAnswer).toBe(false);
  expect(isStopped).toBe(true);
  expect(engine.storedRunOf("send-email-1")?.state).toBe("completed");
});

test("stopFetching ends a pass-over wait at once", async () => {
  const engine = createInMemoryEngine();
  const { loop } = startedLoopOf([], {
    engine,
    loopEngine: { ...engine, fetch: async () => ({ kind: "unknownVersion", recordVersion: 2 }) },
  });
  await vi.advanceTimersByTimeAsync(0);
  let isStopped = false;

  void loop.stopFetching().then(() => {
    isStopped = true;
  });
  await vi.advanceTimersByTimeAsync(0);

  expect(isStopped).toBe(true);
});

type TickStopping = { isStoppedBeforeTheAnswer: boolean; isStoppedAfterTheAnswer: boolean };

async function stoppingOfTheTickAcross(loop: WorkerLoop, heldCalls: HeldCalls): Promise<TickStopping> {
  let isStopped = false;
  void loop.stopTicking().then(() => {
    isStopped = true;
  });
  await vi.advanceTimersByTimeAsync(0);
  const isStoppedBeforeTheAnswer = isStopped;
  heldCalls.release();
  await vi.advanceTimersByTimeAsync(0);
  return { isStoppedBeforeTheAnswer, isStoppedAfterTheAnswer: isStopped };
}

test("stopTicking with a third stall's settle in flight resolves once the settle answers, and its run failed line is written", async () => {
  const sendEmail = defineTask("send-email", { schema: v.object({}), handler: async () => undefined });
  const engine = createInMemoryEngine();
  await engine.enqueue({ runs: runsOf("send-email", 1) });
  await expireTheLockOfAFetchedRun(engine, 2);
  const heldSettles = engine.holdCalls("settle");
  const { loop, lines } = startedLoopOf([sendEmail] as AnyTaskDefinition[], { engine });
  await vi.advanceTimersByTimeAsync(1_000);

  const stopping = await stoppingOfTheTickAcross(loop, heldSettles);

  expect(stopping).toEqual({ isStoppedBeforeTheAnswer: false, isStoppedAfterTheAnswer: true });
  expect(engine.storedRunOf("send-email-1")).toMatchObject({ state: "failed", reason: "stalled" });
  expect(messagesOf(lines)).toEqual(["run failed"]);
});

async function scenarioOfAFetchAnsweredAfterAnOutageOf(
  outageInMilliseconds: number,
): Promise<LoopScenario & { heldTicks: HeldCalls }> {
  const engine = createInMemoryEngine();
  await engine.enqueue({ runs: runsOf("send-email", 1) });
  const heldTicks = engine.holdCalls("tick");
  const recordingLogger = createRecordingLogger();
  const simulatedRedisOutage = createSimulatedRedisOutage(recordingLogger.logger);
  simulatedRedisOutage.startOutage();
  const scenario = startedLoopOf([abortableTaskOf("send-email")], {
    engine,
    loopEngine: engineAwayDuringOutage(engine, simulatedRedisOutage, "rejected"),
    recordingLogger,
    simulatedRedisOutage,
  });
  await vi.advanceTimersByTimeAsync(outageInMilliseconds);
  simulatedRedisOutage.endOutage();
  await vi.advanceTimersByTimeAsync(0);
  return { ...scenario, heldTicks };
}

test("a fetch answered after a resend within the lock duration starts its attempts with the lock deadline of its first send", async () => {
  const { lines, heldTicks } = await scenarioOfAFetchAnsweredAfterAnOutageOf(30_000);

  await vi.advanceTimersByTimeAsync(2 * MINUTE_IN_MILLISECONDS - 30_000 - 1);
  const linesBeforeTheFirstSendDeadline = messagesOf(lines);
  await vi.advanceTimersByTimeAsync(1);

  expect(linesBeforeTheFirstSendDeadline).toEqual(["connection error", "connection restored", "attempt started"]);
  expect(linesWithMessage(lines, "attempt aborted: lock lost")).toEqual([LOCK_LOST_ABORT_LINE]);
  heldTicks.release();
});

test("a fetch answered past its first send time plus the lock duration starts no attempt", async () => {
  const { engine, lines, heldTicks } = await scenarioOfAFetchAnsweredAfterAnOutageOf(
    2 * MINUTE_IN_MILLISECONDS + 1_000,
  );

  expect(messagesOf(lines)).toEqual(["connection error", "connection restored"]);
  expect(engine.storedRunOf("send-email-1")?.state).toBe("executing");
  heldTicks.release();
});

test("an unreadable reply is logged once as worker error, and the loop goes on at its next cue", async () => {
  const sendEmail = defineTask("send-email", { schema: v.object({}), handler: async () => undefined });
  const engine = createInMemoryEngine();
  await engine.enqueue({ runs: runsOf("send-email", 1) });
  const unreadableReply = new Error('The fetch script sent a reply the engine cannot read: ["fetched"]');
  let fetchCount = 0;
  const fetchUnreadableOnce: WorkerLoopEngine["fetch"] = async (request) => {
    fetchCount += 1;
    return fetchCount === 1 ? Promise.reject(unreadableReply) : engine.fetch(request);
  };
  const { lines } = startedLoopOf([sendEmail] as AnyTaskDefinition[], {
    engine,
    loopEngine: { ...engine, fetch: fetchUnreadableOnce },
  });

  await vi.advanceTimersByTimeAsync(1_000);

  expect(linesWithMessage(lines, "worker error")).toEqual([
    { level: "error", message: "worker error", fields: { err: unreadableReply } },
  ]);
  expect(engine.storedRunOf("send-email-1")?.state).toBe("completed");
});

const HOURLY: Schedule = { cron: "0 * * * *" };

const ONE_AM = Date.UTC(2030, 0, 1, 1);

function scheduledTaskOf(name: string, schedule: Schedule): AnyTaskDefinition {
  return defineScheduledTask(name, { schedule, retries: 1, handler: async () => undefined }) as AnyTaskDefinition;
}

async function bootSync(engine: InMemoryEngine, tasks: readonly AnyTaskDefinition[]): Promise<void> {
  const { logger } = createRecordingLogger();
  await syncDeclaredSchedules({ engine, definitions: workerDefinitionsOf(tasks), logger });
}

async function engineSyncedWith(tasks: readonly AnyTaskDefinition[]): Promise<InMemoryEngine> {
  const engine = createInMemoryEngine();
  await bootSync(engine, tasks);
  return engine;
}

function firedScheduledAtsOf(engine: InMemoryEngine): string[] {
  const firedRunIds = new Set(requestsOf<FireRequest>(engine, "fire").map(({ runId }) => runId));
  return [...firedRunIds].flatMap((runId) => {
    const scheduledAt = engine.storedRunOf(runId)?.scheduledAt;
    return scheduledAt === undefined ? [] : [new Date(scheduledAt).toISOString()];
  });
}

type Firings = { fireRequestInstants: EpochMilliseconds[]; firedScheduledAts: string[] };

function firingsOf(engine: InMemoryEngine): Firings {
  return {
    fireRequestInstants: requestsOf<FireRequest>(engine, "fire").map(({ instant }) => instant),
    firedScheduledAts: firedScheduledAtsOf(engine),
  };
}

const FIRED_ONCE_AT_ONE_AM_BY_TWO_FIRE_REQUESTS: Firings = {
  fireRequestInstants: [ONE_AM, ONE_AM],
  firedScheduledAts: ["2030-01-01T01:00:00.000Z"],
};

const TIMING_KEEPING_COMPLETED_RUNS: WorkerTiming = {
  ...DEFAULT_WORKER_TIMING,
  completedRunRetention: { maxAgeInMilliseconds: 30 * 24 * 60 * MINUTE_IN_MILLISECONDS, maxRunCount: 1000 },
};

async function tickAtEachNextFiring(engine: InMemoryEngine, task: string, firingCount: number): Promise<void> {
  for (let firing = 0; firing < firingCount; firing += 1) {
    vi.setSystemTime(engine.storedScheduleOf(task)!.nextFiringAt);
    await vi.advanceTimersByTimeAsync(1_000);
  }
}

test("two loops ticking the same instant fire once, and the loser writes nothing", async () => {
  const buildReport = scheduledTaskOf("build-report", HOURLY);
  const engine = await engineSyncedWith([buildReport]);
  vi.setSystemTime(ONE_AM - 500);
  const heldFires = engine.holdCalls("fire");
  const firstLoop = startedLoopOf([buildReport], { engine });
  const secondLoop = startedLoopOf([buildReport], { engine });
  await vi.advanceTimersByTimeAsync(1_000);

  heldFires.release();
  await vi.advanceTimersByTimeAsync(1_000);

  expect(firingsOf(engine)).toEqual(FIRED_ONCE_AT_ONE_AM_BY_TWO_FIRE_REQUESTS);
  expect([...messagesOf(firstLoop.lines), ...messagesOf(secondLoop.lines)].sort()).toEqual([
    "attempt started",
    "run completed",
  ]);
});

test("every line of a fired run carries its scheduledAt, and the firing itself writes no line", async () => {
  const buildReport = scheduledTaskOf("build-report", HOURLY);
  const engine = await engineSyncedWith([buildReport]);
  vi.setSystemTime(ONE_AM - 500);
  const { lines } = startedLoopOf([buildReport], { engine });

  await vi.advanceTimersByTimeAsync(1_000);

  const runFields = {
    task: "build-report",
    runId: deriveFiringRunId("build-report", ONE_AM),
    attempt: 1,
    scheduledAt: "2030-01-01T01:00:00.000Z",
  };
  expect(lines).toEqual([
    { level: "debug", message: "attempt started", fields: runFields },
    { level: "info", message: "run completed", fields: expect.objectContaining(runFields) as unknown },
  ]);
  expect(requestsOf<FireRequest>(engine, "fire")).toEqual([
    {
      task: "build-report",
      instant: ONE_AM,
      instantAfter: ONE_AM + 60 * MINUTE_IN_MILLISECONDS,
      runId: deriveFiringRunId("build-report", ONE_AM),
      retries: 1,
    },
  ]);
});

test("a worker whose release does not schedule a stored task never fires it", async () => {
  const engine = await engineSyncedWith([
    scheduledTaskOf("build-report", HOURLY),
    scheduledTaskOf("send-digest", HOURLY),
  ]);
  vi.setSystemTime(ONE_AM - 500);
  const unscheduledBuildReport = defineTask("build-report", { schema: v.object({}), handler: async () => undefined });
  startedLoopOf([unscheduledBuildReport as AnyTaskDefinition, scheduledTaskOf("send-digest", HOURLY)], { engine });

  await vi.advanceTimersByTimeAsync(5_000);

  expect(requestsOf<TickRequest>(engine, "tick")[0]?.scheduledTaskNames).toEqual(["send-digest"]);
  expect(requestsOf<FireRequest>(engine, "fire").map(({ task }) => task)).toEqual(["send-digest"]);
  expect(engine.storedScheduleOf("build-report")?.nextFiringAt).toBe(ONE_AM);
});

const RESTARTED_AT = Date.UTC(2030, 0, 1, 3, 30);

test("a worker restarted after three missed instants fires once with the first planned instant as scheduledAt and skips the rest", async () => {
  const buildReport = scheduledTaskOf("build-report", HOURLY);
  const engine = await engineSyncedWith([buildReport]);
  vi.setSystemTime(RESTARTED_AT);
  await bootSync(engine, [buildReport]);
  startedLoopOf([buildReport], { engine });

  await vi.advanceTimersByTimeAsync(5_000);

  expect(firedScheduledAtsOf(engine)).toEqual(["2030-01-01T01:00:00.000Z"]);
  expect(requestsOf<FireRequest>(engine, "fire")).toHaveLength(1);
  expect(engine.storedScheduleOf("build-report")?.nextFiringAt).toBe(Date.UTC(2030, 0, 1, 4));
});

test("a worker restarted after three missed instants with a cron changed at its boot fires none of them", async () => {
  const engine = await engineSyncedWith([scheduledTaskOf("build-report", HOURLY)]);
  vi.setSystemTime(RESTARTED_AT);
  const changedBuildReport = scheduledTaskOf("build-report", { cron: "45 * * * *" });
  await bootSync(engine, [changedBuildReport]);
  startedLoopOf([changedBuildReport], { engine });

  await vi.advanceTimersByTimeAsync(5_000);

  expect(requestsOf<FireRequest>(engine, "fire")).toEqual([]);
  expect(engine.storedScheduleOf("build-report")?.nextFiringAt).toBe(Date.UTC(2030, 0, 1, 3, 45));
});

const EUROPE_PARIS_DAYLIGHT_SAVING_SEQUENCES = [
  {
    description: "at the spring change, 30 2 * * * fires once, at 03:30 summer time",
    cron: "30 2 * * *",
    bootedAt: Date.UTC(2030, 2, 30, 12),
    firedScheduledAts: ["2030-03-31T01:30:00.000Z", "2030-04-01T00:30:00.000Z", "2030-04-02T00:30:00.000Z"],
  },
  {
    description: "at the autumn change, 30 2 * * * fires once, at the first 02:30",
    cron: "30 2 * * *",
    bootedAt: Date.UTC(2030, 9, 26, 12),
    firedScheduledAts: ["2030-10-27T00:30:00.000Z", "2030-10-28T01:30:00.000Z", "2030-10-29T01:30:00.000Z"],
  },
  {
    description: "at the autumn change, 0 * * * * fires at both 02:00s",
    cron: "0 * * * *",
    bootedAt: Date.UTC(2030, 9, 26, 23, 30),
    firedScheduledAts: ["2030-10-27T00:00:00.000Z", "2030-10-27T01:00:00.000Z", "2030-10-27T02:00:00.000Z"],
  },
];

for (const { description, cron, bootedAt, firedScheduledAts } of EUROPE_PARIS_DAYLIGHT_SAVING_SEQUENCES) {
  test(`in Europe/Paris ${description}`, async () => {
    vi.setSystemTime(bootedAt);
    const buildReport = scheduledTaskOf("build-report", { cron, timezone: "Europe/Paris" });
    const engine = await engineSyncedWith([buildReport]);
    startedLoopOf([buildReport], { engine, timing: TIMING_KEEPING_COMPLETED_RUNS });

    await tickAtEachNextFiring(engine, "build-report", firedScheduledAts.length);

    expect(firedScheduledAtsOf(engine)).toEqual(firedScheduledAts);
  });
}

test("two releases with different crons on one record fire one sequence, from the record's cron", async () => {
  const olderBuildReport = scheduledTaskOf("build-report", { cron: "30 * * * *" });
  const newerBuildReport = scheduledTaskOf("build-report", HOURLY);
  const engine = await engineSyncedWith([olderBuildReport]);
  await bootSync(engine, [newerBuildReport]);
  startedLoopOf([olderBuildReport], { engine, timing: TIMING_KEEPING_COMPLETED_RUNS });
  startedLoopOf([newerBuildReport], { engine, timing: TIMING_KEEPING_COMPLETED_RUNS });

  await tickAtEachNextFiring(engine, "build-report", 3);

  expect(firedScheduledAtsOf(engine)).toEqual([
    "2030-01-01T01:00:00.000Z",
    "2030-01-01T02:00:00.000Z",
    "2030-01-01T03:00:00.000Z",
  ]);
  expect(engine.storedScheduleOf("build-report")?.nextFiringAt).toBe(Date.UTC(2030, 0, 1, 4));
});

test("a lost fire reply fires nothing twice: the resend after the outage hears already and writes nothing", async () => {
  const buildReport = scheduledTaskOf("build-report", HOURLY);
  const engine = await engineSyncedWith([buildReport]);
  vi.setSystemTime(ONE_AM - 500);
  const recordingLogger = createRecordingLogger();
  const simulatedRedisOutage = createSimulatedRedisOutage(recordingLogger.logger);
  let isFireReplyLost = true;
  const fireWhoseFirstReplyIsLost: WorkerLoopEngine["fire"] = async (request) => {
    const reply = await engine.fire(request);
    if (!isFireReplyLost) {
      return reply;
    }
    isFireReplyLost = false;
    simulatedRedisOutage.startOutage();
    throw UNANSWERED_COMMAND_ERROR;
  };
  const { lines } = startedLoopOf([buildReport], {
    engine,
    loopEngine: engineAwayDuringOutage(
      { ...engine, fire: fireWhoseFirstReplyIsLost },
      simulatedRedisOutage,
      "rejected",
    ),
    recordingLogger,
    simulatedRedisOutage,
  });
  await vi.advanceTimersByTimeAsync(1_000);

  simulatedRedisOutage.endOutage();
  await vi.advanceTimersByTimeAsync(1_000);

  expect(firingsOf(engine)).toEqual(FIRED_ONCE_AT_ONE_AM_BY_TWO_FIRE_REQUESTS);
  expect(messagesOf(lines).sort()).toEqual([
    "attempt started",
    "connection error",
    "connection restored",
    "run completed",
  ]);
});

test("stopTicking with a fire in flight resolves once the fire answers, and the firing's run is created", async () => {
  const buildReport = scheduledTaskOf("build-report", HOURLY);
  const engine = await engineSyncedWith([buildReport]);
  vi.setSystemTime(ONE_AM - 500);
  const heldFires = engine.holdCalls("fire");
  const { loop } = startedLoopOf([buildReport], { engine });
  await vi.advanceTimersByTimeAsync(1_000);

  const stopping = await stoppingOfTheTickAcross(loop, heldFires);

  expect(stopping).toEqual({ isStoppedBeforeTheAnswer: false, isStoppedAfterTheAnswer: true });
  expect(engine.storedRunOf(deriveFiringRunId("build-report", ONE_AM))?.scheduledAt).toBe(ONE_AM);
});

test("a stored cron this release cannot parse writes one worker error, and the other due schedules still fire", async () => {
  const tasks = [scheduledTaskOf("build-report", HOURLY), scheduledTaskOf("send-digest", HOURLY)];
  const engine = createInMemoryEngine();
  await engine.syncSchedules({
    schedulesToSync: [
      { task: "build-report", cron: "not a cron", timezone: "UTC", nextFiringAt: ONE_AM },
      { task: "send-digest", cron: "0 * * * *", timezone: "UTC", nextFiringAt: ONE_AM },
    ],
  });
  vi.setSystemTime(ONE_AM - 500);
  const { lines } = startedLoopOf(tasks, { engine });

  await vi.advanceTimersByTimeAsync(1_000);

  expect(linesWithMessage(lines, "worker error")).toEqual([
    { level: "error", message: "worker error", fields: { err: expect.any(Error) as unknown } },
  ]);
  expect(requestsOf<FireRequest>(engine, "fire").map(({ task }) => task)).toEqual(["send-digest"]);
  expect(firedScheduledAtsOf(engine)).toEqual(["2030-01-01T01:00:00.000Z"]);
});
