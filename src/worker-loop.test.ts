import * as v from "valibot";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { runFailed } from "./built-in-events.js";
import { defineListener, defineTask } from "./definitions.js";
import type { AnyTaskDefinition } from "./definitions.js";
import type { FetchRequest, RunToCreate, SettleRequest, TickRequest } from "./engine/engine.js";
import { deriveFailureNoticeEventId, deriveListenerRunId } from "./ids.js";
import type { HandlerSender } from "./step-engine.js";
import { startWorkerLoop } from "./worker-loop.js";
import type { WaitForWakeUp, WorkerLoop, WorkerLoopEngine } from "./worker-loop.js";
import { DEFAULT_WORKER_TIMING } from "./worker-timing.js";
import type { WorkerTiming } from "./worker-timing.js";
import { createInMemoryEngine } from "../tests/in-memory-engine.js";
import type { InMemoryEngine } from "../tests/in-memory-engine.js";
import { freshLockTokensOf } from "../tests/lock-tokens.js";
import { createRecordingLogger } from "../tests/recording-logger.js";
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

type LoopScenario = { engine: InMemoryEngine; lines: RecordedLogLine[]; loop: WorkerLoop };

const startedLoops: WorkerLoop[] = [];

function startedLoopOf(
  tasks: readonly AnyTaskDefinition[],
  {
    concurrency = 50,
    engine = createInMemoryEngine(),
    loopEngine = engine,
    waitForWakeUp = engine.waitForWakeUp,
    timing = DEFAULT_WORKER_TIMING,
  }: {
    concurrency?: number;
    engine?: InMemoryEngine;
    loopEngine?: WorkerLoopEngine;
    waitForWakeUp?: WaitForWakeUp;
    timing?: WorkerTiming;
  } = {},
): LoopScenario {
  const { logger, lines } = createRecordingLogger();
  const loop = startWorkerLoop({
    engine: loopEngine,
    definitions: workerDefinitionsOf(tasks),
    timing,
    logger,
    concurrency,
    sender: NO_SENDER,
    waitForWakeUp,
  });
  startedLoops.push(loop);
  return { engine, lines, loop };
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

function requestsOf<Request>(engine: InMemoryEngine, method: "fetch" | "settle" | "tick"): Request[] {
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

test("the loop survives a rejected wake-up wait without spinning, and a run enqueued afterwards still completes", async () => {
  const unhandledRejections: unknown[] = [];
  const recordUnhandledRejection = (reason: unknown) => {
    unhandledRejections.push(reason);
  };
  process.on("unhandledRejection", recordUnhandledRejection);
  try {
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
    expect(unhandledRejections).toEqual([]);
  } finally {
    process.off("unhandledRejection", recordUnhandledRejection);
  }
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
