import * as v from "valibot";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { runFailed } from "./built-in-events.js";
import { defineListener, defineTask } from "./definitions.js";
import type { AnyTaskDefinition } from "./definitions.js";
import type { FetchRequest, RunToCreate, SettleRequest, TickRequest } from "./engine/engine.js";
import { deriveFailureNoticeEventId, deriveListenerRunId } from "./ids.js";
import type { HandlerSender } from "./step-engine.js";
import { startWorkerLoop } from "./worker-loop.js";
import type { WorkerLoop, WorkerLoopEngine } from "./worker-loop.js";
import { DEFAULT_WORKER_TIMING } from "./worker-timing.js";
import { createInMemoryEngine } from "../tests/in-memory-engine.js";
import type { InMemoryEngine } from "../tests/in-memory-engine.js";
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
  }: { concurrency?: number; engine?: InMemoryEngine; loopEngine?: WorkerLoopEngine } = {},
): LoopScenario {
  const { logger, lines } = createRecordingLogger();
  const loop = startWorkerLoop({
    engine: loopEngine,
    definitions: workerDefinitionsOf(tasks),
    timing: DEFAULT_WORKER_TIMING,
    logger,
    concurrency,
    sender: NO_SENDER,
    waitForWakeUp: engine.waitForWakeUp,
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
  const engine = createInMemoryEngine();
  await engine.enqueue({ runs: runsOf("send-email", 1) });
  const { lines } = startedLoopOf([sendEmail] as AnyTaskDefinition[], { engine, concurrency: 1 });
  await vi.advanceTimersByTimeAsync(0);
  engine.expireLock("send-email-1");
  await engine.promote();

  handlerGate.release(1);
  await vi.advanceTimersByTimeAsync(0);

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
