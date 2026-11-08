import { expect, test, vi } from "vitest";

import type { EpochMilliseconds } from "../src/durations.js";
import type { PruneRequest, RunToCreate } from "../src/engine/engine.js";
import type { RunId } from "../src/ids.js";
import { defineEngineContractTests } from "./engine-contract.js";
import { createInMemoryEngine, MOST_RUNS_PRUNED_PER_PRUNE } from "./in-memory-engine.js";
import type { InMemoryEngine } from "./in-memory-engine.js";
import { freshLockTokensOf } from "./lock-tokens.js";

const HOUR_IN_MILLISECONDS = 60 * 60 * 1000;

const DAY_IN_MILLISECONDS = 24 * HOUR_IN_MILLISECONDS;

const PRUNE_REQUEST: PruneRequest = {
  task: "send-email",
  completedRetention: { maxAgeInMilliseconds: DAY_IN_MILLISECONDS, maxRunCount: 1000 },
  failedRetention: { maxAgeInMilliseconds: 14 * DAY_IN_MILLISECONDS, maxRunCount: 10_000 },
};

const ATTEMPT_ERROR = { attempt: 1, name: "Error", message: "boom", stack: "", failedAt: "" };

defineEngineContractTests((name, body) => {
  test(name, async () => {
    const engine = createInMemoryEngine();
    await body({
      engine,
      expireLock: async (runId) => {
        engine.expireLock(runId);
      },
      pastInstant: async () => Date.now() - 1000,
      stateOf: async (runId) => engine.storedRunOf(runId)?.state,
      isWakeUpMarkerArmed: async () => engine.isWakeUpMarkerArmed(),
      disarmWakeUpMarker: () => engine.waitForWakeUp(0),
    });
  });
});

function engineOnAMovableClock(): { engine: InMemoryEngine; moveClockBy: (milliseconds: number) => void } {
  let instant: EpochMilliseconds = Date.UTC(2030, 0, 1);
  return {
    engine: createInMemoryEngine({ now: () => instant }),
    moveClockBy: (milliseconds) => {
      instant += milliseconds;
    },
  };
}

async function endedRunsOf(
  engine: InMemoryEngine,
  runIds: readonly RunId[],
  outcomeKind: "complete" | "fail",
): Promise<void> {
  const runs: RunToCreate[] = runIds.map((runId) => ({ runId, task: "send-email", payload: "{}", retries: 0 }));
  for (let offset = 0; offset < runs.length; offset += 50) {
    const batch = runs.slice(offset, offset + 50);
    await engine.enqueue({ runs: batch });
    const reply = await engine.fetch({
      lockTokens: freshLockTokensOf(batch.length),
      concurrencyLimitsByTask: {},
      lockDurationInMilliseconds: 120_000,
    });
    for (const lockedRun of reply.kind === "fetched" ? reply.lockedRuns : []) {
      await engine.settle({
        ...lockedRun,
        outcome:
          outcomeKind === "complete"
            ? { kind: "complete" }
            : { kind: "fail", reason: "non-retriable", error: ATTEMPT_ERROR, failureNotice: undefined },
      });
    }
  }
}

function runIdsOf(prefix: string, count: number): RunId[] {
  return Array.from({ length: count }, (_, position) => `${prefix}-${String(position + 1).padStart(5, "0")}`);
}

test("a prune removes a completed run older than 24 hours and keeps a failed run of the same age until 14 days", async () => {
  const { engine, moveClockBy } = engineOnAMovableClock();
  await endedRunsOf(engine, ["completed-run"], "complete");
  await endedRunsOf(engine, ["failed-run"], "fail");
  moveClockBy(DAY_IN_MILLISECONDS);

  const dayReply = await engine.prune(PRUNE_REQUEST);
  const failedRunStateAfterADay = engine.storedRunOf("failed-run")?.state;
  moveClockBy(13 * DAY_IN_MILLISECONDS);
  const fortnightReply = await engine.prune(PRUNE_REQUEST);

  expect(dayReply).toEqual({ kind: "pruned", prunedRunCount: 1 });
  expect(engine.storedRunOf("completed-run")).toBeUndefined();
  expect(failedRunStateAfterADay).toBe("failed");
  expect(fortnightReply).toEqual({ kind: "pruned", prunedRunCount: 1 });
  expect(engine.storedRunOf("failed-run")).toBeUndefined();
});

test("a prune keeps the 1,000 newest completed runs of a task, removing the oldest first", async () => {
  const { engine, moveClockBy } = engineOnAMovableClock();
  const runIds = runIdsOf("run", 1200);
  for (const runId of runIds) {
    await endedRunsOf(engine, [runId], "complete");
    moveClockBy(1);
  }

  const reply = await engine.prune(PRUNE_REQUEST);

  expect(reply).toEqual({ kind: "pruned", prunedRunCount: 200 });
  expect(engine.runIdsInState("completed").sort()).toEqual(runIds.slice(200));
});

test(`a prune removes at most ${String(MOST_RUNS_PRUNED_PER_PRUNE)} runs per call and only of its task`, async () => {
  const { engine, moveClockBy } = engineOnAMovableClock();
  await endedRunsOf(engine, runIdsOf("run", 250), "complete");
  await engine.enqueue({ runs: [{ runId: "other-task-run", task: "resize-image", payload: "{}", retries: 0 }] });
  moveClockBy(2 * DAY_IN_MILLISECONDS);

  const firstReply = await engine.prune(PRUNE_REQUEST);
  const secondReply = await engine.prune(PRUNE_REQUEST);

  expect(firstReply).toEqual({ kind: "pruned", prunedRunCount: MOST_RUNS_PRUNED_PER_PRUNE });
  expect(secondReply).toEqual({ kind: "pruned", prunedRunCount: 50 });
  expect(engine.storedRunOf("other-task-run")?.state).toBe("pending");
});

test("a pruned run leaves whole, so its id starts a new run once its dedup entry expired", async () => {
  const { engine, moveClockBy } = engineOnAMovableClock();
  await endedRunsOf(engine, ["run-1"], "complete");
  moveClockBy(DAY_IN_MILLISECONDS);
  await engine.prune(PRUNE_REQUEST);

  const reply = await engine.enqueue({ runs: [{ runId: "run-1", task: "send-email", payload: "{}", retries: 0 }] });

  expect(reply).toEqual({ kind: "enqueued", createdRunIds: ["run-1"], skippedRunIds: [] });
  expect(engine.storedRunOf("run-1")).toMatchObject({ state: "pending", attempt: 1, errors: [] });
});

test("a wait for a wake-up returns once an enqueue arms the marker, and consumes it", async () => {
  vi.useFakeTimers();
  try {
    const engine = createInMemoryEngine();
    let isWokenUp = false;
    const wakeUp = engine.waitForWakeUp(1000).then(() => {
      isWokenUp = true;
    });
    await vi.advanceTimersByTimeAsync(500);
    const isWokenUpBeforeEnqueue = isWokenUp;

    await engine.enqueue({ runs: [{ runId: "run-1", task: "send-email", payload: "{}", retries: 0 }] });
    await wakeUp;

    expect(isWokenUpBeforeEnqueue).toBe(false);
    expect(engine.isWakeUpMarkerArmed()).toBe(false);
  } finally {
    vi.useRealTimers();
  }
});

test("a wait for a wake-up with no marker armed returns at its timeout", async () => {
  vi.useFakeTimers();
  try {
    const engine = createInMemoryEngine();
    let isWokenUp = false;
    void engine.waitForWakeUp(1000).then(() => {
      isWokenUp = true;
    });

    await vi.advanceTimersByTimeAsync(999);
    const isWokenUpBeforeTimeout = isWokenUp;
    await vi.advanceTimersByTimeAsync(1);

    expect(isWokenUpBeforeTimeout).toBe(false);
    expect(isWokenUp).toBe(true);
  } finally {
    vi.useRealTimers();
  }
});

test("held calls of a method wait until released, and every call is recorded with its request", async () => {
  const engine = createInMemoryEngine();
  const heldCalls = engine.holdCalls("promote");
  let isPromoted = false;
  const promotion = engine.promote().then(() => {
    isPromoted = true;
  });
  await Promise.resolve();
  const isPromotedWhileHeld = isPromoted;

  heldCalls.release();
  await promotion;

  expect(isPromotedWhileHeld).toBe(false);
  expect(engine.calls).toEqual([{ method: "promote", request: undefined }]);
});
