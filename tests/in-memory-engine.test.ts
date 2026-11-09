import { expect, test, vi } from "vitest";

import { defineEngineContractTests } from "./engine-contract.js";
import { createInMemoryEngine } from "./in-memory-engine.js";

defineEngineContractTests((name, body) => {
  test(name, async () => {
    const engine = createInMemoryEngine();
    await body({
      engine,
      expireLock: async (runId) => {
        engine.expireLock(runId);
      },
      backdateEndedRun: async (runId, durationInMilliseconds) => {
        engine.backdateEndedRun(runId, durationInMilliseconds);
      },
      pastInstant: async () => Date.now() - 1000,
      stateOf: async (runId) => engine.storedRunOf(runId)?.state,
      isWakeUpMarkerArmed: async () => engine.isWakeUpMarkerArmed(),
      disarmWakeUpMarker: () => engine.waitForWakeUp(0),
    });
  });
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
