import { describe, expect, test } from "vitest";

import type { StoredDelayedRunState, StoredRun } from "./run-queue.js";
import { attemptOf, runStateFieldsOf, runStateOf } from "./run-states.js";
import type { RunJobSnapshot } from "./run-states.js";

const DISPATCHED_AT = Date.parse("2026-11-03T10:00:00.000Z");

function storedRunWith(storedRunFields: Partial<StoredRun> = {}): StoredRun {
  return { runId: "run-1", payload: {}, errors: [], writeToken: "token", ...storedRunFields };
}

function snapshotOf(
  bullmqState: RunJobSnapshot["bullmqState"],
  storedRunFields: Partial<StoredRun> = {},
  delayInMilliseconds = 0,
): RunJobSnapshot {
  return {
    bullmqState,
    storedRun: storedRunWith(storedRunFields),
    dispatchedAt: DISPATCHED_AT,
    delayInMilliseconds,
  };
}

describe("runStateFieldsOf", () => {
  test.each(["waiting", "prioritized", "waiting-children"] as const)("a %s job is pending", (bullmqState) => {
    expect(runStateFieldsOf(snapshotOf(bullmqState))).toEqual({ state: "pending" });
  });

  test("an active job is executing", () => {
    expect(runStateFieldsOf(snapshotOf("active"))).toEqual({ state: "executing" });
  });

  test("a completed job is completed", () => {
    expect(runStateFieldsOf(snapshotOf("completed"))).toEqual({ state: "completed" });
  });

  test("a failed job is failed with its stored reason", () => {
    expect(runStateFieldsOf(snapshotOf("failed", { reason: "non-retriable" }))).toEqual({
      state: "failed",
      reason: "non-retriable",
    });
  });

  test("a failed job with no stored reason failed for exhausting its retries", () => {
    expect(runStateFieldsOf(snapshotOf("failed"))).toEqual({ state: "failed", reason: "retries exhausted" });
  });

  test("a delayed job with no marker is delayed until its dispatch time plus its delay", () => {
    expect(runStateFieldsOf(snapshotOf("delayed", {}, 60_000))).toEqual({
      state: "delayed",
      startsAt: new Date(DISPATCHED_AT + 60_000),
    });
  });

  test.each<[StoredDelayedRunState, unknown]>([
    [{ state: "waitingForConcurrencySlot" }, { state: "waitingForConcurrencySlot" }],
    [
      { state: "suspended", resumesAt: "2026-11-04T08:00:00.000Z" },
      { state: "suspended", resumesAt: new Date("2026-11-04T08:00:00.000Z") },
    ],
    [
      { state: "backingOff", nextAttemptAt: "2026-11-03T10:00:30.000Z" },
      { state: "backingOff", nextAttemptAt: new Date("2026-11-03T10:00:30.000Z") },
    ],
    [{ state: "pending" }, { state: "pending" }],
  ])("a delayed job marked %o reads as %o", (delayedRunState, expectedStateFields) => {
    expect(runStateFieldsOf(snapshotOf("delayed", { delayedRunState }))).toEqual(expectedStateFields);
  });

  test("a marker left from an earlier delay is ignored once the job is no longer delayed", () => {
    expect(
      runStateFieldsOf(snapshotOf("waiting", { delayedRunState: { state: "waitingForConcurrencySlot" } })),
    ).toEqual({ state: "pending" });
  });

  test("an unknown job has no state", () => {
    expect(runStateFieldsOf(snapshotOf("unknown"))).toBeUndefined();
  });
});

describe("runStateOf", () => {
  test.each<[RunJobSnapshot["bullmqState"], StoredDelayedRunState | undefined]>([
    ["waiting", undefined],
    ["prioritized", undefined],
    ["waiting-children", undefined],
    ["active", undefined],
    ["completed", undefined],
    ["failed", undefined],
    ["delayed", undefined],
    ["delayed", { state: "waitingForConcurrencySlot" }],
    ["delayed", { state: "suspended", resumesAt: "2026-11-04T08:00:00.000Z" }],
    ["delayed", { state: "backingOff", nextAttemptAt: "2026-11-03T10:00:30.000Z" }],
    ["delayed", { state: "pending" }],
    ["unknown", undefined],
  ])("names the state runStateFieldsOf derives from a %s job marked %o", (bullmqState, delayedRunState) => {
    const snapshot = snapshotOf(bullmqState, delayedRunState === undefined ? {} : { delayedRunState });

    expect(runStateOf(bullmqState, delayedRunState)).toBe(runStateFieldsOf(snapshot)?.state);
  });
});

describe("attemptOf", () => {
  test("a run not yet finished is on the attempt after the ones made", () => {
    expect(attemptOf({ attemptsMade: 0, data: storedRunWith() }, "pending")).toBe(1);
    expect(attemptOf({ attemptsMade: 1, data: storedRunWith() }, "backingOff")).toBe(2);
  });

  test("a completed run is on its last attempt made", () => {
    expect(attemptOf({ attemptsMade: 3, data: storedRunWith() }, "completed")).toBe(3);
  });

  test("a failed run is on the attempt of its last error", () => {
    const errorRecord = { name: "Error", message: "down", stack: "", failedAt: "2026-11-03T10:00:00.000Z" };
    const storedRun = storedRunWith({
      errors: [
        { ...errorRecord, attempt: 1 },
        { ...errorRecord, attempt: 2 },
      ],
    });

    expect(attemptOf({ attemptsMade: 5, data: storedRun }, "failed")).toBe(2);
  });
});
