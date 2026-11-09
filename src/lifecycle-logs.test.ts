import { describe, expect, test } from "vitest";

import type { LockedRun, SettleReply } from "./engine/engine.js";
import {
  enqueueLinesOf,
  parkLineOf,
  pickupLineOf,
  settleLinesOf,
  stallLineOf,
  workerErrorLineOf,
  writeLifecycleLines,
} from "./lifecycle-logs.js";
import type { ListenerLeftOut } from "./engine-failure-notice.js";
import type { AttemptOutcome } from "./locked-run-attempt.js";
import { createRecordingLogger } from "../tests/recording-logger.js";

const DISPATCHED_AT = Date.UTC(2030, 0, 1, 12, 0, 0);

const STARTED_AT = DISPATCHED_AT + 5_000;

const LOCKED_RUN: LockedRun = {
  lockToken: "token-1",
  runId: "run-1",
  task: "send-email",
  payload: '{"to":"ada@example.com"}',
  attempt: 2,
  retries: 3,
  replays: 0,
  isResume: false,
  dispatchedAt: DISPATCHED_AT,
  startedAt: STARTED_AT,
  lockedUntil: STARTED_AT + 120_000,
};

const RUN_FIELDS = { task: "send-email", runId: "run-1", attempt: 2 };

const ATTEMPT_ERROR = new Error("provider down");

function settleLinesFor(
  attemptOutcome: AttemptOutcome,
  settleReply: SettleReply,
  elapsed = 0,
  listenersLeftOut: readonly ListenerLeftOut[] = [],
) {
  return settleLinesOf({
    settledRun: LOCKED_RUN,
    attemptOutcome,
    settleReply,
    elapsedSinceFetchInMilliseconds: elapsed,
    listenersLeftOut,
  });
}

describe("the lines of a stall and of the listeners left out of a notice", () => {
  test("a stalled run writes attempt stalled with its stalls, its source event and its planned instant", () => {
    const scheduledAt = Date.UTC(2030, 0, 1);
    const sourceEvent = { eventName: "user.signed-up", eventId: "event-1" };

    expect(
      stallLineOf({ runId: "run-1", task: "send-email", attempt: 2, stalls: 1, scheduledAt, sourceEvent }),
    ).toEqual({
      level: "warn",
      message: "attempt stalled",
      fields: { ...RUN_FIELDS, ...sourceEvent, scheduledAt: "2030-01-01T00:00:00.000Z", stalls: 1 },
    });
  });

  const keyError = new Error("no tenant in this payload");
  const failOutcome: AttemptOutcome = {
    kind: "fail",
    reason: "stalled",
    error: ATTEMPT_ERROR,
    failureNotice: undefined,
  };

  test("a failure that stored its notice writes a worker error per listener left out, then run failed", () => {
    const lines = settleLinesFor(failOutcome, { kind: "failed", reason: "stalled", finishedAt: STARTED_AT }, 0, [
      { listenerName: "broken-key-listener", error: keyError },
    ]);

    expect(lines.map(({ message }) => message)).toEqual(["worker error", "run failed"]);
    expect(lines[0]?.fields).toEqual({ err: keyError });
  });

  test.each<SettleReply>([{ kind: "stale" }, { kind: "already" }, { kind: "unknownVersion", recordVersion: 2 }])(
    "a $kind reply to a failure writes no line for the listeners left out",
    (settleReply) => {
      const lines = settleLinesFor(failOutcome, settleReply, 0, [
        { listenerName: "broken-key-listener", error: keyError },
      ]);

      expect(lines.filter(({ fields }) => fields["err"] === keyError)).toEqual([]);
    },
  );
});

describe("the lines of a dispatch and an emit", () => {
  const dispatchRequest = { runs: [{ runId: "run-1", task: "send-email", payload: "{}", retries: 3 }] };
  const eventDedup = { eventName: "user.signed-up", eventId: "event-1" };
  const emitRequest = {
    runs: [
      { runId: "listener-run-1", task: "send-email", payload: "{}", retries: 3, sourceEvent: eventDedup },
      { runId: "listener-run-2", task: "sync-crm", payload: "{}", retries: 3, sourceEvent: eventDedup },
    ],
    eventDedup,
  };

  test("a created run of a dispatch writes run dispatched", () => {
    expect(
      enqueueLinesOf(dispatchRequest, {
        kind: "enqueued",
        createdRuns: [{ task: "send-email", runId: "run-1" }],
        skippedRuns: [],
      }),
    ).toEqual([{ level: "debug", message: "run dispatched", fields: { task: "send-email", runId: "run-1" } }]);
  });

  test("a skipped run of a dispatch writes duplicate dispatch dropped with the task and run id", () => {
    expect(
      enqueueLinesOf(dispatchRequest, {
        kind: "enqueued",
        createdRuns: [],
        skippedRuns: [{ task: "send-email", runId: "run-1" }],
      }),
    ).toEqual([
      { level: "debug", message: "duplicate dispatch dropped", fields: { task: "send-email", runId: "run-1" } },
    ]);
  });

  test("a repeated event id writes duplicate dispatch dropped with the event name and id", () => {
    expect(enqueueLinesOf(emitRequest, { kind: "duplicate" })).toEqual([
      { level: "debug", message: "duplicate dispatch dropped", fields: eventDedup },
    ]);
  });

  test("an emit writes nothing for its created listener runs and a line per listener skipped on a repeat", () => {
    expect(
      enqueueLinesOf(emitRequest, {
        kind: "enqueued",
        createdRuns: [{ task: "send-email", runId: "listener-run-1" }],
        skippedRuns: [{ task: "sync-crm", runId: "listener-run-2" }],
      }),
    ).toEqual([
      {
        level: "debug",
        message: "duplicate dispatch dropped",
        fields: { task: "sync-crm", runId: "listener-run-2", ...eventDedup },
      },
    ]);
  });

  test("an emit with no listener writes no listeners for event at warn", () => {
    expect(enqueueLinesOf({ runs: [], eventDedup }, undefined)).toEqual([
      { level: "warn", message: "no listeners for event", fields: eventDedup },
    ]);
  });
});

describe("the lines of a fetch", () => {
  test("a fresh pickup writes attempt started and a pickup after a suspend writes run resumed, with the same attempt", () => {
    expect([pickupLineOf(LOCKED_RUN), pickupLineOf({ ...LOCKED_RUN, isResume: true })]).toEqual([
      { level: "debug", message: "attempt started", fields: RUN_FIELDS },
      { level: "debug", message: "run resumed", fields: RUN_FIELDS },
    ]);
  });

  test("a parked run writes run waiting for a concurrency slot", () => {
    expect(parkLineOf({ runId: "run-1", task: "send-email", attempt: 2 })).toEqual({
      level: "debug",
      message: "run waiting for a concurrency slot",
      fields: RUN_FIELDS,
    });
  });

  test("every line of a listener run carries its event, and of a fired run its planned instant", () => {
    const sourceEvent = { eventName: "user.signed-up", eventId: "event-1" };
    const scheduledAt = Date.UTC(2030, 0, 1, 3, 30);

    expect(pickupLineOf({ ...LOCKED_RUN, sourceEvent }).fields).toEqual({ ...RUN_FIELDS, ...sourceEvent });
    expect(parkLineOf({ runId: "run-1", task: "send-email", attempt: 2, scheduledAt }).fields).toEqual({
      ...RUN_FIELDS,
      scheduledAt: "2030-01-01T03:30:00.000Z",
    });
  });
});

describe("the lines of a settle", () => {
  const finishedAt = STARTED_AT + 2_000;

  test("a completion writes run completed with the run and execution durations from the server's instants", () => {
    expect(settleLinesFor({ kind: "complete" }, { kind: "completed", finishedAt })).toEqual([
      {
        level: "info",
        message: "run completed",
        fields: { ...RUN_FIELDS, runDurationInMilliseconds: 7_000, executionDurationInMilliseconds: 2_000 },
      },
    ]);
  });

  test("a completion answered already writes run completed, its durations measured from the fetch by the worker's clock", () => {
    expect(settleLinesFor({ kind: "complete" }, { kind: "already" }, 3_000)).toEqual([
      {
        level: "info",
        message: "run completed",
        fields: { ...RUN_FIELDS, runDurationInMilliseconds: 8_000, executionDurationInMilliseconds: 3_000 },
      },
    ]);
  });

  test("a final failure writes run failed with the error, the reason and the durations", () => {
    const attemptOutcome: AttemptOutcome = {
      kind: "fail",
      reason: "retries exhausted",
      error: ATTEMPT_ERROR,
      failureNotice: undefined,
    };

    expect(settleLinesFor(attemptOutcome, { kind: "failed", reason: "retries exhausted", finishedAt })).toEqual([
      {
        level: "error",
        message: "run failed",
        fields: {
          ...RUN_FIELDS,
          err: ATTEMPT_ERROR,
          reason: "retries exhausted",
          runDurationInMilliseconds: 7_000,
          executionDurationInMilliseconds: 2_000,
        },
      },
    ]);
  });

  test("a retry writes attempt failed with the error, the retries left and the backoff", () => {
    const attemptOutcome: AttemptOutcome = {
      kind: "retry",
      error: ATTEMPT_ERROR,
      retriesLeft: 2,
      backoffInMilliseconds: 30_000,
    };

    expect(settleLinesFor(attemptOutcome, { kind: "backingOff", nextAttemptAt: finishedAt + 30_000 })).toEqual([
      {
        level: "warn",
        message: "attempt failed",
        fields: { ...RUN_FIELDS, err: ATTEMPT_ERROR, retriesLeft: 2, backoffInMilliseconds: 30_000 },
      },
    ]);
  });

  test("a suspend writes run suspended with its resume time", () => {
    const resumesAt = Date.UTC(2030, 0, 2);

    expect(settleLinesFor({ kind: "suspend", resumesAt }, { kind: "suspended", resumesAt })).toEqual([
      {
        level: "debug",
        message: "run suspended",
        fields: { ...RUN_FIELDS, resumesAt: "2030-01-02T00:00:00.000Z" },
      },
    ]);
  });

  test("a yield writes run yielded at shutdown", () => {
    expect(settleLinesFor({ kind: "yield" }, { kind: "yielded" })).toEqual([
      { level: "debug", message: "run yielded at shutdown", fields: RUN_FIELDS },
    ]);
  });

  test("a hand back writes run handed back, or run failed once it fails the run", () => {
    const attemptOutcome: AttemptOutcome = { kind: "handBack", error: ATTEMPT_ERROR, failureNotice: undefined };

    expect(settleLinesFor(attemptOutcome, { kind: "handedBack" })).toEqual([
      { level: "debug", message: "run handed back: unknown task", fields: RUN_FIELDS },
    ]);
    expect(
      settleLinesFor(attemptOutcome, { kind: "failed", reason: "unknown task", finishedAt })[0]?.fields,
    ).toMatchObject({ reason: "unknown task", err: ATTEMPT_ERROR });
  });

  test("a settle answered stale writes attempt outcome discarded: lock lost at warn, with the kind as outcome", () => {
    const attemptOutcomes: AttemptOutcome[] = [
      { kind: "complete" },
      { kind: "fail", reason: "non-retriable", error: ATTEMPT_ERROR, failureNotice: undefined },
      { kind: "retry", error: ATTEMPT_ERROR, retriesLeft: 1, backoffInMilliseconds: 1000 },
      { kind: "suspend", resumesAt: finishedAt },
      { kind: "yield" },
    ];

    const lines = attemptOutcomes.flatMap((attemptOutcome) => settleLinesFor(attemptOutcome, { kind: "stale" }));

    expect(lines).toEqual(
      ["complete", "fail", "retry", "suspend", "yield"].map((outcome) => ({
        level: "warn",
        message: "attempt outcome discarded: lock lost",
        fields: { ...RUN_FIELDS, outcome },
      })),
    );
  });

  test("no settle line carries the lock token or the payload", () => {
    const lines = [
      ...settleLinesFor({ kind: "complete" }, { kind: "completed", finishedAt }),
      ...settleLinesFor({ kind: "complete" }, { kind: "stale" }),
      pickupLineOf(LOCKED_RUN),
    ];

    for (const { fields } of lines) {
      expect(JSON.stringify(fields)).not.toContain("token-1");
      expect(JSON.stringify(fields)).not.toContain("ada@example.com");
    }
  });
});

test("a worker error writes worker error at error with the error as err", () => {
  expect(workerErrorLineOf(ATTEMPT_ERROR)).toEqual({
    level: "error",
    message: "worker error",
    fields: { err: ATTEMPT_ERROR },
  });
});

test("lines are written through the logger at their level", () => {
  const { logger, lines } = createRecordingLogger();

  writeLifecycleLines(logger, [
    { level: "debug", message: "attempt started", fields: RUN_FIELDS },
    { level: "warn", message: "attempt failed", fields: RUN_FIELDS },
  ]);

  expect(lines).toEqual([
    { level: "debug", message: "attempt started", fields: RUN_FIELDS },
    { level: "warn", message: "attempt failed", fields: RUN_FIELDS },
  ]);
});
