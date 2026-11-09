import { expect } from "vitest";

import { runFailed } from "../src/built-in-events.js";
import type { DurationInMilliseconds, EpochMilliseconds } from "../src/durations.js";
import { MOST_RUNS_PRUNED_PER_PRUNE } from "../src/engine/engine.js";
import type {
  Engine,
  FailureNotice,
  FetchedReply,
  FireRequest,
  LockedRun,
  RunToCreate,
  ScheduleToSync,
  SettleOutcome,
  SettleReply,
} from "../src/engine/engine.js";
import { deriveFiringRunId } from "../src/ids.js";
import type { RunId } from "../src/ids.js";
import type { ErrorRecord } from "../src/run-queue.js";
import type { RunState } from "../src/run-states.js";
import {
  DAY_IN_MILLISECONDS,
  endRuns,
  MINUTE_IN_MILLISECONDS,
  runIdsOf,
  SEND_EMAIL_PRUNE_REQUEST,
} from "./ended-runs.js";
import { freshLockTokensOf } from "./lock-tokens.js";
import { REPLIED_EMPTY_PROMOTE, REPLIED_ONE_RENEWAL, repliedOf } from "./tick-replies.js";

export type ContractEngine = Pick<
  Engine,
  | "enqueue"
  | "fetch"
  | "renew"
  | "settle"
  | "promote"
  | "saveStep"
  | "readStep"
  | "prune"
  | "tick"
  | "fire"
  | "syncSchedules"
  | "serverInstant"
>;

export type EngineContractHarness = {
  engine: ContractEngine;
  expireLock: (runId: RunId) => Promise<void>;
  backdateEndedRun: (runId: RunId, durationInMilliseconds: DurationInMilliseconds) => Promise<void>;
  backdateFirstFoundUnknownAt: (runId: RunId, durationInMilliseconds: DurationInMilliseconds) => Promise<void>;
  pastInstant: () => Promise<EpochMilliseconds>;
  stateOf: (runId: RunId) => Promise<RunState | undefined>;
  isWakeUpMarkerArmed: () => Promise<boolean>;
  disarmWakeUpMarker: () => Promise<void>;
};

export type EngineContractTest = (name: string, body: (harness: EngineContractHarness) => Promise<void>) => void;

const LOCK_DURATION_IN_MILLISECONDS = 120_000;

const ATTEMPT_ERROR: ErrorRecord = { attempt: 1, name: "Error", message: "boom", stack: "Error: boom", failedAt: "" };

function sendEmailRunsOf(runIds: readonly RunId[], extraFields: Partial<RunToCreate> = {}): RunToCreate[] {
  return runIds.map((runId) => ({
    runId,
    task: "send-email",
    payload: `{"runId":"${runId}"}`,
    retries: 3,
    ...extraFields,
  }));
}

async function fetchedOf(
  engine: ContractEngine,
  lockTokenCount: number,
  concurrencyLimitsByTask: Record<string, number> = {},
): Promise<FetchedReply> {
  const reply = await engine.fetch({
    lockTokens: freshLockTokensOf(lockTokenCount),
    concurrencyLimitsByTask,
    lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS,
  });
  if (reply.kind !== "fetched") {
    throw new Error(`expected a fetched reply, got ${JSON.stringify(reply)}`);
  }
  return reply;
}

async function lockedRunsOf(engine: ContractEngine, lockTokenCount: number): Promise<LockedRun[]> {
  return (await fetchedOf(engine, lockTokenCount)).lockedRuns;
}

async function onlyLockedRunOf(engine: ContractEngine, runId: RunId): Promise<LockedRun> {
  await engine.enqueue({ runs: sendEmailRunsOf([runId]) });
  const [lockedRun] = await lockedRunsOf(engine, 1);
  return lockedRun!;
}

function failureNoticeOf(runId: RunId, listenerRunIds: readonly RunId[]): FailureNotice {
  return {
    payload: {
      task: "send-email",
      runId,
      reason: "retries exhausted",
      error: { name: "Error", message: "boom", stack: "Error: boom" },
      attempt: 1,
    },
    runs: listenerRunIds.map((listenerRunId) => ({
      runId: listenerRunId,
      task: "alert-on-call",
      retries: 1,
      sourceEvent: { eventName: runFailed.name, eventId: `notice-of-${runId}` },
    })),
    eventDedup: { eventName: runFailed.name, eventId: `notice-of-${runId}` },
  };
}

async function statesOf(harness: EngineContractHarness, runIds: readonly RunId[]): Promise<(RunState | undefined)[]> {
  const states: (RunState | undefined)[] = [];
  for (const runId of runIds) {
    states.push(await harness.stateOf(runId));
  }
  return states;
}

const HOUR_IN_MILLISECONDS = 60 * MINUTE_IN_MILLISECONDS;

function hourlyReportOf(nextFiringAt: EpochMilliseconds, task = "build-report"): ScheduleToSync {
  return { task, cron: "0 * * * *", timezone: "UTC", nextFiringAt };
}

function firingOf({ task, nextFiringAt }: ScheduleToSync): FireRequest {
  return {
    task,
    instant: nextFiringAt,
    instantAfter: nextFiringAt + HOUR_IN_MILLISECONDS,
    runId: deriveFiringRunId(task, nextFiringAt),
    retries: 2,
  };
}

async function backdateEndedRuns(
  harness: EngineContractHarness,
  runIds: readonly RunId[],
  durationInMilliseconds: DurationInMilliseconds,
): Promise<void> {
  for (const runId of runIds) {
    await harness.backdateEndedRun(runId, durationInMilliseconds);
  }
}

export function defineEngineContractTests(contractTest: EngineContractTest): void {
  contractTest(
    "a fetch locks one pending run per token, oldest first, and leaves the rest pending",
    async (harness) => {
      const { engine } = harness;
      await engine.enqueue({ runs: sendEmailRunsOf(["run-1", "run-2", "run-3"]) });

      const lockedRuns = await lockedRunsOf(engine, 2);

      expect(lockedRuns.map(({ runId, attempt, isResume }) => [runId, attempt, isResume])).toEqual([
        ["run-1", 1, false],
        ["run-2", 1, false],
      ]);
      expect(lockedRuns[0]!.lockedUntil).toBe(lockedRuns[0]!.startedAt + LOCK_DURATION_IN_MILLISECONDS);
      expect(await harness.stateOf("run-3")).toBe("pending");
    },
  );

  contractTest("a resent fetch with the same tokens returns the same locks and locks nothing new", async (harness) => {
    const { engine } = harness;
    await engine.enqueue({ runs: sendEmailRunsOf(["run-1", "run-2"]) });
    const request = {
      lockTokens: freshLockTokensOf(1),
      concurrencyLimitsByTask: {},
      lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS,
    };
    const firstReply = await engine.fetch(request);

    const resentReply = await engine.fetch(request);

    expect(resentReply).toEqual(firstReply);
    expect(await harness.stateOf("run-2")).toBe("pending");
  });

  contractTest(
    "a fetch parks a run whose concurrency key is at its limit, and a settle freeing the slot makes it pending",
    async (harness) => {
      const { engine } = harness;
      await engine.enqueue({ runs: sendEmailRunsOf(["run-1", "run-2"], { concurrencyKey: "account-7" }) });
      const fetched = await fetchedOf(engine, 2, { "send-email": 1 });
      const [lockedRun] = fetched.lockedRuns;

      await engine.settle({ ...lockedRun!, outcome: { kind: "complete" }, concurrencyLimit: 1 });

      expect(fetched.lockedRuns.map(({ runId }) => runId)).toEqual(["run-1"]);
      expect(fetched.parkedRuns).toEqual([{ runId: "run-2", task: "send-email", attempt: 1 }]);
      expect(await harness.stateOf("run-2")).toBe("pending");
    },
  );

  contractTest("a complete settles once: a resend hears already and another token hears stale", async (harness) => {
    const { engine } = harness;
    const lockedRun = await onlyLockedRunOf(engine, "run-1");
    const instantBefore = await harness.pastInstant();

    const reply = await engine.settle({ ...lockedRun, outcome: { kind: "complete" } });
    const resentReply = await engine.settle({ ...lockedRun, outcome: { kind: "complete" } });
    const staleReply = await engine.settle({ ...lockedRun, lockToken: "other-token", outcome: { kind: "complete" } });

    expect(reply).toEqual({ kind: "completed", finishedAt: expect.any(Number) });
    expect(reply.kind === "completed" ? reply.finishedAt : 0).toBeGreaterThan(instantBefore);
    expect(resentReply).toEqual({ kind: "already" });
    expect(staleReply).toEqual({ kind: "stale" });
    expect(await harness.stateOf("run-1")).toBe("completed");
  });

  contractTest("a retry backs off, then comes back pending at the next attempt once due", async (harness) => {
    const { engine } = harness;
    const lockedRun = await onlyLockedRunOf(engine, "run-1");

    const reply = await engine.settle({
      ...lockedRun,
      outcome: { kind: "retry", error: ATTEMPT_ERROR, backoffInMilliseconds: 0 },
    });
    const stateAfterSettle = await harness.stateOf("run-1");
    const promoteReply = await engine.promote();
    const [retriedRun] = await lockedRunsOf(engine, 1);

    expect(reply).toEqual({ kind: "backingOff", nextAttemptAt: expect.any(Number) });
    expect(stateAfterSettle).toBe("backingOff");
    expect(promoteReply).toEqual({ kind: "promoted", promotedRunCount: 1, stalledRuns: [], thirdStalls: [] });
    expect(retriedRun).toMatchObject({ runId: "run-1", attempt: 2, isResume: false });
  });

  contractTest("a suspend comes back as a resume of the same attempt once due", async (harness) => {
    const { engine } = harness;
    const lockedRun = await onlyLockedRunOf(engine, "run-1");
    const resumesAt = await harness.pastInstant();

    const reply = await engine.settle({ ...lockedRun, outcome: { kind: "suspend", resumesAt } });
    await engine.promote();
    const [resumedRun] = await lockedRunsOf(engine, 1);

    expect(reply).toEqual({ kind: "suspended", resumesAt });
    expect(resumedRun).toMatchObject({ runId: "run-1", attempt: 1, isResume: true });
  });

  contractTest(
    "a fail stores its notice: each listener run is pending under its id with the failure time in its payload",
    async (harness) => {
      const { engine } = harness;
      const lockedRun = await onlyLockedRunOf(engine, "run-1");

      const reply = await engine.settle({
        ...lockedRun,
        outcome: {
          kind: "fail",
          reason: "retries exhausted",
          error: ATTEMPT_ERROR,
          failureNotice: failureNoticeOf("run-1", ["listener-run-1"]),
        },
      });
      const [noticeRun] = await lockedRunsOf(engine, 2);

      expect(reply).toEqual({ kind: "failed", reason: "retries exhausted", finishedAt: expect.any(Number) });
      const finishedAt = reply.kind === "failed" ? reply.finishedAt : 0;
      expect(await harness.stateOf("run-1")).toBe("failed");
      expect(noticeRun).toMatchObject({
        runId: "listener-run-1",
        task: "alert-on-call",
        retries: 1,
        sourceEvent: { eventName: runFailed.name, eventId: "notice-of-run-1" },
      });
      expect(JSON.parse(noticeRun!.payload)).toEqual({
        ...failureNoticeOf("run-1", []).payload,
        failedAt: new Date(finishedAt).toISOString(),
      });
    },
  );

  contractTest("a fail with an empty notice creates no run", async (harness) => {
    const { engine } = harness;
    const lockedRun = await onlyLockedRunOf(engine, "run-1");

    await engine.settle({
      ...lockedRun,
      outcome: {
        kind: "fail",
        reason: "non-retriable",
        error: ATTEMPT_ERROR,
        failureNotice: failureNoticeOf("run-1", []),
      },
    });
    await engine.enqueue({ runs: sendEmailRunsOf(["run-2"]) });

    expect((await lockedRunsOf(engine, 2)).map(({ runId }) => runId)).toEqual(["run-2"]);
  });

  const outcomesBackAtTheFront: [SettleOutcome, SettleReply][] = [
    [{ kind: "yield" }, { kind: "yielded" }],
    [{ kind: "handBack", error: ATTEMPT_ERROR, failureNotice: undefined }, { kind: "handedBack" }],
  ];
  for (const [outcome, expectedReply] of outcomesBackAtTheFront) {
    contractTest(
      `a ${outcome.kind} puts the run back at the front of pending with the same attempt`,
      async (harness) => {
        const { engine } = harness;
        await engine.enqueue({ runs: sendEmailRunsOf(["run-1", "run-2"]) });
        const [lockedRun] = await lockedRunsOf(engine, 1);

        const reply = await engine.settle({ ...lockedRun!, outcome });
        const [nextLockedRun] = await lockedRunsOf(engine, 1);

        expect(reply).toEqual(expectedReply);
        expect(nextLockedRun).toMatchObject({ runId: "run-1", attempt: 1, isResume: false });
      },
    );
  }

  contractTest(
    "a hand back of a run first found unknown 30 minutes ago fails it unknown task and stores its notice",
    async (harness) => {
      const { engine } = harness;
      const handBack = (lockedRun: LockedRun) =>
        engine.settle({
          ...lockedRun,
          outcome: {
            kind: "handBack",
            error: ATTEMPT_ERROR,
            failureNotice: failureNoticeOf("run-1", ["listener-run-1"]),
          },
        });
      await handBack(await onlyLockedRunOf(engine, "run-1"));
      await harness.backdateFirstFoundUnknownAt("run-1", 30 * MINUTE_IN_MILLISECONDS);
      const [lockedRun] = await lockedRunsOf(engine, 1);

      const reply = await handBack(lockedRun!);

      expect(reply).toEqual({ kind: "failed", reason: "unknown task", finishedAt: expect.any(Number) });
      expect(await statesOf(harness, ["run-1", "listener-run-1"])).toEqual(["failed", "pending"]);
    },
  );

  contractTest("a renew of a held lock renews it, and of a lock its settle ended replies stale", async (harness) => {
    const { engine } = harness;
    await engine.enqueue({ runs: sendEmailRunsOf(["run-1", "run-2"]) });
    const [heldRun, settledRun] = await lockedRunsOf(engine, 2);
    await engine.settle({ ...settledRun!, outcome: { kind: "complete" } });

    const renewals = await engine.renew({
      heldLocks: [heldRun!, settledRun!].map(({ runId, lockToken }) => ({ runId, lockToken })),
      lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS,
    });

    expect(renewals).toEqual([{ kind: "renewed", lockedUntil: expect.any(Number) }, { kind: "stale" }]);
  });

  contractTest(
    "a promote stalls an expired lock back to the front of pending twice, then reports the third stall unwritten",
    async (harness) => {
      const { engine } = harness;
      await engine.enqueue({ runs: sendEmailRunsOf(["run-1", "run-2"]) });
      const stallsByPromote: unknown[] = [];
      let lockedRun = (await lockedRunsOf(engine, 1))[0]!;
      for (const expectedStalls of [1, 2]) {
        await harness.expireLock("run-1");
        stallsByPromote.push(await engine.promote());
        lockedRun = (await lockedRunsOf(engine, 1))[0]!;
        expect(lockedRun.runId, `the fetch after stall ${String(expectedStalls)}`).toBe("run-1");
      }
      await harness.expireLock("run-1");

      const thirdPromoteReply = await engine.promote();

      expect(stallsByPromote).toEqual(
        [1, 2].map((stalls) => ({
          kind: "promoted",
          promotedRunCount: 0,
          stalledRuns: [{ runId: "run-1", task: "send-email", attempt: 1, stalls }],
          thirdStalls: [],
        })),
      );
      expect(thirdPromoteReply).toEqual({
        kind: "promoted",
        promotedRunCount: 0,
        stalledRuns: [],
        thirdStalls: [
          {
            runId: "run-1",
            task: "send-email",
            attempt: 1,
            expiredLockToken: lockedRun.lockToken,
            replays: 0,
            dispatchedAt: lockedRun.dispatchedAt,
            startedAt: lockedRun.startedAt,
          },
        ],
      });
      expect(await harness.stateOf("run-1")).toBe("executing");
    },
  );

  contractTest("a stalled run and a third stall carry the source event of their run", async (harness) => {
    const { engine } = harness;
    const sourceEvent = { eventName: "user.signed-up", eventId: "event-1" };
    await engine.enqueue({ runs: sendEmailRunsOf(["run-1"], { sourceEvent }) });
    const stalledRuns: unknown[] = [];
    for (let stallCount = 0; stallCount < 2; stallCount += 1) {
      await lockedRunsOf(engine, 1);
      await harness.expireLock("run-1");
      const reply = await engine.promote();
      stalledRuns.push(...(reply.kind === "promoted" ? reply.stalledRuns : []));
    }
    await lockedRunsOf(engine, 1);
    await harness.expireLock("run-1");

    const thirdPromoteReply = await engine.promote();

    expect(stalledRuns).toEqual([1, 2].map((stalls) => expect.objectContaining({ stalls, sourceEvent })));
    expect(thirdPromoteReply).toMatchObject({ thirdStalls: [{ runId: "run-1", sourceEvent }] });
  });

  contractTest(
    "a reported third stall fails through a fail settle fenced by its expired token, and a second caller hears already",
    async (harness) => {
      const { engine } = harness;
      await engine.enqueue({ runs: sendEmailRunsOf(["run-1"]) });
      for (let stallCount = 0; stallCount < 2; stallCount += 1) {
        await lockedRunsOf(engine, 1);
        await harness.expireLock("run-1");
        await engine.promote();
      }
      await lockedRunsOf(engine, 1);
      await harness.expireLock("run-1");
      const promoteReply = await engine.promote();
      const [thirdStall] = promoteReply.kind === "promoted" ? promoteReply.thirdStalls : [];
      const failStalled = {
        runId: thirdStall!.runId,
        lockToken: thirdStall!.expiredLockToken,
        outcome: {
          kind: "fail" as const,
          reason: "stalled" as const,
          error: ATTEMPT_ERROR,
          failureNotice: failureNoticeOf("run-1", ["listener-run-1"]),
        },
      };

      const firstReply = await engine.settle(failStalled);
      const secondReply = await engine.settle(failStalled);

      expect(firstReply).toEqual({ kind: "failed", reason: "stalled", finishedAt: expect.any(Number) });
      expect(secondReply).toEqual({ kind: "already" });
      expect(await statesOf(harness, ["run-1", "listener-run-1"])).toEqual(["failed", "pending"]);
      expect(await engine.promote()).toEqual({
        kind: "promoted",
        promotedRunCount: 0,
        stalledRuns: [],
        thirdStalls: [],
      });
    },
  );

  for (const isPromoteIncluded of [true, false]) {
    contractTest(
      isPromoteIncluded
        ? "a tick promotes, renews the held locks and lists the task names in one reply"
        : "a tick leaving the promote out stalls nothing and still renews",
      async (harness) => {
        const { engine } = harness;
        await engine.enqueue({ runs: sendEmailRunsOf(["held-run", "stalled-run"]) });
        const [heldRun] = await lockedRunsOf(engine, 2);
        await harness.expireLock("stalled-run");

        const reply = await engine.tick({
          heldLocks: [{ runId: heldRun!.runId, lockToken: heldRun!.lockToken }],
          lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS,
          isPromoteIncluded,
        });

        expect(reply).toEqual({
          promote: isPromoteIncluded
            ? repliedOf({
                kind: "promoted",
                promotedRunCount: 0,
                stalledRuns: [{ runId: "stalled-run", task: "send-email", attempt: 1, stalls: 1 }],
                thirdStalls: [],
              })
            : undefined,
          renewals: REPLIED_ONE_RENEWAL,
          prune: undefined,
          taskNames: repliedOf(["send-email"]),
        });
        expect(await harness.stateOf("stalled-run")).toBe(isPromoteIncluded ? "pending" : "executing");
      },
    );
  }

  contractTest(
    "a saved step reads back under its lock, and a step call after the settle hears stale",
    async (harness) => {
      const { engine } = harness;
      const lockedRun = await onlyLockedRunOf(engine, "run-1");
      const fence = { runId: lockedRun.runId, lockToken: lockedRun.lockToken, stepId: "charge" };

      const saveReply = await engine.saveStep({ ...fence, step: { kind: "value", value: '["paid"]' } });
      const readReply = await engine.readStep(fence);
      await engine.settle({ ...lockedRun, outcome: { kind: "yield" } });
      const staleReadReply = await engine.readStep(fence);

      expect(saveReply).toEqual({ kind: "saved", resumesAt: undefined });
      expect(readReply).toEqual({ kind: "read", savedValue: '["paid"]' });
      expect(staleReadReply).toEqual({ kind: "stale" });
    },
  );

  contractTest(
    "an enqueue skips a run id that exists and refuses a repeated event id as a duplicate",
    async (harness) => {
      const { engine } = harness;
      const eventDedup = { eventName: "user.signed-up", eventId: "event-1" };
      await engine.enqueue({ runs: sendEmailRunsOf(["run-1"]) });

      const repeatedRunReply = await engine.enqueue({ runs: sendEmailRunsOf(["run-1", "run-2"]) });
      const firstEmitReply = await engine.enqueue({ runs: sendEmailRunsOf(["listener-run"]), eventDedup });
      const repeatedEmitReply = await engine.enqueue({ runs: sendEmailRunsOf(["listener-run"]), eventDedup });

      expect(repeatedRunReply).toEqual({ kind: "enqueued", createdRunIds: ["run-2"], skippedRunIds: ["run-1"] });
      expect(firstEmitReply).toEqual({ kind: "enqueued", createdRunIds: ["listener-run"], skippedRunIds: [] });
      expect(repeatedEmitReply).toEqual({ kind: "duplicate" });
    },
  );

  contractTest(
    "an enqueue arms the wake-up marker, a fetch leaving runs pending re-arms it and one emptying pending does not",
    async (harness) => {
      const { engine } = harness;
      await engine.enqueue({ runs: sendEmailRunsOf(["run-1", "run-2"]) });
      const isArmedByEnqueue = await harness.isWakeUpMarkerArmed();
      await harness.disarmWakeUpMarker();

      await lockedRunsOf(engine, 1);
      const isArmedByFetchLeavingARun = await harness.isWakeUpMarkerArmed();
      await harness.disarmWakeUpMarker();
      await lockedRunsOf(engine, 1);

      expect(isArmedByEnqueue).toBe(true);
      expect(isArmedByFetchLeavingARun).toBe(true);
      expect(await harness.isWakeUpMarkerArmed()).toBe(false);
    },
  );

  contractTest(
    "a prune removes a completed run ended 24 hours ago, and keeps a failed run of the same age until 14 days",
    async (harness) => {
      const { engine } = harness;
      await endRuns(engine, "send-email", ["completed-run", "younger-completed-run"], "complete");
      await endRuns(engine, "send-email", ["failed-run"], "fail");
      await backdateEndedRuns(harness, ["completed-run", "failed-run"], DAY_IN_MILLISECONDS);
      await harness.backdateEndedRun("younger-completed-run", DAY_IN_MILLISECONDS - MINUTE_IN_MILLISECONDS);

      const dayReply = await engine.prune(SEND_EMAIL_PRUNE_REQUEST);
      const statesAfterADay = await statesOf(harness, ["completed-run", "younger-completed-run", "failed-run"]);
      await harness.backdateEndedRun("failed-run", 13 * DAY_IN_MILLISECONDS);
      const fortnightReply = await engine.prune(SEND_EMAIL_PRUNE_REQUEST);

      expect(dayReply).toEqual({ kind: "pruned", prunedRunCount: 1 });
      expect(statesAfterADay).toEqual([undefined, "completed", "failed"]);
      expect(fortnightReply).toEqual({ kind: "pruned", prunedRunCount: 1 });
      expect(await harness.stateOf("failed-run")).toBeUndefined();
    },
  );

  contractTest("a prune removes the oldest ended runs over the cap of their state", async (harness) => {
    const { engine } = harness;
    const completedRunIds = runIdsOf("completed-run", 5);
    const failedRunIds = runIdsOf("failed-run", 4);
    await endRuns(engine, "send-email", completedRunIds, "complete");
    await endRuns(engine, "send-email", failedRunIds, "fail");

    const reply = await engine.prune({
      ...SEND_EMAIL_PRUNE_REQUEST,
      completedRetention: { ...SEND_EMAIL_PRUNE_REQUEST.completedRetention, maxRunCount: 3 },
      failedRetention: { ...SEND_EMAIL_PRUNE_REQUEST.failedRetention, maxRunCount: 2 },
    });

    expect(reply).toEqual({ kind: "pruned", prunedRunCount: 4 });
    expect(await statesOf(harness, completedRunIds)).toEqual([undefined, undefined, ...Array(3).fill("completed")]);
    expect(await statesOf(harness, failedRunIds)).toEqual([undefined, undefined, "failed", "failed"]);
  });

  contractTest(
    `a prune removes at most ${String(MOST_RUNS_PRUNED_PER_PRUNE)} runs of its task per call: aged completed, aged failed, then over the cap`,
    async (harness) => {
      const { engine } = harness;
      const agedCompletedRunIds = runIdsOf("aged-completed-run", 150);
      const agedFailedRunIds = runIdsOf("aged-failed-run", 100);
      const completedRunIdsOverTheCap = runIdsOf("completed-run", 50);
      await endRuns(engine, "send-email", agedCompletedRunIds, "complete");
      await endRuns(engine, "send-email", agedFailedRunIds, "fail");
      await endRuns(engine, "send-email", completedRunIdsOverTheCap, "complete");
      await engine.enqueue({ runs: [{ runId: "other-task-run", task: "resize-image", payload: "{}", retries: 0 }] });
      await backdateEndedRuns(harness, [...agedCompletedRunIds, ...agedFailedRunIds], 15 * DAY_IN_MILLISECONDS);
      const pruneRequest = {
        ...SEND_EMAIL_PRUNE_REQUEST,
        completedRetention: { ...SEND_EMAIL_PRUNE_REQUEST.completedRetention, maxRunCount: 0 },
      };

      const firstReply = await engine.prune(pruneRequest);
      const failedStatesAfterTheFirstPrune = await statesOf(harness, agedFailedRunIds);
      const completedStatesAfterTheFirstPrune = await statesOf(harness, completedRunIdsOverTheCap);
      const secondReply = await engine.prune(pruneRequest);

      expect(firstReply).toEqual({ kind: "pruned", prunedRunCount: MOST_RUNS_PRUNED_PER_PRUNE });
      expect(await statesOf(harness, agedCompletedRunIds)).toEqual(Array(150).fill(undefined));
      expect(failedStatesAfterTheFirstPrune).toEqual([...Array(50).fill(undefined), ...Array(50).fill("failed")]);
      expect(completedStatesAfterTheFirstPrune).toEqual(Array(50).fill("completed"));
      expect(secondReply).toEqual({ kind: "pruned", prunedRunCount: 100 });
      expect(await statesOf(harness, [...agedFailedRunIds, ...completedRunIdsOverTheCap])).toEqual(
        Array(150).fill(undefined),
      );
      expect(await harness.stateOf("other-task-run")).toBe("pending");
    },
  );

  contractTest(
    "a pruned run's id is skipped while its dedup entry is live, and starts a new run once it expired",
    async (harness) => {
      const { engine } = harness;
      await endRuns(engine, "send-email", ["run-ended-24h01-ago", "run-ended-23h59-ago"], "complete");
      await harness.backdateEndedRun("run-ended-24h01-ago", DAY_IN_MILLISECONDS + MINUTE_IN_MILLISECONDS);
      await harness.backdateEndedRun("run-ended-23h59-ago", DAY_IN_MILLISECONDS - MINUTE_IN_MILLISECONDS);
      await engine.prune({
        ...SEND_EMAIL_PRUNE_REQUEST,
        completedRetention: { ...SEND_EMAIL_PRUNE_REQUEST.completedRetention, maxRunCount: 0 },
      });

      const reply = await engine.enqueue({ runs: sendEmailRunsOf(["run-ended-24h01-ago", "run-ended-23h59-ago"]) });

      expect(reply).toEqual({
        kind: "enqueued",
        createdRunIds: ["run-ended-24h01-ago"],
        skippedRunIds: ["run-ended-23h59-ago"],
      });
    },
  );

  contractTest(
    "a firing of the planned instant creates the run under its id with scheduledAt, and a second firing of it hears already",
    async (harness) => {
      const { engine } = harness;
      const schedule = hourlyReportOf(await harness.pastInstant());
      await engine.syncSchedules({ schedulesToSync: [schedule] });
      const firing = firingOf(schedule);

      const reply = await engine.fire(firing);
      const secondReply = await engine.fire(firing);
      const [firedRun] = await lockedRunsOf(engine, 2);

      expect(reply).toEqual({ kind: "fired", runId: firing.runId, isRunCreated: true });
      expect(secondReply).toEqual({ kind: "already" });
      expect(firedRun).toMatchObject({
        runId: firing.runId,
        task: "build-report",
        retries: 2,
        payload: "",
        scheduledAt: firing.instant,
      });
    },
  );

  contractTest(
    "a firing of an instant still to come hears notDue and of a task with no schedule noSchedule, creating no run",
    async (harness) => {
      const { engine } = harness;
      const schedule = hourlyReportOf((await harness.pastInstant()) + HOUR_IN_MILLISECONDS);
      await engine.syncSchedules({ schedulesToSync: [schedule] });

      const notDueReply = await engine.fire(firingOf(schedule));
      const noScheduleReply = await engine.fire(firingOf({ ...schedule, task: "send-digest" }));

      expect(notDueReply).toEqual({ kind: "notDue" });
      expect(noScheduleReply).toEqual({ kind: "noSchedule" });
      expect(await lockedRunsOf(engine, 1)).toEqual([]);
    },
  );

  contractTest(
    "a sync adds, changes and removes schedules, keeps an unchanged one's planned firing and drops a changed one's",
    async (harness) => {
      const { engine } = harness;
      const plannedInstant = await harness.pastInstant();
      const [unchanged, changed, removed, added] = ["unchanged", "changed", "removed", "added"].map((task) =>
        hourlyReportOf(plannedInstant, task),
      ) as [ScheduleToSync, ScheduleToSync, ScheduleToSync, ScheduleToSync];
      await engine.syncSchedules({ schedulesToSync: [unchanged, changed, removed] });
      const rewritten = { ...changed, cron: "30 * * * *", nextFiringAt: plannedInstant + HOUR_IN_MILLISECONDS };

      const reply = await engine.syncSchedules({ schedulesToSync: [unchanged, rewritten, added] });
      const fireReplies = [];
      for (const schedule of [unchanged, changed, removed]) {
        fireReplies.push((await engine.fire(firingOf(schedule))).kind);
      }

      expect(reply).toEqual({
        kind: "synced",
        added: [{ task: "added", cron: "0 * * * *", timezone: "UTC" }],
        changed: [{ task: "changed", cron: "30 * * * *", timezone: "UTC" }],
        removed: [{ task: "removed", cron: "0 * * * *", timezone: "UTC" }],
      });
      expect(fireReplies).toEqual(["fired", "already", "noSchedule"]);
    },
  );

  contractTest(
    "a tick reads the due schedules among the task names it is given, with the server's instant",
    async (harness) => {
      const { engine } = harness;
      const pastInstant = await harness.pastInstant();
      const dueReport = hourlyReportOf(pastInstant, "due-report");
      await engine.syncSchedules({
        schedulesToSync: [
          dueReport,
          hourlyReportOf(pastInstant + HOUR_IN_MILLISECONDS, "later-report"),
          hourlyReportOf(pastInstant, "undeclared-report"),
        ],
      });

      const reply = await engine.tick({
        heldLocks: [],
        lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS,
        isPromoteIncluded: false,
        scheduledTaskNames: ["due-report", "later-report", "report-without-a-record"],
      });

      expect(reply.dueSchedules).toEqual(repliedOf({ serverInstant: expect.any(Number), dueSchedules: [dueReport] }));
      const serverInstant = reply.dueSchedules?.kind === "replied" ? reply.dueSchedules.reply.serverInstant : 0;
      expect(serverInstant).toBeGreaterThan(pastInstant);
    },
  );

  contractTest("the server instant is the engine's clock, after an instant already past", async (harness) => {
    const pastInstant = await harness.pastInstant();

    const serverInstant = await harness.engine.serverInstant();

    expect(serverInstant).toBeGreaterThan(pastInstant);
    expect(serverInstant).toBeLessThan(pastInstant + MINUTE_IN_MILLISECONDS);
  });

  contractTest("a tick carrying a prune prunes its task and returns the prune's reply", async (harness) => {
    const { engine } = harness;
    await endRuns(engine, "send-email", ["aged-run"], "complete");
    await harness.backdateEndedRun("aged-run", DAY_IN_MILLISECONDS);

    const reply = await engine.tick({
      heldLocks: [],
      lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS,
      prune: SEND_EMAIL_PRUNE_REQUEST,
    });

    expect(reply).toEqual({
      promote: REPLIED_EMPTY_PROMOTE,
      renewals: repliedOf([]),
      prune: repliedOf({ kind: "pruned", prunedRunCount: 1 }),
      taskNames: repliedOf(["send-email"]),
    });
    expect(await harness.stateOf("aged-run")).toBeUndefined();
  });
}
