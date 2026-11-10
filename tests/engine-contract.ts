import { expect } from "vitest";

import { runFailed } from "../src/built-in-events.js";
import type { DurationInMilliseconds, EpochMilliseconds } from "../src/durations.js";
import type { RunIdentity } from "../src/engine/engine-keys.js";
import { MOST_RUNS_PRUNED_PER_PRUNE } from "../src/engine/engine.js";
import type {
  DispatchOrEmitRequest,
  Engine,
  EnqueueReply,
  FailureNotice,
  FetchedReply,
  FireRequest,
  LockedRun,
  PromoteReply,
  ReadStepReply,
  RunToCreate,
  ScheduleToSync,
  SerializedStepValue,
  SettleOutcome,
  SettleReply,
} from "../src/engine/engine.js";
import { MOST_DUE_RUNS_PROMOTED_PER_PROMOTE } from "../src/engine/promote-script.js";
import { deriveFiringRunId } from "../src/ids.js";
import type { RunId } from "../src/ids.js";
import type { AttemptErrorReport } from "../src/engine/engine.js";
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
  | "makeDue"
>;

export type EngineContractHarness = {
  engine: ContractEngine;
  expireLock: (task: string, runId: RunId) => Promise<void>;
  backdateEndedRun: (task: string, runId: RunId, durationInMilliseconds: DurationInMilliseconds) => Promise<void>;
  backdateFirstFoundUnknownAt: (
    task: string,
    runId: RunId,
    durationInMilliseconds: DurationInMilliseconds,
  ) => Promise<void>;
  pastInstant: () => Promise<EpochMilliseconds>;
  stateOf: (task: string, runId: RunId) => Promise<RunState | undefined>;
  storedErrorsOf: (task: string, runId: RunId) => Promise<unknown[]>;
  isWakeUpMarkerArmed: () => Promise<boolean>;
  disarmWakeUpMarker: () => Promise<void>;
};

export type EngineContractTest = (name: string, body: (harness: EngineContractHarness) => Promise<void>) => void;

const LOCK_DURATION_IN_MILLISECONDS = 120_000;

const ATTEMPT_ERROR: AttemptErrorReport = { attempt: 1, name: "Error", message: "boom", stack: "Error: boom" };

function sendEmailRunsOf(runIds: readonly RunId[], extraFields: Partial<RunToCreate> = {}): RunToCreate[] {
  return runIds.map((runId) => ({
    runId,
    task: "send-email",
    payload: `{"runId":"${runId}"}`,
    retries: 3,
    ...extraFields,
  }));
}

function sendEmailRunIdentitiesOf(runIds: readonly RunId[]): RunIdentity[] {
  return runIds.map((runId) => ({ task: "send-email", runId }));
}

function orderRunsOf(task: string, runIds: readonly RunId[], extraFields: Partial<RunToCreate> = {}): RunToCreate[] {
  return runIds.map((runId) => ({ runId, task, payload: `{"task":"${task}"}`, retries: 3, ...extraFields }));
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

async function retryOfTheOnlyRunOf(
  engine: ContractEngine,
  runId: RunId,
  backoffInMilliseconds: DurationInMilliseconds,
): Promise<SettleReply> {
  const lockedRun = await onlyLockedRunOf(engine, runId);
  return engine.settle({ ...lockedRun, outcome: { kind: "retry", error: ATTEMPT_ERROR, backoffInMilliseconds } });
}

type FencedSendOutcome = {
  reply: EnqueueReply;
  resentReply: EnqueueReply;
  readReply: ReadStepReply;
  stallsAfterTheSend: PromoteReply;
};

async function fencedSendOutcomeOf(
  harness: EngineContractHarness,
  request: DispatchOrEmitRequest,
  stepValue: SerializedStepValue,
  arrangeAfterTheParentIsLocked: () => Promise<unknown> = async () => undefined,
): Promise<FencedSendOutcome> {
  const { engine } = harness;
  await onlyLockedRunOf(engine, "parent-run");
  await harness.expireLock("send-email", "parent-run");
  await engine.promote();
  const [parentRun] = await lockedRunsOf(engine, 1);
  const { task, runId, lockToken } = parentRun!;
  const parentFence = { parentTask: task, parentRunId: runId, lockToken, stepId: "send-1", stepValue };
  await arrangeAfterTheParentIsLocked();

  const reply = await engine.enqueue({ ...request, parentFence });
  const resentReply = await engine.enqueue({ ...request, parentFence });
  const readReply = await engine.readStep({ task, runId, lockToken, stepId: "send-1" });
  await harness.expireLock(task, runId);
  const stallsAfterTheSend = await engine.promote();

  return { reply, resentReply, readReply, stallsAfterTheSend };
}

function stalledParentRunOf(stalls: number): PromoteReply {
  return {
    kind: "promoted",
    promotedRunCount: 0,
    stalledRuns: [{ runId: "parent-run", task: "send-email", attempt: 1, stalls }],
    thirdStalls: [],
  };
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

async function statesOf(
  harness: EngineContractHarness,
  task: string,
  runIds: readonly RunId[],
): Promise<(RunState | undefined)[]> {
  return Promise.all(runIds.map((runId) => harness.stateOf(task, runId)));
}

const HOUR_IN_MILLISECONDS = 60 * MINUTE_IN_MILLISECONDS;

function hourlyReportOf(nextFiringAt: EpochMilliseconds, task = "build-report"): ScheduleToSync {
  return { task, cron: "0 * * * *", timezone: "UTC", nextFiringAt, retries: 2 };
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

async function suspendedOnANapOf(engine: ContractEngine, lockedRun: LockedRun): Promise<EpochMilliseconds> {
  const saveReply = await engine.saveStep({
    task: lockedRun.task,
    runId: lockedRun.runId,
    lockToken: lockedRun.lockToken,
    stepId: "nap",
    step: { kind: "sleep", durationInMilliseconds: HOUR_IN_MILLISECONDS },
  });
  const resumesAt = saveReply.kind === "saved" && saveReply.resumesAt !== undefined ? saveReply.resumesAt : 0;
  await engine.settle({ ...lockedRun, outcome: { kind: "suspend", resumesAt } });
  return resumesAt;
}

async function taskNamesOfTheNextTick(engine: ContractEngine): Promise<string[]> {
  const { taskNames } = await engine.tick({
    heldLocks: [],
    lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS,
    isPromoteIncluded: false,
  });
  if (taskNames.kind !== "replied") {
    throw taskNames.error;
  }
  return [...taskNames.reply].sort();
}

async function backdateEndedRuns(
  harness: EngineContractHarness,
  task: string,
  runIds: readonly RunId[],
  durationInMilliseconds: DurationInMilliseconds,
): Promise<void> {
  await Promise.all(runIds.map((runId) => harness.backdateEndedRun(task, runId, durationInMilliseconds)));
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
      expect(await harness.stateOf("send-email", "run-3")).toBe("pending");
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
    expect(await harness.stateOf("send-email", "run-2")).toBe("pending");
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
      expect(await harness.stateOf("send-email", "run-2")).toBe("pending");
    },
  );

  contractTest(
    "a raised limit lets a fetch lock a new run of the key and leaves the parked ones waiting, until a settle wakes as many as the new limit frees",
    async (harness) => {
      const { engine } = harness;
      await engine.enqueue({
        runs: sendEmailRunsOf(["run-1", "run-2", "run-3", "run-4"], { concurrencyKey: "account-7" }),
      });
      const [lockedRun] = (await fetchedOf(engine, 4, { "send-email": 1 })).lockedRuns;
      await engine.enqueue({ runs: sendEmailRunsOf(["run-5"], { concurrencyKey: "account-7" }) });

      const fetchedAtTheRaisedLimit = await fetchedOf(engine, 3, { "send-email": 3 });
      const parkedStatesAfterTheFetch = await statesOf(harness, "send-email", ["run-2", "run-3", "run-4"]);
      await engine.settle({ ...lockedRun!, outcome: { kind: "complete" }, concurrencyLimit: 3 });

      expect(fetchedAtTheRaisedLimit.lockedRuns.map(({ runId }) => runId)).toEqual(["run-5"]);
      expect(fetchedAtTheRaisedLimit.parkedRuns).toEqual([]);
      expect(parkedStatesAfterTheFetch).toEqual([
        "waitingForConcurrencySlot",
        "waitingForConcurrencySlot",
        "waitingForConcurrencySlot",
      ]);
      expect(await statesOf(harness, "send-email", ["run-2", "run-3", "run-4"])).toEqual([
        "pending",
        "pending",
        "waitingForConcurrencySlot",
      ]);
    },
  );

  contractTest(
    "runs stored without a concurrency key share the task's limit among themselves, not with the runs of a key",
    async (harness) => {
      const { engine } = harness;
      await engine.enqueue({ runs: sendEmailRunsOf(["run-1", "run-2"]) });
      await engine.enqueue({ runs: sendEmailRunsOf(["run-3"], { concurrencyKey: "account-7" }) });

      const fetched = await fetchedOf(engine, 3, { "send-email": 1 });

      expect(fetched.lockedRuns.map(({ runId }) => runId)).toEqual(["run-1", "run-3"]);
      expect(fetched.parkedRuns).toEqual([{ runId: "run-2", task: "send-email", attempt: 1 }]);
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
    expect(await harness.stateOf("send-email", "run-1")).toBe("completed");
  });

  contractTest("a retry backs off, then comes back pending at the next attempt once due", async (harness) => {
    const { engine } = harness;

    const reply = await retryOfTheOnlyRunOf(engine, "run-1", 0);
    const stateAfterSettle = await harness.stateOf("send-email", "run-1");
    const promoteReply = await engine.promote();
    const [retriedRun] = await lockedRunsOf(engine, 1);

    expect(reply).toEqual({ kind: "backingOff", nextAttemptAt: expect.any(Number) });
    expect(stateAfterSettle).toBe("backingOff");
    expect(promoteReply).toEqual({ kind: "promoted", promotedRunCount: 1, stalledRuns: [], thirdStalls: [] });
    expect(retriedRun).toMatchObject({ runId: "run-1", attempt: 2, isResume: false });
  });

  contractTest(
    "a retry stores its attempt's error with the server's instant of the retry, its backoff before the next attempt",
    async (harness) => {
      const reply = await retryOfTheOnlyRunOf(harness.engine, "run-1", HOUR_IN_MILLISECONDS);

      const nextAttemptAt = reply.kind === "backingOff" ? reply.nextAttemptAt : 0;
      expect(await harness.storedErrorsOf("send-email", "run-1")).toEqual([
        { ...ATTEMPT_ERROR, failedAt: new Date(nextAttemptAt - HOUR_IN_MILLISECONDS).toISOString() },
      ]);
    },
  );

  contractTest("a run failed for good stores its last error with its finishedAt as failedAt", async (harness) => {
    const { engine } = harness;
    await retryOfTheOnlyRunOf(engine, "run-1", 0);
    await engine.promote();
    const [retriedRun] = await lockedRunsOf(engine, 1);

    const reply = await engine.settle({
      ...retriedRun!,
      outcome: {
        kind: "fail",
        reason: "retries exhausted",
        error: { ...ATTEMPT_ERROR, attempt: 2 },
        failureNotice: undefined,
      },
    });

    const finishedAt = reply.kind === "failed" ? reply.finishedAt : 0;
    expect((await harness.storedErrorsOf("send-email", "run-1")).at(-1)).toEqual({
      ...ATTEMPT_ERROR,
      attempt: 2,
      failedAt: new Date(finishedAt).toISOString(),
    });
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
      expect(await harness.stateOf("send-email", "run-1")).toBe("failed");
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
    "a hand back of a run first found unknown 30 minutes ago fails it unknown task and stores its notice, and its resend hears that failure, not already",
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
      await harness.backdateFirstFoundUnknownAt("send-email", "run-1", 30 * MINUTE_IN_MILLISECONDS);
      const [lockedRun] = await lockedRunsOf(engine, 1);

      const reply = await handBack(lockedRun!);
      const resentReply = await handBack(lockedRun!);

      expect(reply).toEqual({ kind: "failed", reason: "unknown task", finishedAt: expect.any(Number) });
      expect(resentReply).toEqual(reply);
      expect(await statesOf(harness, "send-email", ["run-1"])).toEqual(["failed"]);
      expect(await statesOf(harness, "alert-on-call", ["listener-run-1"])).toEqual(["pending"]);
    },
  );

  contractTest("a renew of a held lock renews it, and of a lock its settle ended replies stale", async (harness) => {
    const { engine } = harness;
    await engine.enqueue({ runs: sendEmailRunsOf(["run-1", "run-2"]) });
    const [heldRun, settledRun] = await lockedRunsOf(engine, 2);
    await engine.settle({ ...settledRun!, outcome: { kind: "complete" } });

    const renewals = await engine.renew({
      heldLocks: [heldRun!, settledRun!].map(({ task, runId, lockToken }) => ({ task, runId, lockToken })),
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
        await harness.expireLock("send-email", "run-1");
        stallsByPromote.push(await engine.promote());
        lockedRun = (await lockedRunsOf(engine, 1))[0]!;
        expect(lockedRun.runId, `the fetch after stall ${String(expectedStalls)}`).toBe("run-1");
      }
      await harness.expireLock("send-email", "run-1");

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
      expect(await harness.stateOf("send-email", "run-1")).toBe("executing");
    },
  );

  contractTest("a stalled run and a third stall carry the source event of their run", async (harness) => {
    const { engine } = harness;
    const sourceEvent = { eventName: "user.signed-up", eventId: "event-1" };
    await engine.enqueue({ runs: sendEmailRunsOf(["run-1"], { sourceEvent }) });
    const stalledRuns: unknown[] = [];
    for (let stallCount = 0; stallCount < 2; stallCount += 1) {
      await lockedRunsOf(engine, 1);
      await harness.expireLock("send-email", "run-1");
      const reply = await engine.promote();
      stalledRuns.push(...(reply.kind === "promoted" ? reply.stalledRuns : []));
    }
    await lockedRunsOf(engine, 1);
    await harness.expireLock("send-email", "run-1");

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
        await harness.expireLock("send-email", "run-1");
        await engine.promote();
      }
      await lockedRunsOf(engine, 1);
      await harness.expireLock("send-email", "run-1");
      const promoteReply = await engine.promote();
      const [thirdStall] = promoteReply.kind === "promoted" ? promoteReply.thirdStalls : [];
      const failStalled = {
        task: thirdStall!.task,
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
      expect(await statesOf(harness, "send-email", ["run-1"])).toEqual(["failed"]);
      expect(await statesOf(harness, "alert-on-call", ["listener-run-1"])).toEqual(["pending"]);
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
        await harness.expireLock("send-email", "stalled-run");

        const reply = await engine.tick({
          heldLocks: [{ task: heldRun!.task, runId: heldRun!.runId, lockToken: heldRun!.lockToken }],
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
        expect(await harness.stateOf("send-email", "stalled-run")).toBe(isPromoteIncluded ? "pending" : "executing");
      },
    );
  }

  contractTest(
    "a saved step reads back under its lock, and a step call after the settle hears stale",
    async (harness) => {
      const { engine } = harness;
      const lockedRun = await onlyLockedRunOf(engine, "run-1");
      const fence = { task: lockedRun.task, runId: lockedRun.runId, lockToken: lockedRun.lockToken, stepId: "charge" };

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

      expect(repeatedRunReply).toEqual({
        kind: "enqueued",
        createdRuns: sendEmailRunIdentitiesOf(["run-2"]),
        skippedRuns: sendEmailRunIdentitiesOf(["run-1"]),
      });
      expect(firstEmitReply).toEqual({
        kind: "enqueued",
        createdRuns: sendEmailRunIdentitiesOf(["listener-run"]),
        skippedRuns: [],
      });
      expect(repeatedEmitReply).toEqual({ kind: "duplicate" });
    },
  );

  contractTest(
    "a handler emit of no run saves its step, resets stalls and leaves its event id free for an emit with listeners",
    async (harness) => {
      const { engine } = harness;
      const eventDedup = { eventName: "user.signed-up", eventId: "event-1" };

      const outcome = await fencedSendOutcomeOf(harness, { runs: [], eventDedup }, '"event-1"');
      const laterEmitReply = await engine.enqueue({ runs: sendEmailRunsOf(["listener-run"]), eventDedup });

      expect(outcome).toEqual({
        reply: { kind: "enqueued", createdRuns: [], skippedRuns: [] },
        resentReply: { kind: "alreadySent", savedValue: '"event-1"' },
        readReply: { kind: "read", savedValue: '"event-1"' },
        stallsAfterTheSend: stalledParentRunOf(1),
      });
      expect(laterEmitReply).toEqual({
        kind: "enqueued",
        createdRuns: sendEmailRunIdentitiesOf(["listener-run"]),
        skippedRuns: [],
      });
    },
  );

  contractTest(
    "a handler emit whose event id is live creates nothing, replies duplicate, saves its step and resets stalls",
    async (harness) => {
      const { engine } = harness;
      const eventDedup = { eventName: "user.signed-up", eventId: "event-1" };

      const outcome = await fencedSendOutcomeOf(
        harness,
        { runs: sendEmailRunsOf(["repeated-listener-run"]), eventDedup },
        '"event-1"',
        () => engine.enqueue({ runs: sendEmailRunsOf(["listener-run"]), eventDedup }),
      );

      expect(outcome).toEqual({
        reply: { kind: "duplicate" },
        resentReply: { kind: "alreadySent", savedValue: '"event-1"' },
        readReply: { kind: "read", savedValue: '"event-1"' },
        stallsAfterTheSend: stalledParentRunOf(1),
      });
      expect(await harness.stateOf("send-email", "repeated-listener-run")).toBeUndefined();
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
      await backdateEndedRuns(harness, "send-email", ["completed-run", "failed-run"], DAY_IN_MILLISECONDS);
      await harness.backdateEndedRun(
        "send-email",
        "younger-completed-run",
        DAY_IN_MILLISECONDS - MINUTE_IN_MILLISECONDS,
      );

      const dayReply = await engine.prune(SEND_EMAIL_PRUNE_REQUEST);
      const statesAfterADay = await statesOf(harness, "send-email", [
        "completed-run",
        "younger-completed-run",
        "failed-run",
      ]);
      await harness.backdateEndedRun("send-email", "failed-run", 13 * DAY_IN_MILLISECONDS);
      const fortnightReply = await engine.prune(SEND_EMAIL_PRUNE_REQUEST);

      expect(dayReply).toEqual({ kind: "pruned", prunedRunCount: 1 });
      expect(statesAfterADay).toEqual([undefined, "completed", "failed"]);
      expect(fortnightReply).toEqual({ kind: "pruned", prunedRunCount: 1 });
      expect(await harness.stateOf("send-email", "failed-run")).toBeUndefined();
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
    expect(await statesOf(harness, "send-email", completedRunIds)).toEqual([
      undefined,
      undefined,
      ...Array(3).fill("completed"),
    ]);
    expect(await statesOf(harness, "send-email", failedRunIds)).toEqual([undefined, undefined, "failed", "failed"]);
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
      await backdateEndedRuns(
        harness,
        "send-email",
        [...agedCompletedRunIds, ...agedFailedRunIds],
        15 * DAY_IN_MILLISECONDS,
      );
      const pruneRequest = {
        ...SEND_EMAIL_PRUNE_REQUEST,
        completedRetention: { ...SEND_EMAIL_PRUNE_REQUEST.completedRetention, maxRunCount: 0 },
      };

      const firstReply = await engine.prune(pruneRequest);
      const failedStatesAfterTheFirstPrune = await statesOf(harness, "send-email", agedFailedRunIds);
      const completedStatesAfterTheFirstPrune = await statesOf(harness, "send-email", completedRunIdsOverTheCap);
      const secondReply = await engine.prune(pruneRequest);

      expect(firstReply).toEqual({ kind: "pruned", prunedRunCount: MOST_RUNS_PRUNED_PER_PRUNE });
      expect(await statesOf(harness, "send-email", agedCompletedRunIds)).toEqual(Array(150).fill(undefined));
      expect(failedStatesAfterTheFirstPrune).toEqual([...Array(50).fill(undefined), ...Array(50).fill("failed")]);
      expect(completedStatesAfterTheFirstPrune).toEqual(Array(50).fill("completed"));
      expect(secondReply).toEqual({ kind: "pruned", prunedRunCount: 100 });
      expect(await statesOf(harness, "send-email", [...agedFailedRunIds, ...completedRunIdsOverTheCap])).toEqual(
        Array(150).fill(undefined),
      );
      expect(await harness.stateOf("resize-image", "other-task-run")).toBe("pending");
    },
  );

  contractTest(
    "a pruned run's id is skipped while its dedup entry is live, and starts a new run once it expired",
    async (harness) => {
      const { engine } = harness;
      await endRuns(engine, "send-email", ["run-ended-24h01-ago", "run-ended-23h59-ago"], "complete");
      await harness.backdateEndedRun("send-email", "run-ended-24h01-ago", DAY_IN_MILLISECONDS + MINUTE_IN_MILLISECONDS);
      await harness.backdateEndedRun("send-email", "run-ended-23h59-ago", DAY_IN_MILLISECONDS - MINUTE_IN_MILLISECONDS);
      await engine.prune({
        ...SEND_EMAIL_PRUNE_REQUEST,
        completedRetention: { ...SEND_EMAIL_PRUNE_REQUEST.completedRetention, maxRunCount: 0 },
      });

      const reply = await engine.enqueue({ runs: sendEmailRunsOf(["run-ended-24h01-ago", "run-ended-23h59-ago"]) });

      expect(reply).toEqual({
        kind: "enqueued",
        createdRuns: sendEmailRunIdentitiesOf(["run-ended-24h01-ago"]),
        skippedRuns: sendEmailRunIdentitiesOf(["run-ended-23h59-ago"]),
      });
    },
  );

  contractTest(
    "a task whose last run a prune removed, its dedup entry expired, is absent from the next tick's task names",
    async (harness) => {
      const { engine } = harness;
      await endRuns(engine, "send-email", ["aged-run"], "complete");
      await engine.enqueue({ runs: [{ runId: "other-task-run", task: "resize-image", payload: "{}", retries: 0 }] });
      await harness.backdateEndedRun("send-email", "aged-run", 2 * DAY_IN_MILLISECONDS);
      const taskNamesBeforeThePrune = await taskNamesOfTheNextTick(engine);

      const reply = await engine.prune(SEND_EMAIL_PRUNE_REQUEST);

      expect(taskNamesBeforeThePrune).toEqual(["resize-image", "send-email"]);
      expect(reply).toEqual({ kind: "pruned", prunedRunCount: 1 });
      expect(await taskNamesOfTheNextTick(engine)).toEqual(["resize-image"]);
    },
  );

  const arrangementsOfAHeldRunByState: [RunState, (engine: ContractEngine) => Promise<unknown>][] = [
    [
      "delayed",
      (engine) =>
        engine.enqueue({ runs: sendEmailRunsOf(["held-run"], { delayInMilliseconds: HOUR_IN_MILLISECONDS }) }),
    ],
    ["pending", (engine) => engine.enqueue({ runs: sendEmailRunsOf(["held-run"]) })],
    [
      "waitingForConcurrencySlot",
      async (engine) => {
        await engine.enqueue({ runs: sendEmailRunsOf(["locked-run", "held-run"], { concurrencyKey: "account-7" }) });
        await fetchedOf(engine, 2, { "send-email": 1 });
      },
    ],
    ["executing", (engine) => onlyLockedRunOf(engine, "held-run")],
    ["suspended", async (engine) => suspendedOnANapOf(engine, await onlyLockedRunOf(engine, "held-run"))],
    [
      "backingOff",
      async (engine) =>
        engine.settle({
          ...(await onlyLockedRunOf(engine, "held-run")),
          outcome: { kind: "retry", error: ATTEMPT_ERROR, backoffInMilliseconds: HOUR_IN_MILLISECONDS },
        }),
    ],
  ];
  for (const [state, arrangeTheHeldRun] of arrangementsOfAHeldRunByState) {
    contractTest(
      `a task whose ended run a prune removed stays in the task names while it holds a run in state ${state}`,
      async (harness) => {
        const { engine } = harness;
        await endRuns(engine, "send-email", ["aged-run"], "complete");
        await harness.backdateEndedRun("send-email", "aged-run", 2 * DAY_IN_MILLISECONDS);
        await arrangeTheHeldRun(engine);

        const reply = await engine.prune(SEND_EMAIL_PRUNE_REQUEST);

        expect(reply).toEqual({ kind: "pruned", prunedRunCount: 1 });
        expect(await harness.stateOf("send-email", "held-run")).toBe(state);
        expect(await taskNamesOfTheNextTick(engine)).toEqual(["send-email"]);
      },
    );
  }

  contractTest(
    "a task whose last run a prune removed stays in the task names while the run's dedup entry is live",
    async (harness) => {
      const { engine } = harness;
      await endRuns(engine, "send-email", ["recent-run"], "complete");

      const reply = await engine.prune({
        ...SEND_EMAIL_PRUNE_REQUEST,
        completedRetention: { ...SEND_EMAIL_PRUNE_REQUEST.completedRetention, maxRunCount: 0 },
      });

      expect(reply).toEqual({ kind: "pruned", prunedRunCount: 1 });
      expect(await harness.stateOf("send-email", "recent-run")).toBeUndefined();
      expect(await taskNamesOfTheNextTick(engine)).toEqual(["send-email"]);
    },
  );

  contractTest(
    "a dispatch of a task whose name left the task names puts it back, and the next prune of the task works as before",
    async (harness) => {
      const { engine } = harness;
      await endRuns(engine, "send-email", ["aged-run"], "complete");
      await harness.backdateEndedRun("send-email", "aged-run", 2 * DAY_IN_MILLISECONDS);
      await engine.prune(SEND_EMAIL_PRUNE_REQUEST);
      const taskNamesAfterTheNameLeft = await taskNamesOfTheNextTick(engine);

      await endRuns(engine, "send-email", ["new-run"], "complete");
      const taskNamesAfterTheDispatch = await taskNamesOfTheNextTick(engine);
      await harness.backdateEndedRun("send-email", "new-run", 2 * DAY_IN_MILLISECONDS);
      const reply = await engine.prune(SEND_EMAIL_PRUNE_REQUEST);

      expect(taskNamesAfterTheNameLeft).toEqual([]);
      expect(taskNamesAfterTheDispatch).toEqual(["send-email"]);
      expect(reply).toEqual({ kind: "pruned", prunedRunCount: 1 });
      expect(await harness.stateOf("send-email", "new-run")).toBeUndefined();
      expect(await taskNamesOfTheNextTick(engine)).toEqual([]);
    },
  );

  contractTest(
    "a firing of a scheduled task whose name left the task names puts it back with the run it creates",
    async (harness) => {
      const { engine } = harness;
      const firstSchedule = hourlyReportOf((await harness.pastInstant()) - 3 * HOUR_IN_MILLISECONDS);
      await engine.syncSchedules({ schedulesToSync: [firstSchedule] });
      await engine.fire(firingOf(firstSchedule));
      const [firedRun] = await lockedRunsOf(engine, 1);
      await engine.settle({ ...firedRun!, outcome: { kind: "complete" } });
      await harness.backdateEndedRun("build-report", firedRun!.runId, 2 * DAY_IN_MILLISECONDS);
      await engine.prune({ ...SEND_EMAIL_PRUNE_REQUEST, task: "build-report" });
      const taskNamesAfterTheNameLeft = await taskNamesOfTheNextTick(engine);
      const nextFiring = firingOf(hourlyReportOf(firstSchedule.nextFiringAt + HOUR_IN_MILLISECONDS));

      const reply = await engine.fire(nextFiring);

      expect(taskNamesAfterTheNameLeft).toEqual([]);
      expect(reply).toEqual({ kind: "fired", runId: nextFiring.runId, isRunCreated: true });
      expect(await taskNamesOfTheNextTick(engine)).toEqual(["build-report"]);
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
    "a sync where only the retries differ reports no change and keeps the schedule's planned firing",
    async (harness) => {
      const { engine } = harness;
      const schedule = hourlyReportOf(await harness.pastInstant());
      await engine.syncSchedules({ schedulesToSync: [schedule] });

      const reply = await engine.syncSchedules({
        schedulesToSync: [{ ...schedule, retries: 7, nextFiringAt: schedule.nextFiringAt + HOUR_IN_MILLISECONDS }],
      });
      const fireReply = await engine.fire(firingOf(schedule));

      expect(reply).toEqual({ kind: "synced", added: [], changed: [], removed: [] });
      expect(fireReply).toMatchObject({ kind: "fired", isRunCreated: true });
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

      const { task, cron, timezone, nextFiringAt } = dueReport;
      expect(reply.dueSchedules).toEqual(
        repliedOf({ serverInstant: expect.any(Number), dueSchedules: [{ task, cron, timezone, nextFiringAt }] }),
      );
      const serverInstant = reply.dueSchedules?.kind === "replied" ? reply.dueSchedules.reply.serverInstant : 0;
      expect(serverInstant).toBeGreaterThan(pastInstant);
    },
  );

  contractTest(
    "a makeDue makes a delayed and a sleeping run pending at once, and the sleep it woke reads as woken on the fetch",
    async (harness) => {
      const { engine } = harness;
      await suspendedOnANapOf(engine, await onlyLockedRunOf(engine, "sleeping-run"));
      await engine.enqueue({
        runs: sendEmailRunsOf(["delayed-run"], { delayInMilliseconds: 2 * HOUR_IN_MILLISECONDS }),
      });

      const reply = await engine.makeDue();
      const states = await statesOf(harness, "send-email", ["sleeping-run", "delayed-run"]);
      const wokenRun = (await lockedRunsOf(engine, 2)).find(({ runId }) => runId === "sleeping-run");

      expect(reply).toEqual({
        kind: "madeDue",
        madeDueRuns: [
          { task: "send-email", runId: "sleeping-run" },
          { task: "send-email", runId: "delayed-run" },
        ],
      });
      expect(states).toEqual(["pending", "pending"]);
      expect(wokenRun).toMatchObject({ isResume: true, sleepsWokenAt: expect.any(Number) });
      expect(await engine.readStep({ ...wokenRun!, stepId: "nap" })).toEqual({
        kind: "read",
        savedValue: String(wokenRun!.sleepsWokenAt),
      });
    },
  );

  contractTest(
    "a makeDue hands back runs due at the same instant in the byte order of their stored ids, as Redis orders them",
    async (harness) => {
      const { engine } = harness;
      await engine.enqueue({
        runs: sendEmailRunsOf(["\u{1F680}", "～"], { delayInMilliseconds: HOUR_IN_MILLISECONDS }),
      });

      const reply = await engine.makeDue();

      expect(reply).toEqual({ kind: "madeDue", madeDueRuns: sendEmailRunIdentitiesOf(["～", "\u{1F680}"]) });
    },
  );

  contractTest(
    `a makeDue given a task moves only its runs, even behind more than ${String(MOST_DUE_RUNS_PROMOTED_PER_PROMOTE)} timed runs of another task`,
    async (harness) => {
      const { engine } = harness;
      const emailRunIds = Array.from(
        { length: MOST_DUE_RUNS_PROMOTED_PER_PROMOTE + 1 },
        (_, position) => `email-run-${String(position)}`,
      );
      await engine.enqueue({ runs: sendEmailRunsOf(emailRunIds, { delayInMilliseconds: MINUTE_IN_MILLISECONDS }) });
      await engine.enqueue({
        runs: [
          {
            runId: "report-run",
            task: "build-report",
            payload: "{}",
            retries: 2,
            delayInMilliseconds: HOUR_IN_MILLISECONDS,
          },
        ],
      });

      const reply = await engine.makeDue({ task: "build-report" });

      expect(reply).toEqual({ kind: "madeDue", madeDueRuns: [{ task: "build-report", runId: "report-run" }] });
      expect(await statesOf(harness, "build-report", ["report-run"])).toEqual(["pending"]);
      expect(await statesOf(harness, "send-email", ["email-run-0"])).toEqual(["delayed"]);
    },
  );

  const pickupsOfALockResent: {
    pickup: string;
    pickedUpRunOf: (harness: EngineContractHarness) => Promise<void>;
    expectedPickup: Partial<LockedRun>;
  }[] = [
    {
      pickup: "after a suspend, as a resume",
      pickedUpRunOf: async (harness) => {
        const lockedRun = await onlyLockedRunOf(harness.engine, "run-1");
        await harness.engine.settle({
          ...lockedRun,
          outcome: { kind: "suspend", resumesAt: await harness.pastInstant() },
        });
        await harness.engine.promote();
      },
      expectedPickup: { isResume: true },
    },
    {
      pickup: "after a makeDue woke it, with the same sleepsWokenAt",
      pickedUpRunOf: async ({ engine }) => {
        await suspendedOnANapOf(engine, await onlyLockedRunOf(engine, "run-1"));
        await engine.makeDue();
      },
      expectedPickup: { isResume: true, sleepsWokenAt: expect.any(Number) as number },
    },
    {
      pickup: "after a retry of a woken lock, as a new attempt with nothing of the previous lock",
      pickedUpRunOf: async ({ engine }) => {
        await suspendedOnANapOf(engine, await onlyLockedRunOf(engine, "run-1"));
        await engine.makeDue();
        const [wokenRun] = await lockedRunsOf(engine, 1);
        await engine.settle({
          ...wokenRun!,
          outcome: { kind: "retry", error: ATTEMPT_ERROR, backoffInMilliseconds: 0 },
        });
        await engine.promote();
      },
      expectedPickup: { isResume: false, attempt: 2 },
    },
    {
      pickup: "after a stall of a woken lock, with nothing of the previous lock",
      pickedUpRunOf: async (harness) => {
        const { engine } = harness;
        await suspendedOnANapOf(engine, await onlyLockedRunOf(engine, "run-1"));
        await engine.makeDue();
        await lockedRunsOf(engine, 1);
        await harness.expireLock("send-email", "run-1");
        await engine.promote();
      },
      expectedPickup: { isResume: false, attempt: 1 },
    },
  ];
  for (const { pickup, pickedUpRunOf, expectedPickup } of pickupsOfALockResent) {
    contractTest(
      `a fetch resent with the same tokens answers a pickup ${pickup}, as its first send did`,
      async (harness) => {
        const { engine } = harness;
        await pickedUpRunOf(harness);
        const request = {
          lockTokens: freshLockTokensOf(1),
          concurrencyLimitsByTask: {},
          lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS,
        };

        const firstReply = await engine.fetch(request);
        const resentReply = await engine.fetch(request);

        expect(firstReply).toMatchObject({ kind: "fetched", lockedRuns: [{ runId: "run-1", ...expectedPickup }] });
        expect(resentReply).toEqual(firstReply);
        if (expectedPickup.sleepsWokenAt === undefined) {
          expect(firstReply.kind === "fetched" ? firstReply.lockedRuns[0] : undefined).not.toHaveProperty(
            "sleepsWokenAt",
          );
        }
      },
    );
  }

  contractTest("the server instant is the engine's clock, after an instant already past", async (harness) => {
    const pastInstant = await harness.pastInstant();

    const serverInstant = await harness.engine.serverInstant();

    expect(serverInstant).toBeGreaterThan(pastInstant);
    expect(serverInstant).toBeLessThan(pastInstant + MINUTE_IN_MILLISECONDS);
  });

  contractTest("a tick carrying a prune prunes its task and returns the prune's reply", async (harness) => {
    const { engine } = harness;
    await endRuns(engine, "send-email", ["aged-run"], "complete");
    await harness.backdateEndedRun("send-email", "aged-run", DAY_IN_MILLISECONDS);

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
    expect(await harness.stateOf("send-email", "aged-run")).toBeUndefined();
  });

  contractTest(
    `a tick's prune reports the runs it removed: all ${String(MOST_RUNS_PRUNED_PER_PRUNE)} of its budget while more than a batch is over the cap, fewer on the prune that reaches it`,
    async (harness) => {
      const { engine } = harness;
      const overflowingRunCount = MOST_RUNS_PRUNED_PER_PRUNE + 50;
      await endRuns(engine, "send-email", runIdsOf("completed-run", overflowingRunCount), "complete");
      const tickRequest = {
        heldLocks: [],
        lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS,
        prune: {
          ...SEND_EMAIL_PRUNE_REQUEST,
          completedRetention: { ...SEND_EMAIL_PRUNE_REQUEST.completedRetention, maxRunCount: 0 },
        },
      };

      const pruneReplies = [];
      for (let tickCount = 0; tickCount < 3; tickCount += 1) {
        pruneReplies.push((await engine.tick(tickRequest)).prune);
      }

      expect(pruneReplies).toEqual([
        repliedOf({ kind: "pruned", prunedRunCount: MOST_RUNS_PRUNED_PER_PRUNE }),
        repliedOf({ kind: "pruned", prunedRunCount: overflowingRunCount - MOST_RUNS_PRUNED_PER_PRUNE }),
        repliedOf({ kind: "pruned", prunedRunCount: 0 }),
      ]);
    },
  );

  const SHARED_RUN_IDENTITIES: RunIdentity[] = [
    { task: "send-receipt", runId: "order-1" },
    { task: "update-stock", runId: "order-1" },
  ];

  const sharedRunsOf = (extraFields: Partial<RunToCreate> = {}): RunToCreate[] =>
    SHARED_RUN_IDENTITIES.flatMap(({ task, runId }) => orderRunsOf(task, [runId], extraFields));

  const lockedSharedRunsOf = async (engine: ContractEngine): Promise<[LockedRun, LockedRun]> => {
    await engine.enqueue({ runs: sharedRunsOf() });
    const lockedRuns = await lockedRunsOf(engine, 2);
    const lockedRunOfTask = (task: string) => lockedRuns.find((lockedRun) => lockedRun.task === task)!;
    return [lockedRunOfTask("send-receipt"), lockedRunOfTask("update-stock")];
  };

  contractTest(
    "the same run id on two tasks is two runs, and a repeat on either task is skipped while its run exists",
    async (harness) => {
      const { engine } = harness;

      const firstReply = await engine.enqueue({ runs: sharedRunsOf() });
      const repeatedReply = await engine.enqueue({ runs: sharedRunsOf() });

      expect(firstReply).toEqual({ kind: "enqueued", createdRuns: SHARED_RUN_IDENTITIES, skippedRuns: [] });
      expect(repeatedReply).toEqual({ kind: "enqueued", createdRuns: [], skippedRuns: SHARED_RUN_IDENTITIES });
      expect(await harness.stateOf("send-receipt", "order-1")).toBe("pending");
      expect(await harness.stateOf("update-stock", "order-1")).toBe("pending");
    },
  );

  contractTest(
    "two runs sharing an id on two tasks are each fetched, renewed, given a step and settled on their own",
    async (harness) => {
      const { engine } = harness;
      const [receiptRun, stockRun] = await lockedSharedRunsOf(engine);
      const receiptFence = { ...receiptRun, stepId: "charge" };
      const stockFence = { ...stockRun, stepId: "charge" };

      const renewals = await engine.renew({
        heldLocks: [receiptRun, stockRun],
        lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS,
      });
      await engine.saveStep({ ...receiptFence, step: { kind: "value", value: '["receipt"]' } });
      await engine.saveStep({ ...stockFence, step: { kind: "value", value: '["stock"]' } });
      const readReplies = [await engine.readStep(receiptFence), await engine.readStep(stockFence)];
      const crossedReadReply = await engine.readStep({ ...receiptFence, lockToken: stockRun.lockToken });
      const receiptSettleReply = await engine.settle({ ...receiptRun, outcome: { kind: "complete" } });
      const stockStateAfterTheReceipt = await harness.stateOf("update-stock", "order-1");
      const stockReadReply = await engine.readStep(stockFence);
      const stockSettleReply = await engine.settle({
        ...stockRun,
        outcome: { kind: "fail", reason: "non-retriable", error: ATTEMPT_ERROR, failureNotice: undefined },
      });

      expect([receiptRun, stockRun].map(({ task, runId }) => ({ task, runId }))).toEqual(SHARED_RUN_IDENTITIES);
      expect(renewals).toEqual([
        { kind: "renewed", lockedUntil: expect.any(Number) },
        { kind: "renewed", lockedUntil: expect.any(Number) },
      ]);
      expect(readReplies).toEqual([
        { kind: "read", savedValue: '["receipt"]' },
        { kind: "read", savedValue: '["stock"]' },
      ]);
      expect(crossedReadReply).toEqual({ kind: "stale" });
      expect(receiptSettleReply).toMatchObject({ kind: "completed" });
      expect(stockStateAfterTheReceipt).toBe("executing");
      expect(stockReadReply).toEqual({ kind: "read", savedValue: '["stock"]' });
      expect(stockSettleReply).toMatchObject({ kind: "failed", reason: "non-retriable" });
      expect(await harness.stateOf("send-receipt", "order-1")).toBe("completed");
    },
  );

  contractTest(
    "a promote reports the stalls of two tasks sharing a run id, each under its task and the consumer's run id",
    async (harness) => {
      const { engine } = harness;
      await lockedSharedRunsOf(engine);
      for (const { task, runId } of SHARED_RUN_IDENTITIES) {
        await harness.expireLock(task, runId);
      }

      const reply = await engine.promote();

      expect(reply.kind === "promoted" ? reply.stalledRuns : []).toEqual(
        expect.arrayContaining(SHARED_RUN_IDENTITIES.map((identity) => ({ ...identity, attempt: 1, stalls: 1 }))),
      );
      expect(reply).toMatchObject({ stalledRuns: [{}, {}] });
    },
  );

  contractTest(
    "a shared run id is skipped on the task whose dedup entry is live and starts a run on the task whose entry expired",
    async (harness) => {
      const { engine } = harness;
      for (const { task, runId } of SHARED_RUN_IDENTITIES) {
        await endRuns(engine, task, [runId], "complete");
      }
      await harness.backdateEndedRun("send-receipt", "order-1", DAY_IN_MILLISECONDS + MINUTE_IN_MILLISECONDS);
      await harness.backdateEndedRun("update-stock", "order-1", DAY_IN_MILLISECONDS - MINUTE_IN_MILLISECONDS);
      for (const { task } of SHARED_RUN_IDENTITIES) {
        await engine.prune({
          ...SEND_EMAIL_PRUNE_REQUEST,
          task,
          completedRetention: { ...SEND_EMAIL_PRUNE_REQUEST.completedRetention, maxRunCount: 0 },
        });
      }

      const reply = await engine.enqueue({ runs: sharedRunsOf() });

      expect(reply).toEqual({
        kind: "enqueued",
        createdRuns: [SHARED_RUN_IDENTITIES[0]],
        skippedRuns: [SHARED_RUN_IDENTITIES[1]],
      });
    },
  );

  contractTest(
    "a send fenced by a parent of one task hears stale under the lock token of another task's run of the same id",
    async (harness) => {
      const { engine } = harness;
      const [receiptRun, stockRun] = await lockedSharedRunsOf(engine);
      const childRuns = orderRunsOf("notify-customer", ["child-run"]);
      const parentFence = {
        parentTask: receiptRun.task,
        parentRunId: receiptRun.runId,
        stepId: "send-1",
        stepValue: '"child-run"',
      };

      const crossedReply = await engine.enqueue({
        runs: childRuns,
        parentFence: { ...parentFence, lockToken: stockRun.lockToken },
      });
      const childStateAfterTheCrossedSend = await harness.stateOf("notify-customer", "child-run");
      const fencedReply = await engine.enqueue({
        runs: childRuns,
        parentFence: { ...parentFence, lockToken: receiptRun.lockToken },
      });

      expect(crossedReply).toEqual({ kind: "stale" });
      expect(childStateAfterTheCrossedSend).toBeUndefined();
      expect(fencedReply).toEqual({
        kind: "enqueued",
        createdRuns: [{ task: "notify-customer", runId: "child-run" }],
        skippedRuns: [],
      });
    },
  );
}
