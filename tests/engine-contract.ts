import { expect } from "vitest";

import { runFailed } from "../src/built-in-events.js";
import type { EpochMilliseconds } from "../src/durations.js";
import type {
  Engine,
  FailureNotice,
  FetchedReply,
  ImplementedEngine,
  LockedRun,
  RunToCreate,
} from "../src/engine/engine.js";
import type { RunId } from "../src/ids.js";
import type { ErrorRecord } from "../src/run-queue.js";
import type { RunState } from "../src/run-states.js";
import { freshLockTokensOf } from "./lock-tokens.js";

export type ContractEngine = Pick<
  Engine,
  "enqueue" | "fetch" | "renew" | "settle" | "promote" | "saveStep" | "readStep"
> & { tick: ImplementedEngine["tick"] };

export type EngineContractHarness = {
  engine: ContractEngine;
  expireLock: (runId: RunId) => Promise<void>;
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

  contractTest("a yield puts the run back at the front of pending", async (harness) => {
    const { engine } = harness;
    await engine.enqueue({ runs: sendEmailRunsOf(["run-1", "run-2"]) });
    const [lockedRun] = await lockedRunsOf(engine, 1);

    const reply = await engine.settle({ ...lockedRun!, outcome: { kind: "yield" } });
    const [nextLockedRun] = await lockedRunsOf(engine, 1);

    expect(reply).toEqual({ kind: "yielded" });
    expect(nextLockedRun).toMatchObject({ runId: "run-1", attempt: 1, isResume: false });
  });

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
        thirdStalls: [{ runId: "run-1", task: "send-email", attempt: 1, expiredLockToken: lockedRun.lockToken }],
      });
      expect(await harness.stateOf("run-1")).toBe("executing");
    },
  );

  contractTest("a tick promotes, renews the held locks and lists the task names in one reply", async (harness) => {
    const { engine } = harness;
    await engine.enqueue({ runs: sendEmailRunsOf(["held-run", "stalled-run"]) });
    const [heldRun] = await lockedRunsOf(engine, 2);
    await harness.expireLock("stalled-run");

    const reply = await engine.tick({
      heldLocks: [{ runId: heldRun!.runId, lockToken: heldRun!.lockToken }],
      lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS,
    });

    expect(reply).toEqual({
      promote: {
        kind: "promoted",
        promotedRunCount: 0,
        stalledRuns: [{ runId: "stalled-run", task: "send-email", attempt: 1, stalls: 1 }],
        thirdStalls: [],
      },
      renewals: [{ kind: "renewed", lockedUntil: expect.any(Number) }],
      prune: undefined,
      taskNames: ["send-email"],
    });
  });

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
}
