import { expect } from "vitest";

import { engineTest, fetchedLockedRunsOf, storedValuesByKeyOf } from "../../tests/engine-test.js";
import type { EngineTestContext } from "../../tests/engine-test.js";
import { serverInstantInMillisecondsOf } from "../../tests/server-instant.js";
import type { DurationInMilliseconds, EpochMilliseconds } from "../durations.js";
import {
  CUT_ERROR_TEXT_MARKER,
  DEDUP_WINDOW_IN_MILLISECONDS,
  UNKNOWN_TASK_FAILURE_DELAY_IN_MILLISECONDS,
} from "./engine.js";
import type { FailureNotice, LockedRun, SettleOutcome, SettleReply } from "./engine.js";
import { storedRunIdOf } from "./engine-keys.js";

const BACKOFF_IN_MILLISECONDS = 30_000;

const RESUMES_AT = Date.UTC(2030, 0, 1);

const ATTEMPT_ERROR = { attempt: 1, name: "TypeError", message: "boom", stack: "TypeError: boom", failedAt: "" };

const COMPLETE: SettleOutcome = { kind: "complete" };

const YIELD: SettleOutcome = { kind: "yield" };

const UNKNOWN_TASK_ERROR = { attempt: 1, name: "UnknownTaskError", message: "unknown task", stack: "", failedAt: "" };

const HAND_BACK: SettleOutcome = { kind: "handBack", error: UNKNOWN_TASK_ERROR, failureNotice: undefined };

const RETRY: SettleOutcome = { kind: "retry", error: ATTEMPT_ERROR, backoffInMilliseconds: BACKOFF_IN_MILLISECONDS };

const SUSPEND: SettleOutcome = { kind: "suspend", resumesAt: RESUMES_AT };

const NOTICE_EVENT = { eventName: "nextasking/run.failed", eventId: "notice-event-1" };

const FAILURE_NOTICE: FailureNotice = {
  payload: {
    task: "send-email",
    runId: "run-1",
    reason: "retries exhausted",
    error: { name: ATTEMPT_ERROR.name, message: ATTEMPT_ERROR.message, stack: ATTEMPT_ERROR.stack },
    attempt: 1,
  },
  runs: [
    { runId: "notice-run-1", task: "alert-ops", retries: 2, sourceEvent: NOTICE_EVENT },
    { runId: "notice-run-2", task: "page-on-call", retries: 0, concurrencyKey: "team-a", sourceEvent: NOTICE_EVENT },
  ],
  eventDedup: NOTICE_EVENT,
};

const FAIL: SettleOutcome = {
  kind: "fail",
  reason: "retries exhausted",
  error: ATTEMPT_ERROR,
  failureNotice: FAILURE_NOTICE,
};

const OUTCOMES = [COMPLETE, YIELD, HAND_BACK, RETRY, SUSPEND, FAIL];

async function enqueueSendEmailRuns(
  { engine }: EngineTestContext,
  count: number,
  concurrencyKey?: string,
): Promise<void> {
  await engine.enqueue({
    runs: Array.from({ length: count }, (_, position) => ({
      runId: `run-${String(position + 1)}`,
      task: "send-email",
      payload: "{}",
      retries: 0,
      ...(concurrencyKey === undefined ? {} : { concurrencyKey }),
    })),
  });
}

function sendEmailStoredRunIdsOf(runIds: readonly string[]): string[] {
  return runIds.map((runId) => storedRunIdOf("send-email", runId));
}

async function lockedRunOf(context: EngineTestContext): Promise<LockedRun> {
  await enqueueSendEmailRuns(context, 1);
  const [lockedRun] = await fetchedLockedRunsOf(context, 1);
  return lockedRun!;
}

type EndedRun = { runId: string; lockToken: string; reply: SettleReply; finishedAt: EpochMilliseconds };

async function endedRunWithASavedStepOf(context: EngineTestContext, outcome: SettleOutcome): Promise<EndedRun> {
  const { engine, redis, keys } = context;
  const { task, runId, lockToken } = await lockedRunOf(context);
  await redis.hSet(keys.runSteps(task, runId), "step-1", '"saved"');
  const instantBefore = await serverInstantInMillisecondsOf(redis);
  const reply = await engine.settle({ task, runId, lockToken, outcome });
  const instantAfter = await serverInstantInMillisecondsOf(redis);
  const finishedAt = reply.kind === "completed" || reply.kind === "failed" ? reply.finishedAt : Number.NaN;
  expect(finishedAt).toBeGreaterThanOrEqual(instantBefore);
  expect(finishedAt).toBeLessThanOrEqual(instantAfter);
  const finalState = reply.kind === "completed" ? "completed" : "failed";
  expect(Number(await redis.zScore(keys.runIndex("send-email", finalState), storedRunIdOf(task, runId)))).toBe(
    finishedAt,
  );
  expect(Number(await redis.zScore(keys.runDedup("send-email"), storedRunIdOf(task, runId)))).toBe(
    finishedAt + DEDUP_WINDOW_IN_MILLISECONDS,
  );
  expect(await redis.sCard(keys.slotSet("send-email", undefined))).toBe(0);
  return { runId, lockToken, reply, finishedAt };
}

engineTest(
  "a completion drops the steps, writes the dedup entry, moves the run to completed and ends its lock and slot",
  async (context) => {
    const { redis, keys } = context;

    const { runId, lockToken, reply, finishedAt } = await endedRunWithASavedStepOf(context, COMPLETE);

    expect(reply).toEqual({ kind: "completed", finishedAt: expect.any(Number) });
    expect(
      await redis.hmGet(keys.runRecord("send-email", runId), [
        "state",
        "finishedAt",
        "endedLockToken",
        "endedAs",
        "lockToken",
        "lockedUntil",
      ]),
    ).toEqual(["completed", String(finishedAt), lockToken, "complete", null, null]);
    expect(await redis.exists([keys.runSteps("send-email", runId), keys.lock(lockToken)])).toBe(0);
    expect(await redis.zScore(keys.locks, storedRunIdOf("send-email", runId))).toBeNull();
  },
);

engineTest(
  "a yield returns the run to the front of pending with its order kept and ends its lock and slot",
  async (context) => {
    const { engine, redis, keys } = context;
    await enqueueSendEmailRuns(context, 2);
    const [lockedRun] = await fetchedLockedRunsOf(context, 1);
    const orderBefore = await redis.hGet(keys.runRecord("send-email", "run-1"), "order");
    await redis.del(keys.wakeUpMarker);

    const reply = await engine.settle({
      task: "send-email",
      runId: "run-1",
      lockToken: lockedRun!.lockToken,
      outcome: YIELD,
    });

    expect(reply).toEqual({ kind: "yielded" });
    expect(await redis.zRange(keys.pending, 0, -1)).toEqual(sendEmailStoredRunIdsOf(["run-1", "run-2"]));
    expect(await redis.hmGet(keys.runRecord("send-email", "run-1"), ["state", "order", "endedAs"])).toEqual([
      "pending",
      orderBefore,
      "yield",
    ]);
    expect(await redis.exists(keys.lock(lockedRun!.lockToken))).toBe(0);
    expect(await redis.sCard(keys.slotSet("send-email", undefined))).toBe(0);
    expect(await redis.zCard(keys.wakeUpMarker)).toBe(1);
  },
);

engineTest(
  "a hand-back returns the run to the front of pending and keeps the instant it was first found unknown",
  async (context) => {
    const { engine, redis, keys } = context;
    await enqueueSendEmailRuns(context, 2);
    const [firstLock] = await fetchedLockedRunsOf(context, 1);
    const instantBefore = await serverInstantInMillisecondsOf(redis);

    const firstReply = await engine.settle({
      task: "send-email",
      runId: "run-1",
      lockToken: firstLock!.lockToken,
      outcome: HAND_BACK,
    });
    const firstFoundUnknownAt = await redis.hGet(keys.runRecord("send-email", "run-1"), "firstFoundUnknownAt");
    const [secondLock] = await fetchedLockedRunsOf(context, 1);
    await engine.settle({ task: "send-email", runId: "run-1", lockToken: secondLock!.lockToken, outcome: HAND_BACK });

    expect(firstReply).toEqual({ kind: "handedBack" });
    expect(Number(firstFoundUnknownAt)).toBeGreaterThanOrEqual(instantBefore);
    expect(await redis.hGet(keys.runRecord("send-email", "run-1"), "firstFoundUnknownAt")).toBe(firstFoundUnknownAt);
    expect(await redis.zRange(keys.pending, 0, -1)).toEqual(sendEmailStoredRunIdsOf(["run-1", "run-2"]));
    expect(await redis.hGet(keys.runRecord("send-email", "run-1"), "endedAs")).toBe("handBack");
  },
);

engineTest(
  "a retry moves the run to backingOff at TIME plus the backoff, bumps attempt, resets stalls and appends the error",
  async (context) => {
    const { engine, redis, keys } = context;
    const { task, runId, lockToken } = await lockedRunOf(context);
    await redis.hSet(keys.runRecord(task, runId), "stalls", 1);
    const instantBefore = await serverInstantInMillisecondsOf(redis);

    const reply = await engine.settle({ task, runId, lockToken, outcome: RETRY });

    const instantAfter = await serverInstantInMillisecondsOf(redis);
    const nextAttemptAt = reply.kind === "backingOff" ? reply.nextAttemptAt : Number.NaN;
    expect(reply).toEqual({ kind: "backingOff", nextAttemptAt: expect.any(Number) });
    expect(nextAttemptAt).toBeGreaterThanOrEqual(instantBefore + BACKOFF_IN_MILLISECONDS);
    expect(nextAttemptAt).toBeLessThanOrEqual(instantAfter + BACKOFF_IN_MILLISECONDS);
    expect(
      await redis.hmGet(keys.runRecord(task, runId), [
        "state",
        "nextAttemptAt",
        "attempt",
        "stalls",
        "endedAs",
        "lockToken",
      ]),
    ).toEqual(["backingOff", String(nextAttemptAt), "2", "0", "retry", null]);
    expect(Number(await redis.zScore(keys.due, storedRunIdOf(task, runId)))).toBe(nextAttemptAt);
    expect(Number(await redis.zScore(keys.runIndex("send-email", "backingOff"), storedRunIdOf(task, runId)))).toBe(
      nextAttemptAt,
    );
    expect(
      (await redis.lRange(keys.runErrors(task, runId), 0, -1)).map((storedError) => JSON.parse(storedError)),
    ).toEqual([{ ...ATTEMPT_ERROR, failedAt: new Date(nextAttemptAt - BACKOFF_IN_MILLISECONDS).toISOString() }]);
    expect(await redis.exists(keys.lock(lockToken))).toBe(0);
    expect(await redis.sCard(keys.slotSet("send-email", undefined))).toBe(0);
  },
);

engineTest("a suspend moves the run to suspended at the instant sent and keeps attempt", async (context) => {
  const { engine, redis, keys } = context;
  const { task, runId, lockToken } = await lockedRunOf(context);

  const reply = await engine.settle({ task, runId, lockToken, outcome: SUSPEND });

  expect(reply).toEqual({ kind: "suspended", resumesAt: RESUMES_AT });
  expect(await redis.hmGet(keys.runRecord(task, runId), ["state", "resumesAt", "attempt", "endedAs"])).toEqual([
    "suspended",
    String(RESUMES_AT),
    "1",
    "suspend",
  ]);
  expect(Number(await redis.zScore(keys.due, storedRunIdOf(task, runId)))).toBe(RESUMES_AT);
  expect(await redis.exists(keys.runErrors(task, runId))).toBe(0);
  expect(await redis.sCard(keys.slotSet("send-email", undefined))).toBe(0);
});

engineTest("an error of 10,000 characters is stored cut at 4,000 characters with the marker", async (context) => {
  const { engine, redis, keys } = context;
  const { task, runId, lockToken } = await lockedRunOf(context);
  const longText = "0123456789".repeat(1000);
  const error = { ...ATTEMPT_ERROR, message: longText, stack: longText };

  await engine.settle({ task, runId, lockToken, outcome: { ...RETRY, error } });

  const [storedError] = await redis.lRange(keys.runErrors(task, runId), 0, -1);
  const { message, stack } = JSON.parse(storedError!) as { message: string; stack: string };
  expect(message).toBe(`${longText.slice(0, 4000)}${CUT_ERROR_TEXT_MARKER}`);
  expect(stack).toBe(message);
});

engineTest("an error whose text holds quotes and newlines is stored as the JSON of the error", async (context) => {
  const { engine, redis, keys } = context;
  const { task, runId, lockToken } = await lockedRunOf(context);
  const error = { ...ATTEMPT_ERROR, message: 'a "quoted"\nline with "failedAt":"x"', stack: "at \\ path" };

  await engine.settle({ task, runId, lockToken, outcome: { ...RETRY, error } });

  const [storedError] = await redis.lRange(keys.runErrors(task, runId), 0, -1);
  expect(JSON.parse(storedError!)).toEqual({ ...error, failedAt: expect.stringMatching(/^\d{4}-\d\d-\d\dT/) });
});

function finishedAtOf(reply: SettleReply): EpochMilliseconds {
  if (reply.kind !== "failed") {
    throw new Error(`expected a failed reply, got ${JSON.stringify(reply)}`);
  }
  return reply.finishedAt;
}

async function storedErrorsOf({ redis, keys }: EngineTestContext, task: string, runId: string): Promise<unknown[]> {
  return (await redis.lRange(keys.runErrors(task, runId), 0, -1)).map((storedError) => JSON.parse(storedError));
}

engineTest(
  "a failure moves the run to failed at TIME with its reason, appends the error, keeps the steps and writes the dedup entry",
  async (context) => {
    const { redis, keys } = context;

    const { runId, reply, finishedAt } = await endedRunWithASavedStepOf(context, { ...FAIL, failureNotice: undefined });

    expect(reply).toEqual({ kind: "failed", reason: "retries exhausted", finishedAt: expect.any(Number) });
    expect(
      await redis.hmGet(keys.runRecord("send-email", runId), ["state", "finishedAt", "reason", "attempt", "endedAs"]),
    ).toEqual(["failed", String(finishedAt), "retries exhausted", "1", "fail"]);
    expect(await storedErrorsOf(context, "send-email", runId)).toEqual([
      { ...ATTEMPT_ERROR, failedAt: new Date(finishedAt).toISOString() },
    ]);
    expect(await redis.hGetAll(keys.runSteps("send-email", runId))).toEqual({ "step-1": '"saved"' });
  },
);

engineTest(
  "a failure stores its notice as listener runs under the derived event id, failedAt equal to finishedAt",
  async (context) => {
    const { engine, redis, keys } = context;
    const { task, runId, lockToken } = await lockedRunOf(context);

    const finishedAt = finishedAtOf(await engine.settle({ task, runId, lockToken, outcome: FAIL }));

    const expectedPayload = { ...FAILURE_NOTICE.payload, failedAt: new Date(finishedAt).toISOString() };
    for (const { runId: noticeRunId, task } of FAILURE_NOTICE.runs) {
      expect(await redis.hmGet(keys.runRecord(task, noticeRunId), ["state", "task", "eventName", "eventId"])).toEqual([
        "pending",
        task,
        NOTICE_EVENT.eventName,
        NOTICE_EVENT.eventId,
      ]);
      expect(JSON.parse((await redis.get(keys.runPayload(task, noticeRunId)))!)).toEqual(expectedPayload);
    }
    expect(await redis.hmGet(keys.runRecord("alert-ops", "notice-run-1"), ["retries", "concurrencyKey"])).toEqual([
      "2",
      null,
    ]);
    expect(await redis.hmGet(keys.runRecord("page-on-call", "notice-run-2"), ["retries", "concurrencyKey"])).toEqual([
      "0",
      "team-a",
    ]);
    expect(await redis.zRange(keys.pending, 0, -1)).toEqual([
      storedRunIdOf("alert-ops", "notice-run-1"),
      storedRunIdOf("page-on-call", "notice-run-2"),
    ]);
    expect(Number(await redis.zScore(keys.eventDedup(NOTICE_EVENT.eventName), NOTICE_EVENT.eventId))).toBe(
      finishedAt + DEDUP_WINDOW_IN_MILLISECONDS,
    );
  },
);

engineTest(
  "a failure's notice payload carries its error cut at 4,000 characters, as the stored error",
  async (context) => {
    const { engine, redis, keys } = context;
    const { task, runId, lockToken } = await lockedRunOf(context);
    const longText = "0123456789".repeat(1000);
    const failureNotice: FailureNotice = {
      ...FAILURE_NOTICE,
      payload: { ...FAILURE_NOTICE.payload, error: { name: "Error", message: longText, stack: longText } },
    };

    await engine.settle({ task, runId, lockToken, outcome: { ...FAIL, failureNotice } });

    const { error } = JSON.parse((await redis.get(keys.runPayload("alert-ops", "notice-run-1")))!) as {
      error: { message: string };
    };
    expect(error.message).toBe(`${longText.slice(0, 4000)}${CUT_ERROR_TEXT_MARKER}`);
  },
);

for (const failureNotice of [undefined, { ...FAILURE_NOTICE, runs: [] }]) {
  engineTest(
    `a failure with ${failureNotice === undefined ? "no notice" : "an empty notice list"} stores no listener run and no event dedup entry`,
    async (context) => {
      const { engine, redis, keys } = context;
      const { task, runId, lockToken } = await lockedRunOf(context);

      await engine.settle({ task, runId, lockToken, outcome: { ...FAIL, failureNotice } });

      expect(await redis.zCard(keys.pending)).toBe(0);
      expect(await redis.exists(keys.eventDedup(NOTICE_EVENT.eventName))).toBe(0);
      expect(await redis.sMembers(keys.tasks)).toEqual(["send-email"]);
    },
  );
}

engineTest("a failure skips the notice runs that already exist, as an enqueue would", async (context) => {
  const { engine, redis, keys } = context;
  const { task, runId, lockToken } = await lockedRunOf(context);
  await engine.enqueue({ runs: [{ runId: "notice-run-1", task: "alert-ops", payload: '"earlier"', retries: 0 }] });

  await engine.settle({ task, runId, lockToken, outcome: FAIL });

  expect(await redis.get(keys.runPayload("alert-ops", "notice-run-1"))).toBe('"earlier"');
  expect(await redis.hGet(keys.runRecord("page-on-call", "notice-run-2"), "state")).toBe("pending");
});

engineTest("an enqueue of a failed run's id is skipped while its dedup entry is live", async (context) => {
  const { engine, redis, keys } = context;
  const lockedRun = await lockedRunOf(context);
  const { task, runId } = lockedRun;
  await engine.settle({ ...lockedRun, outcome: { ...FAIL, failureNotice: undefined } });
  await redis.del([keys.runRecord(task, runId), keys.runPayload(task, runId), keys.runErrors(task, runId)]);
  await redis.zRem(keys.runIndex("send-email", "failed"), storedRunIdOf(task, runId));

  const reply = await engine.enqueue({ runs: [{ runId, task: "send-email", payload: "{}", retries: 0 }] });

  expect(reply).toEqual({ kind: "enqueued", createdRuns: [], skippedRuns: [{ task: "send-email", runId }] });
});

async function lockedRunFirstFoundUnknownOf(
  context: EngineTestContext,
  unknownForInMilliseconds: DurationInMilliseconds,
): Promise<LockedRun> {
  const { redis, keys } = context;
  const lockedRun = await lockedRunOf(context);
  const now = await serverInstantInMillisecondsOf(redis);
  await redis.hSet(
    keys.runRecord(lockedRun.task, lockedRun.runId),
    "firstFoundUnknownAt",
    now - unknownForInMilliseconds,
  );
  return lockedRun;
}

engineTest(
  "a hand-back of a run first found unknown 30 minutes ago fails it unknown task with its error and notice, and its resend hears that failure and writes nothing",
  async (context) => {
    const { engine, redis, keys, keyPrefix } = context;
    const { task, runId, lockToken } = await lockedRunFirstFoundUnknownOf(
      context,
      UNKNOWN_TASK_FAILURE_DELAY_IN_MILLISECONDS,
    );

    const reply = await engine.settle({
      task,
      runId,
      lockToken,
      outcome: { ...HAND_BACK, failureNotice: FAILURE_NOTICE },
    });
    const storedValuesAfterTheFailure = await storedValuesByKeyOf(redis, keyPrefix);
    const resentReply = await engine.settle({
      task,
      runId,
      lockToken,
      outcome: { ...HAND_BACK, failureNotice: FAILURE_NOTICE },
    });

    const finishedAt = finishedAtOf(reply);
    expect(reply).toEqual({ kind: "failed", reason: "unknown task", finishedAt: expect.any(Number) });
    expect(resentReply).toEqual(reply);
    expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesAfterTheFailure);
    expect(await redis.hmGet(keys.runRecord(task, runId), ["state", "reason", "endedAs"])).toEqual([
      "failed",
      "unknown task",
      "handBack",
    ]);
    expect(await storedErrorsOf(context, task, runId)).toEqual([
      { ...UNKNOWN_TASK_ERROR, failedAt: new Date(finishedAt).toISOString() },
    ]);
    expect(await redis.hGet(keys.runRecord("alert-ops", "notice-run-1"), "state")).toBe("pending");
  },
);

engineTest("a hand-back of a run first found unknown under 30 minutes ago returns it to pending", async (context) => {
  const { engine, redis, keys } = context;
  const { task, runId, lockToken } = await lockedRunFirstFoundUnknownOf(
    context,
    UNKNOWN_TASK_FAILURE_DELAY_IN_MILLISECONDS - 60_000,
  );

  const reply = await engine.settle({ task, runId, lockToken, outcome: HAND_BACK });

  expect(reply).toEqual({ kind: "handedBack" });
  expect(await redis.hGet(keys.runRecord(task, runId), "state")).toBe("pending");
});

for (const outcome of [COMPLETE, YIELD, RETRY, SUSPEND, FAIL]) {
  engineTest(
    `a ${outcome.kind} after a hand-back clears the instant the run was first found unknown`,
    async (context) => {
      const { engine, redis, keys } = context;
      const handedBackLock = await lockedRunOf(context);
      await engine.settle({ ...handedBackLock, outcome: HAND_BACK });
      const [knownTaskLock] = await fetchedLockedRunsOf(context, 1);

      await engine.settle({ ...knownTaskLock!, outcome });

      expect(
        await redis.hExists(keys.runRecord(handedBackLock.task, handedBackLock.runId), "firstFoundUnknownAt"),
      ).toBe(0);
    },
  );
}

for (const outcome of OUTCOMES) {
  engineTest(
    `a ${outcome.kind} with a token that is not the record's replies stale and writes nothing`,
    async (context) => {
      const { engine, redis, keyPrefix } = context;
      const { task, runId } = await lockedRunOf(context);
      const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

      const reply = await engine.settle({ task, runId, lockToken: "stale-token", outcome });

      expect(reply).toEqual({ kind: "stale" });
      expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
    },
  );

  engineTest(`a resent ${outcome.kind} with the ended token replies already and writes nothing`, async (context) => {
    const { engine, redis, keyPrefix } = context;
    const { task, runId, lockToken } = await lockedRunOf(context);
    await engine.settle({ task, runId, lockToken, outcome });
    const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

    const reply = await engine.settle({ task, runId, lockToken, outcome });

    expect(reply).toEqual({ kind: "already" });
    expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
  });
}

engineTest("a settle of another kind with the ended token replies stale", async (context) => {
  const { engine } = context;
  const { task, runId, lockToken } = await lockedRunOf(context);
  await engine.settle({ task, runId, lockToken, outcome: YIELD });

  const reply = await engine.settle({ task, runId, lockToken, outcome: COMPLETE });

  expect(reply).toEqual({ kind: "stale" });
});

engineTest("a settle of a run with no record replies stale and writes nothing", async (context) => {
  const { engine, redis, keyPrefix } = context;
  const { lockToken } = await lockedRunOf(context);
  const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

  const reply = await engine.settle({ task: "send-email", runId: "missing-run", lockToken, outcome: COMPLETE });

  expect(reply).toEqual({ kind: "stale" });
  expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
});

engineTest("a settle of a record of a newer version replies unknownVersion and writes nothing", async (context) => {
  const { engine, redis, keys, keyPrefix } = context;
  const { task, runId, lockToken } = await lockedRunOf(context);
  await redis.hSet(keys.runRecord(task, runId), "version", 2);
  const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

  const reply = await engine.settle({ task, runId, lockToken, outcome: COMPLETE });

  expect(reply).toEqual({ kind: "unknownVersion", recordVersion: 2 });
  expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
});

engineTest("a completion wakes as many parked runs as there are free slots, in waiting-list order", async (context) => {
  const { engine, redis, keys } = context;
  await enqueueSendEmailRuns(context, 5, "account-7");
  const [firstHolder, secondHolder] = await fetchedLockedRunsOf(context, 5, { "send-email": 2 });

  await engine.settle({ ...firstHolder!, outcome: COMPLETE, concurrencyLimit: 2 });
  const pendingAfterFirstCompletion = await redis.zRange(keys.pending, 0, -1);
  await engine.settle({ ...secondHolder!, outcome: COMPLETE, concurrencyLimit: 3 });

  expect(pendingAfterFirstCompletion).toEqual([storedRunIdOf("send-email", "run-3")]);
  expect(await redis.zRange(keys.pending, 0, -1)).toEqual(
    ["run-3", "run-4", "run-5"].map((runId) => storedRunIdOf("send-email", runId)),
  );
  expect(await redis.hGet(keys.runRecord("send-email", "run-4"), "state")).toBe("pending");
  expect(await redis.zCard(keys.waitingList("send-email", "account-7"))).toBe(0);
});

engineTest("a settle with no limit wakes every parked run of the key", async (context) => {
  const { engine, redis, keys } = context;
  await enqueueSendEmailRuns(context, 4, "account-7");
  const [holder] = await fetchedLockedRunsOf(context, 4, { "send-email": 1 });

  await engine.settle({ ...holder!, outcome: YIELD });

  expect(await redis.zRange(keys.pending, 0, -1)).toEqual(
    ["run-1", "run-2", "run-3", "run-4"].map((runId) => storedRunIdOf("send-email", runId)),
  );
  expect(await redis.zCard(keys.waitingList("send-email", "account-7"))).toBe(0);
});

engineTest("a settle with no free slot left wakes no parked run", async (context) => {
  const { engine, redis, keys } = context;
  await enqueueSendEmailRuns(context, 3, "account-7");
  const [holder] = await fetchedLockedRunsOf(context, 3, { "send-email": 2 });

  await engine.settle({ ...holder!, outcome: HAND_BACK, concurrencyLimit: 1 });

  expect(await redis.zRange(keys.waitingList("send-email", "account-7"), 0, -1)).toEqual([
    storedRunIdOf("send-email", "run-3"),
  ]);
});

engineTest("a settle of an unknown kind is rejected and writes nothing", async (context) => {
  const { engine, redis, keyPrefix } = context;
  const { task, runId, lockToken } = await lockedRunOf(context);
  const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

  const settle = engine.settle({
    task,
    runId,
    lockToken,
    outcome: { kind: "explode" } as unknown as SettleOutcome,
  });

  await expect(settle).rejects.toThrow("invalid argument: unknown settle kind explode");
  expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
});
