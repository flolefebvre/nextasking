import { expect } from "vitest";

import { DAY_IN_MILLISECONDS, endRuns, SEND_EMAIL_PRUNE_REQUEST } from "../../tests/ended-runs.js";
import { backdateEndedRun, engineTest, fetchedLockedRunsOf, storedValuesByKeyOf } from "../../tests/engine-test.js";
import type { EngineTestContext } from "../../tests/engine-test.js";
import { storedRunIdOf } from "./engine-keys.js";
import { RECORD_VERSION } from "./engine.js";
import type { SettleOutcome } from "./engine.js";

const ATTEMPT_ERROR = { attempt: 1, name: "Error", message: "boom", stack: "Error: boom" };

const RETRY: SettleOutcome = { kind: "retry", error: ATTEMPT_ERROR, backoffInMilliseconds: 0 };

const FAIL: SettleOutcome = {
  kind: "fail",
  reason: "retries exhausted",
  error: ATTEMPT_ERROR,
  failureNotice: undefined,
};

async function enqueueSendEmailRun({ engine }: EngineTestContext, runId: string): Promise<void> {
  await engine.enqueue({ runs: [{ runId, task: "send-email", payload: '{"to":"ada"}', retries: 1 }] });
}

async function failedRunWithAStepAtAttempt2Of(context: EngineTestContext): Promise<void> {
  const { engine } = context;
  await enqueueSendEmailRun(context, "run-1");
  const [firstAttempt] = await fetchedLockedRunsOf(context, 1);
  await engine.saveStep({ ...firstAttempt!, stepId: "charge", step: { kind: "value", value: '["paid"]' } });
  await engine.settle({ ...firstAttempt!, outcome: RETRY });
  await engine.promote();
  const [secondAttempt] = await fetchedLockedRunsOf(context, 1);
  await engine.settle({ ...secondAttempt!, outcome: FAIL });
}

engineTest(
  "a replay makes a failed run pending at the back at attempt 1, deleting its errors and keeping its steps",
  async (context) => {
    const { engine, redis, keys } = context;
    await failedRunWithAStepAtAttempt2Of(context);
    await enqueueSendEmailRun(context, "run-2");
    const recordBefore = await redis.hGetAll(keys.runRecord("send-email", "run-1"));
    const dedupExpiryBefore = await redis.zScore(keys.runDedup("send-email"), storedRunIdOf("send-email", "run-1"));
    await redis.del(keys.wakeUpMarker);

    const reply = await engine.replay({ task: "send-email", runId: "run-1" });

    expect(reply).toEqual({ kind: "replayed", task: "send-email" });
    const record = await redis.hGetAll(keys.runRecord("send-email", "run-1"));
    expect(record).toEqual({
      version: String(RECORD_VERSION),
      state: "pending",
      task: "send-email",
      attempt: "1",
      retries: "1",
      replays: "1",
      stalls: "0",
      dispatchedAt: recordBefore["dispatchedAt"],
      startedAt: recordBefore["startedAt"],
      isLockAResume: "0",
      order: expect.any(String),
    });
    expect(Number(record["order"])).toBeGreaterThan(
      Number(await redis.hGet(keys.runRecord("send-email", "run-2"), "order")),
    );
    expect(await redis.exists(keys.runErrors("send-email", "run-1"))).toBe(0);
    expect(await redis.hGetAll(keys.runSteps("send-email", "run-1"))).toEqual({ charge: '["paid"]' });
    expect(await redis.get(keys.runPayload("send-email", "run-1"))).toBe('{"to":"ada"}');
    expect(await redis.zScore(keys.runIndex("send-email", "failed"), storedRunIdOf("send-email", "run-1"))).toBeNull();
    expect(await redis.zScore(keys.runIndex("send-email", "pending"), storedRunIdOf("send-email", "run-1"))).toBe(
      Number(record["order"]),
    );
    expect(await redis.zScore(keys.pending, storedRunIdOf("send-email", "run-1"))).toBe(Number(record["order"]));
    expect(await redis.zScore(keys.runDedup("send-email"), storedRunIdOf("send-email", "run-1"))).toBe(
      dedupExpiryBefore,
    );
    expect(await redis.zCard(keys.wakeUpMarker)).toBe(1);
  },
);

engineTest("a replayed run is fetched again at attempt 1 with its replays counted", async (context) => {
  const { engine } = context;
  await failedRunWithAStepAtAttempt2Of(context);

  await engine.replay({ task: "send-email", runId: "run-1" });
  const [lockedRun] = await fetchedLockedRunsOf(context, 1);

  expect(lockedRun).toMatchObject({ runId: "run-1", attempt: 1, replays: 1, isResume: false });
});

engineTest("a replay of a run that has not failed replies wrongState and writes nothing", async (context) => {
  const { engine, redis, keyPrefix } = context;
  await enqueueSendEmailRun(context, "run-1");
  const [lockedRun] = await fetchedLockedRunsOf(context, 1);
  await engine.settle({ ...lockedRun!, outcome: { kind: "complete" } });
  await enqueueSendEmailRun(context, "run-2");
  const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

  const completedRunReply = await engine.replay({ task: "send-email", runId: "run-1" });
  const pendingRunReply = await engine.replay({ task: "send-email", runId: "run-2" });

  expect(completedRunReply).toEqual({ kind: "wrongState", state: "completed" });
  expect(pendingRunReply).toEqual({ kind: "wrongState", state: "pending" });
  expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
});

engineTest(
  "a replay of a pruned failed run replies notFound and writes nothing, whether its dedup entry expired or is live",
  async (context) => {
    const { engine, redis, keys, keyPrefix } = context;
    await failedRunWithAStepAtAttempt2Of(context);
    await endRuns(engine, "send-email", ["run-over-the-cap"], "fail");
    await backdateEndedRun(context, "send-email", "run-1", 14 * DAY_IN_MILLISECONDS);
    const pruneReply = await engine.prune({
      ...SEND_EMAIL_PRUNE_REQUEST,
      failedRetention: { ...SEND_EMAIL_PRUNE_REQUEST.failedRetention, maxRunCount: 0 },
    });
    const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

    const replies = [
      await engine.replay({ task: "send-email", runId: "run-1" }),
      await engine.replay({ task: "send-email", runId: "run-over-the-cap" }),
    ];

    expect(pruneReply).toEqual({ kind: "pruned", prunedRunCount: 2 });
    expect(await redis.zRange(keys.runDedup("send-email"), 0, -1)).toEqual([
      storedRunIdOf("send-email", "run-over-the-cap"),
    ]);
    expect(replies).toEqual([{ kind: "notFound" }, { kind: "notFound" }]);
    expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
  },
);

engineTest(
  "a replay of a failed run of a newer record version replies unknownVersion and writes nothing",
  async (context) => {
    const { engine, redis, keys, keyPrefix } = context;
    await failedRunWithAStepAtAttempt2Of(context);
    await redis.hSet(keys.runRecord("send-email", "run-1"), "version", RECORD_VERSION + 1);
    const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

    const reply = await engine.replay({ task: "send-email", runId: "run-1" });

    expect(reply).toEqual({ kind: "unknownVersion", recordVersion: RECORD_VERSION + 1 });
    expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
  },
);

engineTest(
  "a replay of a task whose run id only another task holds replies notFound and writes nothing",
  async (context) => {
    const { engine, redis, keyPrefix } = context;
    await failedRunWithAStepAtAttempt2Of(context);
    const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

    const reply = await engine.replay({ task: "send-sms", runId: "run-1" });

    expect(reply).toEqual({ kind: "notFound" });
    expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
  },
);
