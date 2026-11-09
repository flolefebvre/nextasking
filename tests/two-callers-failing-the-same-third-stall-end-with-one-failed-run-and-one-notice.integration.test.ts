import { expect } from "vitest";

import type { FailureNotice, SettleOutcome } from "../src/engine/engine.js";
import { storedRunIdOf } from "../src/engine/engine-keys.js";
import { engineTest, expireLock, fetchedLockedRunsOf } from "./engine-test.js";

const NOTICE_EVENT = { eventName: "nextasking/run.failed", eventId: "notice-event-1" };

const STALL_ERROR = { attempt: 1, name: "StalledRunError", message: "stalled three times", stack: "", failedAt: "" };

const FAILURE_NOTICE: FailureNotice = {
  payload: {
    task: "send-email",
    runId: "run-1",
    reason: "stalled",
    error: { name: STALL_ERROR.name, message: STALL_ERROR.message, stack: STALL_ERROR.stack },
    attempt: 1,
  },
  runs: [{ runId: "notice-run-1", task: "alert-ops", retries: 0, sourceEvent: NOTICE_EVENT }],
  eventDedup: NOTICE_EVENT,
};

const FAIL_STALLED: SettleOutcome = {
  kind: "fail",
  reason: "stalled",
  error: STALL_ERROR,
  failureNotice: FAILURE_NOTICE,
};

engineTest(
  "two callers failing the same reported third stall end with one failed run and one stored notice",
  async (context) => {
    const { engine, redis, keys } = context;
    await engine.enqueue({ runs: [{ runId: "run-1", task: "send-email", payload: "{}", retries: 0 }] });
    await fetchedLockedRunsOf(context, 1);
    await redis.hSet(keys.runRecord("send-email", "run-1"), "stalls", 2);
    await expireLock(context, "send-email", "run-1");
    const [firstCallerReply, secondCallerReply] = await Promise.all([engine.promote(), engine.promote()]);
    const reportedStalls = [firstCallerReply, secondCallerReply].map((reply) =>
      reply.kind === "promoted" ? reply.thirdStalls : [],
    );
    const [firstReportedStall] = reportedStalls[0]!;
    const [secondReportedStall] = reportedStalls[1]!;

    const settleReplies = await Promise.all(
      [firstReportedStall!, secondReportedStall!].map(({ task, runId, expiredLockToken }) =>
        engine.settle({ task, runId, lockToken: expiredLockToken, outcome: FAIL_STALLED }),
      ),
    );

    expect(secondReportedStall).toEqual(firstReportedStall);
    expect(settleReplies.filter((reply) => reply.kind === "failed")).toEqual([
      { kind: "failed", reason: "stalled", finishedAt: expect.any(Number) },
    ]);
    expect(settleReplies.filter((reply) => reply.kind === "already" || reply.kind === "stale")).toHaveLength(1);
    expect(await redis.hmGet(keys.runRecord("send-email", "run-1"), ["state", "reason"])).toEqual([
      "failed",
      "stalled",
    ]);
    expect(await redis.lLen(keys.runErrors("send-email", "run-1"))).toBe(1);
    expect(await redis.zRange(keys.pending, 0, -1)).toEqual([storedRunIdOf("alert-ops", "notice-run-1")]);
    expect(await redis.zCard(keys.eventDedup(NOTICE_EVENT.eventName))).toBe(1);
    expect(await engine.promote()).toEqual({ kind: "promoted", promotedRunCount: 0, stalledRuns: [], thirdStalls: [] });
  },
);
