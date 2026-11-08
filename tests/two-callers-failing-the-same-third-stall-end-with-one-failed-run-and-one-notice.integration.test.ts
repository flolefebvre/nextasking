import { expect } from "vitest";

import type { FailureNotice, SettleOutcome } from "../src/engine/engine.js";
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
    await redis.hset(keys.runRecord("run-1"), "stalls", 2);
    await expireLock(context, "run-1");
    const [firstCallerReply, secondCallerReply] = await Promise.all([engine.promote(), engine.promote()]);
    const reportedStalls = [firstCallerReply, secondCallerReply].map((reply) =>
      reply.kind === "promoted" ? reply.thirdStalls : [],
    );
    const [firstReportedStall] = reportedStalls[0]!;
    const [secondReportedStall] = reportedStalls[1]!;

    const settleReplies = await Promise.all(
      [firstReportedStall!, secondReportedStall!].map(({ runId, expiredLockToken }) =>
        engine.settle({ runId, lockToken: expiredLockToken, outcome: FAIL_STALLED }),
      ),
    );

    expect(secondReportedStall).toEqual(firstReportedStall);
    expect(settleReplies.filter((reply) => reply.kind === "failed")).toEqual([
      { kind: "failed", reason: "stalled", finishedAt: expect.any(Number) },
    ]);
    expect(settleReplies.filter((reply) => reply.kind === "already" || reply.kind === "stale")).toHaveLength(1);
    expect(await redis.hmget(keys.runRecord("run-1"), "state", "reason")).toEqual(["failed", "stalled"]);
    expect(await redis.llen(keys.runErrors("run-1"))).toBe(1);
    expect(await redis.zrange(keys.pending, 0, -1)).toEqual(["notice-run-1"]);
    expect(await redis.zcard(keys.eventDedup(NOTICE_EVENT.eventName))).toBe(1);
    expect(await engine.promote()).toEqual({ kind: "promoted", promotedRunCount: 0, stalledRuns: [], thirdStalls: [] });
  },
);
