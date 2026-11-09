import { expect, vi } from "vitest";

import { DAY_IN_MILLISECONDS, SEND_EMAIL_PRUNE_REQUEST } from "../../tests/ended-runs.js";
import { backdateEndedRun, engineTest, expireLock, fetchedLockedRunsOf } from "../../tests/engine-test.js";
import { serverInstantInMillisecondsOf } from "../../tests/server-instant.js";
import { REPLIED_EMPTY_PROMOTE, REPLIED_ONE_RENEWAL, repliedOf } from "../../tests/tick-replies.js";

const LOCK_DURATION_IN_MILLISECONDS = 120_000;

engineTest(
  "a tick promotes, renews the held locks and reads the task names in one pipelined round trip",
  async (context) => {
    const { engine, redis, keys } = context;
    await engine.enqueue({
      runs: [
        { runId: "held-run", task: "send-email", payload: "{}", retries: 0 },
        { runId: "stalled-run", task: "resize-image", payload: "{}", retries: 0 },
      ],
    });
    const lockedRuns = await fetchedLockedRunsOf(context, 2);
    const heldRun = lockedRuns.find(({ runId }) => runId === "held-run")!;
    await expireLock(context, "stalled-run");
    const pipelineSpy = vi.spyOn(redis, "pipeline");
    const instantBefore = await serverInstantInMillisecondsOf(redis);

    const reply = await engine.tick({
      heldLocks: [{ runId: heldRun.runId, lockToken: heldRun.lockToken }],
      lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS,
    });

    expect(pipelineSpy).toHaveBeenCalledTimes(1);
    expect(reply).toEqual({
      promote: repliedOf({
        kind: "promoted",
        promotedRunCount: 0,
        stalledRuns: [{ runId: "stalled-run", task: "resize-image", attempt: 1, stalls: 1 }],
        thirdStalls: [],
      }),
      renewals: REPLIED_ONE_RENEWAL,
      prune: undefined,
      taskNames: { kind: "replied", reply: expect.arrayContaining(["send-email", "resize-image"]) },
    });
    expect(reply.taskNames.kind === "replied" ? reply.taskNames.reply : []).toHaveLength(2);
    const [renewal] = reply.renewals.kind === "replied" ? reply.renewals.reply : [];
    expect(renewal?.kind === "renewed" ? renewal.lockedUntil : 0).toBeGreaterThanOrEqual(
      instantBefore + LOCK_DURATION_IN_MILLISECONDS,
    );
    expect(await redis.hget(keys.runRecord("stalled-run"), "state")).toBe("pending");
  },
);

engineTest("a tick carrying a prune sends it in the same pipelined round trip", async (context) => {
  const { engine, redis, keys } = context;
  await engine.enqueue({ runs: [{ runId: "aged-run", task: "send-email", payload: "{}", retries: 0 }] });
  const [lockedRun] = await fetchedLockedRunsOf(context, 1);
  await engine.settle({ ...lockedRun!, outcome: { kind: "complete" } });
  await backdateEndedRun(context, "aged-run", DAY_IN_MILLISECONDS);
  const pipelineSpy = vi.spyOn(redis, "pipeline");

  const reply = await engine.tick({
    heldLocks: [],
    lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS,
    prune: SEND_EMAIL_PRUNE_REQUEST,
  });

  expect(pipelineSpy).toHaveBeenCalledTimes(1);
  expect(pipelineSpy.mock.results[0]?.value).toHaveLength(4);
  expect(reply.prune).toEqual({ kind: "replied", reply: { kind: "pruned", prunedRunCount: 1 } });
  expect(await redis.exists(keys.runRecord("aged-run"))).toBe(0);
});

engineTest("a tick with no held lock renews nothing and still promotes", async ({ engine }) => {
  const reply = await engine.tick({ heldLocks: [], lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS });

  expect(reply).toEqual({
    promote: REPLIED_EMPTY_PROMOTE,
    renewals: repliedOf([]),
    prune: undefined,
    taskNames: { kind: "replied", reply: [] },
  });
});

engineTest(
  "a tick whose prune fails on the server still reports the promote, the renewals and the task names it applied",
  async (context) => {
    const { engine, redis, keys } = context;
    await engine.enqueue({ runs: [{ runId: "held-run", task: "send-email", payload: "{}", retries: 0 }] });
    const [heldRun] = await fetchedLockedRunsOf(context, 1);
    await redis.set(keys.runDedup("broken-task"), "not a sorted set");

    const reply = await engine.tick({
      heldLocks: [{ runId: heldRun!.runId, lockToken: heldRun!.lockToken }],
      lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS,
      prune: { ...SEND_EMAIL_PRUNE_REQUEST, task: "broken-task" },
    });

    expect(reply).toEqual({
      promote: REPLIED_EMPTY_PROMOTE,
      renewals: REPLIED_ONE_RENEWAL,
      prune: { kind: "failed", error: expect.objectContaining({ message: expect.stringContaining("WRONGTYPE") }) },
      taskNames: { kind: "replied", reply: ["send-email"] },
    });
  },
);

engineTest("a tick leaving the promote out sends three commands", async (context) => {
  const { engine, redis } = context;
  const pipelineSpy = vi.spyOn(redis, "pipeline");

  const reply = await engine.tick({
    heldLocks: [],
    lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS,
    prune: SEND_EMAIL_PRUNE_REQUEST,
    isPromoteIncluded: false,
  });

  expect(pipelineSpy.mock.results[0]?.value).toHaveLength(3);
  expect(reply.promote).toBeUndefined();
  expect(reply.prune).toEqual({ kind: "replied", reply: { kind: "pruned", prunedRunCount: 0 } });
});
