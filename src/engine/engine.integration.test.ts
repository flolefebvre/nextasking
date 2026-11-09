import type { Redis } from "ioredis";
import { expect, vi } from "vitest";

import { DAY_IN_MILLISECONDS, SEND_EMAIL_PRUNE_REQUEST } from "../../tests/ended-runs.js";
import { backdateEndedRun, engineTest, expireLock, fetchedLockedRunsOf } from "../../tests/engine-test.js";
import { integrationTest } from "../../tests/integration-test.js";
import { storedKeyViolationsOf } from "../../tests/stored-key-violations.js";
import { freshLockTokensOf } from "../../tests/lock-tokens.js";
import { serverInstantInMillisecondsOf } from "../../tests/server-instant.js";
import { withRedis } from "../../tests/with-redis.js";
import { REPLIED_EMPTY_PROMOTE, REPLIED_ONE_RENEWAL, repliedOf } from "../../tests/tick-replies.js";
import { engineKeysOf, storedRunIdOf } from "./engine-keys.js";
import { createEngine, RECORD_VERSION, wakeUpWaitOf } from "./engine.js";

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
    await expireLock(context, "resize-image", "stalled-run");
    const pipelineSpy = vi.spyOn(redis, "pipeline");
    const instantBefore = await serverInstantInMillisecondsOf(redis);

    const reply = await engine.tick({
      heldLocks: [{ task: heldRun.task, runId: heldRun.runId, lockToken: heldRun.lockToken }],
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
    expect(await redis.hget(keys.runRecord("resize-image", "stalled-run"), "state")).toBe("pending");
  },
);

engineTest("a tick carrying a prune sends it in the same pipelined round trip", async (context) => {
  const { engine, redis, keys } = context;
  await engine.enqueue({ runs: [{ runId: "aged-run", task: "send-email", payload: "{}", retries: 0 }] });
  const [lockedRun] = await fetchedLockedRunsOf(context, 1);
  await engine.settle({ ...lockedRun!, outcome: { kind: "complete" } });
  await backdateEndedRun(context, "send-email", "aged-run", DAY_IN_MILLISECONDS);
  const pipelineSpy = vi.spyOn(redis, "pipeline");

  const reply = await engine.tick({
    heldLocks: [],
    lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS,
    prune: SEND_EMAIL_PRUNE_REQUEST,
  });

  expect(pipelineSpy).toHaveBeenCalledTimes(1);
  expect(pipelineSpy.mock.results[0]?.value).toHaveLength(4);
  expect(reply.prune).toEqual({ kind: "replied", reply: { kind: "pruned", prunedRunCount: 1 } });
  expect(await redis.exists(keys.runRecord("send-email", "aged-run"))).toBe(0);
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
      heldLocks: [{ task: heldRun!.task, runId: heldRun!.runId, lockToken: heldRun!.lockToken }],
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

const HOURLY_REPORT = { task: "build-report", cron: "0 * * * *", timezone: "UTC" };

engineTest(
  "a tick reads the due schedules in the same pipelined round trip, leaving out a record of a newer version",
  async (context) => {
    const { engine, redis, keys } = context;
    const instantBefore = await serverInstantInMillisecondsOf(redis);
    await engine.syncSchedules({
      schedulesToSync: [
        { ...HOURLY_REPORT, nextFiringAt: instantBefore - 1000, retries: 2 },
        { ...HOURLY_REPORT, task: "newer-report", nextFiringAt: instantBefore - 1000, retries: 2 },
      ],
    });
    await redis.hset(keys.schedule("newer-report"), "version", RECORD_VERSION + 1);
    const pipelineSpy = vi.spyOn(redis, "pipeline");

    const reply = await engine.tick({
      heldLocks: [],
      lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS,
      scheduledTaskNames: ["build-report", "newer-report"],
    });

    expect(pipelineSpy).toHaveBeenCalledTimes(1);
    expect(pipelineSpy.mock.results[0]?.value).toHaveLength(6);
    expect(reply.dueSchedules).toEqual(
      repliedOf({
        serverInstant: expect.any(Number),
        dueSchedules: [{ ...HOURLY_REPORT, nextFiringAt: instantBefore - 1000 }],
      }),
    );
    const serverInstant = reply.dueSchedules?.kind === "replied" ? reply.dueSchedules.reply.serverInstant : 0;
    expect(serverInstant).toBeGreaterThanOrEqual(instantBefore);
    expect(serverInstant).toBeLessThanOrEqual(await serverInstantInMillisecondsOf(redis));
  },
);

engineTest(
  "a tick whose schedule read fails on the server reports it failed and still reports its other commands",
  async (context) => {
    const { engine, redis, keys } = context;
    await redis.set(keys.schedule("broken-report"), "not a hash");

    const reply = await engine.tick({
      heldLocks: [],
      lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS,
      scheduledTaskNames: ["build-report", "broken-report"],
    });

    expect(reply).toEqual({
      promote: REPLIED_EMPTY_PROMOTE,
      renewals: repliedOf([]),
      prune: undefined,
      taskNames: { kind: "replied", reply: [] },
      dueSchedules: {
        kind: "failed",
        error: expect.objectContaining({ message: expect.stringContaining("WRONGTYPE") }),
      },
    });
    await redis.del(keys.schedule("broken-report"));
  },
);

engineTest("the server instant is the server's TIME", async ({ engine, redis }) => {
  const instantBefore = await serverInstantInMillisecondsOf(redis);

  const serverInstant = await engine.serverInstant();

  expect(serverInstant).toBeGreaterThanOrEqual(instantBefore);
  expect(serverInstant).toBeLessThanOrEqual(await serverInstantInMillisecondsOf(redis));
});

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

async function withBlockingClient(redis: Redis, use: (blockingClient: Redis) => Promise<void>): Promise<void> {
  const blockingClient = redis.duplicate();
  blockingClient.on("error", () => undefined);
  try {
    await blockingClient.connect();
    await use(blockingClient);
  } finally {
    blockingClient.disconnect();
  }
}

const LONG_WAKE_UP_TIMEOUT_IN_MILLISECONDS = 5000;

engineTest(
  "a wake-up wait returns at once on an armed marker, and takes it",
  async ({ engine, redis, keys, keyPrefix }) => {
    await engine.enqueue({ runs: [{ runId: "welcome-ada", task: "send-email", payload: "{}", retries: 0 }] });

    await withBlockingClient(redis, async (blockingClient) => {
      const waitStartedAt = Date.now();
      await wakeUpWaitOf(blockingClient, keyPrefix)(LONG_WAKE_UP_TIMEOUT_IN_MILLISECONDS);

      expect(Date.now() - waitStartedAt).toBeLessThan(LONG_WAKE_UP_TIMEOUT_IN_MILLISECONDS);
      expect(await redis.exists(keys.wakeUpMarker)).toBe(0);
    });
  },
);

engineTest(
  "a wake-up wait returns once an enqueue arms the marker while it blocks",
  async ({ engine, redis, keyPrefix }) => {
    await withBlockingClient(redis, async (blockingClient) => {
      const waitStartedAt = Date.now();
      const wakeUpWait = wakeUpWaitOf(blockingClient, keyPrefix)(LONG_WAKE_UP_TIMEOUT_IN_MILLISECONDS);
      await engine.enqueue({ runs: [{ runId: "welcome-ada", task: "send-email", payload: "{}", retries: 0 }] });
      await wakeUpWait;

      expect(Date.now() - waitStartedAt).toBeLessThan(LONG_WAKE_UP_TIMEOUT_IN_MILLISECONDS);
    });
  },
);

engineTest("a wake-up wait on no marker returns at its fractional timeout", async ({ redis, keyPrefix }) => {
  await withBlockingClient(redis, async (blockingClient) => {
    const waitStartedAt = Date.now();
    await wakeUpWaitOf(blockingClient, keyPrefix)(300);

    expect(Date.now() - waitStartedAt).toBeGreaterThanOrEqual(250);
  });
});

integrationTest(
  "a fetch of a record whose task is not the task of its stored run id throws an unreadable reply",
  async ({ redisUrl, keyPrefix, expectInvariantViolation }) => {
    await withRedis(redisUrl, async (redis) => {
      const engine = createEngine(redis, keyPrefix);
      const keys = engineKeysOf(keyPrefix);
      const storedRunId = storedRunIdOf("send-receipt", "order-1");
      const corruptions = [
        `run "${storedRunId}" has a record of task "update-stock" under an id of task "send-receipt"`,
        `run "${storedRunId}" sits in ${keys.runIndex("send-receipt", "pending")} though its record is executing`,
        `run "${storedRunId}" is of task "update-stock", missing from ${keys.tasks}`,
      ];
      for (const corruption of corruptions) {
        expectInvariantViolation("stored keys", corruption);
      }
      await engine.enqueue({ runs: [{ runId: "order-1", task: "send-receipt", payload: "{}", retries: 0 }] });
      await redis.hset(keys.runRecord("send-receipt", "order-1"), "task", "update-stock");

      const fetch = engine.fetch({
        lockTokens: freshLockTokensOf(1),
        concurrencyLimitsByTask: {},
        lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS,
      });

      await expect(fetch).rejects.toThrow("The fetch script sent a reply the engine cannot read");
      expect([...(await storedKeyViolationsOf(redis, keyPrefix))].sort()).toEqual([...corruptions].sort());
    });
  },
);

integrationTest(
  "an enqueue whose reply holds a member that is not a stored run id throws an unreadable reply",
  async ({ redisUrl, keyPrefix }) => {
    await withRedis(redisUrl, async (redis) => {
      const engine = createEngine(redis, keyPrefix);
      Reflect.set(redis, "nextaskingEnqueue", async () => ["enqueued", ["order-1"], []]);

      const enqueue = engine.enqueue({
        runs: [{ runId: "order-1", task: "send-receipt", payload: "{}", retries: 0 }],
      });

      await expect(enqueue).rejects.toThrow("The enqueue script sent a reply the engine cannot read");
    });
  },
);
