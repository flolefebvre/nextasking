import { expect } from "vitest";

import { engineTest, fetchedLockedRunsOf, storedValuesByKeyOf } from "../../tests/engine-test.js";
import type { EngineTestContext } from "../../tests/engine-test.js";
import { serverInstantInMillisecondsOf } from "../../tests/server-instant.js";
import { RECORD_VERSION } from "./engine.js";
import type { RunToCreate } from "./engine.js";
import { MOST_DUE_RUNS_PROMOTED_PER_PROMOTE } from "./promote-script.js";

const ONE_HOUR_IN_MILLISECONDS = 3_600_000;

const NEW_YEAR_2030 = Date.UTC(2030, 0, 1);

const NEW_YEAR_2031 = Date.UTC(2031, 0, 1);

function sendEmailRunOf(runId: string, extraFields: Partial<RunToCreate> = {}): RunToCreate {
  return { runId, task: "send-email", payload: "{}", retries: 3, ...extraFields };
}

async function timedRunsOf(context: EngineTestContext): Promise<void> {
  const { engine } = context;
  await engine.enqueue({ runs: [sendEmailRunOf("suspended-run"), sendEmailRunOf("backing-off-run")] });
  const [suspendedRun, backingOffRun] = await fetchedLockedRunsOf(context, 2);
  await engine.saveStep({ ...suspendedRun!, stepId: "charge-card", step: { kind: "value", value: '"charged"' } });
  await engine.saveStep({
    ...suspendedRun!,
    stepId: "wait-for-2030",
    step: { kind: "sleepUntil", resumesAt: NEW_YEAR_2030 },
  });
  await engine.saveStep({
    ...suspendedRun!,
    stepId: "wait-for-2031",
    step: { kind: "sleepUntil", resumesAt: NEW_YEAR_2031 },
  });
  await engine.settle({ ...suspendedRun!, outcome: { kind: "suspend", resumesAt: NEW_YEAR_2030 } });
  await engine.settle({
    ...backingOffRun!,
    outcome: {
      kind: "retry",
      error: { attempt: 1, name: "Error", message: "boom", stack: "", failedAt: "" },
      backoffInMilliseconds: ONE_HOUR_IN_MILLISECONDS,
    },
  });
  await engine.enqueue({ runs: [sendEmailRunOf("delayed-run", { delayInMilliseconds: ONE_HOUR_IN_MILLISECONDS })] });
}

engineTest("makeDue moves every delayed, suspended and backingOff run to pending at once", async (context) => {
  const { engine, redis, keys } = context;
  await timedRunsOf(context);
  await engine.enqueue({ runs: [sendEmailRunOf("pending-run")] });

  const reply = await engine.makeDue();

  expect(reply).toEqual({ kind: "madeDue", madeDueRunCount: 3 });
  expect(await redis.zcard(keys.due)).toBe(0);
  const pendingRunIds = await redis.zrange(keys.pending, 0, -1);
  expect(pendingRunIds[0]).toBe("pending-run");
  expect(new Set(pendingRunIds.slice(1))).toEqual(new Set(["delayed-run", "suspended-run", "backing-off-run"]));
  expect(await redis.hmget(keys.runRecord("delayed-run"), "startsAt", "nextAttemptAt")).toEqual([null, null]);
  expect(await redis.hget(keys.runRecord("backing-off-run"), "nextAttemptAt")).toBeNull();
});

engineTest(
  "a suspended run made due has its resume time and its sleep step rewritten to TIME, and sleepsWokenAt set",
  async (context) => {
    const { engine, redis, keys } = context;
    await timedRunsOf(context);
    const instantBefore = await serverInstantInMillisecondsOf(redis);

    await engine.makeDue();

    const instantAfter = await serverInstantInMillisecondsOf(redis);
    const [resumesAt, sleepsWokenAt] = await redis.hmget(keys.runRecord("suspended-run"), "resumesAt", "sleepsWokenAt");
    expect(Number(sleepsWokenAt)).toBeGreaterThanOrEqual(instantBefore);
    expect(Number(sleepsWokenAt)).toBeLessThanOrEqual(instantAfter);
    expect(resumesAt).toBe(sleepsWokenAt);
    expect(await redis.hgetall(keys.runSteps("suspended-run"))).toEqual({
      "charge-card": '"charged"',
      "wait-for-2030": sleepsWokenAt,
      "wait-for-2031": String(NEW_YEAR_2031),
    });
    expect(await redis.hget(keys.runRecord("backing-off-run"), "sleepsWokenAt")).toBeNull();
  },
);

engineTest("a suspended run made due is fetched as a resume", async (context) => {
  const { engine } = context;
  await timedRunsOf(context);
  await engine.makeDue();

  const lockedRuns = await fetchedLockedRunsOf(context, 3);

  expect(lockedRuns.find(({ runId }) => runId === "suspended-run")).toMatchObject({ isResume: true });
});

engineTest("makeDue with no timed run writes nothing", async (context) => {
  const { engine, redis, keyPrefix } = context;
  await engine.enqueue({ runs: [sendEmailRunOf("pending-run"), sendEmailRunOf("executing-run")] });
  await fetchedLockedRunsOf(context, 1);
  const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

  const reply = await engine.makeDue();

  expect(reply).toEqual({ kind: "madeDue", madeDueRunCount: 0 });
  expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
});

engineTest("makeDue passes over a record of a newer version and leaves it in place", async (context) => {
  const { engine, redis, keys } = context;
  await engine.enqueue({
    runs: ["newer-run", "older-run"].map((runId) =>
      sendEmailRunOf(runId, { delayInMilliseconds: ONE_HOUR_IN_MILLISECONDS }),
    ),
  });
  await redis.hset(keys.runRecord("newer-run"), "version", RECORD_VERSION + 1);

  const reply = await engine.makeDue();

  expect(reply).toEqual({ kind: "madeDue", madeDueRunCount: 1 });
  expect(await redis.zrange(keys.due, 0, -1)).toEqual(["newer-run"]);
  expect(await redis.hget(keys.runRecord("older-run"), "state")).toBe("pending");
});

engineTest(`makeDue moves at most ${String(MOST_DUE_RUNS_PROMOTED_PER_PROMOTE)} runs per call`, async (context) => {
  const { engine, redis, keys } = context;
  await engine.enqueue({
    runs: Array.from({ length: MOST_DUE_RUNS_PROMOTED_PER_PROMOTE + 1 }, (_, position) =>
      sendEmailRunOf(`run-${String(position)}`, { delayInMilliseconds: ONE_HOUR_IN_MILLISECONDS }),
    ),
  });

  const firstReply = await engine.makeDue();
  const secondReply = await engine.makeDue();

  expect(firstReply).toEqual({ kind: "madeDue", madeDueRunCount: MOST_DUE_RUNS_PROMOTED_PER_PROMOTE });
  expect(secondReply).toEqual({ kind: "madeDue", madeDueRunCount: 1 });
  expect(await redis.zcard(keys.due)).toBe(0);
});
