import { expect } from "vitest";

import { DAY_IN_MILLISECONDS, SEND_EMAIL_PRUNE_REQUEST } from "../../tests/ended-runs.js";
import { backdateEndedRun, engineTest, fetchedLockedRunsOf, storedValuesByKeyOf } from "../../tests/engine-test.js";
import type { EngineTestContext } from "../../tests/engine-test.js";
import { serverInstantInMillisecondsOf } from "../../tests/server-instant.js";
import { deriveFiringRunId } from "../ids.js";
import { storedRunIdOf } from "./engine-keys.js";
import { RECORD_VERSION } from "./engine.js";
import type { FireNowRequest } from "./engine.js";

const NEXT_FIRING_AT = Date.UTC(2030, 0, 1, 3);

const PREVIOUS_FIRING_AT = Date.UTC(2029, 11, 31, 3);

async function storedNightlyReportOf({ engine, redis, keys }: EngineTestContext): Promise<void> {
  await engine.syncSchedules({
    schedulesToSync: [
      { task: "build-report", cron: "0 3 * * *", timezone: "UTC", nextFiringAt: NEXT_FIRING_AT, retries: 4 },
    ],
  });
  await redis.hSet(keys.schedule("build-report"), "previousFiringAt", PREVIOUS_FIRING_AT);
}

async function firingNowOf({ redis }: EngineTestContext, task = "build-report"): Promise<FireNowRequest> {
  const instant = await serverInstantInMillisecondsOf(redis);
  return { task, instant, runId: deriveFiringRunId(task, instant), defaultRetries: 5 };
}

engineTest(
  "a firing now creates the run with scheduledAt and the retries stored on the schedule, leaving the schedule untouched",
  async (context) => {
    const { engine, redis, keys } = context;
    await storedNightlyReportOf(context);
    const scheduleBefore = await redis.hGetAll(keys.schedule("build-report"));
    const schedulesBefore = await redis.zRangeWithScores(keys.schedules, 0, -1);
    const firing = await firingNowOf(context);

    const reply = await engine.fireNow(firing);

    expect(reply).toEqual({ kind: "fired", runId: firing.runId, isRunCreated: true });
    const [lockedRun] = await fetchedLockedRunsOf(context, 1);
    expect(lockedRun).toMatchObject({
      runId: firing.runId,
      task: "build-report",
      payload: "",
      retries: 4,
      scheduledAt: firing.instant,
    });
    expect(await redis.hGetAll(keys.schedule("build-report"))).toEqual(scheduleBefore);
    expect(await redis.zRangeWithScores(keys.schedules, 0, -1)).toEqual(schedulesBefore);
  },
);

engineTest(
  "a firing now of a scheduled task whose name a prune took out of the tasks set puts it back with the run it creates",
  async (context) => {
    const { engine, redis, keys } = context;
    await storedNightlyReportOf(context);
    await engine.fireNow(await firingNowOf(context));
    const [firedRun] = await fetchedLockedRunsOf(context, 1);
    await engine.settle({ ...firedRun!, outcome: { kind: "complete" } });
    await backdateEndedRun(context, "build-report", firedRun!.runId, 2 * DAY_IN_MILLISECONDS);
    await engine.prune({ ...SEND_EMAIL_PRUNE_REQUEST, task: "build-report" });
    const taskNamesAfterTheNameLeft = await redis.sMembers(keys.tasks);
    const firing = { ...(await firingNowOf(context)), runId: "build-report-again" };

    const reply = await engine.fireNow(firing);

    expect(taskNamesAfterTheNameLeft).toEqual([]);
    expect(reply).toEqual({ kind: "fired", runId: firing.runId, isRunCreated: true });
    expect(await redis.sMembers(keys.tasks)).toEqual(["build-report"]);
  },
);

engineTest("a firing now of a schedule stored without retries uses the default retries", async (context) => {
  const { engine, redis, keys } = context;
  await storedNightlyReportOf(context);
  await redis.hDel(keys.schedule("build-report"), "retries");
  const firing = await firingNowOf(context);

  await engine.fireNow(firing);

  expect(await redis.hGet(keys.runRecord("build-report", firing.runId), "retries")).toBe("5");
});

engineTest("a firing now whose run id already exists creates no second run", async (context) => {
  const { engine, redis, keys } = context;
  await storedNightlyReportOf(context);
  const firing = await firingNowOf(context);
  await engine.fireNow(firing);

  const reply = await engine.fireNow(firing);

  expect(reply).toEqual({ kind: "fired", runId: firing.runId, isRunCreated: false });
  expect(await redis.zRange(keys.pending, 0, -1)).toEqual([storedRunIdOf("build-report", firing.runId)]);
});

engineTest("a firing now of a task with no schedule record replies noSchedule and writes nothing", async (context) => {
  const { engine, redis, keyPrefix } = context;
  await storedNightlyReportOf(context);
  const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

  const reply = await engine.fireNow(await firingNowOf(context, "send-digest"));

  expect(reply).toEqual({ kind: "noSchedule" });
  expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
});

engineTest(
  "a firing now of a schedule record of a newer version replies unknownVersion and writes nothing",
  async (context) => {
    const { engine, redis, keys, keyPrefix } = context;
    await storedNightlyReportOf(context);
    await redis.hSet(keys.schedule("build-report"), "version", RECORD_VERSION + 1);
    const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

    const reply = await engine.fireNow(await firingNowOf(context));

    expect(reply).toEqual({ kind: "unknownVersion", recordVersion: RECORD_VERSION + 1 });
    expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
  },
);
