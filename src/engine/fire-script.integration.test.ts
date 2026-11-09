import { expect } from "vitest";

import { engineTest, fetchedLockedRunsOf, storedValuesByKeyOf } from "../../tests/engine-test.js";
import type { EngineTestContext } from "../../tests/engine-test.js";
import { serverInstantInMillisecondsOf } from "../../tests/server-instant.js";
import type { EpochMilliseconds } from "../durations.js";
import { deriveFiringRunId } from "../ids.js";
import { createEngine, RECORD_VERSION } from "./engine.js";
import type { FireReply, FireRequest } from "./engine.js";

const ONE_HOUR_IN_MILLISECONDS = 3_600_000;

async function scheduledFiringOf(
  { engine, redis }: EngineTestContext,
  offsetFromNowInMilliseconds: number,
): Promise<FireRequest> {
  const instant: EpochMilliseconds = (await serverInstantInMillisecondsOf(redis)) + offsetFromNowInMilliseconds;
  await engine.syncSchedules({
    schedulesToSync: [{ task: "build-report", cron: "0 * * * *", timezone: "UTC", nextFiringAt: instant, retries: 2 }],
  });
  return {
    task: "build-report",
    instant,
    instantAfter: instant + ONE_HOUR_IN_MILLISECONDS,
    runId: deriveFiringRunId("build-report", instant),
    retries: 2,
  };
}

engineTest(
  "a firing of the planned instant creates the run under the derived id with scheduledAt and advances the record",
  async (context) => {
    const { engine, redis, keys } = context;
    const firing = await scheduledFiringOf(context, -60_000);

    const reply = await engine.fire(firing);

    expect(reply).toEqual({ kind: "fired", runId: firing.runId, isRunCreated: true });
    expect(await redis.hmget(keys.runRecord(firing.runId), "state", "task", "retries", "scheduledAt")).toEqual([
      "pending",
      "build-report",
      "2",
      String(firing.instant),
    ]);
    expect(await redis.hget(keys.runRecord(firing.runId), "concurrencyKey")).toBeNull();
    expect(await redis.get(keys.runPayload(firing.runId))).toBe("");
    expect(await redis.hmget(keys.schedule("build-report"), "nextFiringAt", "previousFiringAt")).toEqual([
      String(firing.instantAfter),
      String(firing.instant),
    ]);
    const [lockedRun] = await fetchedLockedRunsOf(context, 1);
    expect(lockedRun).toMatchObject({ runId: firing.runId, scheduledAt: firing.instant, payload: "" });
  },
);

engineTest(
  "two callers firing the same instant end with one run and one advanced record, the loser hearing already",
  async (context) => {
    const { engine, redis, keys, keyPrefix } = context;
    const firing = await scheduledFiringOf(context, -60_000);
    const otherRedis = redis.duplicate();
    otherRedis.on("error", () => undefined);
    try {
      const otherEngine = createEngine(otherRedis, keyPrefix);

      const replies = await Promise.all([engine.fire(firing), otherEngine.fire(firing)]);

      expect(replies.map(({ kind }) => kind).sort()).toEqual(["already", "fired"]);
      expect(await redis.zrange(keys.pending, 0, -1)).toEqual([firing.runId]);
      expect(await redis.hget(keys.schedule("build-report"), "nextFiringAt")).toBe(String(firing.instantAfter));
    } finally {
      otherRedis.disconnect();
    }
  },
);

type RefusedFiring = {
  description: string;
  offsetFromNowInMilliseconds: number;
  refusedFiringOf: (firing: FireRequest) => FireRequest;
  refusal: FireReply;
};

const REFUSED_FIRINGS: readonly RefusedFiring[] = [
  {
    description: "an instant other than the planned one replies already",
    offsetFromNowInMilliseconds: -60_000,
    refusedFiringOf: (firing) => ({ ...firing, instant: firing.instant - ONE_HOUR_IN_MILLISECONDS }),
    refusal: { kind: "already" },
  },
  {
    description: "an instant still to come replies notDue",
    offsetFromNowInMilliseconds: 60_000,
    refusedFiringOf: (firing) => firing,
    refusal: { kind: "notDue" },
  },
  {
    description: "a task without a schedule record replies noSchedule",
    offsetFromNowInMilliseconds: -60_000,
    refusedFiringOf: (firing) => ({ ...firing, task: "send-digest" }),
    refusal: { kind: "noSchedule" },
  },
];

for (const { description, offsetFromNowInMilliseconds, refusedFiringOf, refusal } of REFUSED_FIRINGS) {
  engineTest(`a firing of ${description} and writes nothing`, async (context) => {
    const { engine, redis, keyPrefix } = context;
    const firing = await scheduledFiringOf(context, offsetFromNowInMilliseconds);
    const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

    const reply = await engine.fire(refusedFiringOf(firing));

    expect(reply).toEqual(refusal);
    expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
  });
}

engineTest("a firing whose run id is still taken creates no run and still advances the record", async (context) => {
  const { engine, redis, keys } = context;
  const firing = await scheduledFiringOf(context, -60_000);
  await redis.zadd(keys.runDedup("build-report"), firing.instant + 2 * ONE_HOUR_IN_MILLISECONDS, firing.runId);

  const reply = await engine.fire(firing);

  expect(reply).toEqual({ kind: "fired", runId: firing.runId, isRunCreated: false });
  expect(await redis.exists(keys.runRecord(firing.runId))).toBe(0);
  expect(await redis.hmget(keys.schedule("build-report"), "nextFiringAt", "previousFiringAt")).toEqual([
    String(firing.instantAfter),
    String(firing.instant),
  ]);
});

engineTest(
  "a firing of a schedule record of a newer version replies unknownVersion and writes nothing",
  async (context) => {
    const { engine, redis, keys, keyPrefix } = context;
    const firing = await scheduledFiringOf(context, -60_000);
    await redis.hset(keys.schedule("build-report"), "version", RECORD_VERSION + 1);
    const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

    const reply = await engine.fire(firing);

    expect(reply).toEqual({ kind: "unknownVersion", recordVersion: RECORD_VERSION + 1 });
    expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
  },
);

engineTest("a firing whose next instant is not after its instant is rejected", async (context) => {
  const { engine } = context;
  const firing = await scheduledFiringOf(context, -60_000);

  const fire = engine.fire({ ...firing, instantAfter: firing.instant });

  await expect(fire).rejects.toThrow("invalid argument: instantAfter must come after instant");
});
