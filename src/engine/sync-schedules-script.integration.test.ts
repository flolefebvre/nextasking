import type { Redis } from "ioredis";
import { expect } from "vitest";

import { engineTest, storedValuesByKeyOf } from "../../tests/engine-test.js";
import type { EngineTestContext } from "../../tests/engine-test.js";
import type { EngineKeys } from "./engine-keys.js";
import { createEngine, RECORD_VERSION } from "./engine.js";
import type { ScheduleToSync } from "./engine.js";
import { withRedisClient } from "../../tests/with-redis.js";

const NIGHTLY_REPORT: ScheduleToSync = {
  task: "build-report",
  cron: "0 3 * * *",
  timezone: "Europe/Paris",
  nextFiringAt: Date.UTC(2030, 0, 1, 2),
  retries: 3,
};

const HOURLY_RATES: ScheduleToSync = {
  task: "refresh-exchange-rates",
  cron: "0 * * * *",
  timezone: "UTC",
  nextFiringAt: Date.UTC(2030, 0, 1, 1),
  retries: 0,
};

async function storedSchedulesOf(redis: Redis, keys: EngineKeys): Promise<ScheduleToSync[]> {
  const storedSchedules: ScheduleToSync[] = [];
  for (const task of await redis.zrange(keys.schedules, 0, -1)) {
    const [cron, timezone, nextFiringAt, retries] = await redis.hmget(
      keys.schedule(task),
      "cron",
      "timezone",
      "nextFiringAt",
      "retries",
    );
    storedSchedules.push({
      task,
      cron: cron!,
      timezone: timezone!,
      nextFiringAt: Number(nextFiringAt),
      retries: Number(retries),
    });
  }
  return storedSchedules;
}

async function nightlyReportFiredOnceOf({ engine, redis, keys }: EngineTestContext): Promise<void> {
  await engine.syncSchedules({ schedulesToSync: [NIGHTLY_REPORT] });
  await redis.hset(keys.schedule("build-report"), "previousFiringAt", Date.UTC(2029, 11, 31, 2));
}

function summaryOf({ task, cron, timezone }: ScheduleToSync) {
  return { task, cron, timezone };
}

engineTest("a sync adds every missing schedule as a record and in the schedules set", async (context) => {
  const { engine, redis, keys } = context;

  const reply = await engine.syncSchedules({ schedulesToSync: [NIGHTLY_REPORT, HOURLY_RATES] });

  expect(reply).toEqual({
    kind: "synced",
    added: [summaryOf(NIGHTLY_REPORT), summaryOf(HOURLY_RATES)],
    changed: [],
    removed: [],
  });
  expect(await redis.hgetall(keys.schedule("build-report"))).toEqual({
    version: String(RECORD_VERSION),
    cron: "0 3 * * *",
    timezone: "Europe/Paris",
    nextFiringAt: String(NIGHTLY_REPORT.nextFiringAt),
    retries: "3",
  });
  expect(await storedSchedulesOf(redis, keys)).toEqual([HOURLY_RATES, NIGHTLY_REPORT]);
});

for (const changedField of ["cron", "timezone"] as const) {
  engineTest(
    `a sync with a changed ${changedField} rewrites the record, drops the planned firing and keeps the previous one`,
    async (context) => {
      const { engine, redis, keys } = context;
      await nightlyReportFiredOnceOf(context);
      const changedSchedule: ScheduleToSync = {
        ...NIGHTLY_REPORT,
        [changedField]: changedField === "cron" ? "0 4 * * *" : "America/New_York",
        nextFiringAt: Date.UTC(2030, 0, 1, 9),
        retries: 1,
      };

      const reply = await engine.syncSchedules({ schedulesToSync: [changedSchedule] });

      expect(reply).toEqual({ kind: "synced", added: [], changed: [summaryOf(changedSchedule)], removed: [] });
      expect(await storedSchedulesOf(redis, keys)).toEqual([changedSchedule]);
      expect(await redis.hget(keys.schedule("build-report"), "previousFiringAt")).toBe(
        String(Date.UTC(2029, 11, 31, 2)),
      );
    },
  );
}

engineTest("a sync with an unchanged schedule leaves its next firing alone and writes nothing", async (context) => {
  const { engine, redis, keyPrefix } = context;
  await engine.syncSchedules({ schedulesToSync: [NIGHTLY_REPORT] });
  const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

  const reply = await engine.syncSchedules({
    schedulesToSync: [{ ...NIGHTLY_REPORT, nextFiringAt: Date.UTC(2030, 0, 2, 2) }],
  });

  expect(reply).toEqual({ kind: "synced", added: [], changed: [], removed: [] });
  expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
});

engineTest(
  "a sync where only the retries differ rewrites the retries alone, keeping both firings and the version",
  async (context) => {
    const { engine, redis, keys } = context;
    await nightlyReportFiredOnceOf(context);
    const recordBefore = await redis.hgetall(keys.schedule("build-report"));

    const reply = await engine.syncSchedules({
      schedulesToSync: [{ ...NIGHTLY_REPORT, retries: 7, nextFiringAt: Date.UTC(2030, 0, 2, 2) }],
    });

    expect(reply).toEqual({ kind: "synced", added: [], changed: [], removed: [] });
    expect(await redis.hgetall(keys.schedule("build-report"))).toEqual({ ...recordBefore, retries: "7" });
    expect(await redis.zscore(keys.schedules, "build-report")).toBe(String(NIGHTLY_REPORT.nextFiringAt));
  },
);

engineTest("a sync removes the schedule of a task absent from the list and reports what it held", async (context) => {
  const { engine, redis, keys } = context;
  await engine.syncSchedules({ schedulesToSync: [NIGHTLY_REPORT, HOURLY_RATES] });

  const reply = await engine.syncSchedules({ schedulesToSync: [HOURLY_RATES] });

  expect(reply).toEqual({ kind: "synced", added: [], changed: [], removed: [summaryOf(NIGHTLY_REPORT)] });
  expect(await redis.exists(keys.schedule("build-report"))).toBe(0);
  expect(await storedSchedulesOf(redis, keys)).toEqual([HOURLY_RATES]);
});

engineTest("two interleaved syncs of different lists end with exactly one list stored", async (context) => {
  const { engine, redis, keys, keyPrefix, redisUrl } = context;
  await withRedisClient(redisUrl, async (otherClient) => {
    const otherEngine = createEngine(otherClient, keyPrefix);
    const firstList = [NIGHTLY_REPORT, HOURLY_RATES];
    const secondList = [{ ...NIGHTLY_REPORT, cron: "0 4 * * *" }];

    await Promise.all(
      Array.from({ length: 10 }, (_, position) =>
        (position % 2 === 0 ? engine : otherEngine).syncSchedules({
          schedulesToSync: position % 2 === 0 ? firstList : secondList,
        }),
      ),
    );

    expect([[HOURLY_RATES, NIGHTLY_REPORT], secondList]).toContainEqual(await storedSchedulesOf(redis, keys));
  });
});

engineTest(
  "a sync over a stored schedule of a newer version replies unknownVersion and writes nothing",
  async (context) => {
    const { engine, redis, keys, keyPrefix } = context;
    await engine.syncSchedules({ schedulesToSync: [NIGHTLY_REPORT, HOURLY_RATES] });
    await redis.hset(keys.schedule("build-report"), "version", RECORD_VERSION + 1);
    const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

    const reply = await engine.syncSchedules({ schedulesToSync: [{ ...HOURLY_RATES, cron: "*/5 * * * *" }] });

    expect(reply).toEqual({ kind: "unknownVersion", recordVersion: RECORD_VERSION + 1 });
    expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
  },
);

engineTest("a sync naming one task twice is rejected and writes nothing", async ({ engine, redis, keyPrefix }) => {
  const syncSchedules = engine.syncSchedules({ schedulesToSync: [NIGHTLY_REPORT, NIGHTLY_REPORT] });

  await expect(syncSchedules).rejects.toThrow("invalid argument: task build-report is listed twice");
  expect(await redis.keys(`{${keyPrefix}}:*`)).toEqual([]);
});
