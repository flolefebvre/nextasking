import * as v from "valibot";
import { expect, test } from "vitest";

import { defineScheduledTask, defineTask, retriesOf } from "./definitions.js";
import type { AnyTaskDefinition } from "./definitions.js";
import type { EpochMilliseconds } from "./durations.js";
import { nextFiringInstantAfter, syncDeclaredSchedules } from "./engine-schedules.js";
import type { CronAndTimezone, ScheduleSync } from "./engine-schedules.js";
import type { Schedule } from "./schedules.js";
import { createInMemoryEngine } from "../tests/in-memory-engine.js";
import type { InMemoryEngine } from "../tests/in-memory-engine.js";
import { createRecordingLogger } from "../tests/recording-logger.js";
import { workerDefinitionsOf } from "../tests/worker-definitions.js";

function firingInstantsAfter(schedule: CronAndTimezone, instant: EpochMilliseconds, count: number): string[] {
  const firingInstants: string[] = [];
  let firingInstant = instant;
  for (let position = 0; position < count; position += 1) {
    firingInstant = nextFiringInstantAfter(schedule, firingInstant);
    firingInstants.push(new Date(firingInstant).toISOString());
  }
  return firingInstants;
}

const PARIS_AT_HALF_PAST_TWO = { cron: "30 2 * * *", timezone: "Europe/Paris" };

test("at the spring change in Europe/Paris, 30 2 * * * fires once, at 03:30 summer time", () => {
  const firingInstants = firingInstantsAfter(PARIS_AT_HALF_PAST_TWO, Date.UTC(2030, 2, 30, 12), 3);

  expect(firingInstants).toEqual(["2030-03-31T01:30:00.000Z", "2030-04-01T00:30:00.000Z", "2030-04-02T00:30:00.000Z"]);
});

test("at the autumn change in Europe/Paris, 30 2 * * * fires once, at the first 02:30", () => {
  const firingInstants = firingInstantsAfter(PARIS_AT_HALF_PAST_TWO, Date.UTC(2030, 9, 26, 12), 3);

  expect(firingInstants).toEqual(["2030-10-27T00:30:00.000Z", "2030-10-28T01:30:00.000Z", "2030-10-29T01:30:00.000Z"]);
});

test("at the autumn change in Europe/Paris, 0 * * * * fires at both 02:00s", () => {
  const firingInstants = firingInstantsAfter(
    { cron: "0 * * * *", timezone: "Europe/Paris" },
    Date.UTC(2030, 9, 26, 23, 30),
    3,
  );

  expect(firingInstants).toEqual(["2030-10-27T00:00:00.000Z", "2030-10-27T01:00:00.000Z", "2030-10-27T02:00:00.000Z"]);
});

test("the next firing instant is strictly after the instant, and a six-field cron fires on its seconds", () => {
  const hourlyFiringInstants = firingInstantsAfter({ cron: "0 * * * *", timezone: "UTC" }, Date.UTC(2030, 0, 1, 10), 1);
  const secondsFiringInstants = firingInstantsAfter(
    { cron: "*/10 * * * * *", timezone: "UTC" },
    Date.UTC(2030, 0, 1, 10),
    2,
  );

  expect(hourlyFiringInstants).toEqual(["2030-01-01T11:00:00.000Z"]);
  expect(secondsFiringInstants).toEqual(["2030-01-01T10:00:10.000Z", "2030-01-01T10:00:20.000Z"]);
});

const SERVER_INSTANT = Date.UTC(2030, 0, 1, 10, 30);

function scheduledTaskOf(name: string, schedule: Schedule): AnyTaskDefinition {
  return defineScheduledTask(name, { schedule, handler: async () => undefined }) as AnyTaskDefinition;
}

function scheduleSyncOf(engine: ScheduleSync["engine"], tasks: readonly AnyTaskDefinition[]) {
  const { logger, lines } = createRecordingLogger();
  return { sync: () => syncDeclaredSchedules({ engine, definitions: workerDefinitionsOf(tasks), logger }), lines };
}

function engineOnTheServerClock(): InMemoryEngine {
  return createInMemoryEngine({ now: () => SERVER_INSTANT });
}

test("a boot sync adds each declared schedule with its next firing after the server's instant, one line each", async () => {
  const engine = engineOnTheServerClock();
  const { sync, lines } = scheduleSyncOf(engine, [
    scheduledTaskOf("build-report", { cron: "0 * * * *" }),
    scheduledTaskOf("send-digest", { cron: "0 3 * * *", timezone: "Europe/Paris" }),
    defineTask("send-email", { schema: v.object({}), handler: async () => undefined }) as AnyTaskDefinition,
  ]);

  await sync();

  expect(engine.storedScheduleOf("build-report")).toMatchObject({ nextFiringAt: Date.UTC(2030, 0, 1, 11) });
  expect(engine.storedScheduleOf("send-digest")).toMatchObject({ nextFiringAt: Date.UTC(2030, 0, 2, 2) });
  expect(engine.storedScheduleOf("send-email")).toBeUndefined();
  expect(lines).toEqual([
    { level: "info", message: "schedule added", fields: { task: "build-report", cron: "0 * * * *", timezone: "UTC" } },
    {
      level: "info",
      message: "schedule added",
      fields: { task: "send-digest", cron: "0 3 * * *", timezone: "Europe/Paris" },
    },
  ]);
});

test("a boot sync of a release with a changed cron and a schedule gone writes schedule changed and schedule removed", async () => {
  const engine = engineOnTheServerClock();
  await scheduleSyncOf(engine, [
    scheduledTaskOf("build-report", { cron: "0 * * * *" }),
    scheduledTaskOf("send-digest", { cron: "0 3 * * *" }),
  ]).sync();
  const { sync, lines } = scheduleSyncOf(engine, [scheduledTaskOf("build-report", { cron: "15 * * * *" })]);

  await sync();

  expect(engine.storedScheduleOf("build-report")).toMatchObject({
    cron: "15 * * * *",
    nextFiringAt: Date.UTC(2030, 0, 1, 11, 15),
  });
  expect(lines).toEqual([
    {
      level: "info",
      message: "schedule changed",
      fields: { task: "build-report", cron: "15 * * * *", timezone: "UTC" },
    },
    { level: "info", message: "schedule removed", fields: { task: "send-digest", cron: "0 3 * * *", timezone: "UTC" } },
  ]);
});

test("a boot sync stores each scheduled task's retries, and a release changing only them writes no line", async () => {
  let serverInstant = SERVER_INSTANT;
  const engine = createInMemoryEngine({ now: () => serverInstant });
  await scheduleSyncOf(engine, [scheduledTaskOf("build-report", { cron: "0 * * * *" })]).sync();
  const retriesBefore = engine.storedScheduleOf("build-report")?.retries;
  serverInstant += 60 * 60 * 1000;
  const { sync, lines } = scheduleSyncOf(engine, [
    defineScheduledTask("build-report", {
      schedule: { cron: "0 * * * *" },
      retries: 1,
      handler: async () => undefined,
    }) as AnyTaskDefinition,
  ]);

  await sync();

  expect(retriesBefore).toBe(retriesOf(undefined));
  expect(engine.storedScheduleOf("build-report")).toMatchObject({ retries: 1, nextFiringAt: Date.UTC(2030, 0, 1, 11) });
  expect(lines).toEqual([]);
});

test("a boot sync refused for a newer schedule record resolves with one worker error naming the version", async () => {
  const engine = engineOnTheServerClock();
  const { sync, lines } = scheduleSyncOf(
    { ...engine, syncSchedules: async () => ({ kind: "unknownVersion", recordVersion: 2 }) },
    [scheduledTaskOf("build-report", { cron: "0 * * * *" })],
  );

  await sync();

  expect(lines).toEqual([
    {
      level: "error",
      message: "worker error",
      fields: {
        err: new Error(
          "The schedule sync was refused: a newer release wrote a schedule record of version 2 this release cannot read, so the stored schedules were left as they are.",
        ),
      },
    },
  ]);
});

test("a boot sync whose call rejects rejects, writing no line", async () => {
  const engine = engineOnTheServerClock();
  const rejection = new Error("Connection is closed.");
  const { sync, lines } = scheduleSyncOf({ ...engine, syncSchedules: () => Promise.reject(rejection) }, [
    scheduledTaskOf("build-report", { cron: "0 * * * *" }),
  ]);

  await expect(sync()).rejects.toBe(rejection);
  expect(lines).toEqual([]);
});
