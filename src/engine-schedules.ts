import { CronExpressionParser } from "cron-parser";

import type { EpochMilliseconds } from "./durations.js";
import type { Engine, ScheduleSummary } from "./engine/engine.js";
import type { WorkerDefinitions } from "./engine-failure-notice.js";
import { scheduleSyncLinesOf, writeLifecycleLines } from "./lifecycle-logs.js";
import type { Logger } from "./logger.js";
import { timezoneOf } from "./schedules.js";

export type CronAndTimezone = Pick<ScheduleSummary, "cron" | "timezone">;

export function nextFiringInstantAfter(
  { cron, timezone }: CronAndTimezone,
  instant: EpochMilliseconds,
): EpochMilliseconds {
  return CronExpressionParser.parse(cron, { tz: timezone, currentDate: new Date(instant) })
    .next()
    .getTime();
}

export function declaredSchedulesOf({ tasksByName }: WorkerDefinitions): ScheduleSummary[] {
  return [...tasksByName.values()].flatMap((task) =>
    task.schedule === undefined
      ? []
      : [{ task: task.name, cron: task.schedule.cron, timezone: timezoneOf(task.schedule) }],
  );
}

export type ScheduleSync = {
  engine: Pick<Engine, "serverInstant" | "syncSchedules">;
  definitions: WorkerDefinitions;
  logger: Logger;
};

export async function syncDeclaredSchedules({ engine, definitions, logger }: ScheduleSync): Promise<void> {
  const serverInstant = await engine.serverInstant();
  const schedulesToSync = declaredSchedulesOf(definitions).map((declaredSchedule) => ({
    ...declaredSchedule,
    nextFiringAt: nextFiringInstantAfter(declaredSchedule, serverInstant),
  }));
  writeLifecycleLines(logger, scheduleSyncLinesOf(await engine.syncSchedules({ schedulesToSync })));
}
