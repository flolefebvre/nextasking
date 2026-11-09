import { CronExpressionParser } from "cron-parser";

import { errorFromThrownValue } from "./attempt-errors.js";
import { retriesOf } from "./definitions.js";
import type { EpochMilliseconds } from "./durations.js";
import type { DueSchedule, DueSchedulesReply, Engine, FireRequest, ScheduleSummary } from "./engine/engine.js";
import type { WorkerDefinitions } from "./engine-failure-notice.js";
import { deriveFiringRunId } from "./ids.js";
import { scheduleSyncLinesOf, workerErrorLineOf, writeLifecycleLines } from "./lifecycle-logs.js";
import type { Logger } from "./logger.js";
import { answeredDespiteOutage } from "./redis-outage.js";
import type { RedisOutage } from "./redis-outage.js";
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

export type DueScheduleFiring = {
  engine: Pick<Engine, "fire">;
  definitions: WorkerDefinitions;
  logger: Logger;
  redisOutage: RedisOutage;
  stopSignal: AbortSignal;
  loggedUnreadableSchedules: Set<string>;
};

function fireRequestOf(
  { task, cron, timezone, nextFiringAt }: DueSchedule,
  serverInstant: EpochMilliseconds,
  definitions: WorkerDefinitions,
): FireRequest {
  return {
    task,
    instant: nextFiringAt,
    instantAfter: nextFiringInstantAfter({ cron, timezone }, Math.max(nextFiringAt, serverInstant)),
    runId: deriveFiringRunId(task, nextFiringAt),
    retries: retriesOf(definitions.tasksByName.get(task)),
  };
}

export async function fireDueSchedules(
  { engine, definitions, logger, redisOutage, stopSignal, loggedUnreadableSchedules }: DueScheduleFiring,
  { serverInstant, dueSchedules }: DueSchedulesReply,
): Promise<void> {
  await Promise.all(
    dueSchedules.map(async (dueSchedule) => {
      let fireRequest: FireRequest;
      try {
        fireRequest = fireRequestOf(dueSchedule, serverInstant, definitions);
      } catch (error) {
        const unreadableSchedule = JSON.stringify([dueSchedule.task, dueSchedule.cron, dueSchedule.timezone]);
        if (!loggedUnreadableSchedules.has(unreadableSchedule)) {
          loggedUnreadableSchedules.add(unreadableSchedule);
          writeLifecycleLines(logger, [workerErrorLineOf(errorFromThrownValue(error))]);
        }
        return;
      }
      try {
        await answeredDespiteOutage(redisOutage, () => engine.fire(fireRequest), { resendStopSignal: stopSignal });
      } catch (error) {
        if (!stopSignal.aborted) {
          writeLifecycleLines(logger, [workerErrorLineOf(errorFromThrownValue(error))]);
        }
      }
    }),
  );
}
