import { CronExpressionParser } from "cron-parser";

export type CronExpression = string;

export type IanaTimeZone = string;

export type Schedule = {
  cron: CronExpression;
  timezone?: IanaTimeZone;
};

const DEFAULT_TIMEZONE: IanaTimeZone = "UTC";

export function timezoneOf(schedule: Schedule): IanaTimeZone {
  return schedule.timezone ?? DEFAULT_TIMEZONE;
}

export type DeclaredSchedule = {
  taskName: string;
  cron: CronExpression;
  timezone: IanaTimeZone;
};

export type StoredSchedule = {
  taskName: string;
  cron: CronExpression | undefined;
  timezone: IanaTimeZone | undefined;
};

export type ScheduleChange =
  ({ kind: "added" | "changed" } & DeclaredSchedule) | ({ kind: "removed" } & StoredSchedule);

export function scheduleChangesBetween(
  storedSchedules: readonly StoredSchedule[],
  declaredSchedules: readonly DeclaredSchedule[],
): ScheduleChange[] {
  const storedSchedulesByTaskName = new Map(storedSchedules.map((stored) => [stored.taskName, stored]));
  const declaredTaskNames = new Set(declaredSchedules.map((declared) => declared.taskName));
  const addedOrChanged = declaredSchedules.flatMap((declared): ScheduleChange[] => {
    const stored = storedSchedulesByTaskName.get(declared.taskName);
    if (stored === undefined) {
      return [{ kind: "added", ...declared }];
    }
    const isUnchanged = stored.cron === declared.cron && stored.timezone === declared.timezone;
    return isUnchanged ? [] : [{ kind: "changed", ...declared }];
  });
  const removed = storedSchedules
    .filter((stored) => !declaredTaskNames.has(stored.taskName))
    .map((stored): ScheduleChange => ({ kind: "removed", ...stored }));
  return [...addedOrChanged, ...removed];
}

type CronRejection = { reason: string; cause?: unknown };

function cronRejectionOf(cron: unknown, timezone: IanaTimeZone): CronRejection | undefined {
  if (typeof cron !== "string") {
    return { reason: "it is not a string" };
  }
  if (cron.trim() === "") {
    return { reason: "it is empty" };
  }
  try {
    CronExpressionParser.parse(cron, { tz: timezone });
    return undefined;
  } catch (error) {
    return { reason: (error as Error).message, cause: error };
  }
}

export function rejectInvalidSchedule(taskName: string, schedule: Schedule): void {
  const cron: unknown = schedule.cron;
  const timezone = timezoneOf(schedule);
  try {
    new Intl.DateTimeFormat(undefined, { timeZone: timezone });
  } catch (error) {
    throw new Error(
      `Task "${taskName}" has an invalid schedule.timezone: ${JSON.stringify(timezone)} is not an IANA time zone`,
      { cause: error },
    );
  }
  const cronRejection = cronRejectionOf(cron, timezone);
  if (cronRejection !== undefined) {
    const shownCron = typeof cron === "string" ? JSON.stringify(cron) : String(cron);
    throw new Error(
      `Task "${taskName}" has an invalid schedule.cron: ${shownCron} is not a cron expression: ${cronRejection.reason}`,
      cronRejection.cause === undefined ? undefined : { cause: cronRejection.cause },
    );
  }
}
