import type { AnyTaskDefinition, EventDefinition } from "./definitions.js";
import { timezoneOf } from "./schedules.js";
import type { CronExpression, IanaTimeZone } from "./schedules.js";

export type TaskDefinitionSummary<TaskName extends string = string, EventName extends string = string> = {
  readonly name: TaskName;
  readonly event?: EventName;
  readonly schedule?: { readonly cron: CronExpression; readonly timezone: IanaTimeZone };
};

export type EventDefinitionSummary<EventName extends string = string, TaskName extends string = string> = {
  readonly name: EventName;
  readonly listenerNames: readonly TaskName[];
};

export function taskDefinitionSummariesOf(tasks: readonly AnyTaskDefinition[]): readonly TaskDefinitionSummary[] {
  return Object.freeze(
    tasks.map((task) =>
      Object.freeze({
        name: task.name,
        ...(task.on === undefined ? {} : { event: task.on.name }),
        ...(task.schedule === undefined
          ? {}
          : { schedule: Object.freeze({ cron: task.schedule.cron, timezone: timezoneOf(task.schedule) }) }),
      }),
    ),
  );
}

export function eventDefinitionSummariesOf(
  events: readonly EventDefinition[],
  listenerNamesByEventName: ReadonlyMap<string, readonly string[]>,
): readonly EventDefinitionSummary[] {
  return Object.freeze(
    events.map((event) =>
      Object.freeze({
        name: event.name,
        listenerNames: Object.freeze([...(listenerNamesByEventName.get(event.name) ?? [])]),
      }),
    ),
  );
}
