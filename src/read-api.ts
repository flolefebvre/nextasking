import type { AnyTaskDefinition, EventDefinition, Payload, PayloadSchema } from "./definitions.js";
import { UnknownTaskError } from "./errors.js";
import type { EventId, RunId } from "./ids.js";
import { RUN_STATES } from "./run-states.js";
import type { RunState, RunStateFields } from "./run-states.js";
import type { CronExpression, IanaTimeZone } from "./schedules.js";
import type { SavedStepResult } from "./step-engine.js";

export type RunListCursor = string;

export type RunError = {
  attempt: number;
  name: string;
  message: string;
  stack: string;
  failedAt: Date;
};

type PayloadOfTask<Task> = Task extends { schema: infer Schema extends PayloadSchema } ? Payload<Schema> : undefined;

type RunSummaryFields = {
  runId: RunId;
  attempt: number;
  dispatchedAt: Date;
  finishedAt?: Date;
};

export type RunOfTask<Task extends AnyTaskDefinition> = Task extends AnyTaskDefinition
  ? { task: Task["name"] } & RunSummaryFields &
      RunStateFields & {
        payload: PayloadOfTask<Task>;
        errors: RunError[];
        steps: SavedStepResult[];
      }
  : never;

export type ListedRunOfTask<Task extends AnyTaskDefinition> = Task extends AnyTaskDefinition
  ? { task: Task["name"] } & RunSummaryFields & RunStateFields & { lastErrorMessage?: string }
  : never;

export type ListRunsOptionsOf<TaskName extends string, State extends RunState = RunState> = {
  state: State;
  task?: TaskName;
  limit?: number;
  cursor?: RunListCursor;
};

export type RunPageOf<ListedRunOfAnyTask> = {
  runs: ListedRunOfAnyTask[];
  nextCursor?: RunListCursor;
};

export type RunCountsOf<TaskName extends string> = { [Name in TaskName]: Record<RunState, number> };

type ListenerOf<Task, EventName extends string> = Extract<Task, { on: { name: EventName } }>;

export type EventWithRunsOf<
  Task extends AnyTaskDefinition,
  Event extends EventDefinition,
> = Event extends EventDefinition
  ? {
      event: Event["name"];
      eventId: EventId;
      runsByListener: {
        [ListenerName in ListenerOf<Task, Event["name"]>["name"]]:
          RunOfTask<Extract<Task, { name: ListenerName }>> | undefined;
      };
    }
  : never;

export type PreviousFiring = {
  scheduledAt: Date;
  runId: RunId;
};

export type ListedScheduleOf<TaskName extends string> = {
  task: TaskName;
  cron: CronExpression;
  timezone: IanaTimeZone;
  nextFiringAt?: Date;
  previousFiring?: PreviousFiring;
};

export type AnyRun = RunOfTask<AnyTaskDefinition>;

export type AnyListedRun = ListedRunOfTask<AnyTaskDefinition>;

export const DEFAULT_LIST_LIMIT = 50;

export function rejectInvalidState(state: unknown): asserts state is RunState {
  if (!RUN_STATES.includes(state as RunState)) {
    const shownState = typeof state === "string" ? JSON.stringify(state) : String(state);
    const shownStates = RUN_STATES.map((runState) => `"${runState}"`).join(", ");
    throw new Error(`Invalid listRuns state: ${shownState} is not one of ${shownStates}`);
  }
}

export function listLimitOf(limit: unknown): number {
  if (limit === undefined) {
    return DEFAULT_LIST_LIMIT;
  }
  if (typeof limit !== "number" || !Number.isInteger(limit) || limit <= 0) {
    const shownLimit = typeof limit === "string" ? JSON.stringify(limit) : String(limit);
    throw new Error(`Invalid listRuns limit: ${shownLimit} is not a positive integer`);
  }
  return limit;
}

export function rejectUnknownTask(tasksByName: ReadonlyMap<string, AnyTaskDefinition>, taskName: string): void {
  if (!tasksByName.has(taskName)) {
    throw new UnknownTaskError(taskName, [...tasksByName.keys()]);
  }
}

export function zeroRunCountsOf(taskNames: Iterable<string>): RunCountsOf<string> {
  return Object.fromEntries(
    [...taskNames].map((taskName) => [
      taskName,
      Object.fromEntries(RUN_STATES.map((runState) => [runState, 0])) as Record<RunState, number>,
    ]),
  );
}
