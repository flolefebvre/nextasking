import type { ConcurrencyKey } from "../definitions.js";
import type { RunId } from "../ids.js";
import type { RunState } from "../run-states.js";

export type LockToken = string;

export type HashTaggedPrefix = `{${string}}`;

export type EngineKeys = {
  runRecord: (runId: RunId) => string;
  runPayload: (runId: RunId) => string;
  runErrors: (runId: RunId) => string;
  runSteps: (runId: RunId) => string;
  pending: string;
  due: string;
  locks: string;
  lock: (lockToken: LockToken) => string;
  waitingList: (task: string, concurrencyKey: ConcurrencyKey | undefined) => string;
  slotSet: (task: string, concurrencyKey: ConcurrencyKey | undefined) => string;
  runDedup: (task: string) => string;
  eventDedup: (eventName: string) => string;
  schedules: string;
  schedule: (task: string) => string;
  isScheduleKey: (key: string) => boolean;
  tasks: string;
  wakeUpMarker: string;
  order: string;
  runIndex: (task: string, state: RunState) => string;
};

export function hashTaggedPrefixOf(prefix: string): HashTaggedPrefix {
  return `{${prefix}}`;
}

function concurrencyScopeOf(task: string, concurrencyKey: ConcurrencyKey | undefined): string {
  const encodedTask = encodeURIComponent(task);
  return concurrencyKey === undefined ? encodedTask : `${encodedTask}:${encodeURIComponent(concurrencyKey)}`;
}

export function engineKeysOf(prefix: string): EngineKeys {
  const keyOf = (...segments: string[]) => [hashTaggedPrefixOf(prefix), ...segments].join(":");
  const runRecord = (runId: RunId) => keyOf("run", encodeURIComponent(runId));
  const schedules = keyOf("schedules");
  const scheduleRecordKeyStart = `${keyOf("schedule")}:`;
  return {
    runRecord,
    runPayload: (runId) => `${runRecord(runId)}:payload`,
    runErrors: (runId) => `${runRecord(runId)}:errors`,
    runSteps: (runId) => `${runRecord(runId)}:steps`,
    pending: keyOf("pending"),
    due: keyOf("due"),
    locks: keyOf("locks"),
    lock: (lockToken) => keyOf("lock", encodeURIComponent(lockToken)),
    waitingList: (task, concurrencyKey) => keyOf("waiting", concurrencyScopeOf(task, concurrencyKey)),
    slotSet: (task, concurrencyKey) => keyOf("slots", concurrencyScopeOf(task, concurrencyKey)),
    runDedup: (task) => keyOf("dedup", encodeURIComponent(task)),
    eventDedup: (eventName) => keyOf("event-dedup", encodeURIComponent(eventName)),
    schedules,
    schedule: (task) => `${scheduleRecordKeyStart}${encodeURIComponent(task)}`,
    isScheduleKey: (key) => key === schedules || key.startsWith(scheduleRecordKeyStart),
    tasks: keyOf("tasks"),
    wakeUpMarker: keyOf("wake-up"),
    order: keyOf("order"),
    runIndex: (task, state) => keyOf("runs", encodeURIComponent(task), state),
  };
}
