import type { ConcurrencyKey } from "../definitions.js";
import type { RunId } from "../ids.js";
import type { RunState } from "../run-states.js";

export type LockToken = string;

export type HashTaggedPrefix = `{${string}}`;

export type StoredRunId = `${string}:${string}`;

export type RunIdentity = { task: string; runId: RunId };

const STORED_RUN_ID_SEPARATOR = ":";

export function storedRunIdOf(task: string, runId: RunId): StoredRunId {
  if (task === "" || runId === "") {
    throw new Error("invalid argument: a run is identified by a task and a run id, neither empty");
  }
  return `${encodeURIComponent(task)}${STORED_RUN_ID_SEPARATOR}${runId}`;
}

function decodedTaskOf(encodedTask: string): string | undefined {
  try {
    return decodeURIComponent(encodedTask);
  } catch {
    return undefined;
  }
}

export function runIdentityOf(storedRunId: string): RunIdentity | undefined {
  const separatorPosition = storedRunId.indexOf(STORED_RUN_ID_SEPARATOR);
  if (separatorPosition === -1) {
    return undefined;
  }
  const task = decodedTaskOf(storedRunId.slice(0, separatorPosition));
  const runId = storedRunId.slice(separatorPosition + 1);
  if (
    task === undefined ||
    task === "" ||
    runId === "" ||
    encodeURIComponent(task) !== storedRunId.slice(0, separatorPosition)
  ) {
    return undefined;
  }
  return { task, runId };
}

export type EngineKeys = {
  runRecord: (task: string, runId: RunId) => string;
  runPayload: (task: string, runId: RunId) => string;
  runErrors: (task: string, runId: RunId) => string;
  runSteps: (task: string, runId: RunId) => string;
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
  const runRecord = (task: string, runId: RunId) => keyOf("run", encodeURIComponent(storedRunIdOf(task, runId)));
  const schedules = keyOf("schedules");
  const scheduleRecordKeyStart = `${keyOf("schedule")}:`;
  return {
    runRecord,
    runPayload: (task, runId) => `${runRecord(task, runId)}:payload`,
    runErrors: (task, runId) => `${runRecord(task, runId)}:errors`,
    runSteps: (task, runId) => `${runRecord(task, runId)}:steps`,
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
