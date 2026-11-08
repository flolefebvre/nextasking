import { isBuiltInEventName, runFailed } from "./built-in-events.js";
import type { RunFailedPayload } from "./built-in-events.js";
import { concurrencyKeyOf, retriesOf } from "./definitions.js";
import type { AnyTaskDefinition } from "./definitions.js";
import type { FailureNotice, LockedRun } from "./engine/engine.js";
import { deriveFailureNoticeEventId, deriveListenerRunId } from "./ids.js";
import type { FailureReason } from "./run-queue.js";

export type WorkerDefinitions = {
  tasksByName: ReadonlyMap<string, AnyTaskDefinition>;
  listenerNamesByEventName: ReadonlyMap<string, readonly string[]>;
};

export type FailingRun = Pick<LockedRun, "runId" | "task" | "attempt" | "replays" | "sourceEvent">;

export type RunFailure = { error: Error; reason: FailureReason };

export function engineFailureNoticeOf(
  { runId, task, attempt, replays, sourceEvent }: FailingRun,
  { error, reason }: RunFailure,
  { tasksByName, listenerNamesByEventName }: WorkerDefinitions,
): FailureNotice {
  const eventId = deriveFailureNoticeEventId(task, runId, replays);
  const payload: Omit<RunFailedPayload, "failedAt"> = {
    task,
    runId,
    reason,
    error: { name: error.name, message: error.message, stack: error.stack ?? "" },
    attempt,
    ...sourceEvent,
  };
  const isListenerOfABuiltInEvent = sourceEvent !== undefined && isBuiltInEventName(sourceEvent.eventName);
  const listeners = isListenerOfABuiltInEvent
    ? []
    : (listenerNamesByEventName.get(runFailed.name) ?? []).flatMap((listenerName) => {
        const listener = tasksByName.get(listenerName);
        return listener === undefined ? [] : [listener];
      });
  const payloadSeenByListeners: RunFailedPayload = { ...payload, failedAt: new Date().toISOString() };
  return {
    payload,
    runs: listeners.map((listener) => {
      const concurrencyKey = concurrencyKeyOf(listener, payloadSeenByListeners);
      return {
        runId: deriveListenerRunId(eventId, listener.name),
        task: listener.name,
        retries: retriesOf(listener),
        sourceEvent: { eventName: runFailed.name, eventId },
        ...(concurrencyKey === undefined ? {} : { concurrencyKey }),
      };
    }),
    eventDedup: { eventName: runFailed.name, eventId },
  };
}
