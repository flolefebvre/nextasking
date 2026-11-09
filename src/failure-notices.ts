import { errorFromThrownValue } from "./attempt-errors.js";
import { isBuiltInEventName, runFailed } from "./built-in-events.js";
import type { RunFailedPayload } from "./built-in-events.js";
import { concurrencyKeyOf, retriesOf } from "./definitions.js";
import type { WorkerDefinitions } from "./definitions.js";
import type { FailureNotice, LockedRun } from "./engine/engine.js";
import { deriveFailureNoticeEventId, deriveListenerRunId } from "./ids.js";
import type { FailureReason } from "./run-states.js";

export type FailingRun = Pick<LockedRun, "runId" | "task" | "attempt" | "replays" | "sourceEvent">;

export type RunFailure = { error: Error; reason: FailureReason };

export type ListenerLeftOut = { listenerName: string; error: Error };

export type BuiltFailureNotice = { failureNotice: FailureNotice; listenersLeftOut: ListenerLeftOut[] };

type NoticeRun = FailureNotice["runs"][number];

export function builtFailureNoticeOf(
  { runId, task, attempt, replays, sourceEvent }: FailingRun,
  { error, reason }: RunFailure,
  { tasksByName, listenerNamesByEventName }: WorkerDefinitions,
): BuiltFailureNotice {
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
  const noticeRuns: NoticeRun[] = [];
  const listenersLeftOut: ListenerLeftOut[] = [];
  for (const listener of listeners) {
    let concurrencyKey: string | undefined;
    try {
      concurrencyKey = concurrencyKeyOf(listener, payloadSeenByListeners);
    } catch (keyError) {
      listenersLeftOut.push({ listenerName: listener.name, error: errorFromThrownValue(keyError) });
      continue;
    }
    noticeRuns.push({
      runId: deriveListenerRunId(eventId, listener.name),
      task: listener.name,
      retries: retriesOf(listener),
      sourceEvent: { eventName: runFailed.name, eventId },
      ...(concurrencyKey === undefined ? {} : { concurrencyKey }),
    });
  }
  return {
    failureNotice: { payload, runs: noticeRuns, eventDedup: { eventName: runFailed.name, eventId } },
    listenersLeftOut,
  };
}
