import * as v from "valibot";
import { expect, test } from "vitest";

import { fanOutFailed, runFailed } from "./built-in-events.js";
import { defineListener, defineTask } from "./definitions.js";
import type { AnyTaskDefinition } from "./definitions.js";
import { engineFailureNoticeOf } from "./engine-failure-notice.js";
import type { WorkerDefinitions } from "./engine-failure-notice.js";
import { deriveFailureNoticeEventId, deriveListenerRunId } from "./ids.js";

const sendEmail = defineTask("send-email", { schema: v.object({}), handler: async () => undefined });

const alertOnCall = defineListener("alert-on-call", {
  on: runFailed,
  retries: 2,
  concurrency: { key: (payload) => payload.task, limit: 1 },
  handler: async () => undefined,
});

const archiveFailure = defineListener("archive-failure", { on: runFailed, handler: async () => undefined });

function definitionsOf(tasks: readonly AnyTaskDefinition[]): WorkerDefinitions {
  const listenerNamesByEventName = new Map<string, string[]>();
  for (const task of tasks) {
    if (task.on !== undefined) {
      listenerNamesByEventName.set(task.on.name, [...(listenerNamesByEventName.get(task.on.name) ?? []), task.name]);
    }
  }
  return { tasksByName: new Map(tasks.map((task) => [task.name, task])), listenerNamesByEventName };
}

const FAILING_RUN = { runId: "run-1", task: "send-email", attempt: 3, replays: 1 };

const ERROR = Object.assign(new Error("provider down"), { stack: "Error: provider down\n    at send" });

test("a failure notice holds one run of each runFailed listener of the release, under ids derived from the run's life", () => {
  const notice = engineFailureNoticeOf(
    { ...FAILING_RUN, sourceEvent: { eventName: "user.signed-up", eventId: "event-1" } },
    { error: ERROR, reason: "retries exhausted" },
    definitionsOf([sendEmail, alertOnCall, archiveFailure] as AnyTaskDefinition[]),
  );

  const eventId = deriveFailureNoticeEventId("send-email", "run-1", 1);
  expect(notice).toEqual({
    payload: {
      task: "send-email",
      runId: "run-1",
      reason: "retries exhausted",
      error: { name: "Error", message: "provider down", stack: "Error: provider down\n    at send" },
      attempt: 3,
      eventName: "user.signed-up",
      eventId: "event-1",
    },
    runs: [
      {
        runId: deriveListenerRunId(eventId, "alert-on-call"),
        task: "alert-on-call",
        retries: 2,
        concurrencyKey: "send-email",
        sourceEvent: { eventName: runFailed.name, eventId },
      },
      {
        runId: deriveListenerRunId(eventId, "archive-failure"),
        task: "archive-failure",
        retries: 5,
        sourceEvent: { eventName: runFailed.name, eventId },
      },
    ],
    eventDedup: { eventName: runFailed.name, eventId },
  });
});

test("a replayed run that fails again gets a notice of its own event id", () => {
  const definitions = definitionsOf([sendEmail, alertOnCall] as AnyTaskDefinition[]);
  const failure = { error: ERROR, reason: "retries exhausted" as const };

  const firstNotice = engineFailureNoticeOf({ ...FAILING_RUN, replays: 0 }, failure, definitions);
  const replayedNotice = engineFailureNoticeOf({ ...FAILING_RUN, replays: 1 }, failure, definitions);

  expect(firstNotice.eventDedup.eventId).not.toBe(replayedNotice.eventDedup.eventId);
});

test("a failing listener of a built-in event gets an empty notice", () => {
  for (const builtInEvent of [runFailed, fanOutFailed]) {
    const notice = engineFailureNoticeOf(
      { ...FAILING_RUN, task: "alert-on-call", sourceEvent: { eventName: builtInEvent.name, eventId: "notice-1" } },
      { error: ERROR, reason: "non-retriable" },
      definitionsOf([sendEmail, alertOnCall] as AnyTaskDefinition[]),
    );

    expect(notice.runs).toEqual([]);
  }
});

test("a release with no runFailed listener gets an empty notice", () => {
  const notice = engineFailureNoticeOf(
    FAILING_RUN,
    { error: ERROR, reason: "non-retriable" },
    definitionsOf([sendEmail] as AnyTaskDefinition[]),
  );

  expect(notice.runs).toEqual([]);
});
