import * as v from "valibot";
import { expect, test } from "vitest";

import { runFailedEvent } from "./built-in-events.js";
import { defineListener, defineTask } from "./definitions.js";
import type { AnyTaskDefinition } from "./definitions.js";
import { builtFailureNoticeOf } from "./failure-notices.js";
import { deriveFailureNoticeEventId, deriveListenerRunId } from "./ids.js";
import { brokenKeyRunFailedListener, workerDefinitionsOf } from "../tests/worker-definitions.js";

const sendEmail = defineTask("send-email", { schema: v.object({}), handler: async () => undefined });

const alertOnCall = defineListener("alert-on-call", {
  on: runFailedEvent,
  retries: 2,
  concurrency: { key: (payload) => payload.task, limit: 1 },
  handler: async () => undefined,
});

const archiveFailure = defineListener("archive-failure", { on: runFailedEvent, handler: async () => undefined });

const FAILING_RUN = { runId: "run-1", task: "send-email", attempt: 3, replays: 1 };

const ERROR = Object.assign(new Error("provider down"), { stack: "Error: provider down\n    at send" });

test("a failure notice holds one run of each runFailedEvent listener of the release, under ids derived from the run's life", () => {
  const { failureNotice: notice, listenersLeftOut } = builtFailureNoticeOf(
    { ...FAILING_RUN, sourceEvent: { eventName: "user.signed-up", eventId: "event-1" } },
    { error: ERROR, reason: "retries exhausted" },
    workerDefinitionsOf([sendEmail, alertOnCall, archiveFailure] as AnyTaskDefinition[]),
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
        sourceEvent: { eventName: runFailedEvent.name, eventId },
      },
      {
        runId: deriveListenerRunId(eventId, "archive-failure"),
        task: "archive-failure",
        retries: 5,
        sourceEvent: { eventName: runFailedEvent.name, eventId },
      },
    ],
    eventDedup: { eventName: runFailedEvent.name, eventId },
  });
  expect(listenersLeftOut).toEqual([]);
});

test("a listener whose concurrency key throws is left out of the notice with its error, and the others keep their run", () => {
  const { failureNotice, listenersLeftOut } = builtFailureNoticeOf(
    FAILING_RUN,
    { error: ERROR, reason: "retries exhausted" },
    workerDefinitionsOf([sendEmail, brokenKeyRunFailedListener, archiveFailure] as AnyTaskDefinition[]),
  );

  expect(failureNotice.runs.map(({ task }) => task)).toEqual(["archive-failure"]);
  expect(listenersLeftOut).toEqual([{ listenerName: "broken-key-listener", error: expect.any(Error) }]);
  expect(listenersLeftOut[0]?.error.message).toBe(
    'Task "broken-key-listener" has an invalid concurrency.key: it threw "no tenant in this payload"',
  );
});

test("a replayed run that fails again gets a notice of its own event id", () => {
  const definitions = workerDefinitionsOf([sendEmail, alertOnCall] as AnyTaskDefinition[]);
  const failure = { error: ERROR, reason: "retries exhausted" as const };

  const firstNotice = builtFailureNoticeOf({ ...FAILING_RUN, replays: 0 }, failure, definitions);
  const replayedNotice = builtFailureNoticeOf({ ...FAILING_RUN, replays: 1 }, failure, definitions);

  expect(firstNotice.failureNotice.eventDedup.eventId).not.toBe(replayedNotice.failureNotice.eventDedup.eventId);
});

test("a failing listener of a built-in event gets an empty notice", () => {
  const { failureNotice: notice } = builtFailureNoticeOf(
    { ...FAILING_RUN, task: "alert-on-call", sourceEvent: { eventName: runFailedEvent.name, eventId: "notice-1" } },
    { error: ERROR, reason: "non-retriable" },
    workerDefinitionsOf([sendEmail, alertOnCall] as AnyTaskDefinition[]),
  );

  expect(notice.runs).toEqual([]);
});

test("a release with no runFailedEvent listener gets an empty notice", () => {
  const { failureNotice: notice } = builtFailureNoticeOf(
    FAILING_RUN,
    { error: ERROR, reason: "non-retriable" },
    workerDefinitionsOf([sendEmail] as AnyTaskDefinition[]),
  );

  expect(notice.runs).toEqual([]);
});
