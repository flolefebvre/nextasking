import { describe, expect, test, vi } from "vitest";
import { z } from "zod";

import { defineTask } from "./definitions.js";
import { storedRunIdOf } from "./engine/engine-keys.js";
import { RECORD_VERSION } from "./engine/engine.js";
import { createReadApi } from "./read-api.js";
import type { ReadApi } from "./read-api.js";
import type { CommandResult, RedisClient, RedisCommand } from "./redis-client.js";
import { UnknownTaskError } from "./errors.js";
import type { ListRunsOptionsOf } from "./read-api.js";

const UNREADABLE_REPLIES: readonly unknown[] = ["not an array", null, 3, []];

const INDEX_PAGE_OF_ONE_RUN = [[1, 0, 0, storedRunIdOf("send-email", "run-1"), "1"]];

function readerWhoseRunReadsAreAnsweredBy(runReadReply: unknown): ReadApi {
  const replyTo = (command: RedisCommand) =>
    command.kind === "script" && command.script.name === "nextaskingIndexPage" ? INDEX_PAGE_OF_ONE_RUN : runReadReply;
  const client: RedisClient = {
    send: (command) => Promise.resolve(replyTo(command)),
    sendBatch: (commands) =>
      Promise.resolve(commands.map((command): CommandResult => ({ kind: "replied", reply: replyTo(command) }))),
  };
  return createReadApi({
    client,
    prefix: "unreadable",
    tasksByName: new Map([
      ["send-email", defineTask("send-email", { schema: z.string(), handler: async () => undefined })],
    ]),
    listenerNamesByEventName: new Map([["invoice.paid", ["send-email"]]]),
    declaredSchedules: [],
    supportedRecordVersion: RECORD_VERSION,
  });
}

describe("a run read whose whole reply the reader cannot read", () => {
  test.for(UNREADABLE_REPLIES)("reads that run as undefined, never throws, when the reply is %j", async (reply) => {
    const reader = readerWhoseRunReadsAreAnsweredBy(reply);

    const reads = {
      run: await reader.getRun("send-email", "run-1"),
      event: await reader.getEvent("invoice.paid", "event-1"),
      page: await reader.listRuns({ state: "failed" }),
    };

    expect(reads).toEqual({
      run: undefined,
      event: { event: "invoice.paid", eventId: "event-1", runsByListener: { "send-email": undefined } },
      page: { runs: [], totalRunCount: 1, firstRunPosition: 1 },
    });
  });
});

function clientRefusingEveryCommand(): RedisClient {
  return {
    send: vi.fn(() => Promise.reject(new Error("no command expected"))),
    sendBatch: vi.fn(() => Promise.reject(new Error("no batch expected"))),
  };
}

function readerOfTasks(client: RedisClient, taskNames: readonly string[]): ReadApi {
  return createReadApi({
    client,
    prefix: "refused",
    tasksByName: new Map(
      taskNames.map((taskName) => [
        taskName,
        defineTask(taskName, { schema: z.string(), handler: async () => undefined }),
      ]),
    ),
    listenerNamesByEventName: new Map(),
    declaredSchedules: [],
    supportedRecordVersion: RECORD_VERSION,
  });
}

describe("listRuns refuses, before any round trip", () => {
  test.each([
    [
      "an empty list of states",
      { state: [] },
      "Invalid listRuns state: list at least one state, or leave state out to list every state",
    ],
    [
      "a list holding a state that is not one",
      { state: ["failed", "sleeping"] },
      'Invalid listRuns state: "sleeping" is not one of',
    ],
    ["a state that is null", { state: null }, "Invalid listRuns state: null is not one of"],
    ["a state that is a number", { state: 7 }, "Invalid listRuns state: 7 is not one of"],
    [
      "an empty list of tasks",
      { task: [] },
      "Invalid listRuns task: list at least one task, or leave task out to list every task",
    ],
    [
      "a list holding a task the instance does not list, last",
      { task: ["send-email", "removed-task"] },
      'Unknown task "removed-task". Valid task names: "send-email", "build-report".',
    ],
    [
      "a list holding a task the instance does not list, first",
      { task: ["removed-task", "send-email"] },
      'Unknown task "removed-task". Valid task names: "send-email", "build-report".',
    ],
    ["a task that is null", { task: null }, 'Unknown task "null"'],
    ["a task that is a number", { task: 7 }, 'Unknown task "7"'],
    [
      "a start that is not a Date",
      { state: "failed", from: "2026-11-11T08:00:00.000Z" },
      'Invalid listRuns from: pass a Date, got "2026-11-11T08:00:00.000Z"',
    ],
    [
      "an end that is an invalid Date",
      { state: "failed", until: new Date("not a date") },
      "Invalid listRuns until: pass a valid Date, got an Invalid Date",
    ],
    [
      "a start after the end",
      { state: "failed", from: new Date("2026-11-11T10:00:00.000Z"), until: new Date("2026-11-11T08:00:00.000Z") },
      "Invalid listRuns range: from 2026-11-11T10:00:00.000Z is after until 2026-11-11T08:00:00.000Z; pass a from at or before until",
    ],
    [
      "a range over states of several order groups, each ordered by a time",
      { state: ["failed", "executing", "suspended", "delayed"], until: new Date("2026-11-11T08:00:00.000Z") },
      'Invalid listRuns range: the states listed are of several order groups, each ordered its own way: "executing" by startedAt; "delayed" and "suspended" by when they are due, startsAt and resumesAt; "failed" by finishedAt. A range takes the states of one order group ordered by a time: executing, waiting for a time, or ended',
    ],
    [
      "a range over states of several order groups, one of them queued",
      { state: ["pending", "executing"], from: new Date("2026-11-11T08:00:00.000Z") },
      'Invalid listRuns range: the states listed are of several order groups, each ordered its own way: "executing" by startedAt; "pending" by their place in the queue, not by a time. A range takes the states of one order group ordered by a time: executing, waiting for a time, or ended',
    ],
    [
      "a range with state left out",
      { from: new Date("2026-11-11T08:00:00.000Z") },
      'Invalid listRuns range: state left out lists every state, and its order groups are each ordered its own way: "executing" by startedAt; "pending" and "waitingForConcurrencySlot" by their place in the queue, not by a time; "delayed", "suspended" and "backingOff" by when they are due, startsAt, resumesAt and nextAttemptAt; "completed" and "failed" by finishedAt. A range takes the states of one order group ordered by a time: executing, waiting for a time, or ended',
    ],
    [
      "a range over pending runs",
      { state: "pending", until: new Date("2026-11-11T08:00:00.000Z") },
      'Invalid listRuns range: "pending" runs are ordered by their place in the queue, not by a time. A range takes the states of one order group ordered by a time: executing, waiting for a time, or ended',
    ],
    [
      "a range over runs waiting for a concurrency slot",
      { state: "waitingForConcurrencySlot", from: new Date("2026-11-11T08:00:00.000Z") },
      'Invalid listRuns range: "waitingForConcurrencySlot" runs are ordered by their place in the queue, not by a time. A range takes the states of one order group ordered by a time: executing, waiting for a time, or ended',
    ],
  ])("%s", async (_description, options, message) => {
    const client = clientRefusingEveryCommand();

    await expect(
      readerOfTasks(client, ["send-email", "build-report"]).listRuns(options as unknown as ListRunsOptionsOf<string>),
    ).rejects.toThrow(message);
    expect(client.send).not.toHaveBeenCalled();
  });

  test("an unknown task in a list throws UnknownTaskError", async () => {
    await expect(
      readerOfTasks(clientRefusingEveryCommand(), ["send-email"]).listRuns({ task: ["send-email", "removed-task"] }),
    ).rejects.toThrow(UnknownTaskError);
  });
});

test("listRuns of an instance listing no task returns an empty page without a round trip", async () => {
  const client = clientRefusingEveryCommand();
  const reader = readerOfTasks(client, []);

  const page = await reader.listRuns({ state: "failed" });

  expect(page).toEqual({ runs: [], totalRunCount: 0 });
  expect(client.send).not.toHaveBeenCalled();
  expect(client.sendBatch).not.toHaveBeenCalled();
});
