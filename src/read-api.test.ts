import { describe, expect, test, vi } from "vitest";
import { z } from "zod";

import { defineTask } from "./definitions.js";
import { storedRunIdOf } from "./engine/engine-keys.js";
import { RECORD_VERSION } from "./engine/engine.js";
import { createReadApi } from "./read-api.js";
import type { ReadApi } from "./read-api.js";
import type { CommandResult, RedisClient, RedisCommand } from "./redis-client.js";
import type { RunState } from "./run-states.js";

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
      [],
      "Invalid listRuns state: list at least one state, or leave state out to list every state",
    ],
    [
      "a list holding a state that is not one",
      ["failed", "sleeping"],
      'Invalid listRuns state: "sleeping" is not one of',
    ],
    ["a state that is null", null, "Invalid listRuns state: null is not one of"],
    ["a state that is a number", 7, "Invalid listRuns state: 7 is not one of"],
  ])("%s", async (_description, state, message) => {
    const client = clientRefusingEveryCommand();

    await expect(
      readerOfTasks(client, ["send-email"]).listRuns({ state: state as unknown as RunState }),
    ).rejects.toThrow(message);
    expect(client.send).not.toHaveBeenCalled();
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
