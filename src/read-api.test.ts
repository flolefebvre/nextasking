import { describe, expect, test, vi } from "vitest";
import { z } from "zod";

import { defineTask } from "./definitions.js";
import { storedRunIdOf } from "./engine/engine-keys.js";
import { RECORD_VERSION } from "./engine/engine.js";
import { createReadApi } from "./read-api.js";
import type { ReadApi } from "./read-api.js";
import type { CommandResult, RedisClient, RedisCommand } from "./redis-client.js";

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

test("listRuns of an instance listing no task returns an empty page without a round trip", async () => {
  const client: RedisClient = {
    send: vi.fn(() => Promise.reject(new Error("no command expected"))),
    sendBatch: vi.fn(() => Promise.reject(new Error("no batch expected"))),
  };
  const reader = createReadApi({
    client,
    prefix: "no-task",
    tasksByName: new Map(),
    listenerNamesByEventName: new Map(),
    declaredSchedules: [],
    supportedRecordVersion: RECORD_VERSION,
  });

  const page = await reader.listRuns({ state: "failed" });

  expect(page).toEqual({ runs: [], totalRunCount: 0 });
  expect(client.send).not.toHaveBeenCalled();
  expect(client.sendBatch).not.toHaveBeenCalled();
});
