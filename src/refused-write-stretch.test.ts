import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";

import { createConnectionOutageTracker } from "./connection.js";
import { createRefusedWriteStretch } from "./refused-write-stretch.js";
import { createRecordingLogger } from "../tests/recording-logger.js";
import type { RecordedLogLine } from "../tests/recording-logger.js";

const GIVE_UP_AFTER_IN_MILLISECONDS = 1000;

function readOnlyReply(): Error {
  return new Error("READONLY You can't write against a read only replica.");
}

function outOfMemoryReply(): Error {
  return new Error("OOM command not allowed when used memory > 'maxmemory'.");
}

function pairNaming(err: Error): RecordedLogLine[] {
  return [
    { level: "error", fields: { err }, message: "connection error" },
    { level: "info", fields: {}, message: "connection restored" },
  ];
}

function stretchScenario() {
  const { logger, lines } = createRecordingLogger();
  const outageTracker = createConnectionOutageTracker(logger);
  const refusedWriteStretch = createRefusedWriteStretch({
    outageTracker,
    giveUpAfterInMilliseconds: GIVE_UP_AFTER_IN_MILLISECONDS,
  });
  return { outageTracker, refusedWriteStretch, lines, messages: () => lines.map(({ message }) => message) };
}

beforeEach(() => {
  vi.useFakeTimers({ now: Date.UTC(2030, 0, 1) });
});

afterEach(() => {
  vi.useRealTimers();
});

describe("a READONLY stretch", () => {
  test("lasting giveUpAfter writes connection error with its first reply, then connection restored when a learned write is accepted", async () => {
    const { refusedWriteStretch, lines, messages } = stretchScenario();
    const firstReply = readOnlyReply();

    refusedWriteStretch.noteAnsweredReadOnly(["tick"], firstReply);
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS / 2);
    refusedWriteStretch.noteAnsweredReadOnly(["fetch"], readOnlyReply());
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS / 2 - 1);
    const messagesBeforeGiveUpAfter = messages();
    await vi.advanceTimersByTimeAsync(1);
    await vi.advanceTimersByTimeAsync(10 * GIVE_UP_AFTER_IN_MILLISECONDS);
    refusedWriteStretch.noteAccepted(["fetch"]);
    refusedWriteStretch.noteAccepted(["tick"]);

    expect(messagesBeforeGiveUpAfter).toEqual([]);
    expect(lines).toEqual([
      { level: "error", fields: { err: firstReply }, message: "connection error" },
      { level: "info", fields: {}, message: "connection restored" },
    ]);
    expect(vi.getTimerCount()).toBe(0);
  });

  test("ended by an accepted write before giveUpAfter logs nothing and leaves no timer", async () => {
    const { refusedWriteStretch, lines } = stretchScenario();

    refusedWriteStretch.noteAnsweredReadOnly(["tick"], readOnlyReply());
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS - 1);
    refusedWriteStretch.noteAccepted(["tick"]);
    await vi.advanceTimersByTimeAsync(10 * GIVE_UP_AFTER_IN_MILLISECONDS);

    expect(lines).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
  });

  test("is not ended by an accepted command never answered READONLY, as a read on a replica is", async () => {
    const { refusedWriteStretch, messages } = stretchScenario();

    refusedWriteStretch.noteAnsweredReadOnly(["tick"], readOnlyReply());
    refusedWriteStretch.noteAccepted(["readStep", "INFO"]);
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS);
    refusedWriteStretch.noteAccepted(["readStep"]);

    expect(messages()).toEqual(["connection error"]);
  });

  test("a second stretch after a restore opens a second pair, its clock starting at its own first reply", async () => {
    const { refusedWriteStretch, messages } = stretchScenario();
    refusedWriteStretch.noteAnsweredReadOnly(["tick"], readOnlyReply());
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS);
    refusedWriteStretch.noteAccepted(["tick"]);
    await vi.advanceTimersByTimeAsync(5 * GIVE_UP_AFTER_IN_MILLISECONDS);

    refusedWriteStretch.noteAnsweredReadOnly(["settle"], readOnlyReply());
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS - 1);
    const messagesBeforeTheSecondGiveUpAfter = messages();
    await vi.advanceTimersByTimeAsync(1);
    refusedWriteStretch.noteAccepted(["settle"]);

    expect(messagesBeforeTheSecondGiveUpAfter).toEqual(["connection error", "connection restored"]);
    expect(messages()).toEqual(["connection error", "connection restored", "connection error", "connection restored"]);
  });

  test("closed early by a learned script accepted on the replica, as one finding nothing to write is, opens its next pair only after another full giveUpAfter", async () => {
    const { refusedWriteStretch, messages } = stretchScenario();
    refusedWriteStretch.noteAnsweredReadOnly(["renew"], readOnlyReply());
    refusedWriteStretch.noteAnsweredReadOnly(["tick"], readOnlyReply());
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS);

    refusedWriteStretch.noteAccepted(["renew"]);
    refusedWriteStretch.noteAnsweredReadOnly(["tick"], readOnlyReply());
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS - 1);
    const messagesBeforeTheNextGiveUpAfter = messages();
    await vi.advanceTimersByTimeAsync(1);

    expect(messagesBeforeTheNextGiveUpAfter).toEqual(["connection error", "connection restored"]);
    expect(messages()).toEqual(["connection error", "connection restored", "connection error"]);
  });

  test("shares the pair of an overlapping outage, restored only once both are over", async () => {
    const { outageTracker, refusedWriteStretch, messages } = stretchScenario();
    const droppedConnection = {};
    refusedWriteStretch.noteAnsweredReadOnly(["tick"], readOnlyReply());
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS);

    outageTracker.reportClientError(droppedConnection, new Error("connect ECONNREFUSED 127.0.0.1:6379"));
    outageTracker.reportClientReady(droppedConnection);
    const messagesWhileStillReadOnly = messages();
    refusedWriteStretch.noteAccepted(["tick"]);

    expect(messagesWhileStillReadOnly).toEqual(["connection error"]);
    expect(messages()).toEqual(["connection error", "connection restored"]);
  });

  test("forgotten, as when its instance closes, writes no connection restored and leaves no timer", async () => {
    const logged = stretchScenario();
    const notYetLogged = stretchScenario();
    logged.refusedWriteStretch.noteAnsweredReadOnly(["tick"], readOnlyReply());
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS);
    notYetLogged.refusedWriteStretch.noteAnsweredReadOnly(["tick"], readOnlyReply());

    logged.refusedWriteStretch.forget();
    notYetLogged.refusedWriteStretch.forget();
    logged.refusedWriteStretch.noteAccepted(["tick"]);
    await vi.advanceTimersByTimeAsync(10 * GIVE_UP_AFTER_IN_MILLISECONDS);

    expect(logged.messages()).toEqual(["connection error"]);
    expect(notYetLogged.messages()).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
  });
});

describe("an out-of-memory stretch", () => {
  test("writes connection error at its first reply, once across every refusal, then connection restored when a learned write is accepted, leaving no timer", () => {
    const { refusedWriteStretch, lines } = stretchScenario();
    const firstReply = outOfMemoryReply();

    refusedWriteStretch.noteAnsweredOutOfMemory(["saveStep"], firstReply);
    const linesAtTheFirstReply = [...lines];
    refusedWriteStretch.noteAnsweredOutOfMemory(["saveStep"], outOfMemoryReply());
    refusedWriteStretch.noteAnsweredOutOfMemory(["promote", "prune"], outOfMemoryReply());
    refusedWriteStretch.noteAccepted(["prune"]);
    refusedWriteStretch.noteAccepted(["saveStep"]);

    expect(linesAtTheFirstReply).toEqual(pairNaming(firstReply).slice(0, 1));
    expect(lines).toEqual(pairNaming(firstReply));
    expect(vi.getTimerCount()).toBe(0);
  });

  test("is not ended by an accepted command never answered out of memory, as a read or an empty tick is", () => {
    const { refusedWriteStretch, messages } = stretchScenario();

    refusedWriteStretch.noteAnsweredOutOfMemory(["saveStep"], outOfMemoryReply());
    refusedWriteStretch.noteAccepted(["readStep"]);
    refusedWriteStretch.noteAccepted(["promote", "renew", "SMEMBERS"]);

    expect(messages()).toEqual(["connection error"]);
  });

  test("a second stretch after a restore opens a second pair at its own first reply", () => {
    const { refusedWriteStretch, lines } = stretchScenario();
    const firstReply = outOfMemoryReply();
    const secondReply = outOfMemoryReply();
    refusedWriteStretch.noteAnsweredOutOfMemory(["settle"], firstReply);
    refusedWriteStretch.noteAccepted(["settle"]);

    refusedWriteStretch.noteAnsweredOutOfMemory(["saveStep"], secondReply);
    refusedWriteStretch.noteAccepted(["settle"]);
    refusedWriteStretch.noteAccepted(["saveStep"]);

    expect(lines).toEqual([...pairNaming(firstReply), ...pairNaming(secondReply)]);
  });

  test("closed early by a learned script accepted while memory is still short, as one finding nothing to write is, opens its next pair at the next refusal", () => {
    const { refusedWriteStretch, messages } = stretchScenario();
    refusedWriteStretch.noteAnsweredOutOfMemory(["renew", "saveStep"], outOfMemoryReply());

    refusedWriteStretch.noteAccepted(["renew"]);
    refusedWriteStretch.noteAnsweredOutOfMemory(["saveStep"], outOfMemoryReply());

    expect(messages()).toEqual(["connection error", "connection restored", "connection error"]);
  });

  test("whose refused call is abandoned stays open until a call of a learned name is accepted, however long the worker sends only other calls", async () => {
    const { refusedWriteStretch, messages } = stretchScenario();
    refusedWriteStretch.noteAnsweredOutOfMemory(["saveStep"], outOfMemoryReply());

    for (let tickCount = 0; tickCount < 60; tickCount += 1) {
      refusedWriteStretch.noteAccepted(["promote", "renew", "SMEMBERS"]);
      await vi.advanceTimersByTimeAsync(1000);
    }
    const messagesWhileOnlyOtherCallsAreAccepted = messages();
    refusedWriteStretch.noteAccepted(["saveStep"]);

    expect(messagesWhileOnlyOtherCallsAreAccepted).toEqual(["connection error"]);
    expect(messages()).toEqual(["connection error", "connection restored"]);
  });

  test.each([
    { refusalNoted: "READONLY first", isReadOnlyFirst: true },
    { refusalNoted: "out of memory first", isReadOnlyFirst: false },
  ])(
    "overlapping a READONLY stretch past giveUpAfter, $refusalNoted, writes one pair, naming the first reply logged, restored by either learned write",
    async ({ isReadOnlyFirst }) => {
      const { refusedWriteStretch, lines } = stretchScenario();
      const firstReadOnlyReply = readOnlyReply();
      const firstOutOfMemoryReply = outOfMemoryReply();
      const noteReadOnly = () => {
        refusedWriteStretch.noteAnsweredReadOnly(["fetch"], firstReadOnlyReply);
      };
      const noteOutOfMemory = () => {
        refusedWriteStretch.noteAnsweredOutOfMemory(["saveStep"], firstOutOfMemoryReply);
      };
      if (isReadOnlyFirst) {
        noteReadOnly();
        await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS);
        noteOutOfMemory();
      } else {
        noteOutOfMemory();
        noteReadOnly();
        await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS);
      }

      refusedWriteStretch.noteAccepted(isReadOnlyFirst ? ["saveStep"] : ["fetch"]);
      await vi.advanceTimersByTimeAsync(10 * GIVE_UP_AFTER_IN_MILLISECONDS);

      expect(lines).toEqual(pairNaming(isReadOnlyFirst ? firstReadOnlyReply : firstOutOfMemoryReply));
      expect(vi.getTimerCount()).toBe(0);
    },
  );

  test("forgotten, as when its instance closes, writes no connection restored", () => {
    const { refusedWriteStretch, messages } = stretchScenario();
    refusedWriteStretch.noteAnsweredOutOfMemory(["saveStep"], outOfMemoryReply());

    refusedWriteStretch.forget();
    refusedWriteStretch.noteAccepted(["saveStep"]);

    expect(messages()).toEqual(["connection error"]);
  });
});
