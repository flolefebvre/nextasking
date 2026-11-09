import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";

import { createConnectionOutageTracker } from "./connection.js";
import { createReadOnlyStretch } from "./read-only-stretch.js";
import { createRecordingLogger } from "../tests/recording-logger.js";

const GIVE_UP_AFTER_IN_MILLISECONDS = 1000;

function readOnlyReply(): Error {
  return new Error("READONLY You can't write against a read only replica.");
}

function stretchScenario() {
  const { logger, lines } = createRecordingLogger();
  const outageTracker = createConnectionOutageTracker(logger);
  const readOnlyStretch = createReadOnlyStretch({
    outageTracker,
    giveUpAfterInMilliseconds: GIVE_UP_AFTER_IN_MILLISECONDS,
  });
  return { outageTracker, readOnlyStretch, lines, messages: () => lines.map(({ message }) => message) };
}

beforeEach(() => {
  vi.useFakeTimers({ now: Date.UTC(2030, 0, 1) });
});

afterEach(() => {
  vi.useRealTimers();
});

describe("a READONLY stretch", () => {
  test("lasting giveUpAfter writes connection error with its first reply, then connection restored when a learned write is accepted", async () => {
    const { readOnlyStretch, lines, messages } = stretchScenario();
    const firstReply = readOnlyReply();

    readOnlyStretch.noteAnsweredReadOnly(["tick"], firstReply);
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS / 2);
    readOnlyStretch.noteAnsweredReadOnly(["fetch"], readOnlyReply());
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS / 2 - 1);
    const messagesBeforeGiveUpAfter = messages();
    await vi.advanceTimersByTimeAsync(1);
    await vi.advanceTimersByTimeAsync(10 * GIVE_UP_AFTER_IN_MILLISECONDS);
    readOnlyStretch.noteAccepted(["fetch"]);
    readOnlyStretch.noteAccepted(["tick"]);

    expect(messagesBeforeGiveUpAfter).toEqual([]);
    expect(lines).toEqual([
      { level: "error", fields: { err: firstReply }, message: "connection error" },
      { level: "info", fields: {}, message: "connection restored" },
    ]);
    expect(vi.getTimerCount()).toBe(0);
  });

  test("ended by an accepted write before giveUpAfter logs nothing and leaves no timer", async () => {
    const { readOnlyStretch, lines } = stretchScenario();

    readOnlyStretch.noteAnsweredReadOnly(["tick"], readOnlyReply());
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS - 1);
    readOnlyStretch.noteAccepted(["tick"]);
    await vi.advanceTimersByTimeAsync(10 * GIVE_UP_AFTER_IN_MILLISECONDS);

    expect(lines).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
  });

  test("is not ended by an accepted command never answered READONLY, as a read on a replica is", async () => {
    const { readOnlyStretch, messages } = stretchScenario();

    readOnlyStretch.noteAnsweredReadOnly(["tick"], readOnlyReply());
    readOnlyStretch.noteAccepted(["readStep", "INFO"]);
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS);
    readOnlyStretch.noteAccepted(["readStep"]);

    expect(messages()).toEqual(["connection error"]);
  });

  test("a second stretch after a restore opens a second pair, its clock starting at its own first reply", async () => {
    const { readOnlyStretch, messages } = stretchScenario();
    readOnlyStretch.noteAnsweredReadOnly(["tick"], readOnlyReply());
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS);
    readOnlyStretch.noteAccepted(["tick"]);
    await vi.advanceTimersByTimeAsync(5 * GIVE_UP_AFTER_IN_MILLISECONDS);

    readOnlyStretch.noteAnsweredReadOnly(["settle"], readOnlyReply());
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS - 1);
    const messagesBeforeTheSecondGiveUpAfter = messages();
    await vi.advanceTimersByTimeAsync(1);
    readOnlyStretch.noteAccepted(["settle"]);

    expect(messagesBeforeTheSecondGiveUpAfter).toEqual(["connection error", "connection restored"]);
    expect(messages()).toEqual(["connection error", "connection restored", "connection error", "connection restored"]);
  });

  test("closed early by a learned script accepted on the replica, as one finding nothing to write is, opens its next pair only after another full giveUpAfter", async () => {
    const { readOnlyStretch, messages } = stretchScenario();
    readOnlyStretch.noteAnsweredReadOnly(["renew"], readOnlyReply());
    readOnlyStretch.noteAnsweredReadOnly(["tick"], readOnlyReply());
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS);

    readOnlyStretch.noteAccepted(["renew"]);
    readOnlyStretch.noteAnsweredReadOnly(["tick"], readOnlyReply());
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS - 1);
    const messagesBeforeTheNextGiveUpAfter = messages();
    await vi.advanceTimersByTimeAsync(1);

    expect(messagesBeforeTheNextGiveUpAfter).toEqual(["connection error", "connection restored"]);
    expect(messages()).toEqual(["connection error", "connection restored", "connection error"]);
  });

  test("shares the pair of an overlapping outage, restored only once both are over", async () => {
    const { outageTracker, readOnlyStretch, messages } = stretchScenario();
    const droppedConnection = {};
    readOnlyStretch.noteAnsweredReadOnly(["tick"], readOnlyReply());
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS);

    outageTracker.reportClientError(droppedConnection, new Error("connect ECONNREFUSED 127.0.0.1:6379"));
    outageTracker.reportClientReady(droppedConnection);
    const messagesWhileStillReadOnly = messages();
    readOnlyStretch.noteAccepted(["tick"]);

    expect(messagesWhileStillReadOnly).toEqual(["connection error"]);
    expect(messages()).toEqual(["connection error", "connection restored"]);
  });

  test("forgotten, as when its instance closes, writes no connection restored and leaves no timer", async () => {
    const logged = stretchScenario();
    const notYetLogged = stretchScenario();
    logged.readOnlyStretch.noteAnsweredReadOnly(["tick"], readOnlyReply());
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS);
    notYetLogged.readOnlyStretch.noteAnsweredReadOnly(["tick"], readOnlyReply());

    logged.readOnlyStretch.forget();
    notYetLogged.readOnlyStretch.forget();
    logged.readOnlyStretch.noteAccepted(["tick"]);
    await vi.advanceTimersByTimeAsync(10 * GIVE_UP_AFTER_IN_MILLISECONDS);

    expect(logged.messages()).toEqual(["connection error"]);
    expect(notYetLogged.messages()).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
  });
});
