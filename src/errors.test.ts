import { describe, expect, test } from "vitest";

import { InstanceClosedError, RedisUnavailableError, RunNotReplayableError } from "./errors.js";

describe("InstanceClosedError", () => {
  test("says the instance was closed", () => {
    const error = new InstanceClosedError();

    expect(error).toBeInstanceOf(Error);
    expect(error.name).toBe("InstanceClosedError");
    expect(error.message).toBe("this nextasking instance was closed");
  });
});

describe("RedisUnavailableError", () => {
  test("names the wait it gave up after, and keeps the original error and the call's run id", () => {
    const connectionRefused = new Error("connect ECONNREFUSED 127.0.0.1:6379");

    const error = new RedisUnavailableError(
      { kind: "unanswered", giveUpAfterInMilliseconds: 5000 },
      { runId: "welcome-ada" },
      connectionRefused,
    );

    expect(error).toBeInstanceOf(Error);
    expect(error.name).toBe("RedisUnavailableError");
    expect(error.message).toBe("Redis didn't answer within 5s");
    expect(error.cause).toBe(connectionRefused);
    expect(error.runId).toBe("welcome-ada");
    expect(error.eventId).toBeUndefined();
  });

  test("keeps the call's event id", () => {
    const error = new RedisUnavailableError(
      { kind: "unanswered", giveUpAfterInMilliseconds: 1000 },
      { eventId: "invoice-42-paid" },
      new Error("Command timed out"),
    );

    expect(error.eventId).toBe("invoice-42-paid");
    expect(error.runId).toBeUndefined();
  });

  test("says Redis is out of memory, and keeps the reply as its cause", () => {
    const outOfMemoryReply = new Error("OOM command not allowed when used memory > 'maxmemory'.");

    const error = new RedisUnavailableError({ kind: "outOfMemory" }, {}, outOfMemoryReply);

    expect(error.message).toBe("Redis refused the command: it is out of memory");
    expect(error.cause).toBe(outOfMemoryReply);
  });
});

describe("RunNotReplayableError", () => {
  test("says a run whose record a newer release wrote can be replayed from that release", () => {
    const error = new RunNotReplayableError("send-welcome-email", "welcome-ada", "written by a newer release");

    expect(error.message).toBe(
      `Run "welcome-ada" of task "send-welcome-email" can't be replayed: a newer release wrote the run's record, which this release cannot read: replay it from the newer release.`,
    );
  });
});
