import { describe, expect, test } from "vitest";

import { RedisUnavailableError } from "./errors.js";

describe("RedisUnavailableError", () => {
  test("names the wait it gave up after, and keeps the original error and the call's run id", () => {
    const connectionRefused = new Error("connect ECONNREFUSED 127.0.0.1:6379");

    const error = new RedisUnavailableError(5000, { runId: "welcome-ada" }, connectionRefused);

    expect(error).toBeInstanceOf(Error);
    expect(error.name).toBe("RedisUnavailableError");
    expect(error.message).toBe("Redis didn't answer within 5s");
    expect(error.cause).toBe(connectionRefused);
    expect(error.runId).toBe("welcome-ada");
    expect(error.eventId).toBeUndefined();
  });

  test("keeps the call's event id", () => {
    const error = new RedisUnavailableError(1000, { eventId: "invoice-42-paid" }, new Error("Command timed out"));

    expect(error.eventId).toBe("invoice-42-paid");
    expect(error.runId).toBeUndefined();
  });
});
