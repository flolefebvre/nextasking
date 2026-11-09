import { describe, expect, test } from "vitest";

import { startRetryingOnceAPortBindingTimeout } from "./own-server.js";

const PORT_BINDING_TIMEOUT = new Error(
  "Timed out after 30000ms while waiting for container ports to be bound to the host",
);

type StartOutcome = { started: string } | { failure: Error };

function recordedStartsOf(startOutcomes: readonly StartOutcome[], removalFailure?: Error) {
  const steps: string[] = [];
  let startCount = 0;
  return {
    steps,
    start: () => {
      const outcome = startOutcomes[startCount] ?? { failure: new Error("no start outcome left") };
      startCount += 1;
      steps.push(`start ${String(startCount)}`);
      return "started" in outcome ? Promise.resolve(outcome.started) : Promise.reject(outcome.failure);
    },
    removeAbandonedStart: () => {
      steps.push(`remove start ${String(startCount)}`);
      return removalFailure === undefined ? Promise.resolve() : Promise.reject(removalFailure);
    },
  };
}

describe("startRetryingOnceAPortBindingTimeout", () => {
  test("removes the start that timed out waiting for its ports to be bound before it starts again, and returns the second start", async () => {
    const { steps, start, removeAbandonedStart } = recordedStartsOf([
      { failure: PORT_BINDING_TIMEOUT },
      { started: "second start" },
    ]);

    const started = await startRetryingOnceAPortBindingTimeout(start, removeAbandonedStart);

    expect(started).toBe("second start");
    expect(steps).toEqual(["start 1", "remove start 1", "start 2"]);
  });

  test("removes a second start that times out too, then throws its port binding timeout without a third start", async () => {
    const { steps, start, removeAbandonedStart } = recordedStartsOf([
      { failure: PORT_BINDING_TIMEOUT },
      { failure: PORT_BINDING_TIMEOUT },
    ]);

    await expect(startRetryingOnceAPortBindingTimeout(start, removeAbandonedStart)).rejects.toBe(PORT_BINDING_TIMEOUT);
    expect(steps).toEqual(["start 1", "remove start 1", "start 2", "remove start 2"]);
  });

  test("throws the port binding timeout and the removal failure together, without starting again, when the abandoned start cannot be removed", async () => {
    const removalFailure = new Error("no such container");
    const { steps, start, removeAbandonedStart } = recordedStartsOf(
      [{ failure: PORT_BINDING_TIMEOUT }, { started: "second start" }],
      removalFailure,
    );

    const rejection = await startRetryingOnceAPortBindingTimeout(start, removeAbandonedStart).catch(
      (error: unknown) => error,
    );

    expect(rejection).toBeInstanceOf(AggregateError);
    expect((rejection as AggregateError).errors).toEqual([PORT_BINDING_TIMEOUT, removalFailure]);
    expect(steps).toEqual(["start 1", "remove start 1"]);
  });

  test("throws any other start failure as it is, without removing or starting again", async () => {
    const logWaitTimeout = new Error('Log message "/Ready to accept connections/" not received after 60000ms');
    const { steps, start, removeAbandonedStart } = recordedStartsOf([{ failure: logWaitTimeout }]);

    await expect(startRetryingOnceAPortBindingTimeout(start, removeAbandonedStart)).rejects.toBe(logWaitTimeout);
    expect(steps).toEqual(["start 1"]);
  });
});
