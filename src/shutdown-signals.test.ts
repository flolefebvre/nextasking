import { afterEach, beforeEach, expect, test, vi } from "vitest";

import type { DurationInMilliseconds } from "./durations.js";
import { exitOnShutdownSignals } from "./shutdown-signals.js";
import { createRecordingLogger } from "../tests/recording-logger.js";
import type { RecordedLogLine } from "../tests/recording-logger.js";
import { spyOnProcessSignals } from "../tests/process-signal-spies.js";
import type { ProcessSignalSpies } from "../tests/process-signal-spies.js";

const ON_WORKER_STOPPED_BOUND_IN_MILLISECONDS: DurationInMilliseconds = 2000;

type HeldPromise = { promise: Promise<void>; settle: () => void; fail: (error: Error) => void };

function heldPromise(): HeldPromise {
  let settle: () => void = () => undefined;
  let fail: (error: Error) => void = () => undefined;
  const promise = new Promise<void>((resolve, reject) => {
    settle = resolve;
    fail = reject;
  });
  return { promise, settle, fail };
}

type ShutdownScenario = {
  lines: RecordedLogLine[];
  stopExitingOnShutdownSignals: () => void;
};

let processSignals: ProcessSignalSpies;

function shutdownScenarioOf(
  shutDown: () => Promise<void>,
  onWorkerStopped?: () => void | Promise<void>,
): ShutdownScenario {
  const { logger, lines } = createRecordingLogger();
  const stopExitingOnShutdownSignals = exitOnShutdownSignals({
    shutDown,
    onWorkerStopped,
    onWorkerStoppedBoundInMilliseconds: ON_WORKER_STOPPED_BOUND_IN_MILLISECONDS,
    logger,
  });
  return { lines, stopExitingOnShutdownSignals };
}

const SHUTDOWN_FAILURE = new Error("the command connection did not close");

const SHUTDOWN_OUTCOMES = [
  { outcome: "resolves", shutDown: () => Promise.resolve(), exitCode: 0, shutdownLines: [] },
  {
    outcome: "rejects",
    shutDown: () => Promise.reject(SHUTDOWN_FAILURE),
    exitCode: 1,
    shutdownLines: [{ level: "error", message: "worker shutdown failed", fields: { err: SHUTDOWN_FAILURE } }],
  },
];

beforeEach(() => {
  vi.useFakeTimers();
  processSignals = spyOnProcessSignals();
});

afterEach(() => {
  processSignals.restore();
  vi.useRealTimers();
});

test("listens to SIGTERM and SIGINT with one listener, and the function it returns removes that listener from both", () => {
  const { stopExitingOnShutdownSignals } = shutdownScenarioOf(() => Promise.resolve());
  const [onShutdownSignal] = processSignals.attachedListenersOf("SIGTERM");

  stopExitingOnShutdownSignals();

  expect(processSignals.listenerCalls()).toEqual([
    ["on", "SIGTERM", onShutdownSignal],
    ["on", "SIGINT", onShutdownSignal],
    ["off", "SIGTERM", onShutdownSignal],
    ["off", "SIGINT", onShutdownSignal],
  ]);
});

test.each(SHUTDOWN_OUTCOMES)(
  "a shutdown that $outcome writes its lines, then calls onWorkerStopped once, and exits with code $exitCode only once its promise settled",
  async ({ shutDown, exitCode, shutdownLines }) => {
    const flush = heldPromise();
    const linesWhenCalled: RecordedLogLine[][] = [];
    const { lines } = shutdownScenarioOf(shutDown, () => {
      linesWhenCalled.push([...lines]);
      return flush.promise;
    });

    processSignals.sendSignal("SIGTERM");
    await vi.advanceTimersByTimeAsync(ON_WORKER_STOPPED_BOUND_IN_MILLISECONDS - 1);
    const exitCodesWhileFlushing = processSignals.exitCodes();
    flush.settle();
    await vi.advanceTimersByTimeAsync(0);

    expect(linesWhenCalled).toEqual([shutdownLines]);
    expect(exitCodesWhileFlushing).toEqual([]);
    expect(processSignals.exitCodes()).toEqual([exitCode]);
    expect(lines).toEqual(shutdownLines);
  },
);

test.each(SHUTDOWN_OUTCOMES)(
  "after a shutdown that $outcome, an onWorkerStopped still pending at 2 s holds the exit no longer: the process exits with code $exitCode",
  async ({ shutDown, exitCode }) => {
    shutdownScenarioOf(shutDown, () => heldPromise().promise);

    processSignals.sendSignal("SIGTERM");
    await vi.advanceTimersByTimeAsync(ON_WORKER_STOPPED_BOUND_IN_MILLISECONDS - 1);
    const exitCodesBeforeTheBound = processSignals.exitCodes();
    await vi.advanceTimersByTimeAsync(1);

    expect(exitCodesBeforeTheBound).toEqual([]);
    expect(processSignals.exitCodes()).toEqual([exitCode]);
  },
);

test.each(
  SHUTDOWN_OUTCOMES.flatMap((shutdownOutcome) => [
    {
      ...shutdownOutcome,
      failure: "throws",
      onWorkerStoppedFailingWith: (callbackError: Error) => () => {
        throw callbackError;
      },
    },
    {
      ...shutdownOutcome,
      failure: "rejects",
      onWorkerStoppedFailingWith: (callbackError: Error) => () => Promise.reject(callbackError),
    },
  ]),
)(
  "after a shutdown that $outcome, an onWorkerStopped that $failure writes onWorkerStopped failed with what it threw after the shutdown's lines, and the process exits with code $exitCode",
  async ({ shutDown, exitCode, shutdownLines, onWorkerStoppedFailingWith }) => {
    const callbackError = new Error("the app's logger could not flush");
    const { lines } = shutdownScenarioOf(shutDown, onWorkerStoppedFailingWith(callbackError));

    processSignals.sendSignal("SIGTERM");
    await vi.advanceTimersByTimeAsync(0);

    expect(lines).toEqual([
      ...shutdownLines,
      { level: "error", message: "onWorkerStopped failed", fields: { err: callbackError } },
    ]);
    expect(lines.at(-1)?.fields["err"]).toBe(callbackError);
    expect(processSignals.exitCodes()).toEqual([exitCode]);
  },
);

test.each(SHUTDOWN_OUTCOMES)(
  "a shutdown that $outcome with no onWorkerStopped exits with code $exitCode at once",
  async ({ shutDown, exitCode, shutdownLines }) => {
    const { lines } = shutdownScenarioOf(shutDown);

    processSignals.sendSignal("SIGTERM");
    await vi.advanceTimersByTimeAsync(0);

    expect(lines).toEqual(shutdownLines);
    expect(processSignals.exitCodes()).toEqual([exitCode]);
  },
);

test("a second signal during the shutdown or the wait for onWorkerStopped starts nothing again, and the listener stays on both signals until the exit", async () => {
  const shutdown = heldPromise();
  const flush = heldPromise();
  const shutDown = vi.fn(() => shutdown.promise);
  const onWorkerStopped = vi.fn(() => flush.promise);
  const { stopExitingOnShutdownSignals } = shutdownScenarioOf(shutDown, onWorkerStopped);
  const [onShutdownSignal] = processSignals.attachedListenersOf("SIGTERM");

  processSignals.sendSignal("SIGTERM");
  processSignals.sendSignal("SIGINT");
  shutdown.settle();
  await vi.advanceTimersByTimeAsync(0);
  stopExitingOnShutdownSignals();
  processSignals.sendSignal("SIGTERM");
  const listenersDuringTheWait = [
    processSignals.attachedListenersOf("SIGTERM"),
    processSignals.attachedListenersOf("SIGINT"),
  ];
  flush.settle();
  await vi.advanceTimersByTimeAsync(0);

  expect(shutDown).toHaveBeenCalledTimes(1);
  expect(onWorkerStopped).toHaveBeenCalledTimes(1);
  expect(listenersDuringTheWait).toEqual([[onShutdownSignal], [onShutdownSignal]]);
  expect(processSignals.attachedListenersOf("SIGTERM")).toEqual([]);
  expect(processSignals.attachedListenersOf("SIGINT")).toEqual([]);
  expect(processSignals.exitCodes()).toEqual([0]);
});
