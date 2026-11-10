import { afterEach, beforeEach, expect, test, vi } from "vitest";
import type { MockInstance } from "vitest";

import type { Logger } from "./logger.js";
import { exitOnShutdownSignals } from "./shutdown-signals.js";
import { createRecordingLogger } from "../tests/recording-logger.js";

type ProcessListenerCall = [event: string | symbol, listener: (...listenerArguments: unknown[]) => void];

type WatchedShutdownSignals = {
  listenerCallsOfOn: ProcessListenerCall[];
  onShutdownSignal: () => void;
  stopExitingOnShutdownSignals: () => void;
};

function listenerCallsDuring(method: "on" | "off", call: () => void): ProcessListenerCall[] {
  const listenerSpy = vi.spyOn(process, method).mockImplementation(() => process);
  try {
    call();
    return listenerSpy.mock.calls.map(([event, listener]) => [event, listener]);
  } finally {
    listenerSpy.mockRestore();
  }
}

function shutdownSignalsWatchedBy(shutDown: () => Promise<void>, logger: Logger): WatchedShutdownSignals {
  let stopExitingOnShutdownSignals: () => void = () => undefined;
  const listenerCallsOfOn = listenerCallsDuring("on", () => {
    stopExitingOnShutdownSignals = exitOnShutdownSignals(shutDown, logger);
  });
  const [firstCall] = listenerCallsOfOn;
  if (firstCall === undefined) {
    throw new Error("exitOnShutdownSignals installed no listener");
  }
  const [, onShutdownSignal] = firstCall;
  return { listenerCallsOfOn, onShutdownSignal, stopExitingOnShutdownSignals };
}

let exitSpy: MockInstance<typeof process.exit>;

beforeEach(() => {
  exitSpy = vi.spyOn(process, "exit").mockImplementation(() => undefined as never);
});

afterEach(() => {
  exitSpy.mockRestore();
});

test("listens to SIGTERM and SIGINT with one listener, and the function it returns removes that listener from both", () => {
  const { logger } = createRecordingLogger();

  const { listenerCallsOfOn, onShutdownSignal, stopExitingOnShutdownSignals } = shutdownSignalsWatchedBy(
    () => Promise.resolve(),
    logger,
  );
  const listenerCallsOfOff = listenerCallsDuring("off", stopExitingOnShutdownSignals);

  expect(listenerCallsOfOn).toEqual([
    ["SIGTERM", onShutdownSignal],
    ["SIGINT", onShutdownSignal],
  ]);
  expect(listenerCallsOfOff).toEqual([
    ["SIGTERM", onShutdownSignal],
    ["SIGINT", onShutdownSignal],
  ]);
});

test("a shutdown that rejects writes worker shutdown failed with the rejection as err, and exits with code 1", async () => {
  const { logger, lines } = createRecordingLogger();
  const shutdownFailure = new Error("the command connection did not close");
  const { onShutdownSignal } = shutdownSignalsWatchedBy(() => Promise.reject(shutdownFailure), logger);

  onShutdownSignal();
  await vi.waitFor(() => expect(exitSpy).toHaveBeenCalled(), { timeout: 1_000 });

  expect(lines).toEqual([{ level: "error", message: "worker shutdown failed", fields: { err: shutdownFailure } }]);
  expect(lines[0]?.fields["err"]).toBe(shutdownFailure);
  expect(exitSpy.mock.calls).toEqual([[1]]);
});

test("a shutdown that resolves writes nothing and exits with code 0", async () => {
  const { logger, lines } = createRecordingLogger();
  const { onShutdownSignal } = shutdownSignalsWatchedBy(() => Promise.resolve(), logger);

  onShutdownSignal();
  await vi.waitFor(() => expect(exitSpy).toHaveBeenCalled(), { timeout: 1_000 });

  expect(lines).toEqual([]);
  expect(exitSpy.mock.calls).toEqual([[0]]);
});

test("a second signal during the shutdown starts no second shutdown", async () => {
  const { logger } = createRecordingLogger();
  let endShutdown: () => void = () => undefined;
  const shutdownDone = new Promise<void>((resolve) => {
    endShutdown = resolve;
  });
  const shutDown = vi.fn(() => shutdownDone);
  const { onShutdownSignal } = shutdownSignalsWatchedBy(shutDown, logger);

  onShutdownSignal();
  onShutdownSignal();
  endShutdown();
  await vi.waitFor(() => expect(exitSpy).toHaveBeenCalled(), { timeout: 1_000 });

  expect(shutDown).toHaveBeenCalledTimes(1);
  expect(exitSpy.mock.calls).toEqual([[0]]);
});
