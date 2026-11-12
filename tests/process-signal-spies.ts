import { vi } from "vitest";

type ProcessListener = (...listenerArguments: unknown[]) => void;

export type ProcessListenerCall = [method: "on" | "off", event: string | symbol, listener: ProcessListener];

export type ProcessSignalSpies = {
  listenerCalls: () => ProcessListenerCall[];
  attachedListenersOf: (signal: NodeJS.Signals) => ProcessListener[];
  sendSignal: (signal: NodeJS.Signals) => void;
  exitCodes: () => unknown[];
  restore: () => void;
};

export function spyOnProcessSignals(): ProcessSignalSpies {
  const listenerCalls: ProcessListenerCall[] = [];
  const recordListenerCall =
    (method: "on" | "off") =>
    (event: string | symbol, listener: ProcessListener): NodeJS.Process => {
      listenerCalls.push([method, event, listener]);
      return process;
    };
  const onSpy = vi.spyOn(process, "on").mockImplementation(recordListenerCall("on") as typeof process.on);
  const offSpy = vi.spyOn(process, "off").mockImplementation(recordListenerCall("off") as typeof process.off);
  const exitSpy = vi.spyOn(process, "exit").mockImplementation(() => undefined as never);
  const attachedListenersOf = (signal: NodeJS.Signals) =>
    listenerCalls.reduce<ProcessListener[]>((attachedListeners, [method, event, listener]) => {
      if (event !== signal) {
        return attachedListeners;
      }
      return method === "on"
        ? [...attachedListeners, listener]
        : attachedListeners.filter((attachedListener) => attachedListener !== listener);
    }, []);
  return {
    listenerCalls: () => [...listenerCalls],
    attachedListenersOf,
    sendSignal: (signal) => {
      for (const listener of attachedListenersOf(signal)) {
        listener(signal);
      }
    },
    exitCodes: () => exitSpy.mock.calls.map(([exitCode]) => exitCode),
    restore: () => {
      onSpy.mockRestore();
      offSpy.mockRestore();
      exitSpy.mockRestore();
    },
  };
}
