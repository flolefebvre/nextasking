import type { DurationInMilliseconds } from "./durations.js";
import type { Logger } from "./logger.js";
import { isSettledWithin } from "./settled-within.js";
import { callAppCallback } from "./worker-options.js";
import type { StartWorkerOptions } from "./worker-options.js";

const SHUTDOWN_SIGNALS = ["SIGTERM", "SIGINT"] as const;

export const MISSING_MANUAL_SIGNAL_HANDLING_WARNING =
  "NEXT_MANUAL_SIG_HANDLE is not set: on SIGTERM Next exits at once and cuts the attempts running in this worker";

export type ShutdownExit = {
  shutDown: () => Promise<void>;
  onWorkerStopped: StartWorkerOptions["onWorkerStopped"];
  onWorkerStoppedBoundInMilliseconds: DurationInMilliseconds;
  logger: Logger;
};

type ExitCode = 0 | 1;

export function isSignalHandlingLeftToTheApp(env: NodeJS.ProcessEnv = process.env): boolean {
  return Boolean(env.NEXT_MANUAL_SIG_HANDLE);
}

export function isDevelopment(env: NodeJS.ProcessEnv = process.env): boolean {
  return env.NODE_ENV === "development";
}

export function exitOnShutdownSignals({
  shutDown,
  onWorkerStopped,
  onWorkerStoppedBoundInMilliseconds,
  logger,
}: ShutdownExit): () => void {
  let isShuttingDown = false;
  const stopListening = () => {
    for (const signal of SHUTDOWN_SIGNALS) {
      process.off(signal, onShutdownSignal);
    }
  };
  const exitOnceTheAppIsNotified = async (exitCode: ExitCode) => {
    if (onWorkerStopped !== undefined) {
      await isSettledWithin(
        callAppCallback("onWorkerStopped", onWorkerStopped, logger),
        onWorkerStoppedBoundInMilliseconds,
      );
    }
    stopListening();
    process.exit(exitCode);
  };
  function onShutdownSignal() {
    if (isShuttingDown) {
      return;
    }
    isShuttingDown = true;
    void shutDown()
      .then(
        (): ExitCode => 0,
        (error: unknown): ExitCode => {
          logger.error({ err: error }, "worker shutdown failed");
          return 1;
        },
      )
      .then(exitOnceTheAppIsNotified);
  }
  for (const signal of SHUTDOWN_SIGNALS) {
    process.on(signal, onShutdownSignal);
  }
  return () => {
    if (!isShuttingDown) {
      stopListening();
    }
  };
}
