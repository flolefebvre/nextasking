import type { Logger } from "./logger.js";

const SHUTDOWN_SIGNALS = ["SIGTERM", "SIGINT"] as const;

export const MISSING_MANUAL_SIGNAL_HANDLING_WARNING =
  "NEXT_MANUAL_SIG_HANDLE is not set: on SIGTERM Next exits at once and cuts the attempts running in this worker";

export function isSignalHandlingLeftToTheApp(env: NodeJS.ProcessEnv = process.env): boolean {
  return Boolean(env.NEXT_MANUAL_SIG_HANDLE);
}

export function isDevelopment(env: NodeJS.ProcessEnv = process.env): boolean {
  return env.NODE_ENV === "development";
}

export function exitOnShutdownSignals(shutDown: () => Promise<void>, logger: Logger): () => void {
  let isShuttingDown = false;
  const onShutdownSignal = () => {
    if (isShuttingDown) {
      return;
    }
    isShuttingDown = true;
    shutDown().then(
      () => process.exit(0),
      (error: unknown) => {
        logger.error({ err: error }, "worker shutdown failed");
        process.exit(1);
      },
    );
  };
  for (const signal of SHUTDOWN_SIGNALS) {
    process.on(signal, onShutdownSignal);
  }
  return () => {
    for (const signal of SHUTDOWN_SIGNALS) {
      process.off(signal, onShutdownSignal);
    }
  };
}
