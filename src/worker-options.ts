import { parseDuration } from "./durations.js";
import type { Duration, DurationInMilliseconds } from "./durations.js";
import type { Logger } from "./logger.js";

export type StartWorkerOptions = {
  concurrency?: number;
  shutdownTimeout?: Duration;
  onWorkerNotStarted?: (error: unknown) => void | Promise<void>;
  onWorkerStopped?: () => void | Promise<void>;
};

export type WorkerOptions = {
  concurrency: number;
  shutdownTimeoutInMilliseconds: DurationInMilliseconds;
};

type AppCallbackName = "onWorkerNotStarted" | "onWorkerStopped";

const UNLIMITED_CONCURRENCY = Number.MAX_SAFE_INTEGER;

const DEFAULT_SHUTDOWN_TIMEOUT: Duration = "25s";

function shownOptionValueOf(optionValue: unknown): string {
  return typeof optionValue === "string" ? JSON.stringify(optionValue) : String(optionValue);
}

function refuseACallbackThatIsNotAFunction(callbackName: AppCallbackName, callback: unknown): void {
  if (callback !== undefined && typeof callback !== "function") {
    throw new Error(`Invalid startWorker ${callbackName}: ${shownOptionValueOf(callback)} is not a function`);
  }
}

export function workerOptionsFrom(options: StartWorkerOptions): WorkerOptions {
  const concurrency: unknown = options.concurrency ?? UNLIMITED_CONCURRENCY;
  if (typeof concurrency !== "number" || !Number.isInteger(concurrency) || concurrency <= 0) {
    throw new Error(`Invalid startWorker concurrency: ${shownOptionValueOf(concurrency)} is not a positive integer`);
  }
  let shutdownTimeoutInMilliseconds: DurationInMilliseconds;
  try {
    shutdownTimeoutInMilliseconds = parseDuration(options.shutdownTimeout ?? DEFAULT_SHUTDOWN_TIMEOUT);
  } catch (error) {
    throw new Error(`Invalid startWorker shutdownTimeout: ${(error as Error).message}`, { cause: error });
  }
  refuseACallbackThatIsNotAFunction("onWorkerNotStarted", options.onWorkerNotStarted);
  refuseACallbackThatIsNotAFunction("onWorkerStopped", options.onWorkerStopped);
  return { concurrency, shutdownTimeoutInMilliseconds };
}

export async function callAppCallback(
  callbackName: AppCallbackName,
  callback: () => void | Promise<void>,
  logger: Logger,
): Promise<void> {
  try {
    await callback();
  } catch (callbackError) {
    logger.error({ err: callbackError }, `${callbackName} failed`);
  }
}
