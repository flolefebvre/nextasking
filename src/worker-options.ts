import { parseDuration } from "./durations.js";
import type { Duration, DurationInMilliseconds } from "./durations.js";
import type { Logger } from "./logger.js";

export type StartWorkerOptions = {
  concurrency?: number;
  shutdownTimeout?: Duration;
  onWorkerNotStarted?: (error: unknown) => void | Promise<void>;
};

export type WorkerOptions = {
  concurrency: number;
  shutdownTimeoutInMilliseconds: DurationInMilliseconds;
};

const UNLIMITED_CONCURRENCY = Number.MAX_SAFE_INTEGER;

const DEFAULT_SHUTDOWN_TIMEOUT: Duration = "25s";

export function workerOptionsFrom(options: StartWorkerOptions): WorkerOptions {
  const concurrency: unknown = options.concurrency ?? UNLIMITED_CONCURRENCY;
  if (typeof concurrency !== "number" || !Number.isInteger(concurrency) || concurrency <= 0) {
    const shownConcurrency = typeof concurrency === "string" ? JSON.stringify(concurrency) : String(concurrency);
    throw new Error(`Invalid startWorker concurrency: ${shownConcurrency} is not a positive integer`);
  }
  let shutdownTimeoutInMilliseconds: DurationInMilliseconds;
  try {
    shutdownTimeoutInMilliseconds = parseDuration(options.shutdownTimeout ?? DEFAULT_SHUTDOWN_TIMEOUT);
  } catch (error) {
    throw new Error(`Invalid startWorker shutdownTimeout: ${(error as Error).message}`, { cause: error });
  }
  const onWorkerNotStarted: unknown = options.onWorkerNotStarted;
  if (onWorkerNotStarted !== undefined && typeof onWorkerNotStarted !== "function") {
    const shownCallback =
      typeof onWorkerNotStarted === "string" ? JSON.stringify(onWorkerNotStarted) : String(onWorkerNotStarted);
    throw new Error(`Invalid startWorker onWorkerNotStarted: ${shownCallback} is not a function`);
  }
  return { concurrency, shutdownTimeoutInMilliseconds };
}

export async function callOnWorkerNotStarted(
  onWorkerNotStarted: StartWorkerOptions["onWorkerNotStarted"],
  error: unknown,
  logger: Logger,
): Promise<void> {
  try {
    await onWorkerNotStarted?.(error);
  } catch (callbackError) {
    logger.error({ err: callbackError }, "onWorkerNotStarted failed");
  }
}
