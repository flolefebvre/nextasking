import type { DurationInMilliseconds } from "./durations.js";
import type { SendBuiltInEvent } from "./failure-notices.js";
import { sendUnsentFailureNotices } from "./failure-notices.js";
import type { Logger } from "./logger.js";
import type { RunQueue } from "./run-queue.js";
import type { DeclaredSchedule } from "./schedules.js";
import { checkMaxmemoryPolicyReading, readMaxmemoryPolicy } from "./server-info.js";
import { isSettledWithin } from "./settled-within.js";
import { callOnWorkerNotStarted } from "./worker-options.js";
import type { StartWorkerOptions } from "./worker-options.js";

export type WorkerBootConfig = {
  runQueue: RunQueue;
  declaredSchedules: readonly DeclaredSchedule[];
  logger: Logger;
  sendBuiltInEvent: SendBuiltInEvent;
  giveUpAfterInMilliseconds: DurationInMilliseconds;
};

export type WorkerBoot = {
  isFirstRoundTripAnswered: boolean;
  completion: Promise<void>;
};

export async function bootWorker(config: WorkerBootConfig): Promise<WorkerBoot> {
  const { runQueue, declaredSchedules, logger } = config;
  const policyReading = readMaxmemoryPolicy(runQueue.client);
  const completion = (async () => {
    checkMaxmemoryPolicyReading(await policyReading, logger);
    const scheduleChanges = await runQueue.syncSchedules(declaredSchedules);
    for (const { kind, taskName, cron, timezone } of scheduleChanges) {
      logger.info({ task: taskName, cron, timezone }, `schedule ${kind}`);
    }
    await sendUnsentFailureNotices(runQueue, config.sendBuiltInEvent);
  })();
  completion.catch(() => undefined);
  const isFirstRoundTripAnswered = await isSettledWithin(
    policyReading.then(
      () => undefined,
      () => undefined,
    ),
    config.giveUpAfterInMilliseconds,
  );
  return { isFirstRoundTripAnswered, completion };
}

export type BackgroundBootConfig = {
  completion: Promise<void>;
  isWorkerRunQueueOpen: () => boolean;
  startTheWorker: () => void;
  closeWorkerRunQueue: () => Promise<void>;
  onWorkerNotStarted: StartWorkerOptions["onWorkerNotStarted"];
  logger: Logger;
};

export async function finishBootInBackground(config: BackgroundBootConfig): Promise<void> {
  try {
    await config.completion;
  } catch (error) {
    if (config.isWorkerRunQueueOpen()) {
      config.logger.error({ err: error }, "worker not started");
      await callOnWorkerNotStarted(config.onWorkerNotStarted, error, config.logger);
      await config.closeWorkerRunQueue();
    }
    return;
  }
  if (config.isWorkerRunQueueOpen()) {
    config.startTheWorker();
  }
}
