import { Queue } from "bullmq";
import { Redis } from "ioredis";

import type { EpochMilliseconds } from "../src/durations.js";
import { FIRING_JOB_NAME, RUN_QUEUE_NAME } from "../src/run-queue.js";
import type { StoredFiring } from "../src/run-queue.js";

export async function withRunQueue<Result>(
  redisUrl: string,
  keyPrefix: string,
  use: (runQueue: Queue) => Promise<Result>,
): Promise<Result> {
  const redis = new Redis(redisUrl, { maxRetriesPerRequest: null });
  redis.on("error", () => undefined);
  const runQueue = new Queue(RUN_QUEUE_NAME, { connection: redis, prefix: keyPrefix });
  runQueue.on("error", () => undefined);
  try {
    return await use(runQueue);
  } finally {
    await runQueue.close();
    await redis.quit().catch(() => undefined);
  }
}

export async function promoteNextFiring(
  redisUrl: string,
  keyPrefix: string,
  taskName: string,
): Promise<EpochMilliseconds> {
  return withRunQueue(redisUrl, keyPrefix, async (runQueue) => {
    const scheduler = await runQueue.getJobScheduler(taskName);
    const delayedJobs = await runQueue.getDelayed(0, -1);
    const nextFiringJob = delayedJobs.find(
      (delayedJob) => delayedJob.name === FIRING_JOB_NAME && (delayedJob.data as StoredFiring).taskName === taskName,
    );
    if (scheduler?.next === undefined || nextFiringJob === undefined) {
      throw new Error(`No firing of "${taskName}" is planned`);
    }
    await nextFiringJob.promote();
    return scheduler.next;
  });
}

export type StoredScheduler = {
  taskName: string;
  cron: string | undefined;
  timezone: string | undefined;
  firingTemplateData: unknown;
};

export async function storedSchedulers(redisUrl: string, keyPrefix: string): Promise<StoredScheduler[]> {
  return withRunQueue(redisUrl, keyPrefix, async (runQueue) => {
    const schedulers = await runQueue.getJobSchedulers(0, -1);
    return schedulers
      .map((scheduler) => ({
        taskName: scheduler.key,
        cron: scheduler.pattern,
        timezone: scheduler.tz,
        firingTemplateData: scheduler.template?.data,
      }))
      .sort((first, second) => first.taskName.localeCompare(second.taskName));
  });
}
