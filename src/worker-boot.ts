import type { Redis } from "ioredis";

import type { DurationInMilliseconds } from "./durations.js";
import type { SendBuiltInEvent } from "./failure-notices.js";
import { sendUnsentFailureNotices } from "./failure-notices.js";
import type { Logger } from "./logger.js";
import type { RunQueue } from "./run-queue.js";
import type { DeclaredSchedule } from "./schedules.js";
import { isSettledWithin } from "./worker-runtime.js";

const NO_EVICTION_POLICY = "noeviction";

type MaxmemoryPolicyReading = { kind: "read"; policy: string } | { kind: "unreadable"; error: unknown };

export function maxmemoryPolicyRejectionOf(policy: string): Error | undefined {
  return policy === NO_EVICTION_POLICY
    ? undefined
    : new Error(
        `maxmemory-policy is "${policy}": the Valkey or Redis holding the queues must use "${NO_EVICTION_POLICY}", or it evicts locks and runs`,
      );
}

async function readMaxmemoryPolicy(client: Redis): Promise<MaxmemoryPolicyReading> {
  try {
    const reply = await client.config("GET", "maxmemory-policy");
    const policy = Array.isArray(reply) ? (reply as unknown[])[1] : undefined;
    return typeof policy === "string"
      ? { kind: "read", policy }
      : { kind: "unreadable", error: new Error(`CONFIG GET maxmemory-policy returned ${JSON.stringify(reply)}`) };
  } catch (error) {
    if ((error as Error).name === "ReplyError") {
      return { kind: "unreadable", error };
    }
    throw error;
  }
}

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
    const reading = await policyReading;
    if (reading.kind === "unreadable") {
      logger.warn({ err: reading.error }, "maxmemory-policy unreadable");
    } else {
      const rejection = maxmemoryPolicyRejectionOf(reading.policy);
      if (rejection !== undefined) {
        throw rejection;
      }
    }
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
