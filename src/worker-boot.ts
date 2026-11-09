import type { Redis } from "ioredis";

import type { DurationInMilliseconds } from "./durations.js";
import type { SendBuiltInEvent } from "./failure-notices.js";
import { sendUnsentFailureNotices } from "./failure-notices.js";
import type { Logger } from "./logger.js";
import type { RunQueue } from "./run-queue.js";
import type { DeclaredSchedule } from "./schedules.js";
import { isSettledWithin } from "./worker-runtime.js";
import type { StartWorkerOptions } from "./worker-runtime.js";

const NO_EVICTION_POLICY = "noeviction";

type MaxmemoryPolicyReading = { kind: "read"; policy: string } | { kind: "unreadable"; error: unknown };

export function maxmemoryPolicyRejectionOf(policy: string): Error | undefined {
  return policy === NO_EVICTION_POLICY
    ? undefined
    : new Error(
        `maxmemory-policy is "${policy}": the Valkey or Redis holding the queues must use "${NO_EVICTION_POLICY}", or it evicts locks and runs`,
      );
}

export type MaxmemoryPolicySource = Pick<Redis, "info" | "config">;

type PolicyQuery = { kind: "answered"; policy: string } | { kind: "failed"; error: unknown };

const INFO_MAXMEMORY_POLICY_PREFIX = "maxmemory_policy:";

async function queryPolicy<Reply>({
  send,
  policyIn,
  missingPolicyErrorOf,
}: {
  send: () => Promise<Reply>;
  policyIn: (reply: Reply) => string | undefined;
  missingPolicyErrorOf: (reply: Reply) => Error;
}): Promise<PolicyQuery> {
  let reply: Reply;
  try {
    reply = await send();
  } catch (error) {
    if ((error as Error).name === "ReplyError") {
      return { kind: "failed", error };
    }
    throw error;
  }
  const policy = policyIn(reply);
  return policy === undefined ? { kind: "failed", error: missingPolicyErrorOf(reply) } : { kind: "answered", policy };
}

function queryPolicyFromInfo(source: MaxmemoryPolicySource): Promise<PolicyQuery> {
  return queryPolicy({
    send: () => source.info("memory"),
    policyIn: (reply) =>
      reply
        .split(/\r?\n/)
        .find((line) => line.startsWith(INFO_MAXMEMORY_POLICY_PREFIX))
        ?.slice(INFO_MAXMEMORY_POLICY_PREFIX.length)
        .trim(),
    missingPolicyErrorOf: () => new Error("INFO memory has no maxmemory_policy line"),
  });
}

function queryPolicyFromConfig(source: MaxmemoryPolicySource): Promise<PolicyQuery> {
  return queryPolicy<unknown>({
    send: () => source.config("GET", "maxmemory-policy"),
    policyIn: (reply) => {
      const policy = Array.isArray(reply) ? (reply as unknown[])[1] : undefined;
      return typeof policy === "string" ? policy : undefined;
    },
    missingPolicyErrorOf: (reply) => new Error(`CONFIG GET maxmemory-policy returned ${JSON.stringify(reply)}`),
  });
}

export async function readMaxmemoryPolicy(source: MaxmemoryPolicySource): Promise<MaxmemoryPolicyReading> {
  const infoQuery = await queryPolicyFromInfo(source);
  if (infoQuery.kind === "answered") {
    return { kind: "read", policy: infoQuery.policy };
  }
  const configQuery = await queryPolicyFromConfig(source);
  if (configQuery.kind === "answered") {
    return { kind: "read", policy: configQuery.policy };
  }
  return {
    kind: "unreadable",
    error: new AggregateError(
      [infoQuery.error, configQuery.error],
      "maxmemory-policy is readable from neither INFO memory nor CONFIG GET",
    ),
  };
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

export type BackgroundBootConfig = {
  completion: Promise<void>;
  isWorkerRunQueueOpen: () => boolean;
  startTheWorker: () => void;
  closeWorkerRunQueue: () => Promise<void>;
  onWorkerNotStarted: StartWorkerOptions["onWorkerNotStarted"];
  logger: Logger;
};

async function callOnWorkerNotStarted(config: BackgroundBootConfig, error: unknown): Promise<void> {
  try {
    await config.onWorkerNotStarted?.(error);
  } catch (callbackError) {
    config.logger.error({ err: callbackError }, "onWorkerNotStarted failed");
  }
}

export async function finishBootInBackground(config: BackgroundBootConfig): Promise<void> {
  try {
    await config.completion;
  } catch (error) {
    if (config.isWorkerRunQueueOpen()) {
      config.logger.error({ err: error }, "worker not started");
      await callOnWorkerNotStarted(config, error);
      await config.closeWorkerRunQueue();
    }
    return;
  }
  if (config.isWorkerRunQueueOpen()) {
    config.startTheWorker();
  }
}
