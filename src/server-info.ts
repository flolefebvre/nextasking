import type { Redis } from "ioredis";

import type { Logger } from "./logger.js";

const NO_EVICTION_POLICY = "noeviction";

export type MaxmemoryPolicyReading = { kind: "read"; policy: string } | { kind: "unreadable"; error: unknown };

export function maxmemoryPolicyRejectionOf(policy: string): Error | undefined {
  return policy === NO_EVICTION_POLICY
    ? undefined
    : new Error(
        `maxmemory-policy is "${policy}": the Valkey or Redis holding the queues must use "${NO_EVICTION_POLICY}", or it evicts locks and runs`,
      );
}

export type MaxmemoryPolicySource = Pick<Redis, "info" | "config">;

export type SendInfo = () => Promise<string>;

type PolicyQuery = { kind: "answered"; policy: string } | { kind: "failed"; error: unknown };

const INFO_MAXMEMORY_POLICY_FIELD = "maxmemory_policy";

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

function infoFieldOf(infoReply: string, fieldName: string): string | undefined {
  const fieldPrefix = `${fieldName}:`;
  return infoReply
    .split(/\r?\n/)
    .find((line) => line.startsWith(fieldPrefix))
    ?.slice(fieldPrefix.length)
    .trim();
}

function queryPolicyFromInfo(sendInfo: SendInfo): Promise<PolicyQuery> {
  return queryPolicy({
    send: sendInfo,
    policyIn: (reply) => infoFieldOf(reply, INFO_MAXMEMORY_POLICY_FIELD),
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

export async function readMaxmemoryPolicy(
  source: MaxmemoryPolicySource,
  sendInfo: SendInfo = () => source.info("memory"),
): Promise<MaxmemoryPolicyReading> {
  const infoQuery = await queryPolicyFromInfo(sendInfo);
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

export function checkMaxmemoryPolicyReading(reading: MaxmemoryPolicyReading, logger: Logger): void {
  if (reading.kind === "unreadable") {
    logger.warn({ err: reading.error }, "maxmemory-policy unreadable");
    return;
  }
  const rejection = maxmemoryPolicyRejectionOf(reading.policy);
  if (rejection !== undefined) {
    throw rejection;
  }
}
