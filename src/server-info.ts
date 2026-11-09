import type { Logger } from "./logger.js";
import { builtInCommandOf } from "./redis-client.js";
import type { RedisClient } from "./redis-client.js";

const NO_EVICTION_POLICY = "noeviction";

export type MaxmemoryPolicyReading = { kind: "read"; policy: string } | { kind: "unreadable"; error: unknown };

export function maxmemoryPolicyRejectionOf(policy: string): Error | undefined {
  return policy === NO_EVICTION_POLICY
    ? undefined
    : new Error(
        `maxmemory-policy is "${policy}": the Valkey or Redis holding the queues must use "${NO_EVICTION_POLICY}", or it evicts locks and runs`,
      );
}

type SendInfo = () => Promise<unknown>;

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
    policyIn: (reply) => (typeof reply === "string" ? infoFieldOf(reply, INFO_MAXMEMORY_POLICY_FIELD) : undefined),
    missingPolicyErrorOf: () => new Error("INFO has no maxmemory_policy line"),
  });
}

function queryPolicyFromConfig(source: RedisClient): Promise<PolicyQuery> {
  return queryPolicy<unknown>({
    send: () => source.send(builtInCommandOf("CONFIG", "GET", "maxmemory-policy")),
    policyIn: (reply) => {
      const policy = Array.isArray(reply) ? (reply as unknown[])[1] : undefined;
      return typeof policy === "string" ? policy : undefined;
    },
    missingPolicyErrorOf: (reply) => new Error(`CONFIG GET maxmemory-policy returned ${JSON.stringify(reply)}`),
  });
}

async function readMaxmemoryPolicy(source: RedisClient, sendInfo: SendInfo): Promise<MaxmemoryPolicyReading> {
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
      "maxmemory-policy is readable from neither INFO nor CONFIG GET",
    ),
  };
}

export type ServerName = "Valkey" | "Redis";

export type ServerVersionReading =
  { kind: "read"; serverName: ServerName; version: string } | { kind: "unreadable"; error: unknown };

const VERSION_FIELD_BY_SERVER_NAME: readonly (readonly [ServerName, string])[] = [
  ["Valkey", "valkey_version"],
  ["Redis", "redis_version"],
];

const FLOOR_VERSION_BY_SERVER_NAME: Record<ServerName, readonly [major: number, minor: number]> = {
  Valkey: [8, 0],
  Redis: [7, 4],
};

const SHOWN_FLOOR = "Valkey 8 or Redis 7.4";

const VERSION_PATTERN = /^(\d+)\.(\d+)/;

export function serverVersionReadingOf(infoReply: string): ServerVersionReading {
  for (const [serverName, versionField] of VERSION_FIELD_BY_SERVER_NAME) {
    const version = infoFieldOf(infoReply, versionField);
    if (version === undefined) {
      continue;
    }
    return VERSION_PATTERN.test(version)
      ? { kind: "read", serverName, version }
      : {
          kind: "unreadable",
          error: new Error(`INFO reports ${versionField} ${JSON.stringify(version)}, which is not a version`),
        };
  }
  return { kind: "unreadable", error: new Error("INFO has neither a valkey_version nor a redis_version line") };
}

function serverVersionReadingOfReply(infoReply: unknown): ServerVersionReading {
  return typeof infoReply === "string"
    ? serverVersionReadingOf(infoReply)
    : { kind: "unreadable", error: new Error(`INFO sent ${JSON.stringify(infoReply)}, which is not text`) };
}

export function serverFloorRejectionOf({
  serverName,
  version,
}: Extract<ServerVersionReading, { kind: "read" }>): Error | undefined {
  const [, major = "0", minor = "0"] = VERSION_PATTERN.exec(version) ?? [];
  const [floorMajor, floorMinor] = FLOOR_VERSION_BY_SERVER_NAME[serverName];
  const isAtTheFloor = Number(major) > floorMajor || (Number(major) === floorMajor && Number(minor) >= floorMinor);
  return isAtTheFloor ? undefined : new Error(`${serverName} ${version} is below the floor: ${SHOWN_FLOOR}`);
}

export type ServerInfoReading = {
  maxmemoryPolicyReading: MaxmemoryPolicyReading;
  serverVersionReading: ServerVersionReading;
};

export async function readServerInfo(source: RedisClient): Promise<ServerInfoReading> {
  const infoReply = source.send(builtInCommandOf("INFO"));
  infoReply.catch(() => undefined);
  const maxmemoryPolicyReading = await readMaxmemoryPolicy(source, () => infoReply);
  const serverVersionReading = await infoReply.then(
    serverVersionReadingOfReply,
    (error: unknown): ServerVersionReading => ({
      kind: "unreadable",
      error,
    }),
  );
  return { maxmemoryPolicyReading, serverVersionReading };
}

export function checkServerInfoReading(
  { maxmemoryPolicyReading, serverVersionReading }: ServerInfoReading,
  logger: Logger,
): void {
  checkMaxmemoryPolicyReading(maxmemoryPolicyReading, logger);
  if (serverVersionReading.kind === "unreadable") {
    logger.warn({ err: serverVersionReading.error }, "server version unreadable");
    return;
  }
  const rejection = serverFloorRejectionOf(serverVersionReading);
  if (rejection !== undefined) {
    throw rejection;
  }
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
