import { scriptCommandOf } from "../redis-client.js";
import type { RedisCommand, RedisScript } from "../redis-client.js";
import type { EngineKeys, RunIdentity } from "./engine-keys.js";

const RECORD_FIELDS_CHECK = `
if #ARGV == 0 then
  error("invalid argument: a run read takes the record fields to read, got none")
end
`;

export const RUN_READ_SCRIPT = `${RECORD_FIELDS_CHECK}
return {
  redis.call("HMGET", KEYS[1], unpack(ARGV)),
  redis.call("GET", KEYS[2]),
  redis.call("LRANGE", KEYS[3], 0, -1),
  redis.call("HGETALL", KEYS[4])
}
`;

export const LISTED_RUN_READ_SCRIPT = `${RECORD_FIELDS_CHECK}
return {
  redis.call("HMGET", KEYS[1], unpack(ARGV)),
  redis.call("LINDEX", KEYS[2], -1)
}
`;

const RUN_READ_REDIS_SCRIPT: RedisScript = { name: "nextaskingRunRead", lua: RUN_READ_SCRIPT, keyCount: 4 };

const LISTED_RUN_READ_REDIS_SCRIPT: RedisScript = {
  name: "nextaskingListedRunRead",
  lua: LISTED_RUN_READ_SCRIPT,
  keyCount: 2,
};

export function runReadCommandOf(
  keys: EngineKeys,
  { task, runId }: RunIdentity,
  recordFields: readonly string[],
): RedisCommand {
  return scriptCommandOf(
    RUN_READ_REDIS_SCRIPT,
    [
      keys.runRecord(task, runId),
      keys.runPayload(task, runId),
      keys.runErrors(task, runId),
      keys.runSteps(task, runId),
    ],
    recordFields,
  );
}

export function listedRunReadCommandOf(
  keys: EngineKeys,
  { task, runId }: RunIdentity,
  recordFields: readonly string[],
): RedisCommand {
  return scriptCommandOf(
    LISTED_RUN_READ_REDIS_SCRIPT,
    [keys.runRecord(task, runId), keys.runErrors(task, runId)],
    recordFields,
  );
}

export function readPartsOf(reply: unknown): readonly unknown[] {
  return Array.isArray(reply) ? reply : [];
}
