import type { EngineKeys } from "./engine-keys.js";

export function replayKeysOf(keys: EngineKeys): string[] {
  return [keys.pending, keys.wakeUpMarker, keys.order];
}

export const REPLAY_KEYS_ASSIGNMENT = `
engineKeys.pending, engineKeys.wakeUpMarker, engineKeys.order = KEYS[1], KEYS[2], KEYS[3]
`;

export const REPLAY_SCRIPT = `${REPLAY_KEYS_ASSIGNMENT}

local runId = ARGV[3]
if #ARGV ~= 3 then
  error("invalid argument: a replay takes 3 arguments, got " .. tostring(#ARGV))
end

local recordKey = runRecordKey(runId)
if redis.call("EXISTS", recordKey) == 0 then
  return { "notFound" }
end
local refusal = unknownVersionRefusalOf(runId)
if refusal then
  return refusal
end
local record = redis.call("HMGET", recordKey, "state", "task")
local state, task = record[1], record[2]
if state ~= "failed" then
  return { "wrongState", state }
end

leaveState(runId, task, "failed")
redis.call("DEL", runErrorsKey(runId))
redis.call("HDEL", recordKey, "reason", "firstFoundUnknownAt", "endedLockToken", "endedAs", "finishedAt")
redis.call("HSET", recordKey, "attempt", 1, "stalls", 0)
redis.call("HINCRBY", recordKey, "replays", 1)
enterPending(runId, task, nil)

return { "replayed", task }
`;
