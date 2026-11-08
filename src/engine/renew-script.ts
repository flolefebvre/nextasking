import type { EngineKeys } from "./engine-keys.js";

export function renewKeysOf(keys: EngineKeys): string[] {
  return [keys.locks];
}

export const RENEW_KEYS_ASSIGNMENT = `
engineKeys.locks = KEYS[1]
`;

export const RENEW_SCRIPT = `${RENEW_KEYS_ASSIGNMENT}

local lockDurationInMilliseconds = positiveIntegerArgumentOf(ARGV[3], "lockDurationInMilliseconds")
local heldLockCount = nonNegativeIntegerArgumentOf(ARGV[4], "heldLockCount")
local firstHeldLockArgument = 5

if #ARGV ~= firstHeldLockArgument - 1 + heldLockCount * 2 then
  error("invalid argument: " .. tostring(heldLockCount) .. " held locks need " .. tostring(heldLockCount * 2) .. " held lock arguments")
end

local now = currentInstantInMilliseconds()
local lockedUntil = now + lockDurationInMilliseconds
local renewals = {}
for position = 1, heldLockCount do
  local heldLockArgument = firstHeldLockArgument + (position - 1) * 2
  local runId, lockToken = ARGV[heldLockArgument], ARGV[heldLockArgument + 1]
  if isCurrentLockOf(runId, lockToken) then
    redis.call("HSET", runRecordKey(runId), "lockedUntil", lockedUntil)
    redis.call("ZADD", engineKeys.locks, lockedUntil, runId)
    redis.call("SET", lockKey(lockToken), runId, "PX", lockDurationInMilliseconds)
    renewals[position] = { "renewed", lockedUntil }
  else
    renewals[position] = { "stale" }
  end
end

return renewals
`;
