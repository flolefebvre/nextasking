import type { EngineKeys } from "./engine-keys.js";

export function settleKeysOf(keys: EngineKeys): string[] {
  return [keys.pending, keys.due, keys.locks, keys.tasks, keys.wakeUpMarker, keys.order];
}

export const SETTLE_KEYS_ASSIGNMENT = `
engineKeys.pending, engineKeys.due, engineKeys.locks, engineKeys.tasks, engineKeys.wakeUpMarker, engineKeys.order =
  KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6]
`;

export const SETTLE_SCRIPT = `${SETTLE_KEYS_ASSIGNMENT}

local runId, lockToken, settleKind = ARGV[3], ARGV[4], ARGV[5]
local dedupWindowInMilliseconds = nonNegativeIntegerArgumentOf(ARGV[6], "dedupWindowInMilliseconds")
local concurrencyLimit = nil
if ARGV[7] ~= "" then
  concurrencyLimit = positiveIntegerArgumentOf(ARGV[7], "concurrencyLimit")
end
local isSettleKindByName = { complete = true, retry = true, suspend = true, yield = true, handBack = true }
if not isSettleKindByName[settleKind] then
  error("invalid argument: unknown settle kind " .. tostring(settleKind))
end

local errorTextBefore, errorTextAfter = ARGV[8], ARGV[9]
local isErrorCarriedBySettleKind = { retry = true, handBack = true }
if isErrorCarriedBySettleKind[settleKind] and errorTextBefore == "" then
  error("invalid argument: a " .. settleKind .. " carries the error of the attempt")
end
local backoffInMilliseconds, resumesAt = nil, nil
if settleKind == "retry" then
  backoffInMilliseconds = nonNegativeIntegerArgumentOf(ARGV[10], "backoffInMilliseconds")
elseif settleKind == "suspend" then
  resumesAt = nonNegativeIntegerArgumentOf(ARGV[11], "resumesAt")
end

local recordKey = runRecordKey(runId)
if redis.call("EXISTS", recordKey) == 0 then
  return { "stale" }
end
local refusal = unknownVersionRefusalOf(runId)
if refusal then
  return refusal
end
local record = redis.call("HMGET", recordKey, "state", "lockToken", "endedLockToken", "endedAs", "task", "concurrencyKey", "order")
local state, currentLockToken, endedLockToken, endedAs = record[1], record[2], record[3], record[4]
local task, concurrencyKey, order = record[5], record[6] or "", record[7]
if state ~= "executing" or currentLockToken ~= lockToken then
  if endedLockToken == lockToken and endedAs == settleKind then
    return { "already" }
  end
  return { "stale" }
end

local now = currentInstantInMilliseconds()
endLock(runId, task, concurrencyKey, lockToken, settleKind, concurrencyLimit)
if settleKind ~= "handBack" then
  redis.call("HDEL", recordKey, "firstFoundUnknownAt")
end

local function appendError()
  redis.call("RPUSH", runErrorsKey(runId), textWithIsoTimestampOf(errorTextBefore, now, errorTextAfter))
end

if settleKind == "complete" then
  redis.call("DEL", runStepsKey(runId))
  redis.call("ZADD", runDedupKey(task), now + dedupWindowInMilliseconds, runId)
  redis.call("HSET", recordKey, "state", "completed", "finishedAt", now)
  redis.call("ZADD", runIndexKey(task, "completed"), now, runId)
  return { "completed", now }
end

if settleKind == "retry" then
  appendError()
  local nextAttemptAt = now + backoffInMilliseconds
  redis.call("HINCRBY", recordKey, "attempt", 1)
  redis.call("HSET", recordKey, "stalls", 0)
  enterDueState(runId, task, "backingOff", nextAttemptAt)
  return { "backingOff", nextAttemptAt }
end

if settleKind == "suspend" then
  enterDueState(runId, task, "suspended", resumesAt)
  return { "suspended", resumesAt }
end

if settleKind == "handBack" then
  redis.call("HSETNX", recordKey, "firstFoundUnknownAt", now)
  enterPending(runId, task, order)
  return { "handedBack" }
end

enterPending(runId, task, order)
return { "yielded" }
`;
