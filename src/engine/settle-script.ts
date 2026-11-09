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
local isSettleKindByName = { complete = true, fail = true, retry = true, suspend = true, yield = true, handBack = true }
if not isSettleKindByName[settleKind] then
  error("invalid argument: unknown settle kind " .. tostring(settleKind))
end

local errorTextBefore, errorTextAfter = ARGV[8], ARGV[9]
local isErrorCarriedBySettleKind = { fail = true, retry = true, handBack = true }
if isErrorCarriedBySettleKind[settleKind] and errorTextBefore == "" then
  error("invalid argument: a " .. settleKind .. " carries the error of the attempt")
end
local backoffInMilliseconds, resumesAt, failureReason, unknownTaskFailureDelayInMilliseconds = nil, nil, nil, nil
if settleKind == "retry" then
  backoffInMilliseconds = nonNegativeIntegerArgumentOf(ARGV[10], "backoffInMilliseconds")
elseif settleKind == "suspend" then
  resumesAt = nonNegativeIntegerArgumentOf(ARGV[11], "resumesAt")
elseif settleKind == "fail" then
  failureReason = ARGV[12]
  if failureReason == nil or failureReason == "" then
    error("invalid argument: a fail carries its reason")
  end
elseif settleKind == "handBack" then
  unknownTaskFailureDelayInMilliseconds = nonNegativeIntegerArgumentOf(ARGV[13], "unknownTaskFailureDelayInMilliseconds")
end

local noticePayloadBefore, noticePayloadAfter = ARGV[14], ARGV[15]
local noticeEventName, noticeEventId = ARGV[16], ARGV[17]
local noticeRunCount = nonNegativeIntegerArgumentOf(ARGV[18], "noticeRunCount")
local firstNoticeRunArgument = 19
if #ARGV ~= firstNoticeRunArgument - 1 + noticeRunCount * argumentsPerRunToCreate then
  error("invalid argument: " .. tostring(noticeRunCount) .. " notice runs need " .. tostring(noticeRunCount * argumentsPerRunToCreate) .. " run arguments")
end
local noticeRuns = runsToCreateOf(firstNoticeRunArgument, noticeRunCount)

local recordKey = runRecordKey(runId)
if redis.call("EXISTS", recordKey) == 0 then
  return { "stale" }
end
local refusal = unknownVersionRefusalOf(runId)
if refusal then
  return refusal
end
local record = redis.call("HMGET", recordKey,
  "state", "lockToken", "endedLockToken", "endedAs", "task", "concurrencyKey", "order", "firstFoundUnknownAt")
local state, currentLockToken, endedLockToken, endedAs = record[1], record[2], record[3], record[4]
local task, concurrencyKey, order, firstFoundUnknownAt = record[5], record[6] or "", record[7], tonumber(record[8])
if state ~= "executing" or currentLockToken ~= lockToken then
  if endedLockToken == lockToken and endedAs == settleKind then
    if settleKind == "handBack" and state == "failed" then
      local failure = redis.call("HMGET", recordKey, "reason", "finishedAt")
      return { "failed", failure[1], tonumber(failure[2]) }
    end
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

local function endRun(finalState)
  redis.call("ZADD", runDedupKey(task), now + dedupWindowInMilliseconds, runId)
  redis.call("HSET", recordKey, "state", finalState, "finishedAt", now)
  redis.call("ZADD", runIndexKey(task, finalState), now, runId)
end

local function failRun(reason)
  endRun("failed")
  redis.call("HSET", recordKey, "reason", reason)
  appendError()
  local noticePayload = textWithIsoTimestampOf(noticePayloadBefore, now, noticePayloadAfter)
  for _, noticeRun in ipairs(noticeRuns) do
    noticeRun.payload = noticePayload
  end
  enqueueRuns(noticeRuns, noticeEventName, noticeEventId, now, dedupWindowInMilliseconds)
  return { "failed", reason, now }
end

if settleKind == "complete" then
  redis.call("DEL", runStepsKey(runId))
  endRun("completed")
  return { "completed", now }
end

if settleKind == "fail" then
  return failRun(failureReason)
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
  if firstFoundUnknownAt and now - firstFoundUnknownAt >= unknownTaskFailureDelayInMilliseconds then
    return failRun("unknown task")
  end
  redis.call("HSETNX", recordKey, "firstFoundUnknownAt", now)
  enterPending(runId, task, order)
  return { "handedBack" }
end

enterPending(runId, task, order)
return { "yielded" }
`;
