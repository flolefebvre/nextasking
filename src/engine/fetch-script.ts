import type { EngineKeys } from "./engine-keys.js";

export const MOST_LOCK_TOKENS_PER_FETCH = 50;

export const MOST_RUNS_PARKED_PER_FETCH = 50;

export const NEWER_VERSION_RECORDS_PASSED_OVER_PER_FETCH = 10;

export function fetchKeysOf(keys: EngineKeys): string[] {
  return [keys.pending, keys.locks, keys.wakeUpMarker];
}

export const FETCH_KEYS_ASSIGNMENT = `
engineKeys.pending, engineKeys.locks, engineKeys.wakeUpMarker = KEYS[1], KEYS[2], KEYS[3]
`;

export const FETCH_SCRIPT = `${FETCH_KEYS_ASSIGNMENT}

local lockDurationInMilliseconds = positiveIntegerArgumentOf(ARGV[3], "lockDurationInMilliseconds")
local mostLockTokens = positiveIntegerArgumentOf(ARGV[4], "mostLockTokens")
local mostRunsParked = nonNegativeIntegerArgumentOf(ARGV[5], "mostRunsParked")
local mostRecordsPassedOver = positiveIntegerArgumentOf(ARGV[6], "mostRecordsPassedOver")
local lockTokenCount = nonNegativeIntegerArgumentOf(ARGV[7], "lockTokenCount")
local firstLockTokenArgument = 8

if lockTokenCount == 0 then
  error("invalid argument: a fetch carries at least one lock token")
end
if lockTokenCount > mostLockTokens then
  error("invalid argument: a fetch carries at most " .. tostring(mostLockTokens) .. " lock tokens")
end

local lockTokens = {}
local isLockTokenSeenByToken = {}
for position = 1, lockTokenCount do
  local lockToken = ARGV[firstLockTokenArgument + position - 1]
  if lockToken == nil or lockToken == "" then
    error("invalid argument: lockToken must not be empty")
  end
  if isLockTokenSeenByToken[lockToken] then
    error("invalid argument: lock tokens must be distinct")
  end
  isLockTokenSeenByToken[lockToken] = true
  lockTokens[position] = lockToken
end

local limitCountArgument = firstLockTokenArgument + lockTokenCount
local limitCount = nonNegativeIntegerArgumentOf(ARGV[limitCountArgument], "concurrencyLimitCount")
if #ARGV ~= limitCountArgument + limitCount * 2 then
  error("invalid argument: " .. tostring(limitCount) .. " concurrency limits need " .. tostring(limitCount * 2) .. " limit arguments")
end
local concurrencyLimitsByTask = {}
for position = 1, limitCount do
  local limitArgument = limitCountArgument + (position - 1) * 2 + 1
  concurrencyLimitsByTask[ARGV[limitArgument]] = positiveIntegerArgumentOf(ARGV[limitArgument + 1], "concurrencyLimit")
end

local function lockedRunReplyOf(runId, lockToken, isResume)
  local record = redis.call("HMGET", runRecordKey(runId),
    "task", "attempt", "retries", "replays", "dispatchedAt", "startedAt", "lockedUntil",
    "scheduledAt", "eventName", "eventId", "concurrencyKey")
  return {
    lockToken, runId, record[1], redis.call("GET", runPayloadKey(runId)), record[2], record[3], record[4],
    isResume and "1" or "0", record[5], record[6], record[7], record[8], record[9], record[10], record[11],
  }
end

local lockedRuns, parkedRuns, passedOverRecordVersions = {}, {}, {}
local freshLockTokens = {}
for _, lockToken in ipairs(lockTokens) do
  local lockedRunId = redis.call("GET", lockKey(lockToken))
  if lockedRunId and isCurrentLockOf(lockedRunId, lockToken) then
    table.insert(lockedRuns, lockedRunReplyOf(lockedRunId, lockToken, false))
  else
    table.insert(freshLockTokens, lockToken)
  end
end

local now = currentInstantInMilliseconds()

local function lockRun(runId, task, concurrencyKey, lockToken)
  local recordKey = runRecordKey(runId)
  local lockedUntil = now + lockDurationInMilliseconds
  local isResume = redis.call("HEXISTS", recordKey, "resumesAt") == 1
  redis.call("HDEL", recordKey, "resumesAt")
  redis.call("HSET", recordKey, "state", "executing", "lockToken", lockToken, "lockedUntil", lockedUntil, "startedAt", now)
  redis.call("ZADD", engineKeys.locks, lockedUntil, runId)
  redis.call("ZADD", runIndexKey(task, "executing"), now, runId)
  redis.call("SADD", slotSetKey(task, concurrencyKey), runId)
  redis.call("SET", lockKey(lockToken), runId, "PX", lockDurationInMilliseconds)
  table.insert(lockedRuns, lockedRunReplyOf(runId, lockToken, isResume))
end

local function parkRun(runId, task, concurrencyKey, order)
  redis.call("HSET", runRecordKey(runId), "state", "waitingForConcurrencySlot")
  redis.call("ZADD", runIndexKey(task, "waitingForConcurrencySlot"), order, runId)
  redis.call("ZADD", waitingListKey(task, concurrencyKey), order, runId)
  table.insert(parkedRuns, { runId, task, redis.call("HGET", runRecordKey(runId), "attempt") })
end

local firstUnknownVersion = nil
local usedLockTokenCount = 0
while usedLockTokenCount < #freshLockTokens
  and #parkedRuns < mostRunsParked
  and #passedOverRecordVersions < mostRecordsPassedOver do
  local headEntry = redis.call("ZRANGE", engineKeys.pending, #passedOverRecordVersions, #passedOverRecordVersions, "WITHSCORES")
  local runId, order = headEntry[1], headEntry[2]
  if not runId then
    break
  end
  local refusal = unknownVersionRefusalOf(runId)
  if refusal then
    if #passedOverRecordVersions == 0 then
      firstUnknownVersion = refusal[2]
    end
    table.insert(passedOverRecordVersions, refusal[2] or "")
  else
    local record = redis.call("HMGET", runRecordKey(runId), "task", "concurrencyKey")
    local task, concurrencyKey = record[1], record[2] or ""
    local concurrencyLimit = concurrencyLimitsByTask[task]
    leaveState(runId, task, "pending")
    if concurrencyLimit and redis.call("SCARD", slotSetKey(task, concurrencyKey)) >= concurrencyLimit then
      parkRun(runId, task, concurrencyKey, order)
    else
      usedLockTokenCount = usedLockTokenCount + 1
      lockRun(runId, task, concurrencyKey, freshLockTokens[usedLockTokenCount])
    end
  end
end

if redis.call("ZCARD", engineKeys.pending) > #passedOverRecordVersions then
  armWakeUpMarker()
end

if #passedOverRecordVersions == mostRecordsPassedOver and #lockedRuns == 0 and #parkedRuns == 0 then
  return { "unknownVersion", firstUnknownVersion }
end

return { "fetched", lockedRuns, parkedRuns, passedOverRecordVersions }
`;
