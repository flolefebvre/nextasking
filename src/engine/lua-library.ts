export const LUA_LIBRARY = `
local hashTaggedPrefix = ARGV[1]
local supportedRecordVersion = tonumber(ARGV[2])
local engineKeys = {}

local function percentEncodedByteOf(character)
  return string.format("%%%02X", string.byte(character))
end

local function encodedKeySegmentOf(segment)
  return (string.gsub(segment, "[^%w%-_%.!~%*'%(%)]", percentEncodedByteOf))
end

local function keyOf(...)
  return hashTaggedPrefix .. ":" .. table.concat({ ... }, ":")
end

local function runRecordKey(runId)
  return keyOf("run", encodedKeySegmentOf(runId))
end

local function runPayloadKey(runId)
  return runRecordKey(runId) .. ":payload"
end

local function runErrorsKey(runId)
  return runRecordKey(runId) .. ":errors"
end

local function runStepsKey(runId)
  return runRecordKey(runId) .. ":steps"
end

local function lockKey(lockToken)
  return keyOf("lock", encodedKeySegmentOf(lockToken))
end

local function concurrencyScopeOf(task, concurrencyKey)
  if concurrencyKey == "" then
    return encodedKeySegmentOf(task)
  end
  return encodedKeySegmentOf(task) .. ":" .. encodedKeySegmentOf(concurrencyKey)
end

local function waitingListKey(task, concurrencyKey)
  return keyOf("waiting", concurrencyScopeOf(task, concurrencyKey))
end

local function slotSetKey(task, concurrencyKey)
  return keyOf("slots", concurrencyScopeOf(task, concurrencyKey))
end

local function runDedupKey(task)
  return keyOf("dedup", encodedKeySegmentOf(task))
end

local function eventDedupKey(eventName)
  return keyOf("event-dedup", encodedKeySegmentOf(eventName))
end

local function scheduleKey(task)
  return keyOf("schedule", encodedKeySegmentOf(task))
end

local function runIndexKey(task, state)
  return keyOf("runs", encodedKeySegmentOf(task), state)
end

local function currentInstantInMilliseconds()
  local serverTime = redis.call("TIME")
  return tonumber(serverTime[1]) * 1000 + math.floor(tonumber(serverTime[2]) / 1000)
end

local millisecondsPerDay = 86400000

local function civilDateOf(daysSinceEpoch)
  local shiftedDays = daysSinceEpoch + 719468
  local era = math.floor(shiftedDays / 146097)
  local dayOfEra = shiftedDays - era * 146097
  local yearOfEra = math.floor((dayOfEra - math.floor(dayOfEra / 1460) + math.floor(dayOfEra / 36524)
    - math.floor(dayOfEra / 146096)) / 365)
  local dayOfYear = dayOfEra - (365 * yearOfEra + math.floor(yearOfEra / 4) - math.floor(yearOfEra / 100))
  local shiftedMonth = math.floor((5 * dayOfYear + 2) / 153)
  local day = dayOfYear - math.floor((153 * shiftedMonth + 2) / 5) + 1
  local month = shiftedMonth < 10 and shiftedMonth + 3 or shiftedMonth - 9
  local year = yearOfEra + era * 400
  if month <= 2 then
    year = year + 1
  end
  return year, month, day
end

local function isoTimestampOf(instantInMilliseconds)
  local daysSinceEpoch = math.floor(instantInMilliseconds / millisecondsPerDay)
  local millisecondOfDay = instantInMilliseconds - daysSinceEpoch * millisecondsPerDay
  local year, month, day = civilDateOf(daysSinceEpoch)
  return string.format("%04d-%02d-%02dT%02d:%02d:%02d.%03dZ", year, month, day,
    math.floor(millisecondOfDay / 3600000), math.floor(millisecondOfDay / 60000) % 60,
    math.floor(millisecondOfDay / 1000) % 60, millisecondOfDay % 1000)
end

local function textWithIsoTimestampOf(textBefore, instantInMilliseconds, textAfter)
  return textBefore .. isoTimestampOf(instantInMilliseconds) .. textAfter
end

local function unknownVersionRefusalOf(runId)
  local storedVersion = redis.call("HGET", runRecordKey(runId), "version")
  local storedVersionNumber = tonumber(storedVersion)
  if storedVersionNumber and storedVersionNumber <= supportedRecordVersion then
    return nil
  end
  return { "unknownVersion", storedVersion }
end

local function fenceRefusalOf(runId, lockToken)
  if redis.call("EXISTS", runRecordKey(runId)) == 0 then
    return { "stale" }
  end
  local refusal = unknownVersionRefusalOf(runId)
  if refusal then
    return refusal
  end
  local record = redis.call("HMGET", runRecordKey(runId), "state", "lockToken")
  if record[1] ~= "executing" or record[2] ~= lockToken then
    return { "stale" }
  end
  return nil
end

local function isCurrentLockOf(runId, lockToken)
  return fenceRefusalOf(runId, lockToken) == nil
end

local function nonNegativeIntegerArgumentOf(value, argumentName)
  local number = tonumber(value)
  if not number or number < 0 or number ~= math.floor(number) then
    error("invalid argument: " .. argumentName .. " must be a non-negative integer, got " .. tostring(value))
  end
  return number
end

local function positiveIntegerArgumentOf(value, argumentName)
  local number = tonumber(value)
  if not number or number < 1 or number ~= math.floor(number) then
    error("invalid argument: " .. argumentName .. " must be a positive integer, got " .. tostring(value))
  end
  return number
end

local function isLiveDedupEntry(dedupKey, member, now)
  local expiresAt = redis.call("ZSCORE", dedupKey, member)
  return expiresAt ~= false and tonumber(expiresAt) > now
end

local function trimExpiredDedupEntries(dedupKey, now, maximumTrimmed)
  if maximumTrimmed == 0 then
    return
  end
  local expiredMembers = redis.call("ZRANGE", dedupKey, "-inf", now, "BYSCORE", "LIMIT", 0, maximumTrimmed)
  if #expiredMembers > 0 then
    redis.call("ZREM", dedupKey, unpack(expiredMembers))
  end
end

local dueInstantFieldByState ={ delayed = "startsAt", suspended = "resumesAt", backingOff = "nextAttemptAt" }

local function armWakeUpMarker()
  redis.call("ZADD", engineKeys.wakeUpMarker, 0, "wake-up")
end

local function enterPending(runId, task, keptOrder)
  local order = keptOrder or redis.call("INCR", engineKeys.order)
  redis.call("HSET", runRecordKey(runId), "state", "pending", "order", order)
  redis.call("ZADD", engineKeys.pending, order, runId)
  redis.call("ZADD", runIndexKey(task, "pending"), order, runId)
  armWakeUpMarker()
end

local function enterDueState(runId, task, state, dueInstant)
  redis.call("HSET", runRecordKey(runId), "state", state, dueInstantFieldByState[state], dueInstant)
  redis.call("ZADD", engineKeys.due, dueInstant, runId)
  redis.call("ZADD", runIndexKey(task, state), dueInstant, runId)
end

local function leaveState(runId, task, state)
  redis.call("ZREM", runIndexKey(task, state), runId)
  if state == "pending" then
    redis.call("ZREM", engineKeys.pending, runId)
  elseif state == "executing" then
    redis.call("ZREM", engineKeys.locks, runId)
  elseif dueInstantFieldByState[state] then
    redis.call("ZREM", engineKeys.due, runId)
  end
end

local function releaseSlotAndWakeParkedRuns(runId, task, concurrencyKey, concurrencyLimit)
  local slotsKey = slotSetKey(task, concurrencyKey)
  redis.call("SREM", slotsKey, runId)
  local lastWokenRank = -1
  if concurrencyLimit then
    local freeSlotCount = concurrencyLimit - redis.call("SCARD", slotsKey)
    if freeSlotCount <= 0 then
      return
    end
    lastWokenRank = freeSlotCount - 1
  end
  local waitingKey = waitingListKey(task, concurrencyKey)
  local wokenRunsWithOrders = redis.call("ZRANGE", waitingKey, 0, lastWokenRank, "WITHSCORES")
  for position = 1, #wokenRunsWithOrders, 2 do
    local wokenRunId, order = wokenRunsWithOrders[position], wokenRunsWithOrders[position + 1]
    redis.call("ZREM", waitingKey, wokenRunId)
    leaveState(wokenRunId, task, "waitingForConcurrencySlot")
    enterPending(wokenRunId, task, order)
  end
end

local function endLock(runId, task, concurrencyKey, lockToken, endedAs, concurrencyLimit)
  redis.call("DEL", lockKey(lockToken))
  leaveState(runId, task, "executing")
  redis.call("HDEL", runRecordKey(runId), "lockToken", "lockedUntil")
  redis.call("HSET", runRecordKey(runId), "endedLockToken", lockToken, "endedAs", endedAs)
  releaseSlotAndWakeParkedRuns(runId, task, concurrencyKey, concurrencyLimit)
end

local function createRun(run, now)
  local recordKey = runRecordKey(run.runId)
  redis.call("HSET", recordKey,
    "version", supportedRecordVersion,
    "task", run.task,
    "attempt", 1,
    "retries", run.retries,
    "replays", 0,
    "stalls", 0,
    "dispatchedAt", now)
  if run.concurrencyKey ~= "" then
    redis.call("HSET", recordKey, "concurrencyKey", run.concurrencyKey)
  end
  if run.eventName ~= "" then
    redis.call("HSET", recordKey, "eventName", run.eventName, "eventId", run.eventId)
  end
  redis.call("SET", runPayloadKey(run.runId), run.payload)
  redis.call("SADD", engineKeys.tasks, run.task)
  if run.delayInMilliseconds > 0 then
    enterDueState(run.runId, run.task, "delayed", now + run.delayInMilliseconds)
    return false
  end
  enterPending(run.runId, run.task, nil)
  return true
end

local argumentsPerRunToCreate = 8

local function runsToCreateOf(firstRunArgument, runCount)
  local runsToCreate = {}
  for position = 1, runCount do
    local runArgument = firstRunArgument + (position - 1) * argumentsPerRunToCreate
    runsToCreate[position] = {
      runId = ARGV[runArgument],
      task = ARGV[runArgument + 1],
      payload = ARGV[runArgument + 2],
      retries = nonNegativeIntegerArgumentOf(ARGV[runArgument + 3], "retries"),
      concurrencyKey = ARGV[runArgument + 4],
      eventName = ARGV[runArgument + 5],
      eventId = ARGV[runArgument + 6],
      delayInMilliseconds = nonNegativeIntegerArgumentOf(ARGV[runArgument + 7], "delayInMilliseconds"),
    }
  end
  return runsToCreate
end

local function enqueueRuns(runsToCreate, dedupEventName, dedupEventId, now, dedupWindowInMilliseconds)
  local createdRunIds, skippedRunIds = {}, {}
  for _, run in ipairs(runsToCreate) do
    if redis.call("EXISTS", runRecordKey(run.runId)) == 1 or isLiveDedupEntry(runDedupKey(run.task), run.runId, now) then
      table.insert(skippedRunIds, run.runId)
    else
      createRun(run, now)
      table.insert(createdRunIds, run.runId)
    end
  end
  if dedupEventName ~= "" then
    redis.call("ZADD", eventDedupKey(dedupEventName), now + dedupWindowInMilliseconds, dedupEventId)
  end
  return createdRunIds, skippedRunIds
end
`;
