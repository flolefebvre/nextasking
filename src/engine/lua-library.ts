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

local function unknownVersionRefusalOf(runId)
  local storedVersion = redis.call("HGET", runRecordKey(runId), "version")
  local storedVersionNumber = tonumber(storedVersion)
  if storedVersionNumber and storedVersionNumber <= supportedRecordVersion then
    return nil
  end
  return { "unknownVersion", storedVersion }
end

local function nonNegativeIntegerArgumentOf(value, argumentName)
  local number = tonumber(value)
  if not number or number < 0 or number ~= math.floor(number) then
    error("invalid argument: " .. argumentName .. " must be a non-negative integer, got " .. tostring(value))
  end
  return number
end

local dueInstantFieldByState = { delayed = "startsAt", suspended = "resumesAt", backingOff = "nextAttemptAt" }

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
  elseif dueInstantFieldByState[state] then
    redis.call("ZREM", engineKeys.due, runId)
  end
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
`;
