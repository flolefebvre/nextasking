export const LUA_LIBRARY = `
local hashTaggedPrefix = ARGV[1]

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
`;
