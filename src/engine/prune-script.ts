import type { EngineKeys } from "./engine-keys.js";

export const NEWER_VERSION_RECORDS_PASSED_OVER_PER_PRUNE = 10;

export function pruneKeysOf(keys: EngineKeys, task: string): string[] {
  return [keys.runIndex(task, "completed"), keys.runIndex(task, "failed"), keys.runDedup(task)];
}

export const PRUNE_SCRIPT = `
local completedIndexKey, failedIndexKey, dedupKey = KEYS[1], KEYS[2], KEYS[3]
local completedMaxAge = nonNegativeIntegerArgumentOf(ARGV[3], "completedMaxAgeInMilliseconds")
local completedMaxRunCount = nonNegativeIntegerArgumentOf(ARGV[4], "completedMaxRunCount")
local failedMaxAge = nonNegativeIntegerArgumentOf(ARGV[5], "failedMaxAgeInMilliseconds")
local failedMaxRunCount = nonNegativeIntegerArgumentOf(ARGV[6], "failedMaxRunCount")
local mostRunsPruned = positiveIntegerArgumentOf(ARGV[7], "mostRunsPruned")
local mostRecordsPassedOver = positiveIntegerArgumentOf(ARGV[8], "mostRecordsPassedOver")
local mostDedupEntriesTrimmed = nonNegativeIntegerArgumentOf(ARGV[9], "mostDedupEntriesTrimmed")
if #ARGV ~= 9 then
  error("invalid argument: a prune takes 9 arguments, got " .. tostring(#ARGV))
end

local now = currentInstantInMilliseconds()
local prunedRunCount, passedOverRecordCount = 0, 0
local passedOverRecordCountByIndexKey = { [completedIndexKey] = 0, [failedIndexKey] = 0 }

local function removeRunWhole(runId, indexKey)
  redis.call("DEL", runRecordKey(runId), runPayloadKey(runId), runErrorsKey(runId), runStepsKey(runId))
  redis.call("ZREM", indexKey, runId)
end

local function pruneRunsOf(indexKey, nextRunIdAfter, isOverTheLimit)
  while prunedRunCount < mostRunsPruned and passedOverRecordCount < mostRecordsPassedOver and isOverTheLimit() do
    local runId = nextRunIdAfter(passedOverRecordCountByIndexKey[indexKey])
    if not runId then
      return
    end
    if unknownVersionRefusalOf(runId) then
      passedOverRecordCountByIndexKey[indexKey] = passedOverRecordCountByIndexKey[indexKey] + 1
      passedOverRecordCount = passedOverRecordCount + 1
    else
      removeRunWhole(runId, indexKey)
      prunedRunCount = prunedRunCount + 1
    end
  end
end

local function alwaysOverTheLimit()
  return true
end

local function pruneRunsPastTheirAge(indexKey, maxAge)
  pruneRunsOf(indexKey, function(offset)
    return redis.call("ZRANGE", indexKey, "-inf", now - maxAge, "BYSCORE", "LIMIT", offset, 1)[1]
  end, alwaysOverTheLimit)
end

local function pruneRunsOverTheCap(indexKey, maxRunCount)
  pruneRunsOf(indexKey, function(offset)
    return redis.call("ZRANGE", indexKey, offset, offset)[1]
  end, function()
    return redis.call("ZCARD", indexKey) > maxRunCount
  end)
end

pruneRunsPastTheirAge(completedIndexKey, completedMaxAge)
pruneRunsPastTheirAge(failedIndexKey, failedMaxAge)
pruneRunsOverTheCap(completedIndexKey, completedMaxRunCount)
pruneRunsOverTheCap(failedIndexKey, failedMaxRunCount)
trimExpiredDedupEntries(dedupKey, now, mostDedupEntriesTrimmed)

return { "pruned", prunedRunCount }
`;
