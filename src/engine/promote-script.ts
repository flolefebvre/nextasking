import type { EngineKeys } from "./engine-keys.js";

export const MOST_DUE_RUNS_PROMOTED_PER_PROMOTE = 100;

export const NEWER_VERSION_RECORDS_PASSED_OVER_PER_PROMOTE = 10;

export function promoteKeysOf(keys: EngineKeys): string[] {
  return [keys.pending, keys.due, keys.locks, keys.wakeUpMarker, keys.order];
}

export const PROMOTE_KEYS_ASSIGNMENT = `
engineKeys.pending, engineKeys.due, engineKeys.locks, engineKeys.wakeUpMarker, engineKeys.order =
  KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
`;

export const PROMOTE_SCRIPT = `${PROMOTE_KEYS_ASSIGNMENT}

local mostDueRunsPromoted = positiveIntegerArgumentOf(ARGV[3], "mostDueRunsPromoted")
local mostRecordsPassedOver = positiveIntegerArgumentOf(ARGV[4], "mostRecordsPassedOver")
if #ARGV ~= 4 then
  error("invalid argument: a promote takes 4 arguments, got " .. tostring(#ARGV))
end

local now = currentInstantInMilliseconds()
local stalledRuns, thirdStalls = {}, {}

for _, runId in ipairs(redis.call("ZRANGE", engineKeys.locks, "-inf", now, "BYSCORE")) do
  if not unknownVersionRefusalOf(runId) then
    local record = redis.call("HMGET", runRecordKey(runId), "task", "concurrencyKey", "lockToken", "stalls", "attempt", "order")
    local task, concurrencyKey, expiredLockToken = record[1], record[2] or "", record[3]
    local stalls, attempt, order = tonumber(record[4]) or 0, record[5], record[6]
    if stalls >= 2 then
      table.insert(thirdStalls, { runId, task, attempt, expiredLockToken })
    else
      endLock(runId, task, concurrencyKey, expiredLockToken, "stalled", nil)
      redis.call("HSET", runRecordKey(runId), "stalls", stalls + 1)
      enterPending(runId, task, order)
      table.insert(stalledRuns, { runId, task, attempt, tostring(stalls + 1) })
    end
  end
end

local promotedRunCount, passedOverRecordCount, firstUnknownVersion =
  promoteDueRuns(now, mostDueRunsPromoted, mostRecordsPassedOver, nil)

if passedOverRecordCount == mostRecordsPassedOver and promotedRunCount == 0 and #stalledRuns == 0 and #thirdStalls == 0 then
  return { "unknownVersion", firstUnknownVersion }
end

return { "promoted", promotedRunCount, stalledRuns, thirdStalls }
`;
