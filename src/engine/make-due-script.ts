import type { EngineKeys } from "./engine-keys.js";

export function makeDueKeysOf(keys: EngineKeys): string[] {
  return [keys.pending, keys.due, keys.wakeUpMarker, keys.order];
}

export const MAKE_DUE_KEYS_ASSIGNMENT = `
engineKeys.pending, engineKeys.due, engineKeys.wakeUpMarker, engineKeys.order = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
`;

export const MAKE_DUE_SCRIPT = `${MAKE_DUE_KEYS_ASSIGNMENT}

local mostDueRunsMadeDue = positiveIntegerArgumentOf(ARGV[3], "mostDueRunsMadeDue")
local mostRecordsPassedOver = positiveIntegerArgumentOf(ARGV[4], "mostRecordsPassedOver")
if #ARGV ~= 4 then
  error("invalid argument: a makeDue takes 4 arguments, got " .. tostring(#ARGV))
end

local now = currentInstantInMilliseconds()

local function wakeSleepsOf(runId)
  local recordKey, stepsKey = runRecordKey(runId), runStepsKey(runId)
  local oldResumesAt = redis.call("HGET", recordKey, "resumesAt")
  local stepsWithValues = redis.call("HGETALL", stepsKey)
  for position = 1, #stepsWithValues, 2 do
    if stepsWithValues[position + 1] == oldResumesAt then
      redis.call("HSET", stepsKey, stepsWithValues[position], now)
    end
  end
  redis.call("HSET", recordKey, "resumesAt", now, "sleepsWokenAt", now)
end

local madeDueRunCount = promoteDueRuns("+inf", mostDueRunsMadeDue, mostRecordsPassedOver, wakeSleepsOf)

return { "madeDue", madeDueRunCount }
`;
