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
local task = ARGV[5]
if #ARGV ~= 5 then
  error("invalid argument: a makeDue takes 5 arguments, got " .. tostring(#ARGV))
end

local dueIndexKeys = { engineKeys.due }
if task ~= "" then
  dueIndexKeys = { runIndexKey(task, "delayed"), runIndexKey(task, "suspended"), runIndexKey(task, "backingOff") }
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

local madeDueRuns = promoteDueRuns(dueRunWalkOf(dueIndexKeys, "+inf"), mostDueRunsMadeDue, mostRecordsPassedOver,
  wakeSleepsOf)

return { "madeDue", madeDueRuns }
`;
