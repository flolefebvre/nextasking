import type { RunId } from "../ids.js";
import type { EngineKeys } from "./engine-keys.js";

export function saveStepKeysOf(keys: EngineKeys, task: string, runId: RunId): string[] {
  return [keys.runRecord(task, runId)];
}

export const SAVE_STEP_SCRIPT = `
local runId, lockToken, stepId, stepKind = ARGV[3], ARGV[4], ARGV[5], ARGV[6]
if #ARGV ~= 7 then
  error("invalid argument: a saveStep takes 7 arguments, got " .. tostring(#ARGV))
end
if stepId == "" then
  error("invalid argument: stepId must not be empty")
end
local stepValue, durationInMilliseconds, resumesAt = nil, nil, nil
if stepKind == "value" then
  stepValue = ARGV[7]
elseif stepKind == "sleep" then
  durationInMilliseconds = nonNegativeIntegerArgumentOf(ARGV[7], "durationInMilliseconds")
elseif stepKind == "sleepUntil" then
  resumesAt = nonNegativeIntegerArgumentOf(ARGV[7], "resumesAt")
else
  error("invalid argument: unknown step kind " .. tostring(stepKind))
end

local refusal = fenceRefusalOf(runId, lockToken)
if refusal then
  return refusal
end

local stepsKey = runStepsKey(runId)
local savedValue = redis.call("HGET", stepsKey, stepId)
if savedValue then
  if stepKind == "value" then
    return { "saved" }
  end
  if not string.match(savedValue, "^%d+$") then
    error("invalid argument: step " .. stepId .. " is saved but is not a sleep")
  end
  local savedResumesAt = tonumber(savedValue)
  return { "saved", savedResumesAt, savedResumesAt <= currentInstantInMilliseconds() and 1 or 0 }
end

redis.call("HSET", runRecordKey(runId), "stalls", 0)
if stepKind == "value" then
  redis.call("HSET", stepsKey, stepId, stepValue)
  return { "saved" }
end
local now = currentInstantInMilliseconds()
if stepKind == "sleep" then
  resumesAt = now + durationInMilliseconds
end
redis.call("HSET", stepsKey, stepId, string.format("%d", resumesAt))
return { "saved", resumesAt, resumesAt <= now and 1 or 0 }
`;
