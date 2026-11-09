import type { EngineKeys } from "./engine-keys.js";

export function fireNowKeysOf(keys: EngineKeys): string[] {
  return [keys.pending, keys.tasks, keys.wakeUpMarker, keys.order];
}

export const FIRE_NOW_KEYS_ASSIGNMENT = `
engineKeys.pending, engineKeys.tasks, engineKeys.wakeUpMarker, engineKeys.order = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
`;

export const FIRE_NOW_SCRIPT = `${FIRE_NOW_KEYS_ASSIGNMENT}

if #ARGV ~= 6 then
  error("invalid argument: a fireNow takes 6 arguments, got " .. tostring(#ARGV))
end
local task = ARGV[3]
local instant = nonNegativeIntegerArgumentOf(ARGV[4], "instant")
local runId = ARGV[5]
local defaultRetries = nonNegativeIntegerArgumentOf(ARGV[6], "defaultRetries")
if task == "" or runId == "" then
  error("invalid argument: a fireNow carries its task and run id")
end

local recordKey = scheduleKey(task)
if redis.call("EXISTS", recordKey) == 0 then
  return { "noSchedule" }
end
local refusal = unknownVersionRefusalOfRecord(recordKey)
if refusal then
  return refusal
end
local storedRetries = redis.call("HGET", recordKey, "retries")
local retries = defaultRetries
if storedRetries then
  retries = tonumber(storedRetries)
  if not retries or retries < 0 or retries ~= math.floor(retries) then
    error("the schedule record of task " .. task .. " holds unreadable retries: " .. storedRetries)
  end
end

local isRunCreated = createFiringRun(task, runId, retries, instant, currentInstantInMilliseconds())

return { "fired", runId, isRunCreated and 1 or 0 }
`;
