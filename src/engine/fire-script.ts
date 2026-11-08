import type { EngineKeys } from "./engine-keys.js";

export function fireKeysOf(keys: EngineKeys): string[] {
  return [keys.pending, keys.due, keys.tasks, keys.wakeUpMarker, keys.order, keys.schedules];
}

export const FIRE_KEYS_ASSIGNMENT = `
engineKeys.pending, engineKeys.due, engineKeys.tasks, engineKeys.wakeUpMarker, engineKeys.order, engineKeys.schedules =
  KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6]
`;

export const FIRE_SCRIPT = `${FIRE_KEYS_ASSIGNMENT}

if #ARGV ~= 7 then
  error("invalid argument: a fire takes 7 arguments, got " .. tostring(#ARGV))
end
local task = ARGV[3]
local instant = nonNegativeIntegerArgumentOf(ARGV[4], "instant")
local instantAfter = nonNegativeIntegerArgumentOf(ARGV[5], "instantAfter")
local runId = ARGV[6]
local retries = nonNegativeIntegerArgumentOf(ARGV[7], "retries")
if task == "" or runId == "" then
  error("invalid argument: a fire carries its task and run id")
end
if instantAfter <= instant then
  error("invalid argument: instantAfter must come after instant")
end

local recordKey = scheduleKey(task)
if redis.call("EXISTS", recordKey) == 0 then
  return { "noSchedule" }
end
local refusal = unknownVersionRefusalOfRecord(recordKey)
if refusal then
  return refusal
end
if tonumber(redis.call("HGET", recordKey, "nextFiringAt")) ~= instant then
  return { "already" }
end
local now = currentInstantInMilliseconds()
if instant > now then
  return { "notDue" }
end

local firingRun = {
  runId = runId,
  task = task,
  payload = "",
  retries = retries,
  concurrencyKey = "",
  eventName = "",
  eventId = "",
  delayInMilliseconds = 0,
}
local createdRunIds = enqueueRuns({ firingRun }, "", "", now, 0)
local isRunCreated = #createdRunIds == 1
if isRunCreated then
  redis.call("HSET", runRecordKey(runId), "scheduledAt", instant)
end
redis.call("HSET", recordKey, "nextFiringAt", instantAfter, "previousFiringAt", instant)
redis.call("ZADD", engineKeys.schedules, instantAfter, task)

return { "fired", runId, isRunCreated and 1 or 0 }
`;
