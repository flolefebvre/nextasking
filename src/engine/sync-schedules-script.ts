import type { EngineKeys } from "./engine-keys.js";

export function syncSchedulesKeysOf(keys: EngineKeys): string[] {
  return [keys.schedules];
}

export const SYNC_SCHEDULES_KEYS_ASSIGNMENT = `
engineKeys.schedules = KEYS[1]
`;

export const SYNC_SCHEDULES_SCRIPT = `${SYNC_SCHEDULES_KEYS_ASSIGNMENT}

local scheduleCount = nonNegativeIntegerArgumentOf(ARGV[3], "scheduleCount")
local firstScheduleArgument = 4
local argumentsPerSchedule = 5
if #ARGV ~= firstScheduleArgument - 1 + scheduleCount * argumentsPerSchedule then
  error("invalid argument: " .. tostring(scheduleCount) .. " schedules need " .. tostring(scheduleCount * argumentsPerSchedule) .. " schedule arguments")
end

local schedulesToSync, isListedByTask = {}, {}
for position = 1, scheduleCount do
  local scheduleArgument = firstScheduleArgument + (position - 1) * argumentsPerSchedule
  local schedule = {
    task = ARGV[scheduleArgument],
    cron = ARGV[scheduleArgument + 1],
    timezone = ARGV[scheduleArgument + 2],
    nextFiringAt = nonNegativeIntegerArgumentOf(ARGV[scheduleArgument + 3], "nextFiringAt"),
    retries = nonNegativeIntegerArgumentOf(ARGV[scheduleArgument + 4], "retries"),
  }
  if schedule.task == "" or schedule.cron == "" or schedule.timezone == "" then
    error("invalid argument: a schedule carries its task, cron and timezone")
  end
  if isListedByTask[schedule.task] then
    error("invalid argument: task " .. schedule.task .. " is listed twice")
  end
  isListedByTask[schedule.task] = true
  schedulesToSync[position] = schedule
end

local storedTasks = redis.call("ZRANGE", engineKeys.schedules, 0, -1)
for _, task in ipairs(storedTasks) do
  local refusal = unknownVersionRefusalOfRecord(scheduleKey(task))
  if refusal then
    return refusal
  end
end
for _, schedule in ipairs(schedulesToSync) do
  local recordKey = scheduleKey(schedule.task)
  if redis.call("EXISTS", recordKey) == 1 then
    local refusal = unknownVersionRefusalOfRecord(recordKey)
    if refusal then
      return refusal
    end
  end
end

local added, changed, removed = {}, {}, {}

local function writeSchedule(schedule)
  redis.call("HSET", scheduleKey(schedule.task),
    "version", supportedRecordVersion,
    "cron", schedule.cron,
    "timezone", schedule.timezone,
    "nextFiringAt", schedule.nextFiringAt,
    "retries", schedule.retries)
  redis.call("ZADD", engineKeys.schedules, schedule.nextFiringAt, schedule.task)
end

for _, schedule in ipairs(schedulesToSync) do
  local stored = redis.call("HMGET", scheduleKey(schedule.task), "cron", "timezone", "retries")
  local summary = { schedule.task, schedule.cron, schedule.timezone }
  if not stored[1] then
    writeSchedule(schedule)
    table.insert(added, summary)
  elseif stored[1] ~= schedule.cron or stored[2] ~= schedule.timezone then
    writeSchedule(schedule)
    table.insert(changed, summary)
  elseif tonumber(stored[3]) ~= schedule.retries then
    redis.call("HSET", scheduleKey(schedule.task), "retries", schedule.retries)
  end
end

for _, task in ipairs(storedTasks) do
  if not isListedByTask[task] then
    local stored = redis.call("HMGET", scheduleKey(task), "cron", "timezone")
    table.insert(removed, { task, stored[1] or "", stored[2] or "" })
    redis.call("DEL", scheduleKey(task))
    redis.call("ZREM", engineKeys.schedules, task)
  end
end

return { "synced", added, changed, removed }
`;
