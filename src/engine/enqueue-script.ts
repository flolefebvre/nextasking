import type { EngineKeys } from "./engine-keys.js";

export function enqueueKeysOf(keys: EngineKeys): string[] {
  return [keys.pending, keys.due, keys.tasks, keys.wakeUpMarker, keys.order];
}

export const ENQUEUE_KEYS_ASSIGNMENT = `
engineKeys.pending, engineKeys.due, engineKeys.tasks, engineKeys.wakeUpMarker, engineKeys.order =
  KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
`;

export const ENQUEUE_SCRIPT = `${ENQUEUE_KEYS_ASSIGNMENT}

local dedupWindowInMilliseconds = nonNegativeIntegerArgumentOf(ARGV[3], "dedupWindowInMilliseconds")
local expiredDedupEntriesTrimmedPerSet = nonNegativeIntegerArgumentOf(ARGV[4], "expiredDedupEntriesTrimmedPerSet")
local dedupEventName, dedupEventId = ARGV[5], ARGV[6]
local runCount = nonNegativeIntegerArgumentOf(ARGV[7], "runCount")
local argumentsPerRun = 8
local firstRunArgument = 8

if runCount == 0 then
  error("invalid argument: an enqueue creates at least one run")
end
if #ARGV ~= firstRunArgument - 1 + runCount * argumentsPerRun then
  error("invalid argument: " .. tostring(runCount) .. " runs need " .. tostring(runCount * argumentsPerRun) .. " run arguments")
end

local runsToCreate = {}
local isTaskTouchedByName = {}
for position = 1, runCount do
  local runArgument = firstRunArgument + (position - 1) * argumentsPerRun
  local run = {
    runId = ARGV[runArgument],
    task = ARGV[runArgument + 1],
    payload = ARGV[runArgument + 2],
    retries = nonNegativeIntegerArgumentOf(ARGV[runArgument + 3], "retries"),
    concurrencyKey = ARGV[runArgument + 4],
    eventName = ARGV[runArgument + 5],
    eventId = ARGV[runArgument + 6],
    delayInMilliseconds = nonNegativeIntegerArgumentOf(ARGV[runArgument + 7], "delayInMilliseconds"),
  }
  runsToCreate[position] = run
  isTaskTouchedByName[run.task] = true
end

local now = currentInstantInMilliseconds()

if dedupEventName ~= "" and isLiveDedupEntry(eventDedupKey(dedupEventName), dedupEventId, now) then
  return { "duplicate" }
end

if dedupEventName ~= "" then
  trimExpiredDedupEntries(eventDedupKey(dedupEventName), now, expiredDedupEntriesTrimmedPerSet)
end
for task in pairs(isTaskTouchedByName) do
  trimExpiredDedupEntries(runDedupKey(task), now, expiredDedupEntriesTrimmedPerSet)
end

local createdRunIds, skippedRunIds = {}, {}
for _, run in ipairs(runsToCreate) do
  if redis.call("EXISTS", runRecordKey(run.runId)) == 1 or isLiveDedupEntry(runDedupKey(run.task), run.runId, now) then
    table.insert(skippedRunIds, run.runId)
  else
    createRun(run, now)
    table.insert(createdRunIds, run.runId)
  end
end

if dedupEventName ~= "" then
  redis.call("ZADD", eventDedupKey(dedupEventName), now + dedupWindowInMilliseconds, dedupEventId)
end

return { "enqueued", createdRunIds, skippedRunIds }
`;
