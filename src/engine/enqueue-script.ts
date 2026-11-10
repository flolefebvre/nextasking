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
local parentRunId, parentLockToken, parentStepId, parentStepValue = ARGV[7], ARGV[8], ARGV[9], ARGV[10]
local runCount = nonNegativeIntegerArgumentOf(ARGV[11], "runCount")
local firstRunArgument = 12

if parentRunId ~= "" and (parentLockToken == "" or parentStepId == "") then
  error("invalid argument: a parent fence carries the parent's lock token and step id")
end

if runCount == 0 and parentRunId == "" then
  error("invalid argument: an enqueue creates at least one run")
end
if #ARGV ~= firstRunArgument - 1 + runCount * argumentsPerRunToCreate then
  error("invalid argument: " .. tostring(runCount) .. " runs need " .. tostring(runCount * argumentsPerRunToCreate) .. " run arguments")
end

local runsToCreate = runsToCreateOf(firstRunArgument, runCount)
local isTaskTouchedByName = {}
for _, run in ipairs(runsToCreate) do
  isTaskTouchedByName[run.task] = true
end

if parentRunId ~= "" then
  local refusal = fenceRefusalOf(parentRunId, parentLockToken)
  if refusal then
    return refusal
  end
  local savedValue = redis.call("HGET", runStepsKey(parentRunId), parentStepId)
  if savedValue then
    return { "alreadySent", savedValue }
  end
end

local function saveParentStep()
  if parentRunId ~= "" then
    redis.call("HSET", runStepsKey(parentRunId), parentStepId, parentStepValue)
    redis.call("HSET", runRecordKey(parentRunId), "stalls", 0)
  end
end

if runCount == 0 then
  saveParentStep()
  return { "enqueued", {}, {} }
end

local now = currentInstantInMilliseconds()

if dedupEventName ~= "" and isLiveDedupEntry(eventDedupKey(dedupEventName), dedupEventId, now) then
  saveParentStep()
  return { "duplicate" }
end

if dedupEventName ~= "" then
  trimExpiredDedupEntries(eventDedupKey(dedupEventName), now, expiredDedupEntriesTrimmedPerSet)
end
for task in pairs(isTaskTouchedByName) do
  trimExpiredDedupEntries(runDedupKey(task), now, expiredDedupEntriesTrimmedPerSet)
end

local createdRunIds, skippedRunIds = enqueueRuns(runsToCreate, dedupEventName, dedupEventId, now, dedupWindowInMilliseconds)
saveParentStep()

return { "enqueued", createdRunIds, skippedRunIds }
`;
