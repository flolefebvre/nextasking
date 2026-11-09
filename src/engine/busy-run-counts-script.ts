import type { EngineKeys } from "./engine-keys.js";

export function busyRunCountsKeysOf(keys: EngineKeys): string[] {
  return [keys.tasks, keys.due];
}

export const BUSY_RUN_COUNTS_KEYS_ASSIGNMENT = `
engineKeys.tasks, engineKeys.due = KEYS[1], KEYS[2]
`;

export const BUSY_RUN_COUNTS_SCRIPT = `${BUSY_RUN_COUNTS_KEYS_ASSIGNMENT}

if #ARGV ~= 2 then
  error("invalid argument: a busyRunCounts takes 2 arguments, got " .. tostring(#ARGV))
end

local now = currentInstantInMilliseconds()
local pendingRunCount, executingRunCount, waitingForConcurrencySlotRunCount = 0, 0, 0
for _, task in ipairs(redis.call("SMEMBERS", engineKeys.tasks)) do
  pendingRunCount = pendingRunCount + redis.call("ZCARD", runIndexKey(task, "pending"))
  executingRunCount = executingRunCount + redis.call("ZCARD", runIndexKey(task, "executing"))
  waitingForConcurrencySlotRunCount = waitingForConcurrencySlotRunCount
    + redis.call("ZCARD", runIndexKey(task, "waitingForConcurrencySlot"))
end
local dueRunCount = redis.call("ZCOUNT", engineKeys.due, "-inf", now)

return { "counted", pendingRunCount, executingRunCount, waitingForConcurrencySlotRunCount, dueRunCount }
`;
