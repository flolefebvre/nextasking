import type { RunId } from "../ids.js";
import type { EngineKeys } from "./engine-keys.js";

export function readStepKeysOf(keys: EngineKeys, task: string, runId: RunId): string[] {
  return [keys.runRecord(task, runId)];
}

export const READ_STEP_SCRIPT = `
local runId, lockToken, stepId = ARGV[3], ARGV[4], ARGV[5]
if #ARGV ~= 5 then
  error("invalid argument: a readStep takes 5 arguments, got " .. tostring(#ARGV))
end

local refusal = fenceRefusalOf(runId, lockToken)
if refusal then
  return refusal
end

local savedValue = redis.call("HGET", runStepsKey(runId), stepId)
if savedValue then
  return { "read", savedValue }
end
return { "read" }
`;
