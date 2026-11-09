import type { ChainableCommander, Redis } from "ioredis";

import { defineScriptCommand } from "./script-commands.js";

export type IndexScore = number;

export type IndexPosition = { score: IndexScore; storedRunId: string };

export type IndexPageRequest = { indexKey: string; limit: number; after: IndexPosition | undefined };

export type IndexPageCommand = {
  appendTo: (pipeline: ChainableCommander, request: IndexPageRequest) => void;
};

export const INDEX_PAGE_SCRIPT = `
local indexKey = KEYS[1]
if #ARGV ~= 1 and #ARGV ~= 3 then
  error("invalid argument: an index page takes a limit, then optionally a score and a run id, got " .. tostring(#ARGV) .. " arguments")
end
local limit = tonumber(ARGV[1])
if not limit or limit < 1 or limit ~= math.floor(limit) then
  error("invalid argument: limit must be a positive integer, got " .. tostring(ARGV[1]))
end

local function isBytewiseBefore(left, right)
  local sharedLength = math.min(#left, #right)
  for position = 1, sharedLength do
    local leftByte, rightByte = string.byte(left, position), string.byte(right, position)
    if leftByte ~= rightByte then
      return leftByte < rightByte
    end
  end
  return #left < #right
end

local firstRank = 0
if #ARGV == 3 then
  local score, runId = ARGV[2], ARGV[3]
  local higherScoredCount = redis.call("ZCOUNT", indexKey, "(" .. score, "+inf")
  local sameScoredCount = redis.call("ZCOUNT", indexKey, score, score)
  local lowRank, highRank = higherScoredCount, higherScoredCount + sameScoredCount
  while lowRank < highRank do
    local middleRank = math.floor((lowRank + highRank) / 2)
    local middleRunId = redis.call("ZRANGE", indexKey, middleRank, middleRank, "REV")[1]
    if isBytewiseBefore(middleRunId, runId) then
      highRank = middleRank
    else
      lowRank = middleRank + 1
    end
  end
  firstRank = lowRank
end

return redis.call("ZRANGE", indexKey, firstRank, firstRank + limit - 1, "REV", "WITHSCORES")
`;

export function defineIndexPageCommand(client: Redis): IndexPageCommand {
  const command = defineScriptCommand(client, "nextaskingIndexPage", INDEX_PAGE_SCRIPT, 1);
  return {
    appendTo: (pipeline, { indexKey, limit, after }) => {
      command.appendTo(
        pipeline,
        [indexKey],
        after === undefined ? [limit] : [limit, String(after.score), after.storedRunId],
      );
    },
  };
}

export function indexPageOf(reply: unknown): IndexPosition[] {
  const isReadable =
    Array.isArray(reply) && reply.length % 2 === 0 && reply.every((element) => typeof element === "string");
  const indexPage: IndexPosition[] = [];
  for (let position = 0; isReadable && position < reply.length; position += 2) {
    const score = Number(reply[position + 1]);
    if (!Number.isFinite(score)) {
      break;
    }
    indexPage.push({ storedRunId: reply[position] as string, score });
  }
  if (!isReadable || indexPage.length * 2 !== reply.length) {
    throw new Error(`The index page script sent a reply the reader cannot read: ${JSON.stringify(reply)}`);
  }
  return indexPage;
}
