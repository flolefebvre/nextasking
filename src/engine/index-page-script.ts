import { scriptCommandOf } from "../redis-client.js";
import type { RedisCommand, RedisScript } from "../redis-client.js";

export type IndexScore = number;

export type IndexPlace = { score: IndexScore; storedRunId: string };

export type IndexPagesRequest = { indexKeys: readonly string[]; limit: number; after: IndexPlace | undefined };

export type IndexPage = {
  entryCount: number;
  entryCountBeforeBoundary: number;
  firstEntryRank: number;
  places: IndexPlace[];
};

export const INDEX_PAGE_SCRIPT = `
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

local function entryCountBeforeBoundaryOf(indexKey)
  if #ARGV == 1 then
    return 0
  end
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
  return lowRank
end

local indexPages = {}
for keyNumber, indexKey in ipairs(KEYS) do
  local entryCountBeforeBoundary = entryCountBeforeBoundaryOf(indexKey)
  local firstEntryRank = entryCountBeforeBoundary
  local indexPage = { redis.call("ZCARD", indexKey), entryCountBeforeBoundary, firstEntryRank }
  local entries = redis.call("ZRANGE", indexKey, firstEntryRank, firstEntryRank + limit - 1, "REV", "WITHSCORES")
  for _, element in ipairs(entries) do
    indexPage[#indexPage + 1] = element
  end
  indexPages[keyNumber] = indexPage
end
return indexPages
`;

const INDEX_PAGE_REDIS_SCRIPT: RedisScript = {
  name: "nextaskingIndexPage",
  lua: INDEX_PAGE_SCRIPT,
  keyCount: "one or more",
};

export function indexPagesCommandOf({ indexKeys, limit, after }: IndexPagesRequest): RedisCommand {
  return scriptCommandOf(
    INDEX_PAGE_REDIS_SCRIPT,
    indexKeys,
    after === undefined ? [limit] : [limit, String(after.score), after.storedRunId],
  );
}

function isRankCount(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value) && value >= 0;
}

function indexPageOf(reply: unknown): IndexPage | undefined {
  if (!Array.isArray(reply) || reply.length < 3 || reply.length % 2 === 0) {
    return undefined;
  }
  const [entryCount, entryCountBeforeBoundary, firstEntryRank, ...entryElements] = reply as unknown[];
  if (!isRankCount(entryCount) || !isRankCount(entryCountBeforeBoundary) || !isRankCount(firstEntryRank)) {
    return undefined;
  }
  const places: IndexPlace[] = [];
  for (let elementNumber = 0; elementNumber < entryElements.length; elementNumber += 2) {
    const [storedRunId, storedScore] = [entryElements[elementNumber], entryElements[elementNumber + 1]];
    const score = Number(storedScore);
    if (typeof storedRunId !== "string" || typeof storedScore !== "string" || !Number.isFinite(score)) {
      return undefined;
    }
    places.push({ storedRunId, score });
  }
  return { entryCount, entryCountBeforeBoundary, firstEntryRank, places };
}

export function indexPagesOf(reply: unknown, indexCount: number): IndexPage[] {
  const indexPages = Array.isArray(reply) && reply.length === indexCount ? reply.map(indexPageOf) : [undefined];
  if (!indexPages.every((indexPage) => indexPage !== undefined)) {
    throw new Error(`The index page script sent a reply the reader cannot read: ${JSON.stringify(reply)}`);
  }
  return indexPages;
}
