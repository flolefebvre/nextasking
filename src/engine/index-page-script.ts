import { scriptCommandOf } from "../redis-client.js";
import type { RedisCommand, RedisScript } from "../redis-client.js";

export type IndexScore = number;

export type IndexPlace = { score: IndexScore; storedRunId: string };

export type PagingDirection = "forward" | "backward";

export type IndexPageBoundary = { direction: PagingDirection; place: IndexPlace | undefined };

export type IndexPagesRequest = { indexKeys: readonly string[]; limit: number; boundary: IndexPageBoundary };

export type IndexPage = {
  entryCount: number;
  entryCountBeforeBoundary: number;
  firstEntryRank: number;
  places: IndexPlace[];
};

export const INDEX_PAGE_SCRIPT = `
if #ARGV ~= 2 and #ARGV ~= 4 then
  error("invalid argument: an index page takes a limit and a direction, then optionally a score and a run id, got " .. tostring(#ARGV) .. " arguments")
end
local limit = tonumber(ARGV[1])
if not limit or limit < 1 or limit ~= math.floor(limit) then
  error("invalid argument: limit must be a positive integer, got " .. tostring(ARGV[1]))
end
local isForward = ARGV[2] == "forward"
if not isForward and ARGV[2] ~= "backward" then
  error("invalid argument: direction must be forward or backward, got " .. tostring(ARGV[2]))
end

local function isBytewiseBefore(left, right)
  local sharedLength = math.min(#left, #right)
  for byteNumber = 1, sharedLength do
    local leftByte, rightByte = string.byte(left, byteNumber), string.byte(right, byteNumber)
    if leftByte ~= rightByte then
      return leftByte < rightByte
    end
  end
  return #left < #right
end

local function isBeforeBoundary(entryRunId, placeRunId)
  if isForward then
    return not isBytewiseBefore(entryRunId, placeRunId)
  end
  return isBytewiseBefore(placeRunId, entryRunId)
end

local function entryCountBeforeBoundaryOf(indexKey, entryCount)
  if #ARGV == 2 then
    return isForward and 0 or entryCount
  end
  local score, runId = ARGV[3], ARGV[4]
  local higherScoredCount = redis.call("ZCOUNT", indexKey, "(" .. score, "+inf")
  local sameScoredCount = redis.call("ZCOUNT", indexKey, score, score)
  local lowRank, highRank = higherScoredCount, higherScoredCount + sameScoredCount
  while lowRank < highRank do
    local middleRank = math.floor((lowRank + highRank) / 2)
    local middleRunId = redis.call("ZRANGE", indexKey, middleRank, middleRank, "REV")[1]
    if isBeforeBoundary(middleRunId, runId) then
      lowRank = middleRank + 1
    else
      highRank = middleRank
    end
  end
  return lowRank
end

local indexPages = {}
for keyNumber, indexKey in ipairs(KEYS) do
  local entryCount = redis.call("ZCARD", indexKey)
  local entryCountBeforeBoundary = entryCountBeforeBoundaryOf(indexKey, entryCount)
  local firstEntryRank, lastEntryRank = entryCountBeforeBoundary, entryCountBeforeBoundary + limit - 1
  if not isForward then
    firstEntryRank, lastEntryRank = math.max(0, entryCountBeforeBoundary - limit), math.max(entryCountBeforeBoundary, limit) - 1
  end
  local indexPage = { entryCount, entryCountBeforeBoundary, firstEntryRank }
  local entries = redis.call("ZRANGE", indexKey, firstEntryRank, lastEntryRank, "REV", "WITHSCORES")
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

export function indexPagesCommandOf({ indexKeys, limit, boundary }: IndexPagesRequest): RedisCommand {
  const { direction, place } = boundary;
  return scriptCommandOf(
    INDEX_PAGE_REDIS_SCRIPT,
    indexKeys,
    place === undefined ? [limit, direction] : [limit, direction, String(place.score), place.storedRunId],
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
