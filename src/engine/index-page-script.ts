import { scriptCommandOf } from "../redis-client.js";
import type { RedisCommand, RedisScript } from "../redis-client.js";

export type IndexScore = number;

export type IndexPlace = { score: IndexScore; storedRunId: string };

export type PagingDirection = "forward" | "backward";

export type ListOrder = "descending" | "ascending";

export type ScoreRange = { lowestScore: IndexScore | undefined; highestScore: IndexScore | undefined };

export type OrderGroupNumber = number;

export type ListPlace = IndexPlace & { orderGroupNumber: OrderGroupNumber };

export type IndexPageBoundary = { direction: PagingDirection; place: ListPlace | undefined };

export type IndexInList = { indexKey: string; orderGroupNumber: OrderGroupNumber };

export type IndexPagesRequest = {
  indexesInList: readonly IndexInList[];
  limit: number;
  boundary: IndexPageBoundary;
  order: ListOrder;
  scoreRange: ScoreRange;
};

export type IndexPage = {
  entryCount: number;
  entryCountBeforeBoundary: number;
  firstEntryRank: number;
  places: IndexPlace[];
};

export const INDEX_PAGE_SCRIPT = `
local keyCount = #KEYS
local hasPlace = #ARGV == keyCount + 8
if #ARGV ~= keyCount + 5 and not hasPlace then
  error("invalid argument: an index page takes a limit, a direction, an order, the lowest and the highest score, the order group number of each index, then optionally the order group number, the score and the run id of a place, got " .. tostring(#ARGV) .. " arguments for " .. tostring(keyCount) .. " indexes")
end
local function isPositiveInteger(value)
  return value ~= nil and value >= 1 and value == math.floor(value)
end
local limit = tonumber(ARGV[1])
if not isPositiveInteger(limit) then
  error("invalid argument: limit must be a positive integer, got " .. tostring(ARGV[1]))
end
local isForward = ARGV[2] == "forward"
if not isForward and ARGV[2] ~= "backward" then
  error("invalid argument: direction must be forward or backward, got " .. tostring(ARGV[2]))
end
local isDescending = ARGV[3] == "descending"
if not isDescending and ARGV[3] ~= "ascending" then
  error("invalid argument: order must be descending or ascending, got " .. tostring(ARGV[3]))
end
local function scoreBoundOf(argumentNumber, unboundedScore, boundName)
  local bound = ARGV[argumentNumber]
  if bound == unboundedScore then
    return bound, nil
  end
  local boundScore = tonumber(bound)
  if boundScore == nil or boundScore ~= boundScore or boundScore == math.huge or boundScore == -math.huge then
    error("invalid argument: the " .. boundName .. " score must be a finite number or " .. unboundedScore .. ", got " .. tostring(bound))
  end
  return bound, boundScore
end
local lowestScore, lowestScoreNumber = scoreBoundOf(4, "-inf", "lowest")
local highestScore, highestScoreNumber = scoreBoundOf(5, "+inf", "highest")
if lowestScoreNumber and highestScoreNumber and lowestScoreNumber > highestScoreNumber then
  error("invalid argument: the lowest score must not be above the highest, got " .. lowestScore .. " and " .. highestScore)
end
local orderGroupNumbers = {}
for keyNumber = 1, keyCount do
  local orderGroupNumber = tonumber(ARGV[keyNumber + 5])
  if not isPositiveInteger(orderGroupNumber) or orderGroupNumber < (orderGroupNumbers[keyNumber - 1] or 1) then
    error("invalid argument: the order group numbers of the indexes must be positive integers in ascending order, got " .. tostring(ARGV[keyNumber + 5]))
  end
  orderGroupNumbers[keyNumber] = orderGroupNumber
end
local placeOrderGroupNumber, placeScore, placeRunId
if hasPlace then
  placeOrderGroupNumber, placeScore, placeRunId = tonumber(ARGV[keyCount + 6]), ARGV[keyCount + 7], ARGV[keyCount + 8]
  if not isPositiveInteger(placeOrderGroupNumber) then
    error("invalid argument: the order group number of the place must be a positive integer, got " .. tostring(ARGV[keyCount + 6]))
  end
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

local function isListBefore(leftRunId, rightRunId)
  if isDescending then
    return isBytewiseBefore(rightRunId, leftRunId)
  end
  return isBytewiseBefore(leftRunId, rightRunId)
end

local function isBeforeBoundary(entryRunId)
  if isForward then
    return not isListBefore(placeRunId, entryRunId)
  end
  return isListBefore(entryRunId, placeRunId)
end

local function entriesAtListRanksOf(indexKey, firstRank, lastRank, isWithScores)
  local rangeArguments = { indexKey, firstRank, lastRank }
  if isDescending then
    rangeArguments[#rangeArguments + 1] = "REV"
  end
  if isWithScores then
    rangeArguments[#rangeArguments + 1] = "WITHSCORES"
  end
  return redis.call("ZRANGE", unpack(rangeArguments))
end

local function entryCountBeforeRangeOf(indexKey)
  if isDescending then
    return highestScoreNumber and redis.call("ZCOUNT", indexKey, "(" .. highestScore, "+inf") or 0
  end
  return lowestScoreNumber and redis.call("ZCOUNT", indexKey, "-inf", "(" .. lowestScore) or 0
end

local function placeListRankOf(indexKey)
  local precedingScoredCount
  if isDescending then
    precedingScoredCount = redis.call("ZCOUNT", indexKey, "(" .. placeScore, "+inf")
  else
    precedingScoredCount = redis.call("ZCOUNT", indexKey, "-inf", "(" .. placeScore)
  end
  local sameScoredCount = redis.call("ZCOUNT", indexKey, placeScore, placeScore)
  local lowRank, highRank = precedingScoredCount, precedingScoredCount + sameScoredCount
  while lowRank < highRank do
    local middleRank = math.floor((lowRank + highRank) / 2)
    local middleRunId = entriesAtListRanksOf(indexKey, middleRank, middleRank, false)[1]
    if isBeforeBoundary(middleRunId) then
      lowRank = middleRank + 1
    else
      highRank = middleRank
    end
  end
  return lowRank
end

local function entryCountBeforeBoundaryOf(indexKey, entryCount, entryCountBeforeRange, orderGroupNumber)
  if not hasPlace then
    return isForward and 0 or entryCount
  end
  if orderGroupNumber < placeOrderGroupNumber then
    return entryCount
  end
  if orderGroupNumber > placeOrderGroupNumber then
    return 0
  end
  return math.min(math.max(placeListRankOf(indexKey) - entryCountBeforeRange, 0), entryCount)
end

local entryCounts, entryCountsBeforeRange, entryCountsBeforeBoundary = {}, {}, {}
local listEntryCountBeforeBoundary = 0
for keyNumber, indexKey in ipairs(KEYS) do
  entryCounts[keyNumber] = redis.call("ZCOUNT", indexKey, lowestScore, highestScore)
  entryCountsBeforeRange[keyNumber] = entryCountBeforeRangeOf(indexKey)
  entryCountsBeforeBoundary[keyNumber] = entryCountBeforeBoundaryOf(indexKey, entryCounts[keyNumber],
    entryCountsBeforeRange[keyNumber], orderGroupNumbers[keyNumber])
  listEntryCountBeforeBoundary = listEntryCountBeforeBoundary + entryCountsBeforeBoundary[keyNumber]
end

local isHeadOfList = not isForward and listEntryCountBeforeBoundary < limit
local isReadInListOrder = isForward or isHeadOfList

local function pageEntryCountOf(keyNumber)
  if isHeadOfList then
    return entryCounts[keyNumber]
  end
  if isForward then
    return entryCounts[keyNumber] - entryCountsBeforeBoundary[keyNumber]
  end
  return entryCountsBeforeBoundary[keyNumber]
end

local function firstEntryRankOf(keyNumber, readEntryCount)
  if isHeadOfList then
    return 0
  end
  if isForward then
    return entryCountsBeforeBoundary[keyNumber]
  end
  return entryCountsBeforeBoundary[keyNumber] - readEntryCount
end

local keyNumbersInReadingOrder = {}
for keyNumber = 1, keyCount do
  keyNumbersInReadingOrder[keyNumber] = isReadInListOrder and keyNumber or keyCount + 1 - keyNumber
end

local indexPages = {}
local pageEntryCountOfGroupsRead, pageEntryCountOfGroupReading, orderGroupNumberReading = 0, 0, nil
for _, keyNumber in ipairs(keyNumbersInReadingOrder) do
  if orderGroupNumbers[keyNumber] ~= orderGroupNumberReading then
    pageEntryCountOfGroupsRead = pageEntryCountOfGroupsRead + pageEntryCountOfGroupReading
    pageEntryCountOfGroupReading, orderGroupNumberReading = 0, orderGroupNumbers[keyNumber]
  end
  local pageEntryCount = pageEntryCountOf(keyNumber)
  local readEntryCount = math.min(math.max(0, limit - pageEntryCountOfGroupsRead), pageEntryCount)
  pageEntryCountOfGroupReading = pageEntryCountOfGroupReading + pageEntryCount
  local firstEntryRank = firstEntryRankOf(keyNumber, readEntryCount)
  local indexPage = { entryCounts[keyNumber], entryCountsBeforeBoundary[keyNumber], firstEntryRank }
  if readEntryCount > 0 then
    local firstListRank = entryCountsBeforeRange[keyNumber] + firstEntryRank
    local entries = entriesAtListRanksOf(KEYS[keyNumber], firstListRank, firstListRank + readEntryCount - 1, true)
    for _, element in ipairs(entries) do
      indexPage[#indexPage + 1] = element
    end
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

export function indexPagesCommandOf({
  indexesInList,
  limit,
  boundary,
  order,
  scoreRange,
}: IndexPagesRequest): RedisCommand {
  const { direction, place } = boundary;
  return scriptCommandOf(
    INDEX_PAGE_REDIS_SCRIPT,
    indexesInList.map(({ indexKey }) => indexKey),
    [
      limit,
      direction,
      order,
      scoreRange.lowestScore === undefined ? "-inf" : String(scoreRange.lowestScore),
      scoreRange.highestScore === undefined ? "+inf" : String(scoreRange.highestScore),
      ...indexesInList.map(({ orderGroupNumber }) => orderGroupNumber),
      ...(place === undefined ? [] : [place.orderGroupNumber, String(place.score), place.storedRunId]),
    ],
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
