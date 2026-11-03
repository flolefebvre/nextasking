import { Job } from "bullmq";
import type { MinimalJob, MinimalQueue } from "bullmq";
import type { Redis } from "ioredis";

import type { ConcurrencyKey } from "./definitions.js";
import type { DurationInMilliseconds } from "./durations.js";
import type { JobId } from "./ids.js";
import { jobKeyOf } from "./redis-step-store.js";

const CURRENT_TIME_IN_MILLISECONDS = `
local serverTime = redis.call("TIME")
local now = tonumber(serverTime[1]) * 1000 + math.floor(tonumber(serverTime[2]) / 1000)
`;

const OLDEST_WAITING_JOB_ID_IF_A_SLOT_IS_FREE = `
redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", now)
if redis.call("ZCARD", KEYS[1]) < tonumber(ARGV[1]) then
  return redis.call("LINDEX", KEYS[2], 0) or ""
end
return ""
`;

const TAKE_SLOT_OR_JOIN_WAITING_LIST = `${CURRENT_TIME_IN_MILLISECONDS}
local holders, waitingJobIds = KEYS[1], KEYS[2]
local holder, slotTimeToLive, limit, jobId = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3]), ARGV[4]
redis.call("ZREMRANGEBYSCORE", holders, "-inf", now)
local oldestWaitingJobId = redis.call("LINDEX", waitingJobIds, 0)
if redis.call("ZCARD", holders) < limit and ((not oldestWaitingJobId) or oldestWaitingJobId == jobId) then
  redis.call("ZADD", holders, now + slotTimeToLive, holder)
  redis.call("PEXPIRE", holders, slotTimeToLive)
  local hasWaited = 0
  if oldestWaitingJobId == jobId then
    redis.call("LPOP", waitingJobIds)
    hasWaited = 1
  end
  local nextWaitingJobId = ""
  if redis.call("ZCARD", holders) < limit then
    nextWaitingJobId = redis.call("LINDEX", waitingJobIds, 0) or ""
  end
  return {1, hasWaited, nextWaitingJobId}
end
if not redis.call("LPOS", waitingJobIds, jobId) then
  redis.call("RPUSH", waitingJobIds, jobId)
end
return {0, 0, ""}
`;

const PEEK_OLDEST_WAITING_JOB_ID = `${CURRENT_TIME_IN_MILLISECONDS}${OLDEST_WAITING_JOB_ID_IF_A_SLOT_IS_FREE}`;

const RELEASE_SLOT_AND_PEEK_OLDEST_WAITING_JOB_ID = `${CURRENT_TIME_IN_MILLISECONDS}
redis.call("ZREM", KEYS[1], ARGV[2])
${OLDEST_WAITING_JOB_ID_IF_A_SLOT_IS_FREE}`;

const RENEW_SLOT_IF_LOCK_IS_HELD = `
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return 0
end
${CURRENT_TIME_IN_MILLISECONDS}
redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", now)
if not redis.call("ZSCORE", KEYS[2], ARGV[1]) then
  return 0
end
redis.call("ZADD", KEYS[2], now + tonumber(ARGV[2]), ARGV[1])
redis.call("PEXPIRE", KEYS[2], ARGV[2])
return 1
`;

const SLOT_RENEWED = 1;

const FINISHED_JOB_STATES = new Set(["completed", "failed", "unknown"]);

export type ConcurrencySlotTaking = "taken" | "taken after waiting" | "waiting";

export type ConcurrencySlot = {
  take: (limit: number, concurrencyKey: ConcurrencyKey | undefined) => Promise<ConcurrencySlotTaking>;
  wakeOldestWaitingRunIfASlotIsFree: () => Promise<void>;
  stopRenewing: () => void;
  release: () => Promise<void>;
};

export type ConcurrencySlots = {
  slotFor: (job: MinimalJob, token: string, abortAttemptOnSlotLost: () => void) => ConcurrencySlot;
};

type ConcurrencySlotKeys = {
  holders: string;
  waitingJobIds: string;
};

export function concurrencySlotKeysOf(
  job: MinimalJob,
  concurrencyKey: ConcurrencyKey | undefined,
): ConcurrencySlotKeys {
  const scope = [job.name, ...(concurrencyKey === undefined ? [] : [concurrencyKey])]
    .map((part) => encodeURIComponent(part))
    .join(":");
  return {
    holders: `${job.queueQualifiedName}:concurrency-slots:${scope}`,
    waitingJobIds: `${job.queueQualifiedName}:concurrency-waiting:${scope}`,
  };
}

export function createConcurrencySlots(
  client: Redis,
  queue: MinimalQueue,
  slotTimeToLiveInMilliseconds: DurationInMilliseconds,
): ConcurrencySlots {
  const wakeOldestWaitingRun = async (
    keys: ConcurrencySlotKeys,
    limit: number,
    oldestWaitingJobId: JobId,
  ): Promise<void> => {
    let jobIdToWake = oldestWaitingJobId;
    while (jobIdToWake !== "") {
      const job = await Job.fromId(queue, jobIdToWake);
      const state = job === undefined ? "unknown" : await job.getState();
      if (state === "delayed") {
        await job?.promote().catch(() => undefined);
        return;
      }
      if (!FINISHED_JOB_STATES.has(state)) {
        return;
      }
      await client.lrem(keys.waitingJobIds, 1, jobIdToWake);
      jobIdToWake = (await client.eval(
        PEEK_OLDEST_WAITING_JOB_ID,
        2,
        keys.holders,
        keys.waitingJobIds,
        limit,
      )) as JobId;
    }
  };

  return {
    slotFor: (job, token, abortAttemptOnSlotLost) => {
      let heldSlot: { keys: ConcurrencySlotKeys; limit: number } | undefined;
      let requestedSlot: { keys: ConcurrencySlotKeys; limit: number } | undefined;
      let renewalTimer: NodeJS.Timeout | undefined;
      const stopRenewing = () => {
        clearTimeout(renewalTimer);
        renewalTimer = undefined;
      };
      const scheduleRenewal = (keys: ConcurrencySlotKeys) => {
        renewalTimer = setTimeout(() => {
          void renew(keys);
        }, slotTimeToLiveInMilliseconds / 2);
        renewalTimer.unref();
      };
      const renew = async (keys: ConcurrencySlotKeys) => {
        const renewal = await client
          .eval(
            RENEW_SLOT_IF_LOCK_IS_HELD,
            2,
            `${jobKeyOf(job)}:lock`,
            keys.holders,
            token,
            slotTimeToLiveInMilliseconds,
          )
          .catch(() => SLOT_RENEWED);
        if (renewalTimer === undefined) {
          return;
        }
        if (renewal !== SLOT_RENEWED) {
          stopRenewing();
          abortAttemptOnSlotLost();
          return;
        }
        scheduleRenewal(keys);
      };
      const wakeIfASlotIsFree = async (script: string, ...extraArguments: string[]) => {
        if (requestedSlot === undefined) {
          return;
        }
        const { keys, limit } = requestedSlot;
        const oldestWaitingJobId = (await client.eval(
          script,
          2,
          keys.holders,
          keys.waitingJobIds,
          limit,
          ...extraArguments,
        )) as JobId;
        await wakeOldestWaitingRun(keys, limit, oldestWaitingJobId);
      };
      return {
        take: async (limit, concurrencyKey) => {
          const keys = concurrencySlotKeysOf(job, concurrencyKey);
          requestedSlot = { keys, limit };
          const [isTaken, hasWaited, nextWaitingJobId] = (await client.eval(
            TAKE_SLOT_OR_JOIN_WAITING_LIST,
            2,
            keys.holders,
            keys.waitingJobIds,
            token,
            slotTimeToLiveInMilliseconds,
            limit,
            job.id ?? "",
          )) as [number, number, JobId];
          if (isTaken === 0) {
            return "waiting";
          }
          heldSlot = requestedSlot;
          scheduleRenewal(keys);
          await wakeOldestWaitingRun(keys, limit, nextWaitingJobId).catch(() => undefined);
          return hasWaited === 1 ? "taken after waiting" : "taken";
        },
        wakeOldestWaitingRunIfASlotIsFree: () => wakeIfASlotIsFree(PEEK_OLDEST_WAITING_JOB_ID).catch(() => undefined),
        stopRenewing,
        release: async () => {
          stopRenewing();
          if (heldSlot === undefined) {
            return;
          }
          heldSlot = undefined;
          await wakeIfASlotIsFree(RELEASE_SLOT_AND_PEEK_OLDEST_WAITING_JOB_ID, token).catch(() => undefined);
        },
      };
    },
  };
}
