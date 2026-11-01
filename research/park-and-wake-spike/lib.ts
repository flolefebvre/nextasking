import { Redis } from "ioredis";

export const LOCK_MS = 5_000;
export const STALLED_INTERVAL_MS = 5_000;
export const SAFETY_MS = 15_000;
export const WORKER_CONCURRENCY = 10;

const NOW = `
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
`;

const ACQUIRE_OR_PARK = `${NOW}
local slot, park = KEYS[1], KEYS[2]
local holder, ttl, limit, jobId = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3]), ARGV[4]
redis.call('ZREMRANGEBYSCORE', slot, '-inf', now)
if redis.call('ZSCORE', slot, holder) then
  redis.call('ZADD', slot, now + ttl, holder)
  return {1, ''}
end
local head = redis.call('LINDEX', park, 0)
if redis.call('ZCARD', slot) < limit and ((not head) or head == jobId) then
  redis.call('ZADD', slot, now + ttl, holder)
  redis.call('PEXPIRE', slot, ttl)
  if head == jobId then redis.call('LPOP', park) end
  local nextHead = ''
  if redis.call('ZCARD', slot) < limit then nextHead = redis.call('LINDEX', park, 0) or '' end
  return {1, nextHead}
end
if not redis.call('LPOS', park, jobId) then redis.call('RPUSH', park, jobId) end
return {0, ''}
`;

const RELEASE_AND_PEEK = `${NOW}
local slot, park = KEYS[1], KEYS[2]
redis.call('ZREM', slot, ARGV[1])
redis.call('ZREMRANGEBYSCORE', slot, '-inf', now)
if redis.call('ZCARD', slot) < tonumber(ARGV[2]) then
  return redis.call('LINDEX', park, 0) or ''
end
return ''
`;

const RENEW = `${NOW}
if redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  redis.call('ZADD', KEYS[1], now + tonumber(ARGV[2]), ARGV[1])
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0
`;

const TRUTH_ENTER = `
local n = redis.call('INCR', KEYS[1])
local m = tonumber(redis.call('GET', KEYS[2]) or '0')
if n > m then redis.call('SET', KEYS[2], n) end
return n
`;

export type SpikeRedis = Redis & {
  acquireOrPark(slot: string, park: string, holder: string, ttl: number, limit: number, jobId: string): Promise<[number, string]>;
  releaseAndPeek(slot: string, park: string, holder: string, limit: number): Promise<string>;
  renewSlot(slot: string, holder: string, ttl: number): Promise<number>;
  truthEnter(exec: string, max: string): Promise<number>;
};

export function connect(url: string): SpikeRedis {
  const redis = new Redis(url, { maxRetriesPerRequest: null }) as SpikeRedis;
  redis.defineCommand("acquireOrPark", { numberOfKeys: 2, lua: ACQUIRE_OR_PARK });
  redis.defineCommand("releaseAndPeek", { numberOfKeys: 2, lua: RELEASE_AND_PEEK });
  redis.defineCommand("renewSlot", { numberOfKeys: 1, lua: RENEW });
  redis.defineCommand("truthEnter", { numberOfKeys: 2, lua: TRUTH_ENTER });
  return redis;
}

export const keys = {
  slot: (task: string, key: string) => `spk:slot:${task}:${key}`,
  park: (task: string, key: string) => `spk:park:${task}:${key}`,
  exec: (task: string, key: string) => `spk:exec:${task}:${key}`,
  execMax: (task: string, key: string) => `spk:execmax:${task}:${key}`,
  log: "spk:log",
  stats: "spk:stats",
  ready: "spk:ready",
};

export type RunData = {
  key?: string;
  ms?: number;
  failFirst?: boolean;
  busyMs?: number;
  dispatchedAt: number;
};
