local P = ARGV[1]

local function now_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local function run_key(id)
  return P .. 'run:' .. id
end

local function arm_marker()
  redis.call('ZADD', P .. 'marker', 0, 'm')
end

local function make_pending(id, order)
  local rk = run_key(id)
  if not order then
    order = redis.call('INCR', P .. 'seq')
  end
  redis.call('HSET', rk, 'state', 'pending', 'order', order)
  redis.call('HDEL', rk, 'waitReason', 'dueAt')
  redis.call('ZADD', P .. 'pending', order, id)
  arm_marker()
end

local function make_delayed(id, due, reason)
  redis.call('HSET', run_key(id), 'state', 'delayed', 'waitReason', reason, 'dueAt', due)
  redis.call('ZADD', P .. 'delayed', due, id)
end

local function make_failed(id, reason, now)
  redis.call('HSET', run_key(id), 'state', 'failed', 'reason', reason, 'finishedAt', now)
  redis.call('ZADD', P .. 'failed', now, id)
end

local function release_slot(id)
  local scope = redis.call('HGET', run_key(id), 'scope')
  if not scope or scope == '' then
    return
  end
  if redis.call('SREM', P .. 'slots:' .. scope, id) == 1 then
    local woken = redis.call('ZPOPMIN', P .. 'parked:' .. scope)
    if woken[1] then
      make_pending(woken[1], woken[2])
    end
  end
end

local function end_lease(id, epoch, outcome)
  redis.call('ZREM', P .. 'executing', id)
  redis.call('HSET', run_key(id), 'ended:' .. epoch, outcome)
  redis.call('HDEL', run_key(id), 'leaseUntil', 'worker')
  release_slot(id)
end

local function holds_lease(id, epoch)
  local held = redis.call('HMGET', run_key(id), 'state', 'epoch')
  return held[1] == 'executing' and held[2] == epoch
end

local function enqueue(id, task, payload, scope, limit, delay, max_attempts, now)
  local rk = run_key(id)
  if redis.call('EXISTS', rk) == 1 then
    return 0
  end
  redis.call('HSET', rk, 'id', id, 'task', task, 'payload', payload, 'scope', scope, 'limit', limit,
    'maxAttempts', max_attempts, 'attempt', 1, 'epoch', 0, 'stalls', 0, 'createdAt', now)
  if delay > 0 then
    make_delayed(id, now + delay, 'dispatch-delay')
  else
    make_pending(id)
  end
  return 1
end
