local token_key = P .. 'fetch:' .. ARGV[2]
local lease = tonumber(ARGV[3])

local function leased(id, epoch)
  local run = redis.call('HMGET', run_key(id), 'task', 'payload', 'attempt', 'maxAttempts')
  return { id, epoch, run[1], run[2], run[3], run[4] }
end

local prior = redis.call('GET', token_key)
if prior then
  local id, epoch = string.match(prior, '^(.*)|(%d+)$')
  if holds_lease(id, epoch) then
    return leased(id, epoch)
  end
  return false
end

local now = now_ms()
for _ = 1, 50 do
  local popped = redis.call('ZPOPMIN', P .. 'pending')
  local id = popped[1]
  if not id then
    break
  end
  local rk = run_key(id)
  local scope = redis.call('HGET', rk, 'scope')
  local slot_free = true
  if scope ~= '' then
    local slots = P .. 'slots:' .. scope
    if redis.call('SCARD', slots) >= tonumber(redis.call('HGET', rk, 'limit')) then
      slot_free = false
      redis.call('HSET', rk, 'state', 'parked')
      redis.call('ZADD', P .. 'parked:' .. scope, popped[2], id)
    else
      redis.call('SADD', slots, id)
    end
  end
  if slot_free then
    local epoch = tostring(redis.call('HINCRBY', rk, 'epoch', 1))
    redis.call('HSET', rk, 'state', 'executing', 'leaseUntil', now + lease, 'worker', ARGV[4])
    redis.call('ZADD', P .. 'executing', now + lease, id)
    redis.call('SET', token_key, id .. '|' .. epoch, 'PX', 60000)
    if redis.call('ZCARD', P .. 'pending') > 0 then
      arm_marker()
    end
    return leased(id, epoch)
  end
end
if redis.call('ZCARD', P .. 'pending') > 0 then
  arm_marker()
end
return false
