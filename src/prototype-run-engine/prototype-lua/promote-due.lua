local stall_limit = tonumber(ARGV[2])
local now = now_ms()
local due = redis.call('ZRANGEBYSCORE', P .. 'delayed', '-inf', now, 'LIMIT', 0, 200)
for _, id in ipairs(due) do
  redis.call('ZREM', P .. 'delayed', id)
  make_pending(id)
end
local expired = redis.call('ZRANGEBYSCORE', P .. 'executing', '-inf', now, 'LIMIT', 0, 200)
for _, id in ipairs(expired) do
  local rk = run_key(id)
  end_lease(id, redis.call('HGET', rk, 'epoch'), 'stalled')
  if redis.call('HINCRBY', rk, 'stalls', 1) >= stall_limit then
    make_failed(id, 'stalled', now)
  else
    make_pending(id, redis.call('HGET', rk, 'order'))
  end
end
return { #due, #expired }
