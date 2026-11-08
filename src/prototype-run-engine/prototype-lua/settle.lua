local id, epoch, kind, arg = ARGV[2], ARGV[3], ARGV[4], ARGV[5]
local rk = run_key(id)
local ended = redis.call('HGET', rk, 'ended:' .. epoch)
if ended then
  return ended == kind and 'already' or 'stale'
end
if not holds_lease(id, epoch) then
  return 'stale'
end
local now = now_ms()
end_lease(id, epoch, kind)
if kind == 'complete' then
  redis.call('HSET', rk, 'state', 'completed', 'result', arg, 'finishedAt', now)
  redis.call('ZADD', P .. 'completed', now, id)
elseif kind == 'fail' then
  make_failed(id, arg, now)
elseif kind == 'retry' then
  redis.call('HINCRBY', rk, 'attempt', 1)
  make_delayed(id, now + tonumber(arg), 'backoff')
elseif kind == 'suspend' then
  make_delayed(id, math.max(now, tonumber(arg)), 'sleeping')
elseif kind == 'handback' then
  make_pending(id, redis.call('HGET', rk, 'order'))
else
  return redis.error_reply('unknown transition ' .. kind)
end
return 'ok'
