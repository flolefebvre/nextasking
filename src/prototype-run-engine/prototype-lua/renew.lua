local lease = tonumber(ARGV[2])
local now = now_ms()
local renewed = {}
for i = 3, #ARGV, 2 do
  local id, epoch = ARGV[i], ARGV[i + 1]
  if holds_lease(id, epoch) then
    redis.call('HSET', run_key(id), 'leaseUntil', now + lease)
    redis.call('ZADD', P .. 'executing', now + lease, id)
    renewed[#renewed + 1] = 1
  else
    renewed[#renewed + 1] = 0
  end
end
return renewed
