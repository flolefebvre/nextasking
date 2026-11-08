local id, epoch = ARGV[2], ARGV[3]
if not holds_lease(id, epoch) then
  return 0
end
redis.call('HSET', P .. 'steps:' .. id, ARGV[4], ARGV[5])
redis.call('HSET', run_key(id), 'stalls', 0)
return 1
