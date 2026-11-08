local id, epoch = ARGV[2], ARGV[3]
if not holds_lease(id, epoch) then
  return { 0 }
end
return { 1, redis.call('HGET', P .. 'steps:' .. id, ARGV[4]) }
