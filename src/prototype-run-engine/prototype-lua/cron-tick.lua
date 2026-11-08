local schedule, instant = ARGV[2], ARGV[3]
if not redis.call('SET', P .. 'firing:' .. schedule .. ':' .. instant, '1', 'NX', 'PX', 86400000) then
  return 0
end
return enqueue('cron:' .. schedule .. ':' .. instant, ARGV[4], ARGV[5], '', '0', 0, ARGV[6], now_ms())
