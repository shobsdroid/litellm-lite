-- ARGV[1] = request_id. Idempotent: the reservation record is deleted on first release.
local rid = ARGV[1]
local r = redis.call('HMGET', 'resv:' .. rid, 'est', 'tok', 'keys')
if not r[1] then
  redis.call('ZREM', 'resv_deadlines', rid)
  return 0
end
local est = tonumber(r[1])
local tok = tonumber(r[2])
for k in string.gmatch(r[3], '[^,]+') do
  if redis.call('EXISTS', k) == 1 then
    redis.call('HINCRBY', k, 'reserved', -est)
    redis.call('HINCRBY', k, 'tokens_reserved', -tok)
  end
end
redis.call('DEL', 'resv:' .. rid)
redis.call('ZREM', 'resv_deadlines', rid)
return 1
