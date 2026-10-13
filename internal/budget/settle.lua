-- KEYS: budget hashes. ARGV: actual_nano, actual_tokens, settle_id, flag_ttl
if redis.call('SET', 'settled:' .. ARGV[3], '1', 'NX', 'EX', ARGV[4]) == false then
  return 0
end
for i = 1, #KEYS do
  redis.call('HINCRBY', KEYS[i], 'spent', ARGV[1])
  redis.call('HINCRBY', KEYS[i], 'tokens', ARGV[2])
  redis.call('HINCRBY', KEYS[i], 'requests', 1)
end
return 1
