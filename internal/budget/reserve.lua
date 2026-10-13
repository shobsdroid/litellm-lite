-- KEYS: one budget hash per scope in the chain.
-- ARGV: est_nano, est_tokens, request_id, resv_ttl, now, deadline, then per key: limit_nano, limit_tokens, block, expire
local n = #KEYS
local est = tonumber(ARGV[1])
local estTok = tonumber(ARGV[2])
local rid = ARGV[3]
local ttl = tonumber(ARGV[4])
local deadline = tonumber(ARGV[6])
local base = 6
local states = {}
for i = 1, n do
  local h = redis.call('HMGET', KEYS[i], 'spent', 'reserved', 'tokens', 'tokens_reserved')
  local spent = tonumber(h[1]) or 0
  local reserved = tonumber(h[2]) or 0
  local tokens = tonumber(h[3]) or 0
  local tokres = tonumber(h[4]) or 0
  states[i] = {spent, reserved, tokens, tokres}
  local limitN = tonumber(ARGV[base + (i - 1) * 4 + 1])
  local limitT = tonumber(ARGV[base + (i - 1) * 4 + 2])
  local block = tonumber(ARGV[base + (i - 1) * 4 + 3])
  if block == 1 then
    if limitN >= 0 and spent + reserved + est > limitN then
      return {0, i, spent, reserved, tokens, tokres}
    end
    if limitT >= 0 and tokens + tokres + estTok > limitT then
      return {0, i, spent, reserved, tokens, tokres}
    end
  end
end
local out = {1, 0}
for i = 1, n do
  redis.call('HINCRBY', KEYS[i], 'reserved', est)
  redis.call('HINCRBY', KEYS[i], 'tokens_reserved', estTok)
  local exp = tonumber(ARGV[base + (i - 1) * 4 + 4])
  if exp > 0 then redis.call('EXPIRE', KEYS[i], exp) end
  out[#out + 1] = states[i][1]
  out[#out + 1] = states[i][2] + est
  out[#out + 1] = states[i][3]
  out[#out + 1] = states[i][4] + estTok
end
if n > 0 then
  redis.call('HSET', 'resv:' .. rid, 'est', est, 'tok', estTok, 'keys', table.concat(KEYS, ','))
  redis.call('EXPIRE', 'resv:' .. rid, ttl)
  redis.call('ZADD', 'resv_deadlines', deadline, rid)
end
return out
