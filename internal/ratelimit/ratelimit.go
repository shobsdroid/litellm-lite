// Package ratelimit enforces per-minute request and token limits for keys and
// deployments. Fixed one-minute windows in Redis; fails open when Redis is down.
package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const script = `
local r = redis.call('INCR', KEYS[1])
if r == 1 then redis.call('EXPIRE', KEYS[1], 120) end
local rpm = tonumber(ARGV[1])
if rpm > 0 and r > rpm then redis.call('DECR', KEYS[1]) return {0, 'rpm', r - 1, 0} end
local t = redis.call('INCRBY', KEYS[2], ARGV[3])
if t == tonumber(ARGV[3]) then redis.call('EXPIRE', KEYS[2], 120) end
local tpm = tonumber(ARGV[2])
if tpm > 0 and t > tpm then
  redis.call('DECRBY', KEYS[2], ARGV[3]); redis.call('DECR', KEYS[1])
  return {0, 'tpm', r - 1, t - tonumber(ARGV[3])}
end
return {1, '', r, t}
`

type Limiter struct {
	rdb *redis.Client
	s   *redis.Script
}

func New(rdb *redis.Client) *Limiter { return &Limiter{rdb: rdb, s: redis.NewScript(script)} }

type Result struct {
	Allowed      bool
	Kind         string // rpm | tpm when denied
	UsedRequests int64
	UsedTokens   int64
	Degraded     bool
}

// Allow consumes one request and `tokens` tokens from the subject's window.
func (l *Limiter) Allow(ctx context.Context, subject string, rpm int32, tpm int64, tokens int64) Result {
	if rpm <= 0 && tpm <= 0 {
		return Result{Allowed: true}
	}
	minute := time.Now().Unix() / 60
	keys := []string{fmt.Sprintf("rl:%s:r:%d", subject, minute), fmt.Sprintf("rl:%s:t:%d", subject, minute)}
	res, err := l.s.Run(ctx, l.rdb, keys, rpm, tpm, tokens).Slice()
	if err != nil || len(res) < 4 {
		return Result{Allowed: true, Degraded: true}
	}
	ok, _ := res[0].(int64)
	kind, _ := res[1].(string)
	r, _ := res[2].(int64)
	t, _ := res[3].(int64)
	return Result{Allowed: ok == 1, Kind: kind, UsedRequests: r, UsedTokens: t}
}
