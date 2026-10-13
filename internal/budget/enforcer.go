package budget

import (
	"context"
	_ "embed"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rzp/litellm-lite/internal/metrics"
)

//go:embed reserve.lua
var reserveSrc string

//go:embed settle.lua
var settleSrc string

//go:embed release.lua
var releaseSrc string

type Enforcer struct {
	rdb          *redis.Client
	reserve      *redis.Script
	settle       *redis.Script
	release      *redis.Script
	resvTTL      time.Duration
	tolerancePct int64

	// Degraded-mode state. `known` is the last spent+reserved seen from Redis
	// per budget key; `accrued` is spend settled while Redis was down and not
	// yet pushed back.
	mu       sync.Mutex
	known    map[string]int64
	accrued  map[string]int64
	degraded bool
}

func New(rdb *redis.Client, resvTTL time.Duration, tolerancePct int) *Enforcer {
	return &Enforcer{
		rdb: rdb, reserve: redis.NewScript(reserveSrc), settle: redis.NewScript(settleSrc), release: redis.NewScript(releaseSrc),
		resvTTL: resvTTL, tolerancePct: int64(tolerancePct),
		known: map[string]int64{}, accrued: map[string]int64{},
	}
}

type Remaining struct {
	Scope       Scope
	Spent       int64
	Reserved    int64
	Tokens      int64
	RemainingNS *int64 // nil when no USD limit
}

type Decision struct {
	Allowed     bool
	DeniedBy    *Scope
	Enforcement string // redis | local
	Remaining   []Remaining
}

// Reserve atomically checks every scope and reserves the estimate.
func (e *Enforcer) Reserve(ctx context.Context, requestID string, scopes []Scope, estNano, estTokens int64) (Decision, error) {
	now := time.Now()
	if len(scopes) == 0 {
		return Decision{Allowed: true, Enforcement: "redis"}, nil
	}
	keys := make([]string, len(scopes))
	args := []any{estNano, estTokens, requestID, int64(e.resvTTL.Seconds()), now.Unix(), now.Add(e.resvTTL).Unix()}
	for i, s := range scopes {
		keys[i] = s.RedisKey()
		limitN, limitT := int64(-1), int64(-1)
		if s.Budget.LimitNanoUSD != nil {
			limitN = *s.Budget.LimitNanoUSD
		}
		if s.Budget.LimitTokens != nil {
			limitT = *s.Budget.LimitTokens
		}
		block := 0
		if s.Budget.OnExceed == "block" {
			block = 1
		}
		args = append(args, limitN, limitT, block, s.ExpireSeconds(now))
	}
	t0 := time.Now()
	res, err := e.reserve.Run(ctx, e.rdb, keys, args...).Int64Slice()
	metrics.RedisLatency.Observe(time.Since(t0).Seconds())
	if err != nil {
		return e.reserveLocal(scopes, estNano, err)
	}
	e.setDegraded(false)
	d := Decision{Enforcement: "redis"}
	if res[0] == 0 {
		idx := int(res[1]) - 1
		d.DeniedBy = &scopes[idx]
		d.Remaining = []Remaining{{Scope: scopes[idx], Spent: res[2], Reserved: res[3], Tokens: res[4]}}
		e.remember(keys[idx], res[2]+res[3])
		return d, nil
	}
	d.Allowed = true
	for i := range scopes {
		spent, reserved, tokens := res[2+i*4], res[3+i*4], res[4+i*4]
		r := Remaining{Scope: scopes[i], Spent: spent, Reserved: reserved, Tokens: tokens}
		if l := scopes[i].Budget.LimitNanoUSD; l != nil {
			rem := *l - spent
			r.RemainingNS = &rem
		}
		d.Remaining = append(d.Remaining, r)
		e.remember(keys[i], spent+reserved)
	}
	return d, nil
}

func (e *Enforcer) reserveLocal(scopes []Scope, estNano int64, cause error) (Decision, error) {
	e.setDegraded(true)
	e.mu.Lock()
	defer e.mu.Unlock()
	d := Decision{Allowed: true, Enforcement: "local"}
	for i := range scopes {
		s := scopes[i]
		if s.Budget.Enforcement == "strict" {
			return Decision{Enforcement: "local"}, fmt.Errorf("strict budget %s cannot be enforced: %w", s.Budget.ID, cause)
		}
		k := s.RedisKey()
		used := e.known[k] + e.accrued[k]
		if s.Budget.LimitNanoUSD != nil && s.Budget.OnExceed == "block" {
			limit := *s.Budget.LimitNanoUSD
			limit += limit * e.tolerancePct / 100
			if used+estNano > limit {
				d.Allowed = false
				d.DeniedBy = &scopes[i]
				return d, nil
			}
		}
		d.Remaining = append(d.Remaining, Remaining{Scope: s, Spent: used})
	}
	return d, nil
}

// Settle records actual cost for one attempt. Idempotent per settle id.
func (e *Enforcer) Settle(ctx context.Context, requestID string, attempt int, scopes []Scope, nano, tokens int64) {
	if len(scopes) == 0 {
		return
	}
	keys := make([]string, len(scopes))
	for i, s := range scopes {
		keys[i] = s.RedisKey()
	}
	id := requestID + ":" + strconv.Itoa(attempt)
	t0 := time.Now()
	_, err := e.settle.Run(ctx, e.rdb, keys, nano, tokens, id, 86400).Result()
	metrics.RedisLatency.Observe(time.Since(t0).Seconds())
	if err != nil {
		e.setDegraded(true)
		e.mu.Lock()
		for _, k := range keys {
			e.accrued[k] += nano
		}
		e.mu.Unlock()
		return
	}
	e.mu.Lock()
	for _, k := range keys {
		e.known[k] += nano
	}
	e.mu.Unlock()
}

// Release drops the reservation for a request after all attempts settled.
func (e *Enforcer) Release(ctx context.Context, requestID string) {
	if _, err := e.release.Run(ctx, e.rdb, nil, requestID).Result(); err != nil {
		slog.Debug("release failed", "err", err)
	}
}

// Remaining reads live counters for display (gateway/key, budgets/remaining).
func (e *Enforcer) Remaining(ctx context.Context, scopes []Scope) ([]Remaining, error) {
	out := make([]Remaining, 0, len(scopes))
	pipe := e.rdb.Pipeline()
	cmds := make([]*redis.SliceCmd, len(scopes))
	for i, s := range scopes {
		cmds[i] = pipe.HMGet(ctx, s.RedisKey(), "spent", "reserved", "tokens")
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	for i, s := range scopes {
		v := cmds[i].Val()
		r := Remaining{Scope: s, Spent: toInt(v[0]), Reserved: toInt(v[1]), Tokens: toInt(v[2])}
		if l := s.Budget.LimitNanoUSD; l != nil {
			rem := *l - r.Spent
			r.RemainingNS = &rem
		}
		out = append(out, r)
	}
	return out, nil
}

func toInt(v any) int64 {
	if s, ok := v.(string); ok {
		n, _ := strconv.ParseInt(s, 10, 64)
		return n
	}
	return 0
}

func (e *Enforcer) remember(k string, v int64) {
	e.mu.Lock()
	e.known[k] = v
	e.mu.Unlock()
}

func (e *Enforcer) setDegraded(v bool) {
	e.mu.Lock()
	changed := e.degraded != v
	e.degraded = v
	e.mu.Unlock()
	if changed {
		if v {
			metrics.EnforcementDegraded.Set(1)
			slog.Warn("budget enforcement degraded: Redis unavailable, enforcing locally")
		} else {
			metrics.EnforcementDegraded.Set(0)
			slog.Info("budget enforcement recovered")
		}
	}
}

func (e *Enforcer) Degraded() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.degraded
}

// Run hosts the background loops: pushing locally accrued spend back to Redis
// and releasing reservations whose deadline passed (crashed or hung requests).
func (e *Enforcer) Run(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			e.flushAccrued(ctx)
			e.sweep(ctx)
		}
	}
}

func (e *Enforcer) flushAccrued(ctx context.Context) {
	e.mu.Lock()
	if len(e.accrued) == 0 {
		e.mu.Unlock()
		return
	}
	pending := e.accrued
	e.accrued = map[string]int64{}
	e.mu.Unlock()
	for k, v := range pending {
		if err := e.rdb.HIncrBy(ctx, k, "spent", v).Err(); err != nil {
			e.mu.Lock()
			e.accrued[k] += v
			e.mu.Unlock()
			return
		}
		slog.Info("pushed locally accrued spend to redis", "key", k, "nano_usd", v)
	}
	e.setDegraded(false)
}

func (e *Enforcer) sweep(ctx context.Context) {
	ids, err := e.rdb.ZRangeByScore(ctx, "resv_deadlines", &redis.ZRangeBy{Min: "-inf", Max: strconv.FormatInt(time.Now().Unix(), 10), Count: 200}).Result()
	if err != nil || len(ids) == 0 {
		return
	}
	for _, id := range ids {
		if n, err := e.release.Run(ctx, e.rdb, nil, id).Int64(); err == nil && n == 1 {
			metrics.ReservationExpired.Inc()
		}
	}
}
