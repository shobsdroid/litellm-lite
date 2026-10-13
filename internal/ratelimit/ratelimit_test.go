package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newLimiter(t *testing.T) (*miniredis.Miniredis, *Limiter) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), DialTimeout: 200 * time.Millisecond, ReadTimeout: 200 * time.Millisecond, MaxRetries: 0})
	return mr, New(rdb)
}

func TestNoLimitsShortCircuit(t *testing.T) {
	mr, l := newLimiter(t)
	r := l.Allow(context.Background(), "key:a", 0, 0, 10)
	if !r.Allowed || len(mr.Keys()) != 0 {
		t.Fatal("unlimited subjects must not touch Redis")
	}
}

func TestRPM(t *testing.T) {
	_, l := newLimiter(t)
	ctx := context.Background()
	for i := 1; i <= 2; i++ {
		if r := l.Allow(ctx, "key:a", 2, 0, 1); !r.Allowed || r.UsedRequests != int64(i) {
			t.Fatalf("call %d: %+v", i, r)
		}
	}
	r := l.Allow(ctx, "key:a", 2, 0, 1)
	if r.Allowed || r.Kind != "rpm" || r.UsedRequests != 2 {
		t.Fatalf("third call must be denied by rpm: %+v", r)
	}
	// A different subject has its own window.
	if r := l.Allow(ctx, "key:b", 2, 0, 1); !r.Allowed {
		t.Fatal("subjects are independent")
	}
}

func TestTPMRollsBackRequestCount(t *testing.T) {
	_, l := newLimiter(t)
	ctx := context.Background()
	if r := l.Allow(ctx, "key:t", 100, 100, 60); !r.Allowed || r.UsedTokens != 60 {
		t.Fatalf("%+v", r)
	}
	r := l.Allow(ctx, "key:t", 100, 100, 60)
	if r.Allowed || r.Kind != "tpm" || r.UsedTokens != 60 || r.UsedRequests != 1 {
		t.Fatalf("second call must be denied by tpm and not consume a request: %+v", r)
	}
	if r := l.Allow(ctx, "key:t", 100, 100, 40); !r.Allowed || r.UsedTokens != 100 || r.UsedRequests != 2 {
		t.Fatalf("exactly at the cap must pass: %+v", r)
	}
}

func TestWindowKeysExpire(t *testing.T) {
	mr, l := newLimiter(t)
	l.Allow(context.Background(), "key:x", 5, 5, 1)
	for _, k := range mr.Keys() {
		if mr.TTL(k) <= 0 {
			t.Fatalf("window key %s has no TTL", k)
		}
	}
}

// PLAN §4.2: rate limiting fails open when Redis is down.
func TestFailsOpenWithoutRedis(t *testing.T) {
	mr, l := newLimiter(t)
	mr.Close()
	r := l.Allow(context.Background(), "key:a", 1, 1, 100)
	if !r.Allowed || !r.Degraded {
		t.Fatalf("%+v", r)
	}
}
