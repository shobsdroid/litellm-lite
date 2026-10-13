package budget

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rzp/litellm-lite/internal/store"
)

func mustDate(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

// PLAN §8.2: monthly recurring (calendar month in the budget's timezone, or an
// anchor day) and lifetime.
func TestPeriodMonthlyCalendar(t *testing.T) {
	b := &store.Budget{Kind: "monthly", Timezone: "UTC", AnchorDay: 1}
	s, e := Period(b, mustDate("2026-10-13T12:00:00Z"))
	if !s.Equal(mustDate("2026-10-01T00:00:00Z")) || !e.Equal(mustDate("2026-11-01T00:00:00Z")) {
		t.Fatalf("got %s .. %s", s, e)
	}
	// December rolls into the next year.
	s, e = Period(b, mustDate("2026-12-31T23:59:59Z"))
	if !s.Equal(mustDate("2026-12-01T00:00:00Z")) || !e.Equal(mustDate("2027-01-01T00:00:00Z")) {
		t.Fatalf("got %s .. %s", s, e)
	}
}

func TestPeriodAnchorDay(t *testing.T) {
	b := &store.Budget{Kind: "monthly", Timezone: "UTC", AnchorDay: 15}
	s, e := Period(b, mustDate("2026-10-13T00:00:00Z"))
	if !s.Equal(mustDate("2026-09-15T00:00:00Z")) || !e.Equal(mustDate("2026-10-15T00:00:00Z")) {
		t.Fatalf("before anchor: %s .. %s", s, e)
	}
	s, e = Period(b, mustDate("2026-10-15T00:00:00Z"))
	if !s.Equal(mustDate("2026-10-15T00:00:00Z")) || !e.Equal(mustDate("2026-11-15T00:00:00Z")) {
		t.Fatalf("on anchor: %s .. %s", s, e)
	}
	// Anchors above 28 are clamped so every month has the day.
	s, _ = Period(&store.Budget{Kind: "monthly", Timezone: "UTC", AnchorDay: 31}, mustDate("2026-02-10T00:00:00Z"))
	if !s.Equal(mustDate("2026-01-28T00:00:00Z")) {
		t.Fatalf("anchor 31 clamp: %s", s)
	}
	s, _ = Period(&store.Budget{Kind: "monthly", Timezone: "UTC", AnchorDay: 0}, mustDate("2026-02-10T00:00:00Z"))
	if !s.Equal(mustDate("2026-02-01T00:00:00Z")) {
		t.Fatalf("anchor 0 defaults to 1: %s", s)
	}
}

func TestPeriodTimezone(t *testing.T) {
	b := &store.Budget{Kind: "monthly", Timezone: "Asia/Kolkata", AnchorDay: 1}
	// 2026-10-31 19:00 UTC is already 1 Nov 00:30 in Kolkata.
	s, e := Period(b, mustDate("2026-10-31T19:00:00Z"))
	if !s.Equal(mustDate("2026-10-31T18:30:00Z")) || !e.Equal(mustDate("2026-11-30T18:30:00Z")) {
		t.Fatalf("kolkata: %s .. %s", s, e)
	}
	if s.Location() != time.UTC {
		t.Fatal("periods are returned in UTC so Redis keys and Postgres rows agree")
	}
	// Unknown zones fall back to UTC instead of failing the request.
	s, _ = Period(&store.Budget{Kind: "monthly", Timezone: "Mars/Olympus", AnchorDay: 1}, mustDate("2026-10-13T00:00:00Z"))
	if !s.Equal(mustDate("2026-10-01T00:00:00Z")) {
		t.Fatalf("bad tz: %s", s)
	}
}

func TestPeriodLifetime(t *testing.T) {
	b := &store.Budget{Kind: "lifetime"}
	s, e := Period(b, time.Now())
	if s.Year() != 1970 || e.Year() < 2100 {
		t.Fatalf("lifetime: %s .. %s", s, e)
	}
	sc := Scope{Budget: *b, PeriodStart: s, PeriodEnd: e}
	if sc.ExpireSeconds(time.Now()) != 0 {
		t.Fatal("lifetime keys never expire")
	}
}

func TestScopeKeyAndExpiry(t *testing.T) {
	id := uuid.New()
	now := mustDate("2026-10-13T00:00:00Z")
	b := store.Budget{ID: id, Kind: "monthly", Timezone: "UTC", AnchorDay: 1}
	s, e := Period(&b, now)
	sc := Scope{Budget: b, PeriodStart: s, PeriodEnd: e}
	if sc.RedisKey() != "budget:"+id.String()+":"+itoa(s.Unix()) {
		t.Fatalf("redis key %s", sc.RedisKey())
	}
	// PLAN §8.2: TTL to period end plus 7 days.
	if got := sc.ExpireSeconds(now); got != int64(e.Sub(now).Seconds())+7*86400 {
		t.Fatalf("expire %d", got)
	}
}

func TestResolveSkipsInactive(t *testing.T) {
	now := time.Now()
	bs := []store.Budget{
		{ID: uuid.New(), ScopeType: "key", Kind: "monthly", Timezone: "UTC", AnchorDay: 1, Status: "active"},
		{ID: uuid.New(), ScopeType: "project", Kind: "lifetime", Status: "active"},
		{ID: uuid.New(), ScopeType: "vendor", Kind: "monthly", Timezone: "UTC", AnchorDay: 1, Status: "deleted"},
	}
	scopes := Resolve(bs, now)
	if len(scopes) != 2 || scopes[0].Budget.ScopeType != "key" || scopes[1].Budget.ScopeType != "project" {
		t.Fatalf("resolve: %+v", scopes)
	}
	if !strings.HasPrefix(scopes[1].RedisKey(), "budget:") {
		t.Fatal("key prefix")
	}
}

func itoa(n int64) string {
	return strings.TrimSpace(strings.Replace(time.Unix(n, 0).UTC().Format("x"), "x", "", 1)) + func() string {
		s := ""
		if n == 0 {
			return "0"
		}
		neg := n < 0
		if neg {
			n = -n
		}
		for n > 0 {
			s = string(rune('0'+n%10)) + s
			n /= 10
		}
		if neg {
			s = "-" + s
		}
		return s
	}()
}
