// Package budget enforces spend caps on the hot path with Redis Lua scripts,
// and degrades to bounded local enforcement when Redis is unavailable.
package budget

import (
	"fmt"
	"time"

	"github.com/rzp/litellm-lite/internal/store"
)

var lifetimeStart = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
var lifetimeEnd = time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC)

// Period returns the current enforcement window for a budget.
func Period(b *store.Budget, now time.Time) (start, end time.Time) {
	if b.Kind == "lifetime" {
		return lifetimeStart, lifetimeEnd
	}
	loc, err := time.LoadLocation(b.Timezone)
	if err != nil {
		loc = time.UTC
	}
	t := now.In(loc)
	anchor := int(b.AnchorDay)
	if anchor < 1 {
		anchor = 1
	}
	if anchor > 28 {
		anchor = 28
	}
	y, m := t.Year(), t.Month()
	start = time.Date(y, m, anchor, 0, 0, 0, 0, loc)
	if t.Before(start) {
		start = start.AddDate(0, -1, 0)
	}
	end = start.AddDate(0, 1, 0)
	return start.UTC(), end.UTC()
}

// Scope is one budget resolved to its current period and Redis key.
type Scope struct {
	Budget      store.Budget
	PeriodStart time.Time
	PeriodEnd   time.Time
}

func (s Scope) RedisKey() string {
	return fmt.Sprintf("budget:%s:%d", s.Budget.ID, s.PeriodStart.Unix())
}

func (s Scope) ExpireSeconds(now time.Time) int64 {
	if s.Budget.Kind == "lifetime" {
		return 0
	}
	return int64(s.PeriodEnd.Sub(now).Seconds()) + 7*86400
}

func Resolve(budgets []store.Budget, now time.Time) []Scope {
	out := make([]Scope, 0, len(budgets))
	for _, b := range budgets {
		if b.Status != "active" {
			continue
		}
		s, e := Period(&b, now)
		out = append(out, Scope{Budget: b, PeriodStart: s, PeriodEnd: e})
	}
	return out
}
