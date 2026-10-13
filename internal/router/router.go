// Package router selects deployments: primary first, then fallback candidates
// filtered by the key's policy and ordered by preference. Circuit breakers
// per deployment exclude unhealthy targets before ordering.
package router

import (
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rzp/litellm-lite/internal/catalog"
	"github.com/rzp/litellm-lite/internal/metrics"
)

// ---- circuit breaker

type breaker struct {
	mu        sync.Mutex
	window    [30]struct{ ok, fail int32 } // one slot per second
	openUntil time.Time
	probing   bool
}

type Breakers struct {
	mu sync.Mutex
	m  map[uuid.UUID]*breaker
}

func NewBreakers() *Breakers { return &Breakers{m: map[uuid.UUID]*breaker{}} }

func (b *Breakers) get(id uuid.UUID) *breaker {
	b.mu.Lock()
	defer b.mu.Unlock()
	br := b.m[id]
	if br == nil {
		br = &breaker{}
		b.m[id] = br
	}
	return br
}

const (
	breakerMinRequests = 5
	breakerOpenFor     = 10 * time.Second
)

// Allow reports whether the deployment may be tried. In the open state one
// probe request is let through once the cool-down passes.
func (b *Breakers) Allow(id uuid.UUID) bool {
	br := b.get(id)
	br.mu.Lock()
	defer br.mu.Unlock()
	now := time.Now()
	if now.Before(br.openUntil) {
		return false
	}
	if !br.openUntil.IsZero() && !br.probing {
		br.probing = true
		return true
	}
	if br.probing {
		return false
	}
	return true
}

func (b *Breakers) Record(id uuid.UUID, slug string, ok bool) {
	br := b.get(id)
	br.mu.Lock()
	defer br.mu.Unlock()
	now := time.Now()
	slot := &br.window[now.Unix()%30]
	if ok {
		slot.ok++
	} else {
		slot.fail++
	}
	if br.probing {
		br.probing = false
		if ok {
			br.openUntil = time.Time{}
			br.window = [30]struct{ ok, fail int32 }{}
			metrics.Breaker.WithLabelValues(slug).Set(0)
			return
		}
		br.openUntil = now.Add(breakerOpenFor)
		return
	}
	var oks, fails int32
	for _, s := range br.window {
		oks += s.ok
		fails += s.fail
	}
	if fails >= breakerMinRequests && fails*2 > oks+fails {
		br.openUntil = now.Add(breakerOpenFor)
		br.window = [30]struct{ ok, fail int32 }{}
		metrics.Breaker.WithLabelValues(slug).Set(1)
	}
}

// Tick decays the sliding window; call once per second.
func (b *Breakers) Tick() {
	b.mu.Lock()
	defer b.mu.Unlock()
	next := (time.Now().Unix() + 1) % 30
	for _, br := range b.m {
		br.mu.Lock()
		br.window[next] = struct{ ok, fail int32 }{}
		br.mu.Unlock()
	}
}

func (b *Breakers) State(id uuid.UUID) string {
	br := b.get(id)
	br.mu.Lock()
	defer br.mu.Unlock()
	if time.Now().Before(br.openUntil) {
		return "open"
	}
	if !br.openUntil.IsZero() {
		return "half_open"
	}
	return "closed"
}

// ---- candidate selection

type Skip struct {
	Deployment *catalog.Deployment
	Reason     string
}

// Candidates returns the ordered fallback list for a request that needs
// `needed` context tokens, excluding the primary. Explicit candidates come
// first (still subject to context, status, health), then filtered ones.
func Candidates(snap *catalog.Snapshot, primary *catalog.Deployment, pol catalog.FallbackPolicy, needed int64, allowed []uuid.UUID, br *Breakers) ([]*catalog.Deployment, []Skip) {
	var out []*catalog.Deployment
	var skips []Skip
	seen := map[uuid.UUID]bool{primary.ID: true}
	allowedSet := map[uuid.UUID]bool{}
	for _, id := range allowed {
		allowedSet[id] = true
	}
	minCtx := pol.Filters.MinContext(needed)

	basic := func(d *catalog.Deployment) string {
		if d.Status != "active" {
			return "inactive"
		}
		if d.Provider == nil || d.Provider.Status != "active" {
			return "provider_inactive"
		}
		if d.Price == nil {
			return "unpriced"
		}
		if int64(d.ContextWindow) < minCtx {
			return "context_too_small"
		}
		if len(allowedSet) > 0 && !allowedSet[d.ID] {
			return "not_allowed_for_key"
		}
		if br != nil && !br.Allow(d.ID) {
			return "breaker_open"
		}
		return ""
	}

	for _, slug := range pol.Explicit {
		d := snap.BySlug[slug]
		if d == nil || seen[d.ID] {
			continue
		}
		seen[d.ID] = true
		if why := basic(d); why != "" {
			skips = append(skips, Skip{d, why})
			continue
		}
		out = append(out, d)
	}

	f := pol.Filters
	var filtered []*catalog.Deployment
	for _, d := range snap.Deployments {
		if seen[d.ID] {
			continue
		}
		if f.SameModel && d.ModelID != primary.ModelID {
			continue
		}
		if f.SameFamily && (d.Model == nil || primary.Model == nil || d.Model.Family != primary.Model.Family) {
			continue
		}
		if len(f.ProvidersAllow) > 0 && !contains(f.ProvidersAllow, d.Provider.Slug) {
			continue
		}
		if contains(f.ProvidersDeny, d.Provider.Slug) {
			continue
		}
		switch f.Effort {
		case "same":
			if d.Effort != primary.Effort {
				continue
			}
		case "gte":
			if effortRank(d.Effort) < effortRank(primary.Effort) {
				continue
			}
		}
		if f.MaxInputPriceNano > 0 && d.Price != nil && d.Price.Tier(needed).Input > f.MaxInputPriceNano {
			continue
		}
		capOK := true
		for _, c := range f.RequireCapabilities {
			if !d.HasCapability(c) {
				capOK = false
			}
		}
		if !capOK {
			continue
		}
		seen[d.ID] = true
		if why := basic(d); why != "" {
			skips = append(skips, Skip{d, why})
			continue
		}
		filtered = append(filtered, d)
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		a, b := filtered[i], filtered[j]
		for _, k := range pol.OrderBy {
			switch k {
			case "same_model":
				am, bm := a.ModelID == primary.ModelID, b.ModelID == primary.ModelID
				if am != bm {
					return am
				}
			case "price":
				ap, bp := a.Price.Tier(needed).Input, b.Price.Tier(needed).Input
				if ap != bp {
					return ap < bp
				}
			case "priority":
				if a.Priority != b.Priority {
					return a.Priority < b.Priority
				}
			}
		}
		return a.Priority < b.Priority
	})
	return append(out, filtered...), skips
}

func effortRank(e string) int {
	switch e {
	case "low":
		return 1
	case "medium":
		return 2
	case "high":
		return 3
	}
	return 0
}

func contains(xs []string, x string) bool {
	for _, s := range xs {
		if s == x {
			return true
		}
	}
	return false
}
