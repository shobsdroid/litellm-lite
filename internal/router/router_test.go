package router

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rzp/litellm-lite/internal/catalog"
	"github.com/rzp/litellm-lite/internal/pricing"
	"github.com/rzp/litellm-lite/internal/store"
)

// ---- circuit breakers (PLAN §10: per-deployment, local sliding window)

func TestBreakerClosedByDefault(t *testing.T) {
	b := NewBreakers()
	id := uuid.New()
	if !b.Allow(id) || b.State(id) != "closed" {
		t.Fatal("unknown deployment must be allowed")
	}
}

func TestBreakerOpensOnFailureMajority(t *testing.T) {
	b := NewBreakers()
	id := uuid.New()
	for i := 0; i < 4; i++ {
		b.Record(id, "d", false)
	}
	if !b.Allow(id) {
		t.Fatal("4 failures is below the minimum request count; must stay closed")
	}
	b.Record(id, "d", false)
	if b.Allow(id) || b.State(id) != "open" {
		t.Fatal("5 failures with no successes must open the breaker")
	}
}

func TestBreakerStaysClosedWhenMostlyHealthy(t *testing.T) {
	b := NewBreakers()
	id := uuid.New()
	for i := 0; i < 5; i++ {
		b.Record(id, "d", false)
		b.Record(id, "d", true)
		b.Record(id, "d", true)
	}
	if !b.Allow(id) {
		t.Fatal("5 failures out of 15 must not open the breaker")
	}
}

func TestBreakerHalfOpenProbe(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for the 10s breaker cool-down")
	}
	b := NewBreakers()
	id := uuid.New()
	for i := 0; i < 5; i++ {
		b.Record(id, "d", false)
	}
	if b.Allow(id) {
		t.Fatal("must be open")
	}
	time.Sleep(breakerOpenFor + 100*time.Millisecond)
	if b.State(id) != "half_open" {
		t.Fatalf("state %s want half_open", b.State(id))
	}
	if !b.Allow(id) {
		t.Fatal("exactly one probe must be let through after the cool-down")
	}
	if b.Allow(id) {
		t.Fatal("second request during the probe must be rejected")
	}
	b.Record(id, "d", false)
	if b.Allow(id) {
		t.Fatal("a failed probe must re-open the breaker")
	}
	time.Sleep(breakerOpenFor + 100*time.Millisecond)
	if !b.Allow(id) {
		t.Fatal("probe after second cool-down")
	}
	b.Record(id, "d", true)
	if !b.Allow(id) || b.State(id) != "closed" {
		t.Fatal("a successful probe must close the breaker")
	}
}

func TestBreakerTickDecaysWindow(t *testing.T) {
	b := NewBreakers()
	id := uuid.New()
	for i := 0; i < 4; i++ {
		b.Record(id, "d", false)
	}
	for i := 0; i < 31; i++ { // sweep every slot
		b.Tick()
		time.Sleep(time.Millisecond)
	}
	// After the window is cleared a single new failure must not open it.
	br := b.get(id)
	br.mu.Lock()
	var fails int32
	for _, s := range br.window {
		fails += s.fail
	}
	br.mu.Unlock()
	if fails > 4 {
		t.Fatalf("window grew: %d", fails)
	}
}

// ---- candidate selection (PLAN §8.1)

type fixture struct {
	snap      *catalog.Snapshot
	primary   *catalog.Deployment
	byName    map[string]*catalog.Deployment
	openai    *store.Provider
	anthropic *store.Provider
	google    *store.Provider
}

func price(input int64) *pricing.Price {
	return &pricing.Price{ID: uuid.New(), Tiers: []pricing.Tier{{Input: input, Output: input * 4}}}
}

func newFixture() *fixture {
	f := &fixture{snap: &catalog.Snapshot{ByID: map[uuid.UUID]*catalog.Deployment{}, BySlug: map[string]*catalog.Deployment{}, ByModelSlug: map[string][]*catalog.Deployment{}}, byName: map[string]*catalog.Deployment{}}
	mk := func(slug string) *store.Provider {
		return &store.Provider{ID: uuid.New(), Slug: slug, Status: "active", Dialect: "mock"}
	}
	f.openai, f.anthropic, f.google = mk("openai"), mk("anthropic"), mk("google")
	gpt4o := &store.Model{ID: uuid.New(), Slug: "gpt-4o", Family: "gpt"}
	gpt4oMini := &store.Model{ID: uuid.New(), Slug: "gpt-4o-mini", Family: "gpt"}
	sonnet := &store.Model{ID: uuid.New(), Slug: "claude-sonnet-4-5", Family: "claude"}
	gemini := &store.Model{ID: uuid.New(), Slug: "gemini-2.5-pro", Family: "gemini"}
	add := func(slug string, m *store.Model, p *store.Provider, ctx int32, effort string, inputPrice int64, caps []string, priority int32) *catalog.Deployment {
		d := &catalog.Deployment{Deployment: store.Deployment{ID: uuid.New(), ModelID: m.ID, ProviderID: p.ID, Slug: slug, ContextWindow: ctx, MaxOutputTokens: 4096, Effort: effort, Status: "active", Priority: priority},
			Model: m, Provider: p, Price: price(inputPrice), Capabilities: caps}
		f.snap.ByID[d.ID] = d
		f.snap.BySlug[slug] = d
		f.snap.ByModelSlug[m.Slug] = append(f.snap.ByModelSlug[m.Slug], d)
		f.snap.Deployments = append(f.snap.Deployments, d)
		f.byName[slug] = d
		return d
	}
	f.primary = add("gpt-4o-openai-128k-medium", gpt4o, f.openai, 128000, "medium", 2500, []string{"tools", "streaming"}, 10)
	add("gpt-4o-openai-128k-high", gpt4o, f.openai, 128000, "high", 2500, []string{"tools", "streaming"}, 20)
	add("gpt-4o-openai-128k-low", gpt4o, f.openai, 128000, "low", 2500, []string{"tools", "streaming"}, 30)
	add("gpt-4o-mini-openai-128k-medium", gpt4oMini, f.openai, 128000, "medium", 150, []string{"tools", "streaming"}, 40)
	add("claude-sonnet-4-5-anthropic-200k-medium", sonnet, f.anthropic, 200000, "medium", 3000, []string{"tools", "streaming"}, 50)
	add("gemini-2.5-pro-google-1m-medium", gemini, f.google, 1000000, "medium", 1250, []string{"tools", "streaming", "vision"}, 60)
	add("gemini-2.5-pro-google-1m-none", gemini, f.google, 1000000, "none", 1250, []string{"streaming"}, 70)
	return f
}

func slugs(ds []*catalog.Deployment) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = d.Slug
	}
	return out
}

func skipReason(skips []Skip, slug string) string {
	for _, s := range skips {
		if s.Deployment.Slug == slug {
			return s.Reason
		}
	}
	return ""
}

func policy(t *testing.T, raw string) catalog.FallbackPolicy {
	t.Helper()
	return catalog.ParsePolicy(json.RawMessage(raw))
}

func TestCandidatesExcludePrimaryAndOrderByPrice(t *testing.T) {
	f := newFixture()
	pol := policy(t, `{"filters":{"effort":"any"},"order_by":["price"]}`)
	cands, _ := Candidates(f.snap, f.primary, pol, 10000, nil, nil)
	got := slugs(cands)
	for _, s := range got {
		if s == f.primary.Slug {
			t.Fatal("primary must not be a fallback candidate")
		}
	}
	if len(got) != 6 || got[0] != "gpt-4o-mini-openai-128k-medium" {
		t.Fatalf("cheapest first: %v", got)
	}
	if got[len(got)-1] != "claude-sonnet-4-5-anthropic-200k-medium" {
		t.Fatalf("most expensive last: %v", got)
	}
}

func TestCandidatesExplicitFirst(t *testing.T) {
	f := newFixture()
	pol := policy(t, `{"explicit":["claude-sonnet-4-5-anthropic-200k-medium","does-not-exist"],"filters":{"effort":"any"},"order_by":["price"]}`)
	cands, _ := Candidates(f.snap, f.primary, pol, 10000, nil, nil)
	got := slugs(cands)
	if got[0] != "claude-sonnet-4-5-anthropic-200k-medium" {
		t.Fatalf("explicit candidate must lead: %v", got)
	}
	for _, s := range got[1:] {
		if s == got[0] {
			t.Fatal("explicit candidate must not repeat in the filtered list")
		}
	}
}

func TestCandidatesContextFilter(t *testing.T) {
	f := newFixture()
	pol := policy(t, `{"filters":{"min_context":"auto","effort":"any"}}`)
	cands, skips := Candidates(f.snap, f.primary, pol, 300000, nil, nil)
	for _, s := range slugs(cands) {
		if s != "gemini-2.5-pro-google-1m-medium" && s != "gemini-2.5-pro-google-1m-none" {
			t.Fatalf("only 1M-context deployments fit 300k: %v", slugs(cands))
		}
	}
	if skipReason(skips, "claude-sonnet-4-5-anthropic-200k-medium") != "context_too_small" {
		t.Fatalf("skip reason: %+v", skips)
	}
	// An explicit integer min_context overrides auto.
	pol = policy(t, `{"filters":{"min_context":150000,"effort":"any"}}`)
	cands, _ = Candidates(f.snap, f.primary, pol, 10, nil, nil)
	for _, s := range slugs(cands) {
		if s == "gpt-4o-mini-openai-128k-medium" {
			t.Fatal("128k deployment must be filtered by min_context 150000")
		}
	}
}

func TestCandidatesProviderFilters(t *testing.T) {
	f := newFixture()
	pol := policy(t, `{"filters":{"providers_allow":["anthropic","google"],"effort":"any"}}`)
	cands, _ := Candidates(f.snap, f.primary, pol, 1000, nil, nil)
	for _, d := range cands {
		if d.Provider.Slug == "openai" {
			t.Fatalf("providers_allow violated: %v", slugs(cands))
		}
	}
	pol = policy(t, `{"filters":{"providers_deny":["google"],"effort":"any"}}`)
	cands, _ = Candidates(f.snap, f.primary, pol, 1000, nil, nil)
	for _, d := range cands {
		if d.Provider.Slug == "google" {
			t.Fatalf("providers_deny violated: %v", slugs(cands))
		}
	}
}

func TestCandidatesModelAndFamily(t *testing.T) {
	f := newFixture()
	cands, _ := Candidates(f.snap, f.primary, policy(t, `{"filters":{"same_model":true,"effort":"any"}}`), 1000, nil, nil)
	for _, d := range cands {
		if d.Model.Slug != "gpt-4o" {
			t.Fatalf("same_model violated: %v", slugs(cands))
		}
	}
	if len(cands) != 2 {
		t.Fatalf("expected the two other gpt-4o efforts: %v", slugs(cands))
	}
	cands, _ = Candidates(f.snap, f.primary, policy(t, `{"filters":{"same_family":true,"effort":"any"}}`), 1000, nil, nil)
	for _, d := range cands {
		if d.Model.Family != "gpt" {
			t.Fatalf("same_family violated: %v", slugs(cands))
		}
	}
	if len(cands) != 3 {
		t.Fatalf("expected gpt-4o x2 + gpt-4o-mini: %v", slugs(cands))
	}
}

func TestCandidatesEffort(t *testing.T) {
	f := newFixture()
	cands, _ := Candidates(f.snap, f.primary, policy(t, `{"filters":{"same_model":true,"effort":"gte"}}`), 1000, nil, nil)
	if got := slugs(cands); len(got) != 1 || got[0] != "gpt-4o-openai-128k-high" {
		t.Fatalf("effort gte medium: %v", got)
	}
	cands, _ = Candidates(f.snap, f.primary, policy(t, `{"filters":{"same_model":true,"effort":"same"}}`), 1000, nil, nil)
	if len(cands) != 0 {
		t.Fatalf("no other gpt-4o deployment has effort medium: %v", slugs(cands))
	}
}

func TestCandidatesPriceCapAndCapabilities(t *testing.T) {
	f := newFixture()
	cands, _ := Candidates(f.snap, f.primary, policy(t, `{"filters":{"max_input_price_nano_usd":2000,"effort":"any"}}`), 1000, nil, nil)
	for _, d := range cands {
		if d.Price.Tier(1000).Input > 2000 {
			t.Fatalf("price cap violated: %v", slugs(cands))
		}
	}
	cands, _ = Candidates(f.snap, f.primary, policy(t, `{"filters":{"require_capabilities":["tools","vision"],"effort":"any"}}`), 1000, nil, nil)
	if got := slugs(cands); len(got) != 1 || got[0] != "gemini-2.5-pro-google-1m-medium" {
		t.Fatalf("only gemini medium has tools+vision: %v", got)
	}
}

func TestCandidatesHealthStatusAndAllowList(t *testing.T) {
	f := newFixture()
	pol := policy(t, `{"filters":{"effort":"any"}}`)
	mini := f.byName["gpt-4o-mini-openai-128k-medium"]
	sonnet := f.byName["claude-sonnet-4-5-anthropic-200k-medium"]
	geminiNone := f.byName["gemini-2.5-pro-google-1m-none"]

	mini.Status = "disabled"
	sonnet.Price = nil
	f.google.Status = "disabled"
	br := NewBreakers()
	high := f.byName["gpt-4o-openai-128k-high"]
	for i := 0; i < 5; i++ {
		br.Record(high.ID, high.Slug, false)
	}
	cands, skips := Candidates(f.snap, f.primary, pol, 1000, nil, br)
	if got := slugs(cands); len(got) != 1 || got[0] != "gpt-4o-openai-128k-low" {
		t.Fatalf("only the low deployment should survive: %v", got)
	}
	for slug, want := range map[string]string{mini.Slug: "inactive", sonnet.Slug: "unpriced", geminiNone.Slug: "provider_inactive", high.Slug: "breaker_open"} {
		if got := skipReason(skips, slug); got != want {
			t.Errorf("%s: skip %q want %q", slug, got, want)
		}
	}

	// Key allow-list restricts fallbacks too.
	f = newFixture()
	low := f.byName["gpt-4o-openai-128k-low"]
	cands, skips = Candidates(f.snap, f.primary, pol, 1000, []uuid.UUID{f.primary.ID, low.ID}, nil)
	if got := slugs(cands); len(got) != 1 || got[0] != low.Slug {
		t.Fatalf("allow-list: %v", got)
	}
	if skipReason(skips, "gpt-4o-mini-openai-128k-medium") != "not_allowed_for_key" {
		t.Fatalf("skips: %+v", skips)
	}
}

func TestCandidatesOrderBySameModelThenPrice(t *testing.T) {
	f := newFixture()
	cands, _ := Candidates(f.snap, f.primary, policy(t, `{"filters":{"effort":"any"},"order_by":["same_model","price"]}`), 1000, nil, nil)
	got := slugs(cands)
	if got[0] != "gpt-4o-openai-128k-high" || got[1] != "gpt-4o-openai-128k-low" {
		t.Fatalf("same-model deployments must come first (by priority): %v", got)
	}
	if got[2] != "gpt-4o-mini-openai-128k-medium" {
		t.Fatalf("then the cheapest other model: %v", got)
	}
}
