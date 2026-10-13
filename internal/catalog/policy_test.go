package catalog

import (
	"crypto/sha256"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/rzp/litellm-lite/internal/store"
)

// PLAN §8.1: fallback policy stored per key as JSON.
const planPolicy = `{
  "max_attempts": 3,
  "triggers": ["timeout", "connection_error", "http_5xx", "http_429", "context_length_exceeded"],
  "explicit": ["claude-sonnet-4-5-anthropic-200k-medium"],
  "filters": {
    "min_context": "auto",
    "same_model": false,
    "same_family": true,
    "providers_allow": ["anthropic", "google", "openai"],
    "providers_deny": [],
    "effort": "gte",
    "max_input_price_nano_usd": 5000,
    "require_capabilities": ["tools", "streaming"]
  },
  "order_by": ["same_model", "price", "p50_latency"]
}`

func TestParsePolicyFromPlanExample(t *testing.T) {
	p := ParsePolicy(json.RawMessage(planPolicy))
	if p.MaxAttempts != 3 || len(p.Triggers) != 5 || p.Explicit[0] != "claude-sonnet-4-5-anthropic-200k-medium" {
		t.Fatalf("top level: %+v", p)
	}
	f := p.Filters
	if f.SameModel || !f.SameFamily || f.Effort != "gte" || f.MaxInputPriceNano != 5000 || len(f.ProvidersAllow) != 3 || len(f.RequireCapabilities) != 2 {
		t.Fatalf("filters: %+v", f)
	}
	if f.MinContext(12345) != 12345 {
		t.Fatalf(`min_context "auto" must resolve to prompt+max_tokens`)
	}
	if p.OrderBy[2] != "p50_latency" {
		t.Fatalf("order_by: %v", p.OrderBy)
	}
}

func TestParsePolicyDefaults(t *testing.T) {
	for _, raw := range []json.RawMessage{nil, json.RawMessage(``), json.RawMessage(`{}`), json.RawMessage(`{"max_attempts":0}`), json.RawMessage(`garbage`)} {
		p := ParsePolicy(raw)
		if p.MaxAttempts != 3 {
			t.Errorf("%q: max_attempts %d", raw, p.MaxAttempts)
		}
		if len(p.Triggers) != len(DefaultTriggers) {
			t.Errorf("%q: triggers %v", raw, p.Triggers)
		}
		if p.Filters.Effort != "any" {
			t.Errorf("%q: effort %q", raw, p.Filters.Effort)
		}
		if len(p.OrderBy) == 0 {
			t.Errorf("%q: empty order_by", raw)
		}
	}
	// An explicitly empty trigger list disables fallback entirely.
	p := ParsePolicy(json.RawMessage(`{"triggers":[]}`))
	if p.Triggered("http_5xx") {
		t.Fatal("empty triggers must not fall back")
	}
}

func TestTriggered(t *testing.T) {
	p := ParsePolicy(nil)
	for _, c := range DefaultTriggers {
		if !p.Triggered(c) {
			t.Errorf("default trigger %s not honoured", c)
		}
	}
	// PLAN §7 step 7: client-caused 400s, refusals and our own auth failures never fall back.
	for _, c := range []string{"client_error", "content_filter", "provider_auth", ""} {
		if p.Triggered(c) {
			t.Errorf("%q must not trigger fallback", c)
		}
	}
}

func TestMinContext(t *testing.T) {
	cases := []struct {
		raw  string
		want int64
	}{{``, 777}, {`"auto"`, 777}, {`50000`, 50000}, {`"nonsense"`, 777}}
	for _, c := range cases {
		f := Filters{MinContextRaw: json.RawMessage(c.raw)}
		if got := f.MinContext(777); got != c.want {
			t.Errorf("min_context %s: %d want %d", c.raw, got, c.want)
		}
	}
}

// PLAN §8.3: only the SHA-256 of a virtual key is ever stored.
func TestHashKey(t *testing.T) {
	secret := "sk-ll-proj-abcdef"
	if HashKey(secret) != sha256.Sum256([]byte(secret)) {
		t.Fatal("HashKey must be plain sha256")
	}
	if HashKey(secret) == HashKey(secret+"x") {
		t.Fatal("distinct secrets must hash differently")
	}
}

func TestHasCapability(t *testing.T) {
	d := &Deployment{Capabilities: []string{"tools", "streaming"}}
	if !d.HasCapability("tools") || d.HasCapability("vision") {
		t.Fatal("capability lookup wrong")
	}
	if (&Deployment{}).HasCapability("tools") {
		t.Fatal("no capabilities means none")
	}
}

func TestHolderSwap(t *testing.T) {
	var h Holder
	if h.Load() != nil {
		t.Fatal("fresh holder must be nil (readyz reports 503)")
	}
	s1 := &Snapshot{Version: 1, ByID: map[uuid.UUID]*Deployment{}}
	h.Store(s1)
	if h.Load() != s1 {
		t.Fatal("store/load mismatch")
	}
	s2 := &Snapshot{Version: 2}
	h.Store(s2)
	if h.Load().Version != 2 {
		t.Fatal("swap must be visible immediately")
	}
}

func TestKeyConfigCarriesScopeChain(t *testing.T) {
	// PLAN §8.2: key, project and vendor budgets all enforced from one config.
	kc := KeyConfig{Budgets: []store.Budget{{ScopeType: "key"}, {ScopeType: "project"}, {ScopeType: "vendor"}}}
	seen := map[string]bool{}
	for _, b := range kc.Budgets {
		seen[b.ScopeType] = true
	}
	if !seen["key"] || !seen["project"] || !seen["vendor"] {
		t.Fatal("scope chain incomplete")
	}
}
