// Package catalog holds the in-memory configuration snapshot the gateway
// serves from: providers, models, deployments, prices, decrypted credentials,
// and a bounded cache of resolved keys. Nothing here touches Postgres on the
// request path.
package catalog

import (
	"encoding/json"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rzp/litellm-lite/internal/pricing"
	"github.com/rzp/litellm-lite/internal/store"
)

type Deployment struct {
	store.Deployment
	Model        *store.Model
	Provider     *store.Provider
	Price        *pricing.Price // nil means unpriced: requests are refused
	Capabilities []string
}

func (d *Deployment) HasCapability(c string) bool {
	for _, x := range d.Capabilities {
		if x == c {
			return true
		}
	}
	return false
}

type Snapshot struct {
	Version     int64
	LoadedAt    time.Time
	ByID        map[uuid.UUID]*Deployment
	BySlug      map[string]*Deployment
	ByModelSlug map[string][]*Deployment
	Providers   map[uuid.UUID]*store.Provider
	Models      map[uuid.UUID]*store.Model
	Credentials map[uuid.UUID]string // provider id -> decrypted secret
	Deployments []*Deployment
}

// FallbackPolicy is stored as JSON on the key.
type FallbackPolicy struct {
	MaxAttempts int      `json:"max_attempts"`
	Triggers    []string `json:"triggers"`
	Explicit    []string `json:"explicit"`
	Filters     Filters  `json:"filters"`
	OrderBy     []string `json:"order_by"`
}

type Filters struct {
	MinContextRaw       json.RawMessage `json:"min_context"` // "auto" | integer
	SameModel           bool            `json:"same_model"`
	SameFamily          bool            `json:"same_family"`
	ProvidersAllow      []string        `json:"providers_allow"`
	ProvidersDeny       []string        `json:"providers_deny"`
	Effort              string          `json:"effort"` // same | gte | any
	MaxInputPriceNano   int64           `json:"max_input_price_nano_usd"`
	RequireCapabilities []string        `json:"require_capabilities"`
}

var DefaultTriggers = []string{"timeout", "connection_error", "http_5xx", "http_429", "context_length_exceeded"}

func ParsePolicy(raw json.RawMessage) FallbackPolicy {
	p := FallbackPolicy{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &p)
	}
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 3
	}
	if p.Triggers == nil {
		p.Triggers = DefaultTriggers
	}
	if p.Filters.Effort == "" {
		p.Filters.Effort = "any"
	}
	if len(p.OrderBy) == 0 {
		p.OrderBy = []string{"same_model", "price"}
	}
	return p
}

func (p FallbackPolicy) Triggered(class string) bool {
	for _, t := range p.Triggers {
		if t == class {
			return true
		}
	}
	return false
}

// MinContext resolves the filter: auto -> needed, int -> that value, absent -> needed.
func (f Filters) MinContext(needed int64) int64 {
	if len(f.MinContextRaw) == 0 {
		return needed
	}
	var n int64
	if err := json.Unmarshal(f.MinContextRaw, &n); err == nil {
		return n
	}
	return needed
}

// KeyConfig is everything the hot path needs for one virtual key.
type KeyConfig struct {
	Key      *store.Key
	Project  *store.Project
	Vendor   *store.Vendor
	Budgets  []store.Budget // key, project and vendor scopes
	Policy   FallbackPolicy
	LoadedAt time.Time
}

// Holder is an atomic pointer to the current snapshot.
type Holder struct{ p atomic.Pointer[Snapshot] }

func (h *Holder) Load() *Snapshot   { return h.p.Load() }
func (h *Holder) Store(s *Snapshot) { h.p.Store(s) }
