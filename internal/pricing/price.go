// Package pricing is the cost engine. All money is int64 nano-USD
// (1 USD = 1_000_000_000). Prices are per token so cost is a single multiply.
package pricing

import (
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/rzp/litellm-lite/internal/canonical"
)

const NanoPerUSD int64 = 1_000_000_000

// Tier holds per-token prices in nano-USD. A tier applies while the prompt
// token count is <= UpToPromptTokens; 0 means unbounded.
type Tier struct {
	UpToPromptTokens int64 `json:"up_to_prompt_tokens"`
	Input            int64 `json:"input"`
	CachedInput      int64 `json:"cached_input"`
	CacheWrite5m     int64 `json:"cache_write_5m"`
	CacheWrite1h     int64 `json:"cache_write_1h"`
	Output           int64 `json:"output"`
	Reasoning        int64 `json:"reasoning"`
}

type Price struct {
	ID            uuid.UUID `json:"id"`
	DeploymentID  uuid.UUID `json:"deployment_id"`
	EffectiveFrom time.Time `json:"effective_from"`
	Tiers         []Tier    `json:"tiers"`
	Verified      bool      `json:"verified"`
	Source        string    `json:"source"`
}

func ParseTiers(raw []byte) ([]Tier, error) {
	var tiers []Tier
	if err := json.Unmarshal(raw, &tiers); err != nil {
		return nil, err
	}
	if len(tiers) == 0 {
		return nil, errors.New("price must have at least one tier")
	}
	sort.SliceStable(tiers, func(i, j int) bool {
		a, b := tiers[i].UpToPromptTokens, tiers[j].UpToPromptTokens
		if a == 0 {
			return false
		}
		if b == 0 {
			return true
		}
		return a < b
	})
	for _, t := range tiers {
		if t.Input < 0 || t.CachedInput < 0 || t.Output < 0 || t.Reasoning < 0 || t.CacheWrite5m < 0 || t.CacheWrite1h < 0 {
			return nil, errors.New("negative price")
		}
	}
	return tiers, nil
}

// Tier picks the tier for a prompt of the given size.
func (p *Price) Tier(promptTokens int64) Tier {
	for _, t := range p.Tiers {
		if t.UpToPromptTokens == 0 || promptTokens <= t.UpToPromptTokens {
			return t
		}
	}
	return p.Tiers[len(p.Tiers)-1]
}

// Cost prices a normalized usage against a tier. Pure integer arithmetic.
func Cost(t Tier, u canonical.Usage) int64 {
	uncachedInput := u.PromptTokens - u.CachedTokens - u.CacheWriteTokens
	if uncachedInput < 0 {
		uncachedInput = 0
	}
	plainOutput := u.CompletionTokens - u.ReasoningTokens
	if plainOutput < 0 {
		plainOutput = 0
	}
	return uncachedInput*t.Input +
		u.CachedTokens*t.CachedInput +
		u.CacheWriteTokens*t.CacheWrite5m +
		plainOutput*t.Output +
		u.ReasoningTokens*t.Reasoning
}

// CostFor picks the tier from the usage's prompt size and prices it.
func (p *Price) CostFor(u canonical.Usage) int64 {
	return Cost(p.Tier(u.PromptTokens), u)
}

// Estimate is the pessimistic pre-request reservation.
func (p *Price) Estimate(promptTokens, maxOutputTokens int64) int64 {
	t := p.Tier(promptTokens)
	return promptTokens*t.Input + maxOutputTokens*t.Output
}

// USD renders nano-USD as a decimal string for display only.
func USD(nano int64) string {
	neg := nano < 0
	if neg {
		nano = -nano
	}
	whole := nano / NanoPerUSD
	frac := nano % NanoPerUSD
	s := itoa(whole) + "." + pad9(frac)
	if neg {
		return "-" + s
	}
	return s
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func pad9(n int64) string {
	s := itoa(n)
	for len(s) < 9 {
		s = "0" + s
	}
	return s
}
