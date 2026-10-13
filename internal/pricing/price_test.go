package pricing

import (
	"testing"

	"github.com/rzp/litellm-lite/internal/canonical"
)

// Golden cases: provider-shaped usage, published list prices, expected nano-USD.
// Prices are per token (per-1M price / 1e6 * 1e9 = per-1M price * 1000 nano-USD).
func TestGoldenCosts(t *testing.T) {
	gpt4o := &Price{Tiers: []Tier{{Input: 2500, CachedInput: 1250, Output: 10000, Reasoning: 10000}}}                     // $2.50 / $1.25 / $10 per 1M
	sonnet := &Price{Tiers: []Tier{{Input: 3000, CachedInput: 300, CacheWrite5m: 3750, Output: 15000, Reasoning: 15000}}} // $3 / $0.30 / $3.75 / $15
	gemini := &Price{Tiers: []Tier{                                                                                       // $1.25 / $10 up to 200k, then $2.50 / $15
		{UpToPromptTokens: 200000, Input: 1250, CachedInput: 312, Output: 10000, Reasoning: 10000},
		{Input: 2500, CachedInput: 625, Output: 15000, Reasoning: 15000},
	}}
	o3 := &Price{Tiers: []Tier{{Input: 2000, CachedInput: 500, Output: 8000, Reasoning: 8000}}}

	cases := []struct {
		name string
		p    *Price
		u    canonical.Usage
		want int64
	}{
		{"openai plain", gpt4o, canonical.Usage{PromptTokens: 1000, CompletionTokens: 500}, 1000*2500 + 500*10000},
		{"openai cached prompt", gpt4o, canonical.Usage{PromptTokens: 1000, CachedTokens: 800, CompletionTokens: 0}, 200*2500 + 800*1250},
		{"openai reasoning billed as output", o3, canonical.Usage{PromptTokens: 100, CompletionTokens: 1200, ReasoningTokens: 1000}, 100*2000 + 200*8000 + 1000*8000},
		{"anthropic cache write and read", sonnet, canonical.Usage{PromptTokens: 10000, CachedTokens: 6000, CacheWriteTokens: 3000, CompletionTokens: 100}, 1000*3000 + 6000*300 + 3000*3750 + 100*15000},
		{"gemini below tier", gemini, canonical.Usage{PromptTokens: 150000, CompletionTokens: 1000}, 150000*1250 + 1000*10000},
		{"gemini above tier", gemini, canonical.Usage{PromptTokens: 250000, CompletionTokens: 1000}, 250000*2500 + 1000*15000},
		{"gemini exactly at boundary", gemini, canonical.Usage{PromptTokens: 200000, CompletionTokens: 0}, 200000 * 1250},
		{"zero usage", gpt4o, canonical.Usage{}, 0},
		{"malformed cached > prompt clamps", gpt4o, canonical.Usage{PromptTokens: 10, CachedTokens: 50}, 50 * 1250},
	}
	for _, c := range cases {
		if got := c.p.CostFor(c.u); got != c.want {
			t.Errorf("%s: got %d want %d", c.name, got, c.want)
		}
	}
}

func TestEstimateIsPessimistic(t *testing.T) {
	p := &Price{Tiers: []Tier{{Input: 2500, Output: 10000}}}
	est := p.Estimate(1000, 4096)
	actual := p.CostFor(canonical.Usage{PromptTokens: 1000, CompletionTokens: 300})
	if est < actual {
		t.Fatalf("estimate %d < actual %d", est, actual)
	}
}

func TestUSD(t *testing.T) {
	if s := USD(1_234_500_000); s != "1.234500000" {
		t.Fatal(s)
	}
	if s := USD(7); s != "0.000000007" {
		t.Fatal(s)
	}
	if s := USD(-NanoPerUSD); s != "-1.000000000" {
		t.Fatal(s)
	}
}

func TestParseTiersSorts(t *testing.T) {
	tiers, err := ParseTiers([]byte(`[{"input":2},{"up_to_prompt_tokens":100,"input":1}]`))
	if err != nil {
		t.Fatal(err)
	}
	if tiers[0].UpToPromptTokens != 100 || tiers[1].UpToPromptTokens != 0 {
		t.Fatalf("unsorted: %+v", tiers)
	}
}

func TestTokenizerCounts(t *testing.T) {
	tk := Default()
	n := tk.CountText("o200k_base", "The quick brown fox jumps over the lazy dog.")
	if n < 8 || n > 12 {
		t.Fatalf("unexpected count %d", n)
	}
	req := &canonical.Request{Messages: []canonical.Message{{Role: "user", Content: "hello world"}}}
	if c := tk.CountRequest("o200k_base", 1.0, req); c < 5 {
		t.Fatalf("count %d", c)
	}
	if c := tk.CountRequest("o200k_base", 2.0, req); c < 10 {
		t.Fatalf("multiplier not applied: %d", c)
	}
}
