package pricing

import (
	"strings"
	"testing"
	"time"

	"github.com/rzp/litellm-lite/internal/canonical"
)

// PLAN §5.1: tiers chosen once per attempt from the prompt token count.
func TestTierSelection(t *testing.T) {
	p := &Price{Tiers: []Tier{
		{UpToPromptTokens: 200000, Input: 1250, Output: 10000},
		{Input: 2500, Output: 15000},
	}}
	cases := []struct {
		prompt int64
		input  int64
	}{{0, 1250}, {1, 1250}, {199999, 1250}, {200000, 1250}, {200001, 2500}, {5_000_000, 2500}}
	for _, c := range cases {
		if got := p.Tier(c.prompt).Input; got != c.input {
			t.Errorf("prompt=%d: input price %d want %d", c.prompt, got, c.input)
		}
	}
	single := &Price{Tiers: []Tier{{Input: 7}}}
	if single.Tier(1<<40).Input != 7 {
		t.Fatal("single unbounded tier must apply to any prompt size")
	}
	// An empty tier list is rejected at parse time, so Tier never sees one.
	if _, err := ParseTiers([]byte(`[]`)); err == nil {
		t.Fatal("empty tier list must be rejected by ParseTiers")
	}
	if _, err := ParseTiers([]byte(`[{"input":-1}]`)); err == nil {
		t.Fatal("negative prices must be rejected by ParseTiers")
	}
}

// PLAN §5.1: six token types priced separately, pure integer math.
func TestCostPerTokenType(t *testing.T) {
	tier := Tier{Input: 1, CachedInput: 10, CacheWrite5m: 100, CacheWrite1h: 1000, Output: 10000, Reasoning: 100000}
	u := canonical.Usage{PromptTokens: 100, CachedTokens: 30, CacheWriteTokens: 20, CompletionTokens: 50, ReasoningTokens: 5}
	// uncached input = 100-30-20 = 50
	want := int64(50*1 + 30*10 + 20*100 + 45*10000 + 5*100000)
	if got := Cost(tier, u); got != want {
		t.Fatalf("cost %d want %d", got, want)
	}
}

func TestCostIsExactAtScale(t *testing.T) {
	// $2.50 per 1M input tokens stored as 2500 nano-USD per token.
	p := &Price{Tiers: []Tier{{Input: 2500, Output: 10000}}}
	got := p.CostFor(canonical.Usage{PromptTokens: 1_000_000, CompletionTokens: 1_000_000})
	if got != 2_500_000_000+10_000_000_000 {
		t.Fatalf("cost %d", got)
	}
	if USD(got) != "12.500000000" {
		t.Fatalf("usd %s", USD(got))
	}
	// A whole month of heavy traffic stays far inside int64.
	big := p.CostFor(canonical.Usage{PromptTokens: 1e12, CompletionTokens: 1e11})
	if big <= 0 {
		t.Fatalf("overflow: %d", big)
	}
}

// PLAN §5.3: estimate = tier(est_input).input * est_input + tier.output * est_output.
func TestEstimateUsesPromptTier(t *testing.T) {
	gemini := &Price{Tiers: []Tier{
		{UpToPromptTokens: 200000, Input: 1250, Output: 10000},
		{Input: 2500, Output: 15000},
	}}
	if got := gemini.Estimate(150000, 1000); got != 150000*1250+1000*10000 {
		t.Fatalf("below tier: %d", got)
	}
	if got := gemini.Estimate(250000, 1000); got != 250000*2500+1000*15000 {
		t.Fatalf("above tier: %d", got)
	}
	if got := gemini.Estimate(100, 0); got != 100*1250 {
		t.Fatalf("no output: %d", got)
	}
}

func TestEstimateNeverBelowActualForSameTokens(t *testing.T) {
	p := &Price{Tiers: []Tier{{Input: 3000, CachedInput: 300, CacheWrite5m: 3750, Output: 15000, Reasoning: 15000}}}
	for _, u := range []canonical.Usage{
		{PromptTokens: 5000, CompletionTokens: 1000},
		{PromptTokens: 5000, CachedTokens: 4000, CompletionTokens: 1000},
		{PromptTokens: 5000, CompletionTokens: 1000, ReasoningTokens: 900},
	} {
		if est := p.Estimate(u.PromptTokens, 1000); est < p.CostFor(u) {
			t.Errorf("estimate %d below actual %d for %+v", est, p.CostFor(u), u)
		}
	}
}

func TestUSDFormatting(t *testing.T) {
	cases := map[int64]string{0: "0.000000000", 1: "0.000000001", 999_999_999: "0.999999999", NanoPerUSD: "1.000000000", 2_500_000_000: "2.500000000", -7: "-0.000000007"}
	for in, want := range cases {
		if got := USD(in); got != want {
			t.Errorf("USD(%d)=%s want %s", in, got, want)
		}
	}
}

func TestParseTiers(t *testing.T) {
	if _, err := ParseTiers([]byte(`not json`)); err == nil {
		t.Fatal("expected error for malformed tiers")
	}
	tiers, err := ParseTiers([]byte(`[{"up_to_prompt_tokens":0,"input":2},{"up_to_prompt_tokens":100,"input":1},{"up_to_prompt_tokens":50,"input":3}]`))
	if err != nil {
		t.Fatal(err)
	}
	got := []int64{tiers[0].UpToPromptTokens, tiers[1].UpToPromptTokens, tiers[2].UpToPromptTokens}
	if got[0] != 50 || got[1] != 100 || got[2] != 0 {
		t.Fatalf("bounded tiers ascending then unbounded last: %v", got)
	}
}

// PLAN §5.3: unknown tokenizer falls back to len/4 rather than failing.
func TestTokenizerFallback(t *testing.T) {
	tk := Default()
	if got := tk.CountText("no_such_encoding", "abcdefgh"); got != 2 {
		t.Fatalf("fallback count %d want 2", got)
	}
	if got := tk.CountText("no_such_encoding", ""); got != 0 {
		t.Fatalf("empty fallback %d", got)
	}
}

func TestCountRequestComponents(t *testing.T) {
	tk := Default()
	base := &canonical.Request{Messages: []canonical.Message{{Role: "user", Content: "hi"}}}
	n := tk.CountRequest("o200k_base", 1.0, base)
	if n < 5 { // 4 framing + at least one token
		t.Fatalf("framing overhead missing: %d", n)
	}
	withImage := &canonical.Request{Messages: []canonical.Message{{Role: "user", Parts: []canonical.Part{{Type: "text", Text: "hi"}, {Type: "image_url", ImageURL: "https://x/y.png"}}}}}
	if got := tk.CountRequest("o200k_base", 1.0, withImage); got < n+1000 {
		t.Fatalf("image budget not added: %d vs %d", got, n)
	}
	withTools := &canonical.Request{Messages: base.Messages, Tools: []canonical.Tool{{Name: "get_weather", Description: "weather", Parameters: []byte(`{"type":"object","properties":{"city":{"type":"string"}}}`)}}}
	if got := tk.CountRequest("o200k_base", 1.0, withTools); got <= n {
		t.Fatalf("tool definitions not counted: %d vs %d", got, n)
	}
	if got := tk.CountRequest("o200k_base", 0, base); got != n {
		t.Fatalf("multiplier<=0 must mean 1.0: %d vs %d", got, n)
	}
	if got := tk.CountRequest("o200k_base", 1.15, base); got < n {
		t.Fatalf("calibration multiplier must scale up: %d vs %d", got, n)
	}
}

// PLAN §5.4 speed budget. Loose upper bounds so CI noise cannot flake them;
// the benchmarks below give the real numbers.
func TestSpeedBudget(t *testing.T) {
	tk := Default()
	if err := tk.Warm("o200k_base"); err != nil {
		t.Fatal(err)
	}
	prompt := strings.Repeat("The quick brown fox jumps over the lazy dog. ", 450) // ~4k tokens
	req := &canonical.Request{Messages: []canonical.Message{{Role: "user", Content: prompt}}}
	start := time.Now()
	n := tk.CountRequest("o200k_base", 1.0, req)
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Fatalf("tokenizing %d tokens took %s (budget 300us, hard ceiling 50ms)", n, d)
	}
	p := &Price{Tiers: []Tier{{UpToPromptTokens: 200000, Input: 1250, Output: 10000}, {Input: 2500, Output: 15000}}}
	u := canonical.Usage{PromptTokens: n, CompletionTokens: 500}
	start = time.Now()
	for i := 0; i < 100000; i++ {
		_ = p.CostFor(u)
	}
	if per := time.Since(start) / 100000; per > 2*time.Microsecond {
		t.Fatalf("CostFor %s per call (budget 1us)", per)
	}
}

func BenchmarkCostFor(b *testing.B) {
	p := &Price{Tiers: []Tier{{UpToPromptTokens: 200000, Input: 1250, Output: 10000}, {Input: 2500, Output: 15000}}}
	u := canonical.Usage{PromptTokens: 4000, CachedTokens: 1000, CompletionTokens: 500, ReasoningTokens: 100}
	for i := 0; i < b.N; i++ {
		_ = p.CostFor(u)
	}
}

func BenchmarkTokenize4k(b *testing.B) {
	tk := Default()
	_ = tk.Warm("o200k_base")
	req := &canonical.Request{Messages: []canonical.Message{{Role: "user", Content: strings.Repeat("The quick brown fox jumps over the lazy dog. ", 450)}}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = tk.CountRequest("o200k_base", 1.0, req)
	}
}
