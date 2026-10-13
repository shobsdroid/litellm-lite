package canonical

import "testing"

// PLAN §2.1 / §5.2: the canonical model is what every ingress decoder produces
// and every adapter consumes. These tests pin the small helpers the hot path
// relies on.

func TestMessageText(t *testing.T) {
	plain := Message{Role: "user", Content: "hello"}
	if plain.Text() != "hello" {
		t.Fatalf("plain content: %q", plain.Text())
	}
	multi := Message{Role: "user", Parts: []Part{
		{Type: "text", Text: "describe "},
		{Type: "image_url", ImageURL: "data:image/png;base64,AAAA"},
		{Type: "text", Text: "this"},
	}}
	if multi.Text() != "describe this" {
		t.Fatalf("multimodal text view: %q", multi.Text())
	}
	// Parts win over Content when both are set.
	both := Message{Content: "ignored", Parts: []Part{{Type: "text", Text: "parts"}}}
	if both.Text() != "parts" {
		t.Fatalf("parts should take precedence: %q", both.Text())
	}
}

func TestUsageTotal(t *testing.T) {
	u := Usage{PromptTokens: 1000, CachedTokens: 800, CacheWriteTokens: 100, CompletionTokens: 50, ReasoningTokens: 20}
	// Cached and cache-write are subsets of prompt; reasoning is a subset of
	// completion. Total must not double count them.
	if u.Total() != 1050 {
		t.Fatalf("total %d want 1050", u.Total())
	}
}

func TestEventIsContent(t *testing.T) {
	// PLAN §7 step 7: fallback is only possible before the first *content*
	// event is forwarded, so the classification must be exact.
	cases := map[EventType]bool{EventTextDelta: true, EventToolCallDelta: true, EventFinish: false, EventUsage: false}
	for typ, want := range cases {
		if got := (Event{Type: typ}).IsContent(); got != want {
			t.Errorf("%s: IsContent=%v want %v", typ, got, want)
		}
	}
}
