package adapters

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rzp/litellm-lite/internal/canonical"
)

func userReq(text string, stream bool) *canonical.Request {
	return &canonical.Request{Messages: []canonical.Message{{Role: "system", Content: "sys"}, {Role: "user", Content: text}}, Stream: stream}
}

func noEmit(canonical.Event) error { return nil }

func TestMockPlainCompletion(t *testing.T) {
	m := &Mock{}
	resp, err := m.Complete(context.Background(), Call{Req: userReq("hello", false), ProviderModel: "mock-1"}, noEmit)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(resp.Content, "mock(mock-1)") || resp.FinishReason != "stop" || resp.ID == "" {
		t.Fatalf("resp: %+v", resp)
	}
	// PLAN §2.1: realistic usage numbers so pricing is exercised end to end.
	if resp.Usage.Source != "provider" || resp.Usage.PromptTokens <= 0 || resp.Usage.CompletionTokens <= 0 {
		t.Fatalf("usage: %+v", resp.Usage)
	}
}

func TestMockHints(t *testing.T) {
	m := &Mock{}
	resp, _ := m.Complete(context.Background(), Call{Req: userReq("[output:7] go", false)}, noEmit)
	if n := len(strings.Fields(resp.Content)); n != 7 {
		t.Fatalf("output hint: %d words", n)
	}
	req := userReq("[output:50]", false)
	req.MaxTokens = 10
	resp, _ = m.Complete(context.Background(), Call{Req: req}, noEmit)
	if len(strings.Fields(resp.Content)) != 10 || resp.FinishReason != "length" {
		t.Fatalf("max_tokens cap: %d words, finish %s", len(strings.Fields(resp.Content)), resp.FinishReason)
	}
	for hint, class := range map[string]string{"[fail:500]": Class5xx, "[fail:429]": Class429, "[fail:timeout]": ClassTimeout, "[fail:context]": ClassContextLength, "[fail:400]": ClassClient} {
		_, err := m.Complete(context.Background(), Call{Req: userReq(hint, false)}, noEmit)
		ae, ok := err.(*Error)
		if !ok || ae.Class != class {
			t.Errorf("%s: %v", hint, err)
		}
	}
	start := time.Now()
	m.Complete(context.Background(), Call{Req: userReq("[latency:60]", false)}, noEmit)
	if time.Since(start) < 60*time.Millisecond {
		t.Fatal("latency hint ignored")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := m.Complete(ctx, Call{Req: userReq("[latency:500]", false)}, noEmit); err == nil {
		t.Fatal("context deadline during latency must error")
	}
}

func TestMockStreamEventOrder(t *testing.T) {
	m := &Mock{}
	var types []canonical.EventType
	var text strings.Builder
	resp, err := m.Complete(context.Background(), Call{Req: userReq("[output:12]", true)}, func(ev canonical.Event) error {
		types = append(types, ev.Type)
		text.WriteString(ev.Text)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(types) < 3 || types[len(types)-2] != canonical.EventFinish || types[len(types)-1] != canonical.EventUsage {
		t.Fatalf("events must end with finish then usage: %v", types)
	}
	for _, ty := range types[:len(types)-2] {
		if ty != canonical.EventTextDelta {
			t.Fatalf("unexpected event before finish: %v", types)
		}
	}
	if text.String() != resp.Content {
		t.Fatalf("streamed text %q != final content %q", text.String(), resp.Content)
	}
}

func TestMockEmitErrorAborts(t *testing.T) {
	m := &Mock{}
	_, err := m.Complete(context.Background(), Call{Req: userReq("x", true)}, func(canonical.Event) error { return context.Canceled })
	if err == nil {
		t.Fatal("emit error must abort the stream")
	}
}

func TestMockEmbed(t *testing.T) {
	m := &Mock{}
	r, err := m.Embed(context.Background(), EmbedCall{Req: &canonical.EmbeddingRequest{Input: []string{"alpha", "beta"}}, ProviderModel: "emb"})
	if err != nil || len(r.Vectors) != 2 || len(r.Vectors[0]) == 0 || r.Usage.PromptTokens <= 0 || r.Usage.Source != "provider" {
		t.Fatalf("%v %+v", err, r)
	}
}
