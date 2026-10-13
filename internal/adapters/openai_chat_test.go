package adapters

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/rzp/litellm-lite/internal/canonical"
)

type capture struct {
	mu   sync.Mutex
	path string
	hdr  http.Header
	body map[string]any
}

func (c *capture) record(r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.path = r.URL.String()
	c.hdr = r.Header.Clone()
	c.body = map[string]any{}
	_ = json.Unmarshal(b, &c.body)
}

func TestOpenAIChatNonStreaming(t *testing.T) {
	cap := &capture{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cap.record(r)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"chatcmpl-1","choices":[{"message":{"content":"Hi there","tool_calls":[{"id":"call_a","function":{"name":"get","arguments":"{\"x\":1}"}}]},"finish_reason":"tool_calls"}],
			"usage":{"prompt_tokens":120,"completion_tokens":30,"prompt_tokens_details":{"cached_tokens":100},"completion_tokens_details":{"reasoning_tokens":10}}}`)
	}))
	defer srv.Close()
	temp := 0.5
	req := &canonical.Request{Messages: []canonical.Message{{Role: "user", Content: "hi"}}, Temperature: &temp, Stop: []string{"END"},
		Tools: []canonical.Tool{{Name: "get", Parameters: []byte(`{"type":"object"}`)}}, ToolChoice: []byte(`"auto"`), ResponseFormat: []byte(`{"type":"json_object"}`)}
	a := &OpenAIChat{}
	resp, err := a.Complete(context.Background(), Call{Req: req, ProviderModel: "gpt-4o-2024", BaseURL: srv.URL + "/v1", APIKey: "sk-test", Headers: map[string]string{"OpenAI-Organization": "org"}, MaxOutputTokens: 777}, noEmit)
	if err != nil {
		t.Fatal(err)
	}
	if cap.path != "/v1/chat/completions" || cap.hdr.Get("Authorization") != "Bearer sk-test" || cap.hdr.Get("OpenAI-Organization") != "org" {
		t.Fatalf("request: %s %v", cap.path, cap.hdr)
	}
	b := cap.body
	if b["model"] != "gpt-4o-2024" || b["temperature"] != 0.5 || b["max_tokens"] != float64(777) || b["tool_choice"] != "auto" {
		t.Fatalf("body: %v", b)
	}
	if _, has := b["stream"]; has {
		t.Fatal("non-streaming request must not set stream")
	}
	if resp.ID != "chatcmpl-1" || resp.Content != "Hi there" || resp.FinishReason != "tool_calls" || len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Arguments != `{"x":1}` {
		t.Fatalf("resp: %+v", resp)
	}
	// PLAN §5.2: provider-reported usage, normalized.
	u := resp.Usage
	if u.Source != "provider" || u.PromptTokens != 120 || u.CachedTokens != 100 || u.CompletionTokens != 30 || u.ReasoningTokens != 10 {
		t.Fatalf("usage: %+v", u)
	}
}

func TestOpenAIChatStreamingInjectsUsageOption(t *testing.T) {
	cap := &capture{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cap.record(r)
		w.Header().Set("Content-Type", "text/event-stream")
		for _, line := range []string{
			`{"id":"c1","choices":[{"delta":{"role":"assistant","content":"Hel"}}]}`,
			`{"id":"c1","choices":[{"delta":{"content":"lo"}}]}`,
			`{"id":"c1","choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"f","arguments":"{\"a\""}}]}}]}`,
			`{"id":"c1","choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":":1}"}}]}}]}`,
			`{"id":"c1","choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
			`{"id":"c1","choices":[],"usage":{"prompt_tokens":9,"completion_tokens":4}}`,
			`[DONE]`,
		} {
			io.WriteString(w, "data: "+line+"\n\n")
		}
	}))
	defer srv.Close()
	var events []canonical.Event
	a := &OpenAIChat{}
	resp, err := a.Complete(context.Background(), Call{Req: &canonical.Request{Messages: []canonical.Message{{Role: "user", Content: "hi"}}, Stream: true}, BaseURL: srv.URL, APIKey: "k"},
		func(ev canonical.Event) error { events = append(events, ev); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if cap.body["stream"] != true || cap.body["stream_options"].(map[string]any)["include_usage"] != true {
		t.Fatalf("stream_options.include_usage must be injected: %v", cap.body)
	}
	if resp.Content != "Hello" || resp.FinishReason != "tool_calls" || len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Arguments != `{"a":1}` {
		t.Fatalf("resp: %+v", resp)
	}
	if resp.Usage.Source != "provider" || resp.Usage.PromptTokens != 9 {
		t.Fatalf("usage from final chunk: %+v", resp.Usage)
	}
	got := []canonical.EventType{}
	for _, e := range events {
		got = append(got, e.Type)
	}
	want := []canonical.EventType{canonical.EventTextDelta, canonical.EventTextDelta, canonical.EventToolCallDelta, canonical.EventToolCallDelta, canonical.EventFinish, canonical.EventUsage}
	if strings.Join(toStrings(got), ",") != strings.Join(toStrings(want), ",") {
		t.Fatalf("events %v want %v", got, want)
	}
}

func toStrings(ts []canonical.EventType) []string {
	out := make([]string, len(ts))
	for i, t := range ts {
		out[i] = string(t)
	}
	return out
}

func TestOpenAIChatStreamWithoutUsageIsMarkedForEstimation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\ndata: [DONE]\n\n")
	}))
	defer srv.Close()
	usageEvents := 0
	resp, err := (&OpenAIChat{}).Complete(context.Background(), Call{Req: &canonical.Request{Messages: []canonical.Message{{Role: "user", Content: "hi"}}, Stream: true}, BaseURL: srv.URL},
		func(ev canonical.Event) error {
			if ev.Type == canonical.EventUsage {
				usageEvents++
			}
			return nil
		})
	if err != nil || resp.Content != "x" {
		t.Fatal(err)
	}
	if resp.Usage.Source == "provider" || usageEvents != 0 {
		t.Fatal("a host that sends no usage must leave Source empty so the gateway estimates (PLAN §5.2)")
	}
}

func TestOpenAIChatUpstreamErrors(t *testing.T) {
	cases := []struct {
		status int
		body   string
		class  string
	}{{500, "boom", Class5xx}, {429, "slow", Class429}, {401, "bad", ClassAuth}, {400, `{"error":{"code":"context_length_exceeded"}}`, ClassContextLength}, {400, `{"error":{"message":"bad param"}}`, ClassClient}}
	for _, c := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(c.status); io.WriteString(w, c.body) }))
		_, err := (&OpenAIChat{}).Complete(context.Background(), Call{Req: &canonical.Request{Messages: []canonical.Message{{Role: "user", Content: "hi"}}}, BaseURL: srv.URL}, noEmit)
		srv.Close()
		ae, ok := err.(*Error)
		if !ok || ae.Class != c.class {
			t.Errorf("status %d: %v", c.status, err)
		}
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "not json") }))
	defer srv.Close()
	if _, err := (&OpenAIChat{}).Complete(context.Background(), Call{Req: &canonical.Request{Messages: []canonical.Message{{Role: "user", Content: "hi"}}}, BaseURL: srv.URL}, noEmit); err == nil {
		t.Fatal("malformed upstream JSON must error")
	}
	if _, err := (&OpenAIChat{}).Complete(context.Background(), Call{Req: &canonical.Request{Messages: []canonical.Message{{Role: "user", Content: "hi"}}}, BaseURL: "http://127.0.0.1:1"}, noEmit); err == nil || err.(*Error).Class != ClassConnection {
		t.Fatalf("unreachable upstream: %v", err)
	}
}

func TestOpenAIChatBodyTranslation(t *testing.T) {
	a := &OpenAIChat{}
	req := &canonical.Request{Messages: []canonical.Message{
		{Role: "user", Parts: []canonical.Part{{Type: "text", Text: "see"}, {Type: "image_url", ImageURL: "https://x/y.png"}}},
		{Role: "assistant", ToolCalls: []canonical.ToolCall{{ID: "c", Name: "f", Arguments: "{}"}}},
		{Role: "tool", ToolCallID: "c", Content: "ok"},
	}, MaxTokens: 5}
	var b map[string]any
	json.Unmarshal(a.body(Call{Req: req, ProviderModel: "m", BaseURL: "https://api.openai.com/v1", Effort: "high"}, false), &b)
	msgs := b["messages"].([]any)
	parts := msgs[0].(map[string]any)["content"].([]any)
	if len(parts) != 2 || parts[1].(map[string]any)["image_url"].(map[string]any)["url"] != "https://x/y.png" {
		t.Fatalf("multimodal: %v", parts)
	}
	if _, has := msgs[1].(map[string]any)["content"]; has {
		t.Fatal("assistant tool-call message must omit content")
	}
	if msgs[2].(map[string]any)["tool_call_id"] != "c" {
		t.Fatalf("tool result: %v", msgs[2])
	}
	// api.openai.com uses max_completion_tokens and accepts reasoning_effort.
	if b["max_completion_tokens"] != float64(5) || b["reasoning_effort"] != "high" {
		t.Fatalf("openai-specific fields: %v", b)
	}
	if _, has := b["max_tokens"]; has {
		t.Fatal("max_tokens must not be sent to api.openai.com")
	}
	b = map[string]any{}
	json.Unmarshal(a.body(Call{Req: req, ProviderModel: "kimi-k2", BaseURL: "https://api.moonshot.ai/v1", Effort: "high"}, false), &b)
	if b["max_tokens"] != float64(5) {
		t.Fatalf("openai-compatible hosts use max_tokens: %v", b)
	}
	if _, has := b["reasoning_effort"]; has {
		t.Fatal("reasoning_effort must not be sent to non-OpenAI hosts")
	}
}

func TestOpenAIEmbed(t *testing.T) {
	cap := &capture{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cap.record(r)
		io.WriteString(w, `{"data":[{"embedding":[0.1,0.2]},{"embedding":[0.3]}],"usage":{"prompt_tokens":5}}`)
	}))
	defer srv.Close()
	r, err := (&OpenAIChat{}).Embed(context.Background(), EmbedCall{Req: &canonical.EmbeddingRequest{Input: []string{"a", "b"}}, ProviderModel: "te3", BaseURL: srv.URL, APIKey: "k"})
	if err != nil || cap.path != "/embeddings" || cap.body["model"] != "te3" || len(r.Vectors) != 2 || r.Usage.PromptTokens != 5 || r.Usage.Source != "provider" {
		t.Fatalf("%v %+v %v", err, r, cap.body)
	}
}
