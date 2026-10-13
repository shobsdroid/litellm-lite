package adapters

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rzp/litellm-lite/internal/canonical"
)

func TestAnthropicBodyTranslation(t *testing.T) {
	a := &AnthropicMessages{}
	temp := 0.3
	req := &canonical.Request{Messages: []canonical.Message{
		{Role: "system", Content: "You are terse."},
		{Role: "user", Content: "first"},
		{Role: "user", Parts: []canonical.Part{{Type: "text", Text: "look"}, {Type: "image_url", ImageURL: "data:image/jpeg;base64,/9j/"}}},
		{Role: "assistant", Content: "calling", ToolCalls: []canonical.ToolCall{{ID: "tu_1", Name: "f", Arguments: `{"q":"x"}`}}},
		{Role: "tool", ToolCallID: "tu_1", Content: "result"},
	}, Temperature: &temp, Stop: []string{"END"}, Tools: []canonical.Tool{{Name: "f", Description: "d"}}}
	var b map[string]any
	json.Unmarshal(a.body(Call{Req: req, ProviderModel: "claude-sonnet-4-5", MaxOutputTokens: 2048}), &b)
	if b["system"] != "You are terse." || b["max_tokens"] != float64(2048) || b["temperature"] != 0.3 || b["stream"] != false {
		t.Fatalf("top level: %v", b)
	}
	if b["stop_sequences"].([]any)[0] != "END" {
		t.Fatalf("stop_sequences: %v", b["stop_sequences"])
	}
	msgs := b["messages"].([]any)
	// Two consecutive user messages are merged so roles alternate.
	if len(msgs) != 3 || msgs[0].(map[string]any)["role"] != "user" || msgs[1].(map[string]any)["role"] != "assistant" || msgs[2].(map[string]any)["role"] != "user" {
		t.Fatalf("message alternation: %v", msgs)
	}
	first := msgs[0].(map[string]any)["content"].([]any)
	if len(first) != 3 || first[2].(map[string]any)["type"] != "image" || first[2].(map[string]any)["source"].(map[string]any)["media_type"] != "image/jpeg" {
		t.Fatalf("merged user content: %v", first)
	}
	asst := msgs[1].(map[string]any)["content"].([]any)
	if asst[1].(map[string]any)["type"] != "tool_use" || asst[1].(map[string]any)["input"].(map[string]any)["q"] != "x" {
		t.Fatalf("tool_use block: %v", asst)
	}
	toolRes := msgs[2].(map[string]any)["content"].([]any)[0].(map[string]any)
	if toolRes["type"] != "tool_result" || toolRes["tool_use_id"] != "tu_1" {
		t.Fatalf("tool_result block: %v", toolRes)
	}
	tools := b["tools"].([]any)[0].(map[string]any)
	if tools["name"] != "f" || tools["input_schema"] == nil {
		t.Fatalf("tools need an input_schema even when none was given: %v", tools)
	}
	if _, has := b["thinking"]; has {
		t.Fatal("thinking must be off without an effort")
	}
	// Effort enables extended thinking and drops temperature (API constraint).
	b = map[string]any{}
	json.Unmarshal(a.body(Call{Req: req, MaxOutputTokens: 16000, Effort: "medium"}), &b)
	if b["thinking"].(map[string]any)["budget_tokens"] != float64(4096) {
		t.Fatalf("thinking budget: %v", b["thinking"])
	}
	if _, has := b["temperature"]; has {
		t.Fatal("temperature must be removed when thinking is enabled")
	}
	b = map[string]any{}
	json.Unmarshal(a.body(Call{Req: &canonical.Request{Messages: []canonical.Message{{Role: "user", Content: "x"}}}}), &b)
	if b["max_tokens"] != float64(4096) {
		t.Fatalf("max_tokens default: %v", b["max_tokens"])
	}
}

func TestAnthropicNonStreaming(t *testing.T) {
	cap := &capture{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cap.record(r)
		io.WriteString(w, `{"id":"msg_1","content":[{"type":"text","text":"Sure. "},{"type":"tool_use","id":"tu_9","name":"get","input":{"city":"Pune"}}],"stop_reason":"tool_use",
			"usage":{"input_tokens":1000,"output_tokens":50,"cache_creation_input_tokens":300,"cache_read_input_tokens":600}}`)
	}))
	defer srv.Close()
	resp, err := (&AnthropicMessages{}).Complete(context.Background(), Call{Req: &canonical.Request{Messages: []canonical.Message{{Role: "user", Content: "hi"}}}, BaseURL: srv.URL, APIKey: "ak", ProviderModel: "claude"}, noEmit)
	if err != nil {
		t.Fatal(err)
	}
	if cap.path != "/v1/messages" || cap.hdr.Get("x-api-key") != "ak" || cap.hdr.Get("anthropic-version") == "" {
		t.Fatalf("request: %s %v", cap.path, cap.hdr)
	}
	if resp.ID != "msg_1" || resp.Content != "Sure. " || resp.FinishReason != "tool_calls" || len(resp.ToolCalls) != 1 || resp.ToolCalls[0].ID != "tu_9" || resp.ToolCalls[0].Arguments != `{"city":"Pune"}` {
		t.Fatalf("resp: %+v", resp)
	}
	// PLAN §5.2 normalization: PromptTokens includes cache write and read.
	u := resp.Usage
	if u.Source != "provider" || u.PromptTokens != 1900 || u.CachedTokens != 600 || u.CacheWriteTokens != 300 || u.CompletionTokens != 50 {
		t.Fatalf("usage: %+v", u)
	}
}

func TestAnthropicStopReasons(t *testing.T) {
	for in, want := range map[string]string{"end_turn": "stop", "stop_sequence": "stop", "max_tokens": "length", "tool_use": "tool_calls", "refusal": "refusal"} {
		if got := mapStop(in); got != want {
			t.Errorf("%s -> %s want %s", in, got, want)
		}
	}
}

func TestAnthropicStreaming(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		frames := []string{
			`event: message_start` + "\n" + `data: {"type":"message_start","message":{"id":"msg_s","usage":{"input_tokens":25,"cache_read_input_tokens":5}}}`,
			`event: content_block_start` + "\n" + `data: {"type":"content_block_start","index":0,"content_block":{"type":"text"}}`,
			`event: content_block_delta` + "\n" + `data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hel"}}`,
			`event: content_block_delta` + "\n" + `data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"lo"}}`,
			`event: content_block_start` + "\n" + `data: {"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"tu_1","name":"get"}}`,
			`event: content_block_delta` + "\n" + `data: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"a\":"}}`,
			`event: content_block_delta` + "\n" + `data: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"1}"}}`,
			`event: message_delta` + "\n" + `data: {"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":12}}`,
			`event: message_stop` + "\n" + `data: {"type":"message_stop"}`,
		}
		for _, f := range frames {
			io.WriteString(w, f+"\n\n")
		}
	}))
	defer srv.Close()
	var events []canonical.Event
	resp, err := (&AnthropicMessages{}).Complete(context.Background(), Call{Req: &canonical.Request{Messages: []canonical.Message{{Role: "user", Content: "hi"}}, Stream: true}, BaseURL: srv.URL},
		func(ev canonical.Event) error { events = append(events, ev); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if resp.ID != "msg_s" || resp.Content != "Hello" || resp.FinishReason != "tool_calls" || len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Arguments != `{"a":1}` {
		t.Fatalf("resp: %+v", resp)
	}
	// Usage assembled from message_start (input) and message_delta (output).
	if resp.Usage.PromptTokens != 30 || resp.Usage.CachedTokens != 5 || resp.Usage.CompletionTokens != 12 || resp.Usage.Source != "provider" {
		t.Fatalf("usage: %+v", resp.Usage)
	}
	last := events[len(events)-1]
	if last.Type != canonical.EventUsage || last.Usage.CompletionTokens != 12 {
		t.Fatalf("final event must be usage: %+v", last)
	}
	if events[0].Type != canonical.EventTextDelta || events[0].Text != "Hel" {
		t.Fatalf("first event: %+v", events[0])
	}
}

func TestAnthropicStreamErrorEvent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n")
	}))
	defer srv.Close()
	_, err := (&AnthropicMessages{}).Complete(context.Background(), Call{Req: &canonical.Request{Messages: []canonical.Message{{Role: "user", Content: "hi"}}, Stream: true}, BaseURL: srv.URL}, noEmit)
	ae, ok := err.(*Error)
	if !ok || ae.Class != Class5xx || ae.Code != "overloaded_error" {
		t.Fatalf("stream error must be classified retriable: %v", err)
	}
}

func TestAnthropicEmbedUnsupported(t *testing.T) {
	_, err := (&AnthropicMessages{}).Embed(context.Background(), EmbedCall{})
	if ae, ok := err.(*Error); !ok || ae.Class != ClassUnsupported {
		t.Fatalf("%v", err)
	}
}
