package adapters

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rzp/litellm-lite/internal/canonical"
)

func TestGeminiBodyTranslation(t *testing.T) {
	a := &GeminiGenerate{}
	req := &canonical.Request{Messages: []canonical.Message{
		{Role: "system", Content: "sys"},
		{Role: "user", Parts: []canonical.Part{{Type: "text", Text: "img?"}, {Type: "image_url", ImageURL: "data:image/png;base64,QUJD"}}},
		{Role: "assistant", ToolCalls: []canonical.ToolCall{{ID: "c1", Name: "f", Arguments: `{"k":1}`}}},
		{Role: "tool", ToolCallID: "c1", Content: `{"ok":true}`},
	}, MaxTokens: 33, Stop: []string{"X"}, ResponseFormat: []byte(`{"type":"json_object"}`), Tools: []canonical.Tool{{Name: "f", Description: "d", Parameters: []byte(`{"type":"object"}`)}}}
	var b map[string]any
	json.Unmarshal(a.body(Call{Req: req, Effort: "low"}), &b)
	if b["systemInstruction"].(map[string]any)["parts"].([]any)[0].(map[string]any)["text"] != "sys" {
		t.Fatalf("systemInstruction: %v", b["systemInstruction"])
	}
	contents := b["contents"].([]any)
	if len(contents) != 3 || contents[1].(map[string]any)["role"] != "model" {
		t.Fatalf("contents: %v", contents)
	}
	userParts := contents[0].(map[string]any)["parts"].([]any)
	if userParts[1].(map[string]any)["inlineData"].(map[string]any)["mimeType"] != "image/png" {
		t.Fatalf("inlineData: %v", userParts)
	}
	fc := contents[1].(map[string]any)["parts"].([]any)[0].(map[string]any)["functionCall"].(map[string]any)
	if fc["name"] != "f" || fc["args"].(map[string]any)["k"] != float64(1) {
		t.Fatalf("functionCall: %v", fc)
	}
	fr := contents[2].(map[string]any)["parts"].([]any)[0].(map[string]any)["functionResponse"].(map[string]any)
	if fr["name"] != "f" || fr["response"].(map[string]any)["ok"] != true {
		t.Fatalf("functionResponse must carry the tool name resolved from the call id: %v", fr)
	}
	gen := b["generationConfig"].(map[string]any)
	if gen["maxOutputTokens"] != float64(33) || gen["stopSequences"].([]any)[0] != "X" || gen["responseMimeType"] != "application/json" || gen["thinkingConfig"].(map[string]any)["thinkingBudget"] != float64(1024) {
		t.Fatalf("generationConfig: %v", gen)
	}
	if b["tools"].([]any)[0].(map[string]any)["functionDeclarations"].([]any)[0].(map[string]any)["name"] != "f" {
		t.Fatalf("tools: %v", b["tools"])
	}
}

func TestGeminiNonStreaming(t *testing.T) {
	cap := &capture{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cap.record(r)
		io.WriteString(w, `{"candidates":[{"content":{"parts":[{"text":"Hi"},{"functionCall":{"name":"get","args":{"x":1}}}]},"finishReason":"STOP"}],
			"usageMetadata":{"promptTokenCount":40,"candidatesTokenCount":10,"cachedContentTokenCount":8,"thoughtsTokenCount":6}}`)
	}))
	defer srv.Close()
	resp, err := (&GeminiGenerate{}).Complete(context.Background(), Call{Req: &canonical.Request{Messages: []canonical.Message{{Role: "user", Content: "hi"}}}, BaseURL: srv.URL, APIKey: "gk", ProviderModel: "gemini-2.5-pro"}, noEmit)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(cap.path, "/v1beta/models/gemini-2.5-pro:generateContent") || cap.hdr.Get("x-goog-api-key") != "gk" {
		t.Fatalf("request: %s %v", cap.path, cap.hdr)
	}
	if resp.Content != "Hi" || len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Name != "get" || resp.ToolCalls[0].Arguments != `{"x":1}` || resp.FinishReason != "tool_calls" {
		t.Fatalf("resp: %+v", resp)
	}
	// PLAN §5.1: thoughts are reasoning tokens; completion includes them.
	u := resp.Usage
	if u.Source != "provider" || u.PromptTokens != 40 || u.CachedTokens != 8 || u.CompletionTokens != 16 || u.ReasoningTokens != 6 {
		t.Fatalf("usage: %+v", u)
	}
}

func TestGeminiFinishReasons(t *testing.T) {
	for in, want := range map[string]string{"STOP": "stop", "MAX_TOKENS": "length", "SAFETY": "content_filter", "RECITATION": "content_filter", "OTHER": "stop", "": ""} {
		if got := mapGemFinish(in); got != want {
			t.Errorf("%s -> %s want %s", in, got, want)
		}
	}
}

func TestGeminiStreaming(t *testing.T) {
	cap := &capture{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cap.record(r)
		for _, line := range []string{
			`{"candidates":[{"content":{"parts":[{"text":"Hel"}]}}]}`,
			`{"candidates":[{"content":{"parts":[{"text":"lo"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":2}}`,
		} {
			io.WriteString(w, "data: "+line+"\n\n")
		}
	}))
	defer srv.Close()
	var events []canonical.Event
	resp, err := (&GeminiGenerate{}).Complete(context.Background(), Call{Req: &canonical.Request{Messages: []canonical.Message{{Role: "user", Content: "hi"}}, Stream: true}, BaseURL: srv.URL, ProviderModel: "g"},
		func(ev canonical.Event) error { events = append(events, ev); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(cap.path, ":streamGenerateContent?alt=sse") {
		t.Fatalf("stream path: %s", cap.path)
	}
	if resp.Content != "Hello" || resp.FinishReason != "stop" || resp.Usage.PromptTokens != 3 || resp.Usage.Source != "provider" {
		t.Fatalf("resp: %+v", resp)
	}
	n := len(events)
	if n != 4 || events[0].Type != canonical.EventTextDelta || events[n-2].Type != canonical.EventFinish || events[n-1].Type != canonical.EventUsage {
		t.Fatalf("events: %+v", events)
	}
}

func TestGeminiEmbedUnsupported(t *testing.T) {
	if _, err := (&GeminiGenerate{}).Embed(context.Background(), EmbedCall{}); err == nil || err.(*Error).Class != ClassUnsupported {
		t.Fatalf("%v", err)
	}
}
