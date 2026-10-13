package formats

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/rzp/litellm-lite/internal/canonical"
)

// ---- ingress (PLAN §6.1: drop-in OpenAI Chat Completions)

func TestDecodeChatBasics(t *testing.T) {
	body := []byte(`{"model":"gpt-4o","messages":[{"role":"developer","content":"be terse"},{"role":"user","content":"hi"}],
		"max_tokens":50,"temperature":0.2,"top_p":0.9,"stop":"END","stream":true,"reasoning_effort":"high","user":"u1","n":1,
		"response_format":{"type":"json_object"},"tool_choice":"auto"}`)
	req, err := DecodeOpenAIChat(body)
	if err != nil {
		t.Fatal(err)
	}
	if req.Model != "gpt-4o" || len(req.Messages) != 2 || req.MaxTokens != 50 || *req.Temperature != 0.2 || *req.TopP != 0.9 || !req.Stream || req.Effort != "high" || req.User != "u1" {
		t.Fatalf("decoded: %+v", req)
	}
	if req.Messages[0].Role != "system" {
		t.Fatalf("developer role must map to system: %s", req.Messages[0].Role)
	}
	if len(req.Stop) != 1 || req.Stop[0] != "END" {
		t.Fatalf("stop string: %v", req.Stop)
	}
	if string(req.ResponseFormat) != `{"type":"json_object"}` || string(req.ToolChoice) != `"auto"` {
		t.Fatalf("raw passthrough fields: %s %s", req.ResponseFormat, req.ToolChoice)
	}
}

func TestDecodeChatMaxCompletionTokensWins(t *testing.T) {
	req, err := DecodeOpenAIChat([]byte(`{"messages":[{"role":"user","content":"x"}],"max_tokens":10,"max_completion_tokens":99}`))
	if err != nil || req.MaxTokens != 99 {
		t.Fatalf("%v %+v", err, req)
	}
	req, _ = DecodeOpenAIChat([]byte(`{"messages":[{"role":"user","content":"x"}],"stop":["a","b"]}`))
	if len(req.Stop) != 2 {
		t.Fatalf("stop array: %v", req.Stop)
	}
}

func TestDecodeChatMultimodalAndTools(t *testing.T) {
	body := []byte(`{"messages":[
		{"role":"user","content":[{"type":"text","text":"what is this"},{"type":"image_url","image_url":{"url":"data:image/png;base64,AAAA"}}]},
		{"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{\"q\":1}"}}]},
		{"role":"tool","tool_call_id":"call_1","content":"42"}],
		"tools":[{"type":"function","function":{"name":"lookup","description":"d","parameters":{"type":"object"}}},{"type":"web_search"}]}`)
	req, err := DecodeOpenAIChat(body)
	if err != nil {
		t.Fatal(err)
	}
	u := req.Messages[0]
	if len(u.Parts) != 2 || u.Parts[0].Text != "what is this" || u.Parts[1].ImageURL != "data:image/png;base64,AAAA" {
		t.Fatalf("parts: %+v", u.Parts)
	}
	a := req.Messages[1]
	if len(a.ToolCalls) != 1 || a.ToolCalls[0].ID != "call_1" || a.ToolCalls[0].Name != "lookup" || a.ToolCalls[0].Arguments != `{"q":1}` {
		t.Fatalf("tool calls: %+v", a.ToolCalls)
	}
	if req.Messages[2].Role != "tool" || req.Messages[2].ToolCallID != "call_1" || req.Messages[2].Content != "42" {
		t.Fatalf("tool result: %+v", req.Messages[2])
	}
	if len(req.Tools) != 1 || req.Tools[0].Name != "lookup" {
		t.Fatalf("only function tools are kept: %+v", req.Tools)
	}
}

func TestDecodeChatRejectsMalformed(t *testing.T) {
	// PLAN §7 step 1: reject malformed requests before any I/O.
	for name, body := range map[string]string{
		"not json":        `{`,
		"no messages":     `{"model":"x"}`,
		"empty messages":  `{"messages":[]}`,
		"numeric content": `{"messages":[{"role":"user","content":5}]}`,
	} {
		if _, err := DecodeOpenAIChat([]byte(body)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

// ---- egress

func TestEncodeChatShape(t *testing.T) {
	resp := &canonical.Response{Content: "hello", FinishReason: "stop", Usage: canonical.Usage{PromptTokens: 10, CachedTokens: 4, CompletionTokens: 5, ReasoningTokens: 2}}
	var out map[string]any
	if err := json.Unmarshal(EncodeOpenAIChat("req_1", "gpt-4o-openai-128k-medium", resp), &out); err != nil {
		t.Fatal(err)
	}
	if out["id"] != "req_1" || out["object"] != "chat.completion" || out["model"] != "gpt-4o-openai-128k-medium" {
		t.Fatalf("envelope: %v", out)
	}
	choice := out["choices"].([]any)[0].(map[string]any)
	msg := choice["message"].(map[string]any)
	if msg["role"] != "assistant" || msg["content"] != "hello" || choice["finish_reason"] != "stop" {
		t.Fatalf("choice: %v", choice)
	}
	usage := out["usage"].(map[string]any)
	if usage["prompt_tokens"].(float64) != 10 || usage["completion_tokens"].(float64) != 5 || usage["total_tokens"].(float64) != 15 {
		t.Fatalf("usage: %v", usage)
	}
	if usage["prompt_tokens_details"].(map[string]any)["cached_tokens"].(float64) != 4 || usage["completion_tokens_details"].(map[string]any)["reasoning_tokens"].(float64) != 2 {
		t.Fatalf("usage details: %v", usage)
	}
}

func TestEncodeChatToolCalls(t *testing.T) {
	resp := &canonical.Response{FinishReason: "tool_calls", ToolCalls: []canonical.ToolCall{{ID: "c1", Name: "f", Arguments: `{"a":1}`}}}
	var out struct {
		Choices []struct {
			Message struct {
				Content   *string `json:"content"`
				ToolCalls []struct {
					ID       string `json:"id"`
					Type     string `json:"type"`
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(EncodeOpenAIChat("r", "m", resp), &out); err != nil {
		t.Fatal(err)
	}
	m := out.Choices[0].Message
	if m.Content != nil {
		t.Fatal("content must be null when only tool calls are present")
	}
	if len(m.ToolCalls) != 1 || m.ToolCalls[0].Type != "function" || m.ToolCalls[0].Function.Name != "f" || m.ToolCalls[0].Function.Arguments != `{"a":1}` {
		t.Fatalf("tool calls: %+v", m.ToolCalls)
	}
}

func parseSSE(t *testing.T, frame []byte) map[string]any {
	t.Helper()
	if !bytes.HasPrefix(frame, []byte("data: ")) || !bytes.HasSuffix(frame, []byte("\n\n")) {
		t.Fatalf("not an SSE frame: %q", frame)
	}
	var out map[string]any
	if err := json.Unmarshal(bytes.TrimSuffix(bytes.TrimPrefix(frame, []byte("data: ")), []byte("\n\n")), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestStreamEncoder(t *testing.T) {
	enc := NewOpenAIStreamEncoder("req_s", "dep")
	first := parseSSE(t, enc.Encode(canonical.Event{Type: canonical.EventTextDelta, Text: "Hel"}))
	if first["object"] != "chat.completion.chunk" || first["id"] != "req_s" || first["model"] != "dep" {
		t.Fatalf("chunk envelope: %v", first)
	}
	d := first["choices"].([]any)[0].(map[string]any)["delta"].(map[string]any)
	if d["role"] != "assistant" || d["content"] != "Hel" {
		t.Fatalf("first delta must carry the role: %v", d)
	}
	second := parseSSE(t, enc.Encode(canonical.Event{Type: canonical.EventTextDelta, Text: "lo"}))
	d = second["choices"].([]any)[0].(map[string]any)["delta"].(map[string]any)
	if _, has := d["role"]; has {
		t.Fatal("role must only be sent once")
	}
	tc := parseSSE(t, enc.Encode(canonical.Event{Type: canonical.EventToolCallDelta, ToolCall: &canonical.ToolCallDelta{Index: 0, ID: "c1", Name: "f", ArgumentsDelta: "{"}}))
	calls := tc["choices"].([]any)[0].(map[string]any)["delta"].(map[string]any)["tool_calls"].([]any)
	if c := calls[0].(map[string]any); c["id"] != "c1" || c["type"] != "function" || c["function"].(map[string]any)["name"] != "f" {
		t.Fatalf("tool call delta: %v", c)
	}
	fin := parseSSE(t, enc.Encode(canonical.Event{Type: canonical.EventFinish, FinishReason: "stop"}))
	if fin["choices"].([]any)[0].(map[string]any)["finish_reason"] != "stop" {
		t.Fatalf("finish: %v", fin)
	}
	// PLAN §5.2: the usage chunk is the OpenAI stream_options.include_usage shape: empty choices + usage.
	us := parseSSE(t, enc.Encode(canonical.Event{Type: canonical.EventUsage, Usage: &canonical.Usage{PromptTokens: 3, CompletionTokens: 2}}))
	if len(us["choices"].([]any)) != 0 || us["usage"].(map[string]any)["total_tokens"].(float64) != 5 {
		t.Fatalf("usage chunk: %v", us)
	}
	if enc.Encode(canonical.Event{Type: canonical.EventUsage}) != nil {
		t.Fatal("usage event without usage must encode to nothing")
	}
	if string(enc.Done()) != "data: [DONE]\n\n" {
		t.Fatalf("done: %q", enc.Done())
	}
}

func TestEncodeError(t *testing.T) {
	b := EncodeOpenAIError(&APIError{Status: 429, Type: "budget_exceeded", Code: "budget_exceeded", Message: "the key monthly budget is exhausted"})
	var out struct {
		Error struct {
			Message, Type, Code string
		} `json:"error"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Error.Type != "budget_exceeded" || out.Error.Code != "budget_exceeded" || !strings.Contains(out.Error.Message, "exhausted") {
		t.Fatalf("error body: %s", b)
	}
	if (&APIError{Message: "m"}).Error() != "m" {
		t.Fatal("APIError must implement error with its message")
	}
}

// ---- embeddings

func TestDecodeEmbeddings(t *testing.T) {
	req, err := DecodeOpenAIEmbeddings([]byte(`{"model":"text-embedding-3-small","input":"one"}`))
	if err != nil || len(req.Input) != 1 || req.Input[0] != "one" {
		t.Fatalf("%v %+v", err, req)
	}
	req, err = DecodeOpenAIEmbeddings([]byte(`{"input":["a","b"],"user":"u"}`))
	if err != nil || len(req.Input) != 2 || req.User != "u" {
		t.Fatalf("%v %+v", err, req)
	}
	for name, body := range map[string]string{"empty": `{"input":[]}`, "numeric": `{"input":7}`, "missing": `{}`, "bad json": `[`} {
		if _, err := DecodeOpenAIEmbeddings([]byte(body)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestEncodeEmbeddings(t *testing.T) {
	b := EncodeOpenAIEmbeddings("dep", &canonical.EmbeddingResponse{Vectors: [][]float32{{0.1, 0.2}, {0.3}}, Usage: canonical.Usage{PromptTokens: 7}})
	var out struct {
		Object string `json:"object"`
		Model  string `json:"model"`
		Data   []struct {
			Object    string    `json:"object"`
			Index     int       `json:"index"`
			Embedding []float32 `json:"embedding"`
		} `json:"data"`
		Usage struct {
			PromptTokens int64 `json:"prompt_tokens"`
			TotalTokens  int64 `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Object != "list" || out.Model != "dep" || len(out.Data) != 2 || out.Data[1].Index != 1 || out.Data[1].Object != "embedding" || out.Usage.TotalTokens != 7 {
		t.Fatalf("embeddings body: %s", b)
	}
}

func TestJoinText(t *testing.T) {
	req := &canonical.Request{Messages: []canonical.Message{{Role: "system", Content: "a"}, {Role: "user", Parts: []canonical.Part{{Type: "text", Text: "b"}}}}}
	if JoinText(req) != "a\nb\n" {
		t.Fatalf("%q", JoinText(req))
	}
}
