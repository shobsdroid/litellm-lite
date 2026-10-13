// Package formats decodes ingress requests into the canonical model and
// encodes canonical responses/events into the client's requested wire format.
// Only the OpenAI format is implemented in the MVP.
package formats

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/rzp/litellm-lite/internal/canonical"
)

// ---- OpenAI Chat Completions ingress

type oaiMessage struct {
	Role       string          `json:"role"`
	Content    json.RawMessage `json:"content"`
	Name       string          `json:"name,omitempty"`
	ToolCalls  []oaiToolCall   `json:"tool_calls,omitempty"`
	ToolCallID string          `json:"tool_call_id,omitempty"`
}

type oaiToolCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type oaiTool struct {
	Type     string `json:"type"`
	Function struct {
		Name        string          `json:"name"`
		Description string          `json:"description,omitempty"`
		Parameters  json.RawMessage `json:"parameters,omitempty"`
	} `json:"function"`
}

type oaiRequest struct {
	Model               string          `json:"model"`
	Messages            []oaiMessage    `json:"messages"`
	MaxTokens           int             `json:"max_tokens,omitempty"`
	MaxCompletionTokens int             `json:"max_completion_tokens,omitempty"`
	Temperature         *float64        `json:"temperature,omitempty"`
	TopP                *float64        `json:"top_p,omitempty"`
	Stop                json.RawMessage `json:"stop,omitempty"`
	Stream              bool            `json:"stream,omitempty"`
	Tools               []oaiTool       `json:"tools,omitempty"`
	ToolChoice          json.RawMessage `json:"tool_choice,omitempty"`
	ResponseFormat      json.RawMessage `json:"response_format,omitempty"`
	ReasoningEffort     string          `json:"reasoning_effort,omitempty"`
	User                string          `json:"user,omitempty"`
	N                   int             `json:"n,omitempty"`
}

func DecodeOpenAIChat(body []byte) (*canonical.Request, error) {
	var in oaiRequest
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	if len(in.Messages) == 0 {
		return nil, fmt.Errorf("messages is required")
	}
	req := &canonical.Request{
		Model: in.Model, MaxTokens: in.MaxCompletionTokens, Temperature: in.Temperature, TopP: in.TopP,
		Stream: in.Stream, ToolChoice: in.ToolChoice, ResponseFormat: in.ResponseFormat, Effort: in.ReasoningEffort, User: in.User, N: in.N,
	}
	if req.MaxTokens == 0 {
		req.MaxTokens = in.MaxTokens
	}
	if len(in.Stop) > 0 {
		var one string
		if json.Unmarshal(in.Stop, &one) == nil {
			req.Stop = []string{one}
		} else {
			_ = json.Unmarshal(in.Stop, &req.Stop)
		}
	}
	for _, m := range in.Messages {
		cm := canonical.Message{Role: m.Role, Name: m.Name, ToolCallID: m.ToolCallID}
		if cm.Role == "developer" {
			cm.Role = "system"
		}
		if len(m.Content) > 0 && string(m.Content) != "null" {
			var s string
			if json.Unmarshal(m.Content, &s) == nil {
				cm.Content = s
			} else {
				var parts []struct {
					Type     string `json:"type"`
					Text     string `json:"text"`
					ImageURL struct {
						URL string `json:"url"`
					} `json:"image_url"`
				}
				if err := json.Unmarshal(m.Content, &parts); err != nil {
					return nil, fmt.Errorf("message content must be a string or an array of parts")
				}
				for _, p := range parts {
					switch p.Type {
					case "text":
						cm.Parts = append(cm.Parts, canonical.Part{Type: "text", Text: p.Text})
					case "image_url":
						cm.Parts = append(cm.Parts, canonical.Part{Type: "image_url", ImageURL: p.ImageURL.URL})
					}
				}
			}
		}
		for _, tc := range m.ToolCalls {
			cm.ToolCalls = append(cm.ToolCalls, canonical.ToolCall{ID: tc.ID, Name: tc.Function.Name, Arguments: tc.Function.Arguments})
		}
		req.Messages = append(req.Messages, cm)
	}
	for _, t := range in.Tools {
		if t.Type != "function" {
			continue
		}
		req.Tools = append(req.Tools, canonical.Tool{Name: t.Function.Name, Description: t.Function.Description, Parameters: t.Function.Parameters})
	}
	return req, nil
}

// ---- OpenAI egress

type oaiUsage struct {
	PromptTokens        int64 `json:"prompt_tokens"`
	CompletionTokens    int64 `json:"completion_tokens"`
	TotalTokens         int64 `json:"total_tokens"`
	PromptTokensDetails *struct {
		CachedTokens int64 `json:"cached_tokens"`
	} `json:"prompt_tokens_details,omitempty"`
	CompletionTokensDetails *struct {
		ReasoningTokens int64 `json:"reasoning_tokens"`
	} `json:"completion_tokens_details,omitempty"`
}

func usageOut(u canonical.Usage) oaiUsage {
	o := oaiUsage{PromptTokens: u.PromptTokens, CompletionTokens: u.CompletionTokens, TotalTokens: u.PromptTokens + u.CompletionTokens}
	if u.CachedTokens > 0 {
		o.PromptTokensDetails = &struct {
			CachedTokens int64 `json:"cached_tokens"`
		}{u.CachedTokens}
	}
	if u.ReasoningTokens > 0 {
		o.CompletionTokensDetails = &struct {
			ReasoningTokens int64 `json:"reasoning_tokens"`
		}{u.ReasoningTokens}
	}
	return o
}

func toolCallsOut(tcs []canonical.ToolCall) []map[string]any {
	if len(tcs) == 0 {
		return nil
	}
	out := make([]map[string]any, 0, len(tcs))
	for _, tc := range tcs {
		out = append(out, map[string]any{"id": tc.ID, "type": "function", "function": map[string]any{"name": tc.Name, "arguments": tc.Arguments}})
	}
	return out
}

// EncodeOpenAIChat renders a non-streaming completion. `model` is the slug shown to the client.
func EncodeOpenAIChat(id, model string, resp *canonical.Response) []byte {
	msg := map[string]any{"role": "assistant", "content": nil}
	if resp.Content != "" || len(resp.ToolCalls) == 0 {
		msg["content"] = resp.Content
	}
	if tcs := toolCallsOut(resp.ToolCalls); tcs != nil {
		msg["tool_calls"] = tcs
	}
	out := map[string]any{
		"id": id, "object": "chat.completion", "created": time.Now().Unix(), "model": model,
		"choices": []map[string]any{{"index": 0, "message": msg, "finish_reason": resp.FinishReason}},
		"usage":   usageOut(resp.Usage),
	}
	b, _ := json.Marshal(out)
	return b
}

// StreamEncoder turns canonical events into OpenAI SSE chunks.
type StreamEncoder struct {
	id, model string
	created   int64
	sentRole  bool
}

func NewOpenAIStreamEncoder(id, model string) *StreamEncoder {
	return &StreamEncoder{id: id, model: model, created: time.Now().Unix()}
}

func (s *StreamEncoder) chunk(delta map[string]any, finish any, usage any) []byte {
	choices := []map[string]any{}
	if delta != nil || finish != nil {
		c := map[string]any{"index": 0, "delta": delta, "finish_reason": finish}
		if delta == nil {
			c["delta"] = map[string]any{}
		}
		choices = append(choices, c)
	}
	out := map[string]any{"id": s.id, "object": "chat.completion.chunk", "created": s.created, "model": s.model, "choices": choices}
	if usage != nil {
		out["usage"] = usage
	}
	b, _ := json.Marshal(out)
	return append(append([]byte("data: "), b...), '\n', '\n')
}

// Encode returns zero or more SSE frames for an event.
func (s *StreamEncoder) Encode(ev canonical.Event) []byte {
	switch ev.Type {
	case canonical.EventTextDelta:
		d := map[string]any{"content": ev.Text}
		if !s.sentRole {
			d["role"] = "assistant"
			s.sentRole = true
		}
		return s.chunk(d, nil, nil)
	case canonical.EventToolCallDelta:
		tc := map[string]any{"index": ev.ToolCall.Index, "function": map[string]any{"arguments": ev.ToolCall.ArgumentsDelta}}
		if ev.ToolCall.ID != "" {
			tc["id"] = ev.ToolCall.ID
			tc["type"] = "function"
			tc["function"].(map[string]any)["name"] = ev.ToolCall.Name
		}
		d := map[string]any{"tool_calls": []map[string]any{tc}}
		if !s.sentRole {
			d["role"] = "assistant"
			s.sentRole = true
		}
		return s.chunk(d, nil, nil)
	case canonical.EventFinish:
		return s.chunk(map[string]any{}, ev.FinishReason, nil)
	case canonical.EventUsage:
		if ev.Usage == nil {
			return nil
		}
		return s.chunk(nil, nil, usageOut(*ev.Usage))
	}
	return nil
}

func (s *StreamEncoder) Done() []byte { return []byte("data: [DONE]\n\n") }

// ---- errors

type APIError struct {
	Status  int    `json:"-"`
	Type    string `json:"type"`
	Code    string `json:"code,omitempty"`
	Message string `json:"message"`
	Param   string `json:"param,omitempty"`
}

func (e *APIError) Error() string { return e.Message }

func EncodeOpenAIError(e *APIError) []byte {
	b, _ := json.Marshal(map[string]any{"error": map[string]any{"message": e.Message, "type": e.Type, "code": e.Code, "param": e.Param}})
	return b
}

// ---- embeddings

func DecodeOpenAIEmbeddings(body []byte) (*canonical.EmbeddingRequest, error) {
	var in struct {
		Model string          `json:"model"`
		Input json.RawMessage `json:"input"`
		User  string          `json:"user"`
	}
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	req := &canonical.EmbeddingRequest{Model: in.Model, User: in.User}
	var one string
	if json.Unmarshal(in.Input, &one) == nil {
		req.Input = []string{one}
	} else if err := json.Unmarshal(in.Input, &req.Input); err != nil {
		return nil, fmt.Errorf("input must be a string or array of strings")
	}
	if len(req.Input) == 0 {
		return nil, fmt.Errorf("input is required")
	}
	return req, nil
}

func EncodeOpenAIEmbeddings(model string, resp *canonical.EmbeddingResponse) []byte {
	data := make([]map[string]any, len(resp.Vectors))
	for i, v := range resp.Vectors {
		data[i] = map[string]any{"object": "embedding", "index": i, "embedding": v}
	}
	b, _ := json.Marshal(map[string]any{"object": "list", "data": data, "model": model, "usage": map[string]any{"prompt_tokens": resp.Usage.PromptTokens, "total_tokens": resp.Usage.PromptTokens}})
	return b
}

// JoinText is used by estimators and the mock adapter.
func JoinText(req *canonical.Request) string {
	var sb strings.Builder
	for _, m := range req.Messages {
		sb.WriteString(m.Text())
		sb.WriteByte('\n')
	}
	return sb.String()
}
