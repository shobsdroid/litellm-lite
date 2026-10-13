package adapters

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/rzp/litellm-lite/internal/canonical"
)

// AnthropicMessages speaks the Messages API.
type AnthropicMessages struct{}

func (AnthropicMessages) Dialect() string { return "anthropic_messages" }

var effortBudget = map[string]int{"low": 1024, "medium": 4096, "high": 16384}

func (a *AnthropicMessages) body(call Call) []byte {
	req := call.Req
	var system strings.Builder
	msgs := []map[string]any{}
	for _, m := range req.Messages {
		switch m.Role {
		case "system":
			system.WriteString(m.Text())
			system.WriteString("\n")
		case "tool":
			msgs = append(msgs, map[string]any{"role": "user", "content": []map[string]any{{"type": "tool_result", "tool_use_id": m.ToolCallID, "content": m.Text()}}})
		case "assistant":
			blocks := []map[string]any{}
			if t := m.Text(); t != "" {
				blocks = append(blocks, map[string]any{"type": "text", "text": t})
			}
			for _, tc := range m.ToolCalls {
				var input any = map[string]any{}
				_ = json.Unmarshal([]byte(tc.Arguments), &input)
				blocks = append(blocks, map[string]any{"type": "tool_use", "id": tc.ID, "name": tc.Name, "input": input})
			}
			msgs = append(msgs, map[string]any{"role": "assistant", "content": blocks})
		default:
			blocks := []map[string]any{}
			if len(m.Parts) > 0 {
				for _, p := range m.Parts {
					if p.Type == "image_url" {
						if strings.HasPrefix(p.ImageURL, "data:") {
							meta, data, _ := strings.Cut(strings.TrimPrefix(p.ImageURL, "data:"), ",")
							mt, _, _ := strings.Cut(meta, ";")
							blocks = append(blocks, map[string]any{"type": "image", "source": map[string]any{"type": "base64", "media_type": mt, "data": data}})
						} else {
							blocks = append(blocks, map[string]any{"type": "image", "source": map[string]any{"type": "url", "url": p.ImageURL}})
						}
					} else {
						blocks = append(blocks, map[string]any{"type": "text", "text": p.Text})
					}
				}
			} else {
				blocks = append(blocks, map[string]any{"type": "text", "text": m.Content})
			}
			msgs = append(msgs, map[string]any{"role": "user", "content": blocks})
		}
	}
	// Merge consecutive same-role messages (Anthropic requires alternation).
	merged := []map[string]any{}
	for _, m := range msgs {
		if n := len(merged); n > 0 && merged[n-1]["role"] == m["role"] {
			merged[n-1]["content"] = append(merged[n-1]["content"].([]map[string]any), m["content"].([]map[string]any)...)
			continue
		}
		merged = append(merged, m)
	}
	maxTok := req.MaxTokens
	if maxTok == 0 {
		maxTok = call.MaxOutputTokens
	}
	if maxTok == 0 {
		maxTok = 4096
	}
	out := map[string]any{"model": call.ProviderModel, "messages": merged, "max_tokens": maxTok, "stream": req.Stream}
	if s := strings.TrimSpace(system.String()); s != "" {
		out["system"] = s
	}
	if req.Temperature != nil {
		out["temperature"] = *req.Temperature
	}
	if req.TopP != nil {
		out["top_p"] = *req.TopP
	}
	if len(req.Stop) > 0 {
		out["stop_sequences"] = req.Stop
	}
	if len(req.Tools) > 0 {
		tools := []map[string]any{}
		for _, t := range req.Tools {
			params := t.Parameters
			if len(params) == 0 {
				params = json.RawMessage(`{"type":"object","properties":{}}`)
			}
			tools = append(tools, map[string]any{"name": t.Name, "description": t.Description, "input_schema": params})
		}
		out["tools"] = tools
	}
	if b, ok := effortBudget[call.Effort]; ok && maxTok > b+256 {
		out["thinking"] = map[string]any{"type": "enabled", "budget_tokens": b}
		delete(out, "temperature")
		delete(out, "top_p")
	}
	b, _ := json.Marshal(out)
	return b
}

func (a *AnthropicMessages) headers(call Call) map[string]string {
	h := map[string]string{"x-api-key": call.APIKey, "anthropic-version": "2023-06-01"}
	for k, v := range call.Headers {
		h[k] = v
	}
	return h
}

type anthUsage struct {
	InputTokens              int64 `json:"input_tokens"`
	OutputTokens             int64 `json:"output_tokens"`
	CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
	CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
}

func (u anthUsage) canonical() canonical.Usage {
	return canonical.Usage{PromptTokens: u.InputTokens + u.CacheCreationInputTokens + u.CacheReadInputTokens,
		CachedTokens: u.CacheReadInputTokens, CacheWriteTokens: u.CacheCreationInputTokens, CompletionTokens: u.OutputTokens, Source: "provider"}
}

func mapStop(s string) string {
	switch s {
	case "end_turn", "stop_sequence":
		return "stop"
	case "max_tokens":
		return "length"
	case "tool_use":
		return "tool_calls"
	}
	return s
}

func (a *AnthropicMessages) Complete(ctx context.Context, call Call, emit Emit) (*canonical.Response, error) {
	ctx, cancel := withTimeout(ctx, call.TotalTimeout)
	defer cancel()
	url := joinURL(call.BaseURL, "/v1/messages")
	resp, aerr := doJSON(ctx, "POST", url, a.headers(call), a.body(call))
	if aerr != nil {
		return nil, aerr
	}
	defer resp.Body.Close()
	r := &canonical.Response{Model: call.ProviderModel}
	if !call.Req.Stream {
		var out struct {
			ID      string `json:"id"`
			Content []struct {
				Type  string          `json:"type"`
				Text  string          `json:"text"`
				ID    string          `json:"id"`
				Name  string          `json:"name"`
				Input json.RawMessage `json:"input"`
			} `json:"content"`
			StopReason string    `json:"stop_reason"`
			Usage      anthUsage `json:"usage"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return nil, &Error{Status: 502, Class: Class5xx, Code: "bad_upstream_json", Message: err.Error()}
		}
		r.ID = out.ID
		for _, c := range out.Content {
			switch c.Type {
			case "text":
				r.Content += c.Text
			case "tool_use":
				r.ToolCalls = append(r.ToolCalls, canonical.ToolCall{ID: c.ID, Name: c.Name, Arguments: string(c.Input)})
			}
		}
		r.FinishReason = mapStop(out.StopReason)
		r.Usage = out.Usage.canonical()
		return r, nil
	}
	var sb strings.Builder
	var usage anthUsage
	blockIdx := map[int]int{} // content block index -> tool call index
	var tools []canonical.ToolCall
	err := readSSE(resp.Body, func(event, data string) error {
		var ev struct {
			Type    string `json:"type"`
			Index   int    `json:"index"`
			Message struct {
				ID    string    `json:"id"`
				Usage anthUsage `json:"usage"`
			} `json:"message"`
			ContentBlock struct {
				Type string `json:"type"`
				ID   string `json:"id"`
				Name string `json:"name"`
			} `json:"content_block"`
			Delta struct {
				Type        string `json:"type"`
				Text        string `json:"text"`
				PartialJSON string `json:"partial_json"`
				StopReason  string `json:"stop_reason"`
			} `json:"delta"`
			Usage *anthUsage `json:"usage"`
			Error struct {
				Type    string `json:"type"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal([]byte(data), &ev) != nil {
			return nil
		}
		switch ev.Type {
		case "message_start":
			r.ID = ev.Message.ID
			usage.InputTokens = ev.Message.Usage.InputTokens
			usage.CacheCreationInputTokens = ev.Message.Usage.CacheCreationInputTokens
			usage.CacheReadInputTokens = ev.Message.Usage.CacheReadInputTokens
		case "content_block_start":
			if ev.ContentBlock.Type == "tool_use" {
				blockIdx[ev.Index] = len(tools)
				tools = append(tools, canonical.ToolCall{ID: ev.ContentBlock.ID, Name: ev.ContentBlock.Name})
				return emit(canonical.Event{Type: canonical.EventToolCallDelta, ToolCall: &canonical.ToolCallDelta{Index: len(tools) - 1, ID: ev.ContentBlock.ID, Name: ev.ContentBlock.Name}})
			}
		case "content_block_delta":
			if ev.Delta.Type == "text_delta" && ev.Delta.Text != "" {
				sb.WriteString(ev.Delta.Text)
				return emit(canonical.Event{Type: canonical.EventTextDelta, Text: ev.Delta.Text})
			}
			if ev.Delta.Type == "input_json_delta" {
				if ti, ok := blockIdx[ev.Index]; ok {
					tools[ti].Arguments += ev.Delta.PartialJSON
					return emit(canonical.Event{Type: canonical.EventToolCallDelta, ToolCall: &canonical.ToolCallDelta{Index: ti, ArgumentsDelta: ev.Delta.PartialJSON}})
				}
			}
		case "message_delta":
			if ev.Usage != nil {
				usage.OutputTokens = ev.Usage.OutputTokens
			}
			if ev.Delta.StopReason != "" {
				r.FinishReason = mapStop(ev.Delta.StopReason)
				return emit(canonical.Event{Type: canonical.EventFinish, FinishReason: r.FinishReason})
			}
		case "error":
			return &Error{Status: 502, Class: Class5xx, Code: ev.Error.Type, Message: ev.Error.Message}
		}
		return nil
	})
	if err != nil {
		if ae, ok := err.(*Error); ok {
			return r, ae
		}
		return r, Classify(err, 0, nil)
	}
	r.Content = sb.String()
	r.ToolCalls = tools
	r.Usage = usage.canonical()
	if err := emit(canonical.Event{Type: canonical.EventUsage, Usage: &r.Usage}); err != nil {
		return r, Classify(err, 0, nil)
	}
	return r, nil
}

func (a *AnthropicMessages) Embed(ctx context.Context, call EmbedCall) (*canonical.EmbeddingResponse, error) {
	return nil, &Error{Status: 400, Class: ClassUnsupported, Code: "unsupported", Message: "anthropic has no embeddings endpoint"}
}
