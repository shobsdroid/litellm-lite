package adapters

import (
	"context"
	"encoding/json"
	"io"
	"strings"

	"github.com/rzp/litellm-lite/internal/canonical"
)

// OpenAIChat speaks the Chat Completions dialect: OpenAI, Moonshot/Kimi,
// DeepSeek, Groq, Together, OpenRouter, Azure OpenAI (with base_url).
type OpenAIChat struct{}

func (OpenAIChat) Dialect() string { return "openai_chat" }

func (a *OpenAIChat) body(call Call, stream bool) []byte {
	req := call.Req
	msgs := make([]map[string]any, 0, len(req.Messages))
	for _, m := range req.Messages {
		om := map[string]any{"role": m.Role}
		if len(m.Parts) > 0 {
			parts := []map[string]any{}
			for _, p := range m.Parts {
				if p.Type == "image_url" {
					parts = append(parts, map[string]any{"type": "image_url", "image_url": map[string]any{"url": p.ImageURL}})
				} else {
					parts = append(parts, map[string]any{"type": "text", "text": p.Text})
				}
			}
			om["content"] = parts
		} else if m.Content != "" || len(m.ToolCalls) == 0 {
			om["content"] = m.Content
		}
		if m.Name != "" {
			om["name"] = m.Name
		}
		if m.ToolCallID != "" {
			om["tool_call_id"] = m.ToolCallID
		}
		if len(m.ToolCalls) > 0 {
			tcs := []map[string]any{}
			for _, tc := range m.ToolCalls {
				tcs = append(tcs, map[string]any{"id": tc.ID, "type": "function", "function": map[string]any{"name": tc.Name, "arguments": tc.Arguments}})
			}
			om["tool_calls"] = tcs
		}
		msgs = append(msgs, om)
	}
	out := map[string]any{"model": call.ProviderModel, "messages": msgs}
	maxTok := req.MaxTokens
	if maxTok == 0 {
		maxTok = call.MaxOutputTokens
	}
	if maxTok > 0 {
		// api.openai.com rejects max_tokens for reasoning models; other OpenAI-compatible hosts still require it.
		if strings.Contains(call.BaseURL, "api.openai.com") {
			out["max_completion_tokens"] = maxTok
		} else {
			out["max_tokens"] = maxTok
		}
	}
	if req.Temperature != nil {
		out["temperature"] = *req.Temperature
	}
	if req.TopP != nil {
		out["top_p"] = *req.TopP
	}
	if len(req.Stop) > 0 {
		out["stop"] = req.Stop
	}
	if req.User != "" {
		out["user"] = req.User
	}
	if len(req.Tools) > 0 {
		tools := []map[string]any{}
		for _, t := range req.Tools {
			fn := map[string]any{"name": t.Name}
			if t.Description != "" {
				fn["description"] = t.Description
			}
			if len(t.Parameters) > 0 {
				fn["parameters"] = t.Parameters
			}
			tools = append(tools, map[string]any{"type": "function", "function": fn})
		}
		out["tools"] = tools
		if len(req.ToolChoice) > 0 {
			out["tool_choice"] = req.ToolChoice
		}
	}
	if len(req.ResponseFormat) > 0 {
		out["response_format"] = req.ResponseFormat
	}
	if call.Effort != "" && call.Effort != "none" && strings.Contains(call.BaseURL, "api.openai.com") {
		out["reasoning_effort"] = call.Effort
	}
	if stream {
		out["stream"] = true
		out["stream_options"] = map[string]any{"include_usage": true}
	}
	b, _ := json.Marshal(out)
	return b
}

func (a *OpenAIChat) headers(call Call) map[string]string {
	h := map[string]string{"Authorization": "Bearer " + call.APIKey}
	for k, v := range call.Headers {
		h[k] = v
	}
	return h
}

type oaiUsageIn struct {
	PromptTokens        int64 `json:"prompt_tokens"`
	CompletionTokens    int64 `json:"completion_tokens"`
	PromptTokensDetails struct {
		CachedTokens int64 `json:"cached_tokens"`
	} `json:"prompt_tokens_details"`
	CompletionTokensDetails struct {
		ReasoningTokens int64 `json:"reasoning_tokens"`
	} `json:"completion_tokens_details"`
}

func (u *oaiUsageIn) canonical() canonical.Usage {
	return canonical.Usage{PromptTokens: u.PromptTokens, CachedTokens: u.PromptTokensDetails.CachedTokens,
		CompletionTokens: u.CompletionTokens, ReasoningTokens: u.CompletionTokensDetails.ReasoningTokens, Source: "provider"}
}

func (a *OpenAIChat) Complete(ctx context.Context, call Call, emit Emit) (*canonical.Response, error) {
	ctx, cancel := withTimeout(ctx, call.TotalTimeout)
	defer cancel()
	url := joinURL(call.BaseURL, "/chat/completions")
	if !call.Req.Stream {
		resp, aerr := doJSON(ctx, "POST", url, a.headers(call), a.body(call, false))
		if aerr != nil {
			return nil, aerr
		}
		defer resp.Body.Close()
		var out struct {
			ID      string `json:"id"`
			Choices []struct {
				Message struct {
					Content   string `json:"content"`
					ToolCalls []struct {
						ID       string `json:"id"`
						Function struct {
							Name      string `json:"name"`
							Arguments string `json:"arguments"`
						} `json:"function"`
					} `json:"tool_calls"`
				} `json:"message"`
				FinishReason string `json:"finish_reason"`
			} `json:"choices"`
			Usage *oaiUsageIn `json:"usage"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return nil, &Error{Status: 502, Class: Class5xx, Code: "bad_upstream_json", Message: err.Error()}
		}
		r := &canonical.Response{ID: out.ID, Model: call.ProviderModel}
		if len(out.Choices) > 0 {
			c := out.Choices[0]
			r.Content = c.Message.Content
			r.FinishReason = c.FinishReason
			for _, tc := range c.Message.ToolCalls {
				r.ToolCalls = append(r.ToolCalls, canonical.ToolCall{ID: tc.ID, Name: tc.Function.Name, Arguments: tc.Function.Arguments})
			}
		}
		if out.Usage != nil {
			r.Usage = out.Usage.canonical()
		}
		return r, nil
	}

	resp, aerr := doJSON(ctx, "POST", url, a.headers(call), a.body(call, true))
	if aerr != nil {
		return nil, aerr
	}
	defer resp.Body.Close()
	r := &canonical.Response{Model: call.ProviderModel}
	var sb strings.Builder
	tools := map[int]*canonical.ToolCall{}
	err := readSSE(resp.Body, func(_, data string) error {
		if data == "[DONE]" {
			return io.EOF
		}
		var ch struct {
			ID      string `json:"id"`
			Choices []struct {
				Delta struct {
					Content   *string `json:"content"`
					ToolCalls []struct {
						Index    int    `json:"index"`
						ID       string `json:"id"`
						Function struct {
							Name      string `json:"name"`
							Arguments string `json:"arguments"`
						} `json:"function"`
					} `json:"tool_calls"`
				} `json:"delta"`
				FinishReason *string `json:"finish_reason"`
			} `json:"choices"`
			Usage *oaiUsageIn `json:"usage"`
		}
		if err := json.Unmarshal([]byte(data), &ch); err != nil {
			return nil
		}
		if ch.ID != "" {
			r.ID = ch.ID
		}
		for _, c := range ch.Choices {
			if c.Delta.Content != nil && *c.Delta.Content != "" {
				sb.WriteString(*c.Delta.Content)
				if err := emit(canonical.Event{Type: canonical.EventTextDelta, Text: *c.Delta.Content}); err != nil {
					return err
				}
			}
			for _, tc := range c.Delta.ToolCalls {
				cur := tools[tc.Index]
				if cur == nil {
					cur = &canonical.ToolCall{}
					tools[tc.Index] = cur
				}
				d := &canonical.ToolCallDelta{Index: tc.Index, ArgumentsDelta: tc.Function.Arguments}
				if tc.ID != "" {
					cur.ID, cur.Name = tc.ID, tc.Function.Name
					d.ID, d.Name = tc.ID, tc.Function.Name
				}
				cur.Arguments += tc.Function.Arguments
				if err := emit(canonical.Event{Type: canonical.EventToolCallDelta, ToolCall: d}); err != nil {
					return err
				}
			}
			if c.FinishReason != nil && *c.FinishReason != "" {
				r.FinishReason = *c.FinishReason
				if err := emit(canonical.Event{Type: canonical.EventFinish, FinishReason: r.FinishReason}); err != nil {
					return err
				}
			}
		}
		if ch.Usage != nil {
			r.Usage = ch.Usage.canonical()
		}
		return nil
	})
	if err != nil && err != io.EOF {
		return r, Classify(err, 0, nil)
	}
	r.Content = sb.String()
	for i := 0; i < len(tools); i++ {
		if tc := tools[i]; tc != nil {
			r.ToolCalls = append(r.ToolCalls, *tc)
		}
	}
	if r.Usage.Source == "provider" {
		if err := emit(canonical.Event{Type: canonical.EventUsage, Usage: &r.Usage}); err != nil {
			return r, Classify(err, 0, nil)
		}
	}
	return r, nil
}

func (a *OpenAIChat) Embed(ctx context.Context, call EmbedCall) (*canonical.EmbeddingResponse, error) {
	b, _ := json.Marshal(map[string]any{"model": call.ProviderModel, "input": call.Req.Input})
	h := map[string]string{"Authorization": "Bearer " + call.APIKey}
	for k, v := range call.Headers {
		h[k] = v
	}
	resp, aerr := doJSON(ctx, "POST", joinURL(call.BaseURL, "/embeddings"), h, b)
	if aerr != nil {
		return nil, aerr
	}
	defer resp.Body.Close()
	var out struct {
		Data []struct {
			Embedding []float32 `json:"embedding"`
		} `json:"data"`
		Usage oaiUsageIn `json:"usage"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, &Error{Status: 502, Class: Class5xx, Code: "bad_upstream_json", Message: err.Error()}
	}
	r := &canonical.EmbeddingResponse{Model: call.ProviderModel, Usage: out.Usage.canonical()}
	for _, d := range out.Data {
		r.Vectors = append(r.Vectors, d.Embedding)
	}
	return r, nil
}
