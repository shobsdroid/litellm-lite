package adapters

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/rzp/litellm-lite/internal/canonical"
)

// GeminiGenerate speaks the Gemini generateContent API (Google AI Studio).
type GeminiGenerate struct{}

func (GeminiGenerate) Dialect() string { return "gemini_generate" }

func (a *GeminiGenerate) body(call Call) []byte {
	req := call.Req
	var system strings.Builder
	contents := []map[string]any{}
	toolNames := map[string]string{} // tool_call_id -> name, for functionResponse
	for _, m := range req.Messages {
		switch m.Role {
		case "system":
			system.WriteString(m.Text())
			system.WriteString("\n")
		case "assistant":
			parts := []map[string]any{}
			if t := m.Text(); t != "" {
				parts = append(parts, map[string]any{"text": t})
			}
			for _, tc := range m.ToolCalls {
				var args any = map[string]any{}
				_ = json.Unmarshal([]byte(tc.Arguments), &args)
				toolNames[tc.ID] = tc.Name
				parts = append(parts, map[string]any{"functionCall": map[string]any{"name": tc.Name, "args": args}})
			}
			contents = append(contents, map[string]any{"role": "model", "parts": parts})
		case "tool":
			var resp any
			if json.Unmarshal([]byte(m.Text()), &resp) != nil {
				resp = map[string]any{"result": m.Text()}
			}
			contents = append(contents, map[string]any{"role": "user", "parts": []map[string]any{{"functionResponse": map[string]any{"name": toolNames[m.ToolCallID], "response": resp}}}})
		default:
			parts := []map[string]any{}
			if len(m.Parts) > 0 {
				for _, p := range m.Parts {
					if p.Type == "image_url" && strings.HasPrefix(p.ImageURL, "data:") {
						meta, data, _ := strings.Cut(strings.TrimPrefix(p.ImageURL, "data:"), ",")
						mt, _, _ := strings.Cut(meta, ";")
						parts = append(parts, map[string]any{"inlineData": map[string]any{"mimeType": mt, "data": data}})
					} else if p.Type == "text" {
						parts = append(parts, map[string]any{"text": p.Text})
					}
				}
			} else {
				parts = append(parts, map[string]any{"text": m.Content})
			}
			contents = append(contents, map[string]any{"role": "user", "parts": parts})
		}
	}
	gen := map[string]any{}
	if maxTok := req.MaxTokens; maxTok > 0 {
		gen["maxOutputTokens"] = maxTok
	} else if call.MaxOutputTokens > 0 {
		gen["maxOutputTokens"] = call.MaxOutputTokens
	}
	if req.Temperature != nil {
		gen["temperature"] = *req.Temperature
	}
	if req.TopP != nil {
		gen["topP"] = *req.TopP
	}
	if len(req.Stop) > 0 {
		gen["stopSequences"] = req.Stop
	}
	if len(req.ResponseFormat) > 0 && strings.Contains(string(req.ResponseFormat), "json") {
		gen["responseMimeType"] = "application/json"
	}
	if b, ok := effortBudget[call.Effort]; ok {
		gen["thinkingConfig"] = map[string]any{"thinkingBudget": b}
	}
	out := map[string]any{"contents": contents, "generationConfig": gen}
	if s := strings.TrimSpace(system.String()); s != "" {
		out["systemInstruction"] = map[string]any{"parts": []map[string]any{{"text": s}}}
	}
	if len(req.Tools) > 0 {
		decls := []map[string]any{}
		for _, t := range req.Tools {
			d := map[string]any{"name": t.Name, "description": t.Description}
			if len(t.Parameters) > 0 {
				d["parameters"] = t.Parameters
			}
			decls = append(decls, d)
		}
		out["tools"] = []map[string]any{{"functionDeclarations": decls}}
	}
	b, _ := json.Marshal(out)
	return b
}

type gemUsage struct {
	PromptTokenCount        int64 `json:"promptTokenCount"`
	CandidatesTokenCount    int64 `json:"candidatesTokenCount"`
	CachedContentTokenCount int64 `json:"cachedContentTokenCount"`
	ThoughtsTokenCount      int64 `json:"thoughtsTokenCount"`
}

func (u gemUsage) canonical() canonical.Usage {
	return canonical.Usage{PromptTokens: u.PromptTokenCount, CachedTokens: u.CachedContentTokenCount,
		CompletionTokens: u.CandidatesTokenCount + u.ThoughtsTokenCount, ReasoningTokens: u.ThoughtsTokenCount, Source: "provider"}
}

type gemChunk struct {
	Candidates []struct {
		Content struct {
			Parts []struct {
				Text         string `json:"text"`
				FunctionCall *struct {
					Name string          `json:"name"`
					Args json.RawMessage `json:"args"`
				} `json:"functionCall"`
			} `json:"parts"`
		} `json:"content"`
		FinishReason string `json:"finishReason"`
	} `json:"candidates"`
	UsageMetadata *gemUsage `json:"usageMetadata"`
}

func mapGemFinish(s string) string {
	switch s {
	case "STOP":
		return "stop"
	case "MAX_TOKENS":
		return "length"
	case "SAFETY", "RECITATION", "BLOCKLIST", "PROHIBITED_CONTENT":
		return "content_filter"
	case "":
		return ""
	}
	return "stop"
}

func (a *GeminiGenerate) Complete(ctx context.Context, call Call, emit Emit) (*canonical.Response, error) {
	ctx, cancel := withTimeout(ctx, call.TotalTimeout)
	defer cancel()
	method := "generateContent"
	if call.Req.Stream {
		method = "streamGenerateContent?alt=sse"
	}
	url := joinURL(call.BaseURL, fmt.Sprintf("/v1beta/models/%s:%s", call.ProviderModel, method))
	h := map[string]string{"x-goog-api-key": call.APIKey}
	for k, v := range call.Headers {
		h[k] = v
	}
	resp, aerr := doJSON(ctx, "POST", url, h, a.body(call))
	if aerr != nil {
		return nil, aerr
	}
	defer resp.Body.Close()
	r := &canonical.Response{Model: call.ProviderModel}
	toolN := 0
	apply := func(ch *gemChunk, stream bool) error {
		for _, c := range ch.Candidates {
			for _, p := range c.Content.Parts {
				if p.Text != "" {
					r.Content += p.Text
					if stream {
						if err := emit(canonical.Event{Type: canonical.EventTextDelta, Text: p.Text}); err != nil {
							return err
						}
					}
				}
				if p.FunctionCall != nil {
					tc := canonical.ToolCall{ID: fmt.Sprintf("call_%d", toolN), Name: p.FunctionCall.Name, Arguments: string(p.FunctionCall.Args)}
					if tc.Arguments == "" {
						tc.Arguments = "{}"
					}
					r.ToolCalls = append(r.ToolCalls, tc)
					if stream {
						if err := emit(canonical.Event{Type: canonical.EventToolCallDelta, ToolCall: &canonical.ToolCallDelta{Index: toolN, ID: tc.ID, Name: tc.Name, ArgumentsDelta: tc.Arguments}}); err != nil {
							return err
						}
					}
					toolN++
				}
			}
			if f := mapGemFinish(c.FinishReason); f != "" {
				if len(r.ToolCalls) > 0 && f == "stop" {
					f = "tool_calls"
				}
				r.FinishReason = f
			}
		}
		if ch.UsageMetadata != nil {
			r.Usage = ch.UsageMetadata.canonical()
		}
		return nil
	}
	if !call.Req.Stream {
		var ch gemChunk
		if err := json.NewDecoder(resp.Body).Decode(&ch); err != nil {
			return nil, &Error{Status: 502, Class: Class5xx, Code: "bad_upstream_json", Message: err.Error()}
		}
		return r, apply(&ch, false)
	}
	err := readSSE(resp.Body, func(_, data string) error {
		var ch gemChunk
		if json.Unmarshal([]byte(data), &ch) != nil {
			return nil
		}
		return apply(&ch, true)
	})
	if err != nil {
		return r, Classify(err, 0, nil)
	}
	if r.FinishReason != "" {
		if err := emit(canonical.Event{Type: canonical.EventFinish, FinishReason: r.FinishReason}); err != nil {
			return r, Classify(err, 0, nil)
		}
	}
	if err := emit(canonical.Event{Type: canonical.EventUsage, Usage: &r.Usage}); err != nil {
		return r, Classify(err, 0, nil)
	}
	return r, nil
}

func (a *GeminiGenerate) Embed(ctx context.Context, call EmbedCall) (*canonical.EmbeddingResponse, error) {
	return nil, &Error{Status: 400, Class: ClassUnsupported, Code: "unsupported", Message: "gemini embeddings not implemented in MVP"}
}
