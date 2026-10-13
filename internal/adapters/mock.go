package adapters

import (
	"context"
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/rzp/litellm-lite/internal/canonical"
	"github.com/rzp/litellm-lite/internal/pricing"
)

// Mock is a provider for local development and load tests. It never leaves
// the process. Behaviour is driven by hints in the last user message:
//
//	[fail:500] [fail:429] [fail:timeout] [fail:context]   fail the attempt
//	[fail:500@mock-b]                                      fail only on the named provider
//	[latency:250]                                          sleep before answering (ms)
//	[output:120]                                           number of completion tokens
//
// Usage is computed with the real tokenizer so pricing is exercised end to end.
type Mock struct{}

func (Mock) Dialect() string { return "mock" }

var hintRe = regexp.MustCompile(`\[(fail|latency|output):([a-z0-9]+)(?:@([a-z0-9-]+))?\]`)

func (a *Mock) Complete(ctx context.Context, call Call, emit Emit) (*canonical.Response, error) {
	req := call.Req
	last := ""
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == "user" {
			last = req.Messages[i].Text()
			break
		}
	}
	outputTokens := 20
	for _, m := range hintRe.FindAllStringSubmatch(last, -1) {
		if m[3] != "" && m[3] != call.ProviderSlug {
			continue
		}
		switch m[1] {
		case "fail":
			switch m[2] {
			case "500":
				return nil, &Error{Status: 502, Class: Class5xx, Code: "mock_500", Message: "mock upstream failure"}
			case "429":
				return nil, &Error{Status: 429, Class: Class429, Code: "mock_429", Message: "mock rate limited"}
			case "timeout":
				return nil, &Error{Status: 504, Class: ClassTimeout, Code: "mock_timeout", Message: "mock timeout"}
			case "context":
				return nil, &Error{Status: 400, Class: ClassContextLength, Code: "context_length_exceeded", Message: "mock: prompt is too long"}
			case "400":
				return nil, &Error{Status: 400, Class: ClassClient, Code: "mock_400", Message: "mock bad request"}
			}
		case "latency":
			ms, _ := strconv.Atoi(m[2])
			select {
			case <-time.After(time.Duration(ms) * time.Millisecond):
			case <-ctx.Done():
				return nil, Classify(ctx.Err(), 0, nil)
			}
		case "output":
			outputTokens, _ = strconv.Atoi(m[2])
		}
	}
	if outputTokens <= 0 {
		outputTokens = 1
	}
	if req.MaxTokens > 0 && outputTokens > req.MaxTokens {
		outputTokens = req.MaxTokens
	}
	words := make([]string, 0, outputTokens)
	words = append(words, "mock("+call.ProviderModel+")")
	for len(words) < outputTokens {
		words = append(words, fmt.Sprintf("w%d", len(words)))
	}
	text := strings.Join(words, " ")
	tk := pricing.Default()
	usage := canonical.Usage{PromptTokens: tk.CountRequest("o200k_base", 1.0, req), CompletionTokens: tk.CountText("o200k_base", text), Source: "provider"}
	finish := "stop"
	if req.MaxTokens > 0 && outputTokens >= req.MaxTokens {
		finish = "length"
	}
	resp := &canonical.Response{ID: fmt.Sprintf("mock-%d", rand.Int63()), Model: call.ProviderModel, Content: text, FinishReason: finish, Usage: usage}
	if !req.Stream {
		return resp, nil
	}
	for i := 0; i < len(words); i += 5 {
		end := i + 5
		if end > len(words) {
			end = len(words)
		}
		chunk := strings.Join(words[i:end], " ")
		if end < len(words) {
			chunk += " "
		}
		if err := emit(canonical.Event{Type: canonical.EventTextDelta, Text: chunk}); err != nil {
			return resp, Classify(err, 0, nil)
		}
	}
	if err := emit(canonical.Event{Type: canonical.EventFinish, FinishReason: finish}); err != nil {
		return resp, Classify(err, 0, nil)
	}
	if err := emit(canonical.Event{Type: canonical.EventUsage, Usage: &resp.Usage}); err != nil {
		return resp, Classify(err, 0, nil)
	}
	return resp, nil
}

func (a *Mock) Embed(ctx context.Context, call EmbedCall) (*canonical.EmbeddingResponse, error) {
	r := &canonical.EmbeddingResponse{Model: call.ProviderModel}
	tk := pricing.Default()
	for _, s := range call.Req.Input {
		v := make([]float32, 8)
		for i := range v {
			v[i] = float32((int(s[0])*(i+1))%97) / 97
		}
		r.Vectors = append(r.Vectors, v)
		r.Usage.PromptTokens += tk.CountText("o200k_base", s)
	}
	r.Usage.Source = "provider"
	return r, nil
}
