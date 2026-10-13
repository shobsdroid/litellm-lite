// Package canonical is the provider-independent request/response model.
// Ingress decoders produce a Request; adapters consume it and produce Events
// and a Response; egress encoders turn those into the client's wire format.
package canonical

import "encoding/json"

type Part struct {
	Type      string `json:"type"` // text | image_url
	Text      string `json:"text,omitempty"`
	ImageURL  string `json:"image_url,omitempty"`  // http(s) or data: URL
	MediaType string `json:"media_type,omitempty"` // for data URLs decoded by adapters
}

type ToolCall struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"` // JSON string
}

type Message struct {
	Role       string     `json:"role"` // system | user | assistant | tool
	Content    string     `json:"content,omitempty"`
	Parts      []Part     `json:"parts,omitempty"` // multimodal; Content is used when Parts is empty
	Name       string     `json:"name,omitempty"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
}

// Text returns the plain-text view of the message for tokenizing and for providers without multimodal support.
func (m Message) Text() string {
	if len(m.Parts) == 0 {
		return m.Content
	}
	s := ""
	for _, p := range m.Parts {
		if p.Type == "text" {
			s += p.Text
		}
	}
	return s
}

type Tool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

type Request struct {
	Model          string          `json:"model"`
	Messages       []Message       `json:"messages"`
	MaxTokens      int             `json:"max_tokens,omitempty"`
	Temperature    *float64        `json:"temperature,omitempty"`
	TopP           *float64        `json:"top_p,omitempty"`
	Stop           []string        `json:"stop,omitempty"`
	Stream         bool            `json:"stream,omitempty"`
	Tools          []Tool          `json:"tools,omitempty"`
	ToolChoice     json.RawMessage `json:"tool_choice,omitempty"`
	ResponseFormat json.RawMessage `json:"response_format,omitempty"` // OpenAI-shaped
	Effort         string          `json:"effort,omitempty"`          // reasoning effort requested by the client
	User           string          `json:"user,omitempty"`
	N              int             `json:"n,omitempty"`
}

// Usage is normalized so that:
//
//	PromptTokens     = all input tokens, including cached and cache-write tokens
//	CachedTokens     = input tokens served from provider cache (billed at cached rate)
//	CacheWriteTokens = input tokens written to provider cache (billed at cache-write rate)
//	CompletionTokens = all output tokens, including reasoning
//	ReasoningTokens  = reasoning/thinking tokens (billed at reasoning rate)
type Usage struct {
	PromptTokens     int64  `json:"prompt_tokens"`
	CachedTokens     int64  `json:"cached_tokens,omitempty"`
	CacheWriteTokens int64  `json:"cache_write_tokens,omitempty"`
	CompletionTokens int64  `json:"completion_tokens"`
	ReasoningTokens  int64  `json:"reasoning_tokens,omitempty"`
	Source           string `json:"source,omitempty"` // provider | estimated
}

func (u Usage) Total() int64 { return u.PromptTokens + u.CompletionTokens }

type Response struct {
	ID           string     `json:"id"`
	Model        string     `json:"model"`
	Content      string     `json:"content"`
	ToolCalls    []ToolCall `json:"tool_calls,omitempty"`
	FinishReason string     `json:"finish_reason"` // stop | length | tool_calls | content_filter
	Usage        Usage      `json:"usage"`
}

type EventType string

const (
	EventTextDelta     EventType = "text_delta"
	EventToolCallDelta EventType = "tool_call_delta"
	EventFinish        EventType = "finish"
	EventUsage         EventType = "usage"
)

type ToolCallDelta struct {
	Index          int
	ID             string
	Name           string
	ArgumentsDelta string
}

// Event is one streaming increment from an adapter.
type Event struct {
	Type         EventType
	Text         string
	ToolCall     *ToolCallDelta
	FinishReason string
	Usage        *Usage
}

// IsContent reports whether the event carries bytes the client would see.
// Fallback is only possible before the first content event is forwarded.
func (e Event) IsContent() bool {
	return e.Type == EventTextDelta || e.Type == EventToolCallDelta
}

// Embedding request/response.
type EmbeddingRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
	User  string   `json:"user,omitempty"`
}

type EmbeddingResponse struct {
	Model   string      `json:"model"`
	Vectors [][]float32 `json:"vectors"`
	Usage   Usage       `json:"usage"`
}
