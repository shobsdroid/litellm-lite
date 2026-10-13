// Package adapters translates canonical requests to provider dialects.
// One adapter per API dialect; many providers share a dialect.
package adapters

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/rzp/litellm-lite/internal/canonical"
)

type Call struct {
	Req             *canonical.Request
	ProviderModel   string
	ProviderSlug    string // informational; the mock adapter uses it for targeted failures
	BaseURL         string
	APIKey          string
	Headers         map[string]string
	MaxOutputTokens int // deployment default when the client did not set max_tokens
	Effort          string
	TTFTTimeout     time.Duration
	TotalTimeout    time.Duration
}

type EmbedCall struct {
	Req           *canonical.EmbeddingRequest
	ProviderModel string
	BaseURL       string
	APIKey        string
	Headers       map[string]string
}

// Emit receives streaming events. Returning an error aborts the stream.
type Emit func(canonical.Event) error

type Adapter interface {
	Dialect() string
	// Complete runs a chat completion. When Req.Stream is set, events are
	// emitted as they arrive; the returned Response always carries final usage.
	Complete(ctx context.Context, call Call, emit Emit) (*canonical.Response, error)
	Embed(ctx context.Context, call EmbedCall) (*canonical.EmbeddingResponse, error)
}

// Error classes drive fallback decisions and log fields.
const (
	ClassTimeout       = "timeout"
	ClassConnection    = "connection_error"
	Class5xx           = "http_5xx"
	Class429           = "http_429"
	ClassContextLength = "context_length_exceeded"
	ClassClient        = "client_error"
	ClassAuth          = "provider_auth"
	ClassContentFilter = "content_filter"
	ClassUnsupported   = "unsupported"
)

type Error struct {
	Status  int
	Class   string
	Code    string
	Message string
}

func (e *Error) Error() string { return e.Class + ": " + e.Message }

func (e *Error) Retriable() bool {
	switch e.Class {
	case ClassTimeout, ClassConnection, Class5xx, Class429, ClassContextLength:
		return true
	}
	return false
}

// Classify maps a transport error or an upstream HTTP status + body to an Error.
func Classify(err error, status int, body []byte) *Error {
	if err != nil {
		var ne net.Error
		if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()) {
			return &Error{Status: 504, Class: ClassTimeout, Code: "upstream_timeout", Message: err.Error()}
		}
		if errors.Is(err, context.Canceled) {
			return &Error{Status: 499, Class: ClassConnection, Code: "client_closed", Message: "client closed request"}
		}
		return &Error{Status: 502, Class: ClassConnection, Code: "upstream_unreachable", Message: err.Error()}
	}
	msg := strings.TrimSpace(string(body))
	if len(msg) > 2000 {
		msg = msg[:2000]
	}
	lower := strings.ToLower(msg)
	switch {
	case status == 429:
		return &Error{Status: 429, Class: Class429, Code: "upstream_rate_limited", Message: msg}
	case status >= 500:
		return &Error{Status: 502, Class: Class5xx, Code: "upstream_error", Message: msg}
	case status == 401 || status == 403:
		return &Error{Status: 502, Class: ClassAuth, Code: "provider_auth_failed", Message: "provider rejected gateway credentials"}
	case strings.Contains(lower, "context_length") || strings.Contains(lower, "context length") || strings.Contains(lower, "maximum context") ||
		strings.Contains(lower, "prompt is too long") || strings.Contains(lower, "too many tokens") || strings.Contains(lower, "exceeds the maximum number of tokens"):
		return &Error{Status: 400, Class: ClassContextLength, Code: "context_length_exceeded", Message: msg}
	case strings.Contains(lower, "content_filter") || strings.Contains(lower, "safety"):
		return &Error{Status: 400, Class: ClassContentFilter, Code: "content_filter", Message: msg}
	}
	return &Error{Status: 400, Class: ClassClient, Code: "upstream_rejected", Message: msg}
}

// Shared HTTP client: HTTP/2 where offered, large keep-alive pool.
var httpClient = &http.Client{Transport: &http.Transport{
	MaxIdleConns: 4096, MaxIdleConnsPerHost: 1024, IdleConnTimeout: 90 * time.Second,
	TLSHandshakeTimeout: 5 * time.Second, ForceAttemptHTTP2: true,
	DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
}}

// doJSON posts a JSON body; the caller owns the response body when err == nil.
func doJSON(ctx context.Context, method, url string, headers map[string]string, body []byte) (*http.Response, *Error) {
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		return nil, &Error{Status: 500, Class: ClassClient, Code: "bad_request", Message: err.Error()}
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, Classify(err, 0, nil)
	}
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
		resp.Body.Close()
		return nil, Classify(nil, resp.StatusCode, b)
	}
	return resp, nil
}

// readSSE calls fn for each `data:` payload (and event name, when present) until EOF.
func readSSE(r io.Reader, fn func(event, data string) error) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 8*1024*1024)
	event := ""
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			event = ""
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(line[6:])
		case strings.HasPrefix(line, "data:"):
			if err := fn(event, strings.TrimSpace(line[5:])); err != nil {
				return err
			}
		}
	}
	return sc.Err()
}

func withTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, d)
}

func joinURL(base, path string) string {
	return strings.TrimRight(base, "/") + path
}

// Registry maps dialect names to adapters.
type Registry map[string]Adapter

func NewRegistry() Registry {
	r := Registry{}
	for _, a := range []Adapter{&OpenAIChat{}, &AnthropicMessages{}, &GeminiGenerate{}, &Mock{}} {
		r[a.Dialect()] = a
	}
	return r
}
