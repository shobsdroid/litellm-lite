package adapters

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

var _ net.Error = timeoutErr{}

// PLAN §7 step 7: retriable = connection error, timeout, 429, 5xx,
// context_length_exceeded. Non-retriable = client 400, refusals, our own 401.
func TestClassifyTransportErrors(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		class  string
		status int
	}{
		{"deadline", context.DeadlineExceeded, ClassTimeout, 504},
		{"net timeout", timeoutErr{}, ClassTimeout, 504},
		{"canceled", context.Canceled, ClassConnection, 499},
		{"refused", errors.New("dial tcp: connection refused"), ClassConnection, 502},
	}
	for _, c := range cases {
		e := Classify(c.err, 0, nil)
		if e.Class != c.class || e.Status != c.status {
			t.Errorf("%s: %+v", c.name, e)
		}
	}
}

func TestClassifyHTTPStatus(t *testing.T) {
	cases := []struct {
		status int
		body   string
		class  string
		want   int
	}{
		{429, `{"error":"slow down"}`, Class429, 429},
		{500, `oops`, Class5xx, 502},
		{503, ``, Class5xx, 502},
		{401, `bad key`, ClassAuth, 502},
		{403, `forbidden`, ClassAuth, 502},
		{400, `{"error":{"code":"context_length_exceeded","message":"This model's maximum context length is 128000 tokens"}}`, ClassContextLength, 400},
		{400, `{"error":{"message":"prompt is too long: 210000 tokens > 200000 maximum"}}`, ClassContextLength, 400},
		{400, `{"error":{"code":"content_filter"}}`, ClassContentFilter, 400},
		{400, `{"promptFeedback":{"blockReason":"SAFETY"}}`, ClassContentFilter, 400},
		{400, `{"error":{"message":"invalid temperature"}}`, ClassClient, 400},
		{422, `unprocessable`, ClassClient, 400},
	}
	for _, c := range cases {
		e := Classify(nil, c.status, []byte(c.body))
		if e.Class != c.class || e.Status != c.want {
			t.Errorf("%d %q: got %s/%d want %s/%d", c.status, c.body, e.Class, e.Status, c.class, c.want)
		}
	}
	if e := Classify(nil, 401, []byte("secret-key-leak")); e.Message == "secret-key-leak" {
		t.Fatal("provider auth failures must not echo the upstream body")
	}
}

func TestRetriable(t *testing.T) {
	want := map[string]bool{ClassTimeout: true, ClassConnection: true, Class5xx: true, Class429: true, ClassContextLength: true,
		ClassClient: false, ClassAuth: false, ClassContentFilter: false, ClassUnsupported: false}
	for class, r := range want {
		if (&Error{Class: class}).Retriable() != r {
			t.Errorf("%s retriable=%v want %v", class, !r, r)
		}
	}
	if (&Error{Class: ClassTimeout, Message: "m"}).Error() != "timeout: m" {
		t.Fatal("Error() format")
	}
}

func TestRegistryHasEveryMVPDialect(t *testing.T) {
	// PLAN §9: openai_chat, anthropic_messages, gemini_generate, mock.
	r := NewRegistry()
	for _, d := range []string{"openai_chat", "anthropic_messages", "gemini_generate", "mock"} {
		if a, ok := r[d]; !ok || a.Dialect() != d {
			t.Errorf("dialect %s missing", d)
		}
	}
}

func TestWithTimeoutAndJoinURL(t *testing.T) {
	ctx, cancel := withTimeout(context.Background(), 0)
	defer cancel()
	if _, has := ctx.Deadline(); has {
		t.Fatal("zero timeout must mean no deadline")
	}
	ctx2, cancel2 := withTimeout(context.Background(), time.Second)
	defer cancel2()
	if _, has := ctx2.Deadline(); !has {
		t.Fatal("positive timeout must set a deadline")
	}
	if joinURL("https://api.openai.com/v1/", "/chat/completions") != "https://api.openai.com/v1/chat/completions" {
		t.Fatal("joinURL must collapse the slash")
	}
}
