// Package events carries usage events from the gateway to the workers.
// The MVP bus is an in-process channel with disk spill; Kafka slots in behind
// the same interface.
package events

import (
	"bufio"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rzp/litellm-lite/internal/metrics"
)

type BudgetCharge struct {
	BudgetID    uuid.UUID `json:"budget_id"`
	VendorID    uuid.UUID `json:"vendor_id"`
	ScopeType   string    `json:"scope_type"`
	PeriodStart time.Time `json:"period_start"`
	PeriodEnd   time.Time `json:"period_end"`
}

// UsageEvent is one provider attempt (or one gateway-side rejection).
type UsageEvent struct {
	TS                    time.Time      `json:"ts"`
	RequestID             string         `json:"request_id"`
	TraceID               string         `json:"trace_id"`
	VendorID              uuid.UUID      `json:"vendor_id"`
	ProjectID             uuid.UUID      `json:"project_id"`
	KeyID                 uuid.UUID      `json:"key_id"`
	IngressFormat         string         `json:"ingress_format"`
	ResponseFormat        string         `json:"response_format"`
	ModelSlug             string         `json:"model_slug"`
	RequestedDeploymentID string         `json:"requested_deployment_id"`
	ServedDeploymentID    string         `json:"served_deployment_id"`
	ServedDeploymentSlug  string         `json:"served_deployment_slug"`
	ProviderSlug          string         `json:"provider_slug"`
	AttemptNo             int            `json:"attempt_no"`
	AttemptsTotal         int            `json:"attempts_total"`
	FallbackUsed          bool           `json:"fallback_used"`
	FallbackReason        string         `json:"fallback_reason"`
	StatusCode            int            `json:"status_code"`
	ErrorClass            string         `json:"error_class"`
	ErrorCode             string         `json:"error_code"`
	Stream                bool           `json:"stream"`
	PromptTokens          int64          `json:"prompt_tokens"`
	CachedTokens          int64          `json:"cached_tokens"`
	CacheWriteTokens      int64          `json:"cache_write_tokens"`
	CompletionTokens      int64          `json:"completion_tokens"`
	ReasoningTokens       int64          `json:"reasoning_tokens"`
	PriceID               string         `json:"price_id"`
	CostNanoUSD           int64          `json:"cost_nano_usd"`
	EstimatedCostNanoUSD  int64          `json:"estimated_cost_nano_usd"`
	UsageSource           string         `json:"usage_source"`
	Enforcement           string         `json:"enforcement"`
	LatencyMS             int64          `json:"latency_ms"`
	TTFTMS                int64          `json:"ttft_ms"`
	UpstreamLatencyMS     int64          `json:"upstream_latency_ms"`
	ClientIP              string         `json:"client_ip"`
	UserAgent             string         `json:"user_agent"`
	PayloadRef            string         `json:"payload_ref"`
	Budgets               []BudgetCharge `json:"budgets"`
	DeniedBudgetID        *uuid.UUID     `json:"denied_budget_id,omitempty"`
}

type Handler func(ctx context.Context, batch []*UsageEvent)

type Bus interface {
	Publish(ev *UsageEvent)
	Subscribe(name string, h Handler)
	Run(ctx context.Context)
}

type ChannelBus struct {
	ch       chan *UsageEvent
	spillDir string
	mu       sync.Mutex
	handlers []Handler
	names    []string
}

func NewChannelBus(buffer int, spillDir string) *ChannelBus {
	_ = os.MkdirAll(spillDir, 0o755)
	return &ChannelBus{ch: make(chan *UsageEvent, buffer), spillDir: spillDir}
}

func (b *ChannelBus) Publish(ev *UsageEvent) {
	select {
	case b.ch <- ev:
		metrics.EventsPublished.Inc()
	default:
		b.spill(ev)
	}
}

func (b *ChannelBus) spill(ev *UsageEvent) {
	metrics.EventsSpilled.Inc()
	b.mu.Lock()
	defer b.mu.Unlock()
	f, err := os.OpenFile(filepath.Join(b.spillDir, "events.spill"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		slog.Error("spill open failed; event dropped", "err", err, "request_id", ev.RequestID)
		return
	}
	defer f.Close()
	line, _ := json.Marshal(ev)
	_, _ = f.Write(append(line, '\n'))
}

func (b *ChannelBus) Subscribe(name string, h Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers = append(b.handlers, h)
	b.names = append(b.names, name)
}

// Run batches events (up to 1000 or 500ms) and hands every batch to each
// subscriber in order. It also replays spilled events when the channel drains.
func (b *ChannelBus) Run(ctx context.Context) {
	go b.replayLoop(ctx)
	batch := make([]*UsageEvent, 0, 1000)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		b.mu.Lock()
		hs := append([]Handler(nil), b.handlers...)
		b.mu.Unlock()
		for _, h := range hs {
			h(ctx, batch)
		}
		batch = batch[:0]
	}
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case ev := <-b.ch:
			batch = append(batch, ev)
			if len(batch) >= 1000 {
				flush()
			}
		case <-t.C:
			flush()
		}
	}
}

func (b *ChannelBus) replayLoop(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if len(b.ch) > cap(b.ch)/2 {
				continue
			}
			b.replay()
		}
	}
}

func (b *ChannelBus) replay() {
	path := filepath.Join(b.spillDir, "events.spill")
	b.mu.Lock()
	if _, err := os.Stat(path); err != nil {
		b.mu.Unlock()
		return
	}
	tmp := path + ".replaying"
	if err := os.Rename(path, tmp); err != nil {
		b.mu.Unlock()
		return
	}
	b.mu.Unlock()
	f, err := os.Open(tmp)
	if err != nil {
		return
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	n := 0
	for sc.Scan() {
		var ev UsageEvent
		if json.Unmarshal(sc.Bytes(), &ev) == nil {
			b.ch <- &ev
			n++
		}
	}
	_ = os.Remove(tmp)
	if n > 0 {
		slog.Info("replayed spilled events", "count", n)
	}
}
