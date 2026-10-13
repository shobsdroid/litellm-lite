package events

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// PLAN §4.1: usage emission never blocks the response; buffer -> bus -> disk spill.

type collector struct {
	mu      sync.Mutex
	batches [][]*UsageEvent
}

func (c *collector) handler(_ context.Context, batch []*UsageEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := append([]*UsageEvent(nil), batch...)
	c.batches = append(c.batches, cp)
}

func (c *collector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, b := range c.batches {
		n += len(b)
	}
	return n
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

func TestPublishDeliversBatchesToEverySubscriber(t *testing.T) {
	bus := NewChannelBus(100, t.TempDir())
	ledger, analytics := &collector{}, &collector{}
	bus.Subscribe("ledger", ledger.handler)
	bus.Subscribe("analytics", analytics.handler)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go bus.Run(ctx)
	for i := 0; i < 3; i++ {
		bus.Publish(&UsageEvent{RequestID: "r", AttemptNo: i + 1})
	}
	waitFor(t, 2*time.Second, func() bool { return ledger.count() == 3 && analytics.count() == 3 })
	if len(ledger.batches) != 1 {
		t.Fatalf("three quick events should arrive in one time-based batch, got %d batches", len(ledger.batches))
	}
	if ledger.batches[0][2].AttemptNo != 3 {
		t.Fatal("order within a batch must be preserved")
	}
}

func TestLargeBatchFlushesBySize(t *testing.T) {
	bus := NewChannelBus(5000, t.TempDir())
	c := &collector{}
	bus.Subscribe("c", c.handler)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go bus.Run(ctx)
	for i := 0; i < 2500; i++ {
		bus.Publish(&UsageEvent{RequestID: "r"})
	}
	waitFor(t, 3*time.Second, func() bool { return c.count() == 2500 })
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.batches) < 3 || len(c.batches[0]) != 1000 {
		t.Fatalf("expected 1000-event batches, got sizes %v", func() []int {
			s := []int{}
			for _, b := range c.batches {
				s = append(s, len(b))
			}
			return s
		}())
	}
}

func TestFullBufferSpillsToDiskAndReplays(t *testing.T) {
	dir := t.TempDir()
	bus := NewChannelBus(1, dir)
	// No consumer running: the first event fills the buffer, the rest spill.
	id := uuid.New()
	bus.Publish(&UsageEvent{RequestID: "kept", VendorID: id})
	bus.Publish(&UsageEvent{RequestID: "spilled-1", VendorID: id, CostNanoUSD: 7})
	bus.Publish(&UsageEvent{RequestID: "spilled-2", VendorID: id})
	raw, err := os.ReadFile(filepath.Join(dir, "events.spill"))
	if err != nil {
		t.Fatalf("spill file missing: %v", err)
	}
	lines := 0
	for _, l := range splitLines(raw) {
		var ev UsageEvent
		if err := json.Unmarshal(l, &ev); err != nil {
			t.Fatalf("spill line not json: %v", err)
		}
		lines++
	}
	if lines != 2 {
		t.Fatalf("expected 2 spilled lines, got %d", lines)
	}

	// Start consuming; drain the one buffered event, then replay the spill.
	c := &collector{}
	bus.Subscribe("c", c.handler)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go bus.Run(ctx)
	waitFor(t, 2*time.Second, func() bool { return c.count() == 1 })
	bus.replay()
	waitFor(t, 2*time.Second, func() bool { return c.count() == 3 })
	if _, err := os.Stat(filepath.Join(dir, "events.spill")); !os.IsNotExist(err) {
		t.Fatal("spill file must be consumed after replay")
	}
	seen := map[string]int64{}
	c.mu.Lock()
	for _, b := range c.batches {
		for _, ev := range b {
			seen[ev.RequestID] = ev.CostNanoUSD
		}
	}
	c.mu.Unlock()
	if v, ok := seen["spilled-1"]; !ok || v != 7 {
		t.Fatalf("replayed event lost fields: %v", seen)
	}
}

func TestRunFlushesOnShutdown(t *testing.T) {
	bus := NewChannelBus(10, t.TempDir())
	c := &collector{}
	bus.Subscribe("c", c.handler)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { bus.Run(ctx); close(done) }()
	bus.Publish(&UsageEvent{RequestID: "last"})
	time.Sleep(50 * time.Millisecond) // let Run pick it into the pending batch
	cancel()
	<-done
	if c.count() != 1 {
		t.Fatal("graceful shutdown must flush the pending batch (PLAN §10)")
	}
}

func TestUsageEventJSONRoundTrip(t *testing.T) {
	bid := uuid.New()
	ev := &UsageEvent{TS: time.Now().UTC().Truncate(time.Millisecond), RequestID: "req_x", VendorID: uuid.New(), AttemptNo: 2, AttemptsTotal: 2, FallbackUsed: true, FallbackReason: "http_5xx",
		PromptTokens: 10, CachedTokens: 2, CompletionTokens: 5, ReasoningTokens: 1, CostNanoUSD: 12345, UsageSource: "provider", Enforcement: "redis",
		Budgets: []BudgetCharge{{BudgetID: bid, ScopeType: "key"}}, DeniedBudgetID: &bid}
	b, _ := json.Marshal(ev)
	var back UsageEvent
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if back.RequestID != ev.RequestID || back.CostNanoUSD != 12345 || !back.FallbackUsed || back.Budgets[0].BudgetID != bid || *back.DeniedBudgetID != bid || !back.TS.Equal(ev.TS) {
		t.Fatalf("round trip lost data: %+v", back)
	}
}

func splitLines(b []byte) [][]byte {
	var out [][]byte
	start := 0
	for i, c := range b {
		if c == '\n' {
			if i > start {
				out = append(out, b[start:i])
			}
			start = i + 1
		}
	}
	if start < len(b) {
		out = append(out, b[start:])
	}
	return out
}
