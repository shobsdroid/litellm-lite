package budget

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/rzp/litellm-lite/internal/store"
)

// PLAN §4.1: check+reserve, settle and release are each one atomic Lua call.
// miniredis runs the real scripts in-process.

type harness struct {
	mr  *miniredis.Miniredis
	rdb *redis.Client
	enf *Enforcer
	ctx context.Context
}

func newHarness(t *testing.T, tolerancePct int) *harness {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), DialTimeout: 200 * time.Millisecond, ReadTimeout: 200 * time.Millisecond, WriteTimeout: 200 * time.Millisecond, MaxRetries: 0})
	return &harness{mr: mr, rdb: rdb, enf: New(rdb, 660*time.Second, tolerancePct), ctx: context.Background()}
}

func ptr(v int64) *int64 { return &v }

func scope(scopeType, kind string, limitNano *int64, limitTokens *int64, onExceed, enforcement string) Scope {
	b := store.Budget{ID: uuid.New(), ScopeType: scopeType, Kind: kind, Timezone: "UTC", AnchorDay: 1, LimitNanoUSD: limitNano, LimitTokens: limitTokens, OnExceed: onExceed, Enforcement: enforcement, Status: "active"}
	s, e := Period(&b, time.Now())
	return Scope{Budget: b, PeriodStart: s, PeriodEnd: e}
}

func (h *harness) field(t *testing.T, s Scope, f string) int64 {
	t.Helper()
	v := h.mr.HGet(s.RedisKey(), f)
	if v == "" {
		return 0
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestReserveAllowsAndReservesWholeChain(t *testing.T) {
	h := newHarness(t, 5)
	key := scope("key", "monthly", ptr(1_000_000), nil, "block", "available")
	proj := scope("project", "monthly", ptr(5_000_000), nil, "block", "available")
	vendor := scope("vendor", "lifetime", nil, nil, "block", "available")
	chain := []Scope{key, proj, vendor}
	dec, err := h.enf.Reserve(h.ctx, "req_1", chain, 400, 50)
	if err != nil || !dec.Allowed || dec.Enforcement != "redis" || dec.DeniedBy != nil {
		t.Fatalf("decision %+v err %v", dec, err)
	}
	if len(dec.Remaining) != 3 || *dec.Remaining[0].RemainingNS != 1_000_000 || dec.Remaining[2].RemainingNS != nil {
		t.Fatalf("remaining: %+v", dec.Remaining)
	}
	for _, s := range chain {
		if h.field(t, s, "reserved") != 400 || h.field(t, s, "tokens_reserved") != 50 {
			t.Fatalf("%s not reserved", s.Budget.ScopeType)
		}
	}
	// Monthly keys get a TTL; lifetime keys never expire (PLAN §8.2).
	if h.mr.TTL(key.RedisKey()) <= 0 {
		t.Fatal("monthly budget hash must have a TTL")
	}
	if h.mr.TTL(vendor.RedisKey()) != 0 {
		t.Fatal("lifetime budget hash must not expire")
	}
	// Reservation record + deadline for the sweeper (PLAN §4.1 reservation expiry).
	if !h.mr.Exists("resv:req_1") {
		t.Fatal("reservation record missing")
	}
	if members, _ := h.mr.ZMembers("resv_deadlines"); len(members) != 1 || members[0] != "req_1" {
		t.Fatalf("deadline set: %v", members)
	}
}

func TestReserveDeniesWithoutPartialReservation(t *testing.T) {
	h := newHarness(t, 5)
	key := scope("key", "monthly", ptr(10_000), nil, "block", "available")
	proj := scope("project", "monthly", ptr(1_000), nil, "block", "available")
	h.mr.HSet(proj.RedisKey(), "spent", "900")
	dec, err := h.enf.Reserve(h.ctx, "req_2", []Scope{key, proj}, 200, 10)
	if err != nil || dec.Allowed || dec.DeniedBy == nil || dec.DeniedBy.Budget.ScopeType != "project" {
		t.Fatalf("decision %+v err %v", dec, err)
	}
	if h.field(t, key, "reserved") != 0 || h.field(t, proj, "reserved") != 0 {
		t.Fatal("a denied chain must leave no partial reservation on any scope")
	}
	if h.mr.Exists("resv:req_2") {
		t.Fatal("no reservation record on denial")
	}
	// Reserved amounts count against the limit too (concurrent in-flight requests).
	h.mr.HSet(proj.RedisKey(), "spent", "0")
	h.mr.HSet(proj.RedisKey(), "reserved", "950")
	if dec, _ := h.enf.Reserve(h.ctx, "req_3", []Scope{proj}, 100, 1); dec.Allowed {
		t.Fatal("spent+reserved+estimate over the limit must be denied")
	}
	if dec, _ := h.enf.Reserve(h.ctx, "req_4", []Scope{proj}, 50, 1); !dec.Allowed {
		t.Fatal("exactly at the limit must be allowed")
	}
}

func TestReserveTokenLimitAndAlertMode(t *testing.T) {
	h := newHarness(t, 5)
	tok := scope("key", "monthly", nil, ptr(100), "block", "available")
	h.mr.HSet(tok.RedisKey(), "tokens", "90")
	if dec, _ := h.enf.Reserve(h.ctx, "r1", []Scope{tok}, 1, 20); dec.Allowed {
		t.Fatal("token cap must be enforced")
	}
	// PLAN §8.2: on_exceed=alert lets the request through.
	alert := scope("vendor", "monthly", ptr(100), nil, "alert", "available")
	h.mr.HSet(alert.RedisKey(), "spent", "1000")
	if dec, _ := h.enf.Reserve(h.ctx, "r2", []Scope{alert}, 500, 1); !dec.Allowed {
		t.Fatal("alert budgets must not block")
	}
	if dec, err := h.enf.Reserve(h.ctx, "r3", nil, 1, 1); err != nil || !dec.Allowed {
		t.Fatal("no budgets means allowed")
	}
}

func TestSettleIsIdempotentPerAttempt(t *testing.T) {
	h := newHarness(t, 5)
	key := scope("key", "monthly", ptr(1_000_000), nil, "block", "available")
	chain := []Scope{key}
	h.enf.Reserve(h.ctx, "req_s", chain, 500, 10)
	h.enf.Settle(h.ctx, "req_s", 1, chain, 300, 40)
	h.enf.Settle(h.ctx, "req_s", 1, chain, 300, 40) // replay
	if h.field(t, key, "spent") != 300 || h.field(t, key, "tokens") != 40 || h.field(t, key, "requests") != 1 {
		t.Fatalf("replayed settle must no-op: spent=%d tokens=%d requests=%d", h.field(t, key, "spent"), h.field(t, key, "tokens"), h.field(t, key, "requests"))
	}
	// PLAN §5.2: every attempt that produced usage is charged separately.
	h.enf.Settle(h.ctx, "req_s", 2, chain, 100, 5)
	if h.field(t, key, "spent") != 400 {
		t.Fatalf("second attempt must add: %d", h.field(t, key, "spent"))
	}
	if h.mr.TTL("settled:req_s:1") <= 0 || h.mr.TTL("settled:req_s:1") > 24*time.Hour {
		t.Fatal("settle marker must expire within 24h")
	}
}

func TestReleaseDropsReservationOnce(t *testing.T) {
	h := newHarness(t, 5)
	key := scope("key", "monthly", ptr(1_000_000), nil, "block", "available")
	proj := scope("project", "lifetime", nil, nil, "block", "available")
	chain := []Scope{key, proj}
	h.enf.Reserve(h.ctx, "req_r", chain, 500, 10)
	h.enf.Release(h.ctx, "req_r")
	for _, s := range chain {
		if h.field(t, s, "reserved") != 0 || h.field(t, s, "tokens_reserved") != 0 {
			t.Fatalf("%s reservation not released", s.Budget.ScopeType)
		}
	}
	if h.mr.Exists("resv:req_r") {
		t.Fatal("reservation record must be deleted")
	}
	if members, _ := h.mr.ZMembers("resv_deadlines"); len(members) != 0 {
		t.Fatal("deadline entry must be removed")
	}
	h.enf.Release(h.ctx, "req_r") // second release
	if h.field(t, key, "reserved") != 0 {
		t.Fatal("double release must not drive reserved negative")
	}
	// Reserve, settle, release: spent stays, reserved goes.
	h.enf.Reserve(h.ctx, "req_q", chain, 500, 10)
	h.enf.Settle(h.ctx, "req_q", 1, chain, 123, 7)
	h.enf.Release(h.ctx, "req_q")
	if h.field(t, key, "spent") != 123 || h.field(t, key, "reserved") != 0 {
		t.Fatalf("after full cycle: spent=%d reserved=%d", h.field(t, key, "spent"), h.field(t, key, "reserved"))
	}
}

func TestRemainingReadsLiveCounters(t *testing.T) {
	h := newHarness(t, 5)
	key := scope("key", "monthly", ptr(1000), nil, "block", "available")
	h.mr.HSet(key.RedisKey(), "spent", "250", "reserved", "50", "tokens", "9")
	rems, err := h.enf.Remaining(h.ctx, []Scope{key})
	if err != nil || len(rems) != 1 || rems[0].Spent != 250 || rems[0].Reserved != 50 || rems[0].Tokens != 9 || *rems[0].RemainingNS != 750 {
		t.Fatalf("%v %+v", err, rems)
	}
}

// PLAN §4.1: a sweeper releases reservations whose deadline passed so a
// crashed pod cannot pin spend.
func TestSweeperReleasesExpiredReservations(t *testing.T) {
	h := newHarness(t, 5)
	h.enf = New(h.rdb, time.Second, 5) // deadline one second out
	key := scope("key", "monthly", ptr(1_000_000), nil, "block", "available")
	h.enf.Reserve(h.ctx, "req_old", []Scope{key}, 500, 10)
	if h.field(t, key, "reserved") != 500 {
		t.Fatal("setup")
	}
	ctx, cancel := context.WithTimeout(h.ctx, 2500*time.Millisecond)
	defer cancel()
	h.enf.Run(ctx) // ticks at 1s and 2s; the deadline has passed by then
	if h.field(t, key, "reserved") != 0 || h.mr.Exists("resv:req_old") {
		t.Fatalf("expired reservation not swept: reserved=%d", h.field(t, key, "reserved"))
	}
	if members, _ := h.mr.ZMembers("resv_deadlines"); len(members) != 0 {
		t.Fatal("deadline entry must be removed after sweep")
	}
}

// The reservation record carries the same TTL as the sweep deadline. If Redis
// expires the record before the sweeper runs, release.lua finds nothing to
// undo and the `reserved` counter stays pinned until the period key expires.
// This test pins the invariant the plan requires (a crashed pod cannot pin
// spend); it fails on the current implementation.
func TestSweeperSurvivesRecordExpiry(t *testing.T) {
	h := newHarness(t, 5)
	h.enf = New(h.rdb, 2*time.Second, 5)
	key := scope("key", "monthly", ptr(1_000_000), nil, "block", "available")
	h.enf.Reserve(h.ctx, "req_crash", []Scope{key}, 500, 10)
	h.mr.FastForward(2 * time.Second) // record TTL elapses exactly at the deadline
	if h.mr.Exists("resv:req_crash") {
		t.Fatal("precondition: record should have expired")
	}
	ctx, cancel := context.WithTimeout(h.ctx, 3500*time.Millisecond)
	defer cancel()
	h.enf.Run(ctx)
	if got := h.field(t, key, "reserved"); got != 0 {
		t.Fatalf("KNOWN BUG: reservation record expired before the sweep, %d nano-USD stays reserved for the rest of the period (resv TTL == deadline; give the record a margin or release from the deadline set)", got)
	}
}

// PLAN §4.2: Redis down -> degraded local enforcement with bounded overrun;
// strict budgets fail closed.
func TestDegradedModeBoundedOverrun(t *testing.T) {
	h := newHarness(t, 10)
	key := scope("key", "monthly", ptr(1000), nil, "block", "available")
	h.mr.HSet(key.RedisKey(), "spent", "900")
	if dec, _ := h.enf.Reserve(h.ctx, "warm", []Scope{key}, 50, 1); !dec.Allowed {
		t.Fatal("warm-up reserve")
	}
	if h.enf.Degraded() {
		t.Fatal("must not be degraded while Redis answers")
	}
	h.mr.Close()
	// Local view: 900 spent + 50 reserved = 950. Tolerance 10% -> local limit 1100.
	dec, err := h.enf.Reserve(h.ctx, "d1", []Scope{key}, 100, 1)
	if err != nil || !dec.Allowed || dec.Enforcement != "local" {
		t.Fatalf("within tolerance must pass locally: %+v %v", dec, err)
	}
	if !h.enf.Degraded() {
		t.Fatal("Degraded() must report the switch")
	}
	dec, err = h.enf.Reserve(h.ctx, "d2", []Scope{key}, 200, 1)
	if err != nil || dec.Allowed || dec.DeniedBy == nil {
		t.Fatalf("beyond tolerance must be denied locally: %+v %v", dec, err)
	}
	strict := scope("project", "monthly", ptr(1000), nil, "block", "strict")
	if _, err := h.enf.Reserve(h.ctx, "d3", []Scope{key, strict}, 1, 1); err == nil {
		t.Fatal("strict budget must fail closed when Redis is down")
	}
}

func TestDegradedSettleAccruesAndFlushesOnRecovery(t *testing.T) {
	h := newHarness(t, 10)
	key := scope("key", "monthly", ptr(1_000_000), nil, "block", "available")
	h.enf.Reserve(h.ctx, "w", []Scope{key}, 1, 1)
	h.mr.Close()
	h.enf.Settle(h.ctx, "off1", 1, []Scope{key}, 300, 3)
	h.enf.Settle(h.ctx, "off2", 1, []Scope{key}, 200, 2)
	if !h.enf.Degraded() {
		t.Fatal("settle failure must flag degraded")
	}
	// Locally accrued spend counts against the local limit while Redis is down.
	small := scope("project", "monthly", ptr(600), nil, "block", "available")
	h.enf.Settle(h.ctx, "off3", 1, []Scope{small}, 550, 1)
	if dec, _ := h.enf.Reserve(h.ctx, "d", []Scope{small}, 200, 1); dec.Allowed {
		t.Fatal("accrued spend must be enforced locally (600*1.1=660 < 750)")
	}
	if err := h.mr.Restart(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(h.ctx, 1500*time.Millisecond)
	defer cancel()
	h.enf.Run(ctx)
	if got := h.field(t, key, "spent"); got != 500 {
		t.Fatalf("accrued spend must be pushed to Redis on recovery: %d", got)
	}
	if h.enf.Degraded() {
		t.Fatal("degraded flag must clear after a successful flush")
	}
}
