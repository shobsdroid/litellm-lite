package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/rzp/litellm-lite/internal/adapters"
	"github.com/rzp/litellm-lite/internal/budget"
	"github.com/rzp/litellm-lite/internal/canonical"
	"github.com/rzp/litellm-lite/internal/catalog"
	"github.com/rzp/litellm-lite/internal/config"
	"github.com/rzp/litellm-lite/internal/events"
	"github.com/rzp/litellm-lite/internal/pricing"
	"github.com/rzp/litellm-lite/internal/ratelimit"
	"github.com/rzp/litellm-lite/internal/router"
	"github.com/rzp/litellm-lite/internal/store"
)

// In-process gateway tests: real router, real budget/rate-limit Lua on
// miniredis, real pricing and tokenizer, a scripted provider adapter.
// Vendor-side auth is out of scope (PLAN §8.3); keys are resolved from a
// primed catalog so no Postgres is needed.

// scripted is a provider dialect whose behaviour is keyed by ProviderModel.
type scripted struct {
	mu    sync.Mutex
	calls []string
}

func (s *scripted) Dialect() string { return "scripted" }

func (s *scripted) Complete(ctx context.Context, call adapters.Call, emit adapters.Emit) (*canonical.Response, error) {
	s.mu.Lock()
	s.calls = append(s.calls, call.ProviderModel)
	s.mu.Unlock()
	usage := canonical.Usage{PromptTokens: 100, CompletionTokens: 20, Source: "provider"}
	switch call.ProviderModel {
	case "fail500":
		return nil, &adapters.Error{Status: 502, Class: adapters.Class5xx, Code: "upstream_error", Message: "boom"}
	case "fail500-with-usage": // partial output billed by the provider
		return &canonical.Response{Content: "partial", Usage: canonical.Usage{PromptTokens: 100, CompletionTokens: 5, Source: "provider"}},
			&adapters.Error{Status: 502, Class: adapters.Class5xx, Code: "upstream_error", Message: "died mid-stream"}
	case "fail400":
		return nil, &adapters.Error{Status: 400, Class: adapters.ClassClient, Code: "bad_param", Message: "temperature out of range"}
	case "no-usage":
		return &canonical.Response{Content: "answer without usage", FinishReason: "stop"}, nil
	case "slow":
		select {
		case <-time.After(300 * time.Millisecond):
		case <-ctx.Done():
			return nil, adapters.Classify(ctx.Err(), 0, nil)
		}
	}
	resp := &canonical.Response{ID: "scripted-1", Model: call.ProviderModel, Content: "ok from " + call.ProviderModel, FinishReason: "stop", Usage: usage}
	if call.Req.Stream {
		for _, chunk := range []string{"ok ", "from ", call.ProviderModel} {
			if err := emit(canonical.Event{Type: canonical.EventTextDelta, Text: chunk}); err != nil {
				return resp, err
			}
		}
		emit(canonical.Event{Type: canonical.EventFinish, FinishReason: "stop"})
		emit(canonical.Event{Type: canonical.EventUsage, Usage: &resp.Usage})
	}
	return resp, nil
}

func (s *scripted) Embed(ctx context.Context, call adapters.EmbedCall) (*canonical.EmbeddingResponse, error) {
	return (&adapters.Mock{}).Embed(ctx, call)
}

func (s *scripted) seen() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

type world struct {
	t        *testing.T
	mr       *miniredis.Miniredis
	srv      *httptest.Server
	adapter  *scripted
	breakers *router.Breakers
	enf      *budget.Enforcer
	events   *collector
	snap     *catalog.Snapshot
	keys     map[string]*catalog.KeyConfig
	deps     map[string]*catalog.Deployment
	vendor   *store.Vendor
	project  *store.Project
	price    *pricing.Price
}

type collector struct {
	mu  sync.Mutex
	evs []*events.UsageEvent
}

func (c *collector) handle(_ context.Context, batch []*events.UsageEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.evs = append(c.evs, batch...)
}

func (c *collector) all() []*events.UsageEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*events.UsageEvent(nil), c.evs...)
}

const (
	secretMain  = "sk-ll-test-main"
	secretTiny  = "sk-ll-test-tiny"
	secretRPM   = "sk-ll-test-rpm"
	secretDead  = "sk-ll-test-revoked"
	secretOld   = "sk-ll-test-expired"
	secretNoFB  = "sk-ll-test-nofallback"
	secretLimit = "sk-ll-test-allowlist"
)

var gpt4oPrice = []pricing.Tier{{Input: 2500, CachedInput: 1250, Output: 10000, Reasoning: 10000}}

func newWorld(t *testing.T, maxInflight int) *world {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), DialTimeout: 200 * time.Millisecond, ReadTimeout: 200 * time.Millisecond, MaxRetries: 0})
	w := &world{t: t, mr: mr, adapter: &scripted{}, breakers: router.NewBreakers(), events: &collector{}, keys: map[string]*catalog.KeyConfig{}, deps: map[string]*catalog.Deployment{}}
	w.enf = budget.New(rdb, 660*time.Second, 5)

	// Catalog: one provider speaking the scripted dialect, one model, five deployments.
	prov := &store.Provider{ID: uuid.New(), Slug: "scripted", Dialect: "scripted", Status: "active", DefaultHeaders: []byte(`{}`)}
	model := &store.Model{ID: uuid.New(), Slug: "gpt-4o", Family: "gpt", Tokenizer: "o200k_base", TokenizerMultiplier: 1, Capabilities: []byte(`["tools","streaming"]`), Status: "active"}
	w.price = &pricing.Price{ID: uuid.New(), Tiers: gpt4oPrice}
	w.snap = &catalog.Snapshot{Version: 1, LoadedAt: time.Now(), ByID: map[uuid.UUID]*catalog.Deployment{}, BySlug: map[string]*catalog.Deployment{}, ByModelSlug: map[string][]*catalog.Deployment{},
		Providers: map[uuid.UUID]*store.Provider{prov.ID: prov}, Models: map[uuid.UUID]*store.Model{model.ID: model}, Credentials: map[uuid.UUID]string{prov.ID: "provider-secret"}}
	add := func(slug, providerModel string, ctx int32, priority int32, price *pricing.Price) *catalog.Deployment {
		d := &catalog.Deployment{Deployment: store.Deployment{ID: uuid.New(), ModelID: model.ID, ProviderID: prov.ID, Slug: slug, ProviderModelName: providerModel, ContextWindow: ctx, MaxOutputTokens: 4096, Effort: "medium", Priority: priority, Status: "active", CreatedAt: time.Now()},
			Model: model, Provider: prov, Price: price, Capabilities: []string{"tools", "streaming"}}
		w.snap.ByID[d.ID] = d
		w.snap.BySlug[slug] = d
		w.snap.ByModelSlug[model.Slug] = append(w.snap.ByModelSlug[model.Slug], d)
		w.snap.Deployments = append(w.snap.Deployments, d)
		w.deps[slug] = d
		return d
	}
	primary := add("gpt-4o-scripted-128k-medium", "ok", 128000, 10, w.price)
	add("gpt-4o-scripted-fail-medium", "fail500", 128000, 5, w.price)               // cheaper priority, fails
	add("gpt-4o-scripted-partial-medium", "fail500-with-usage", 128000, 6, w.price) // fails after billing usage
	add("gpt-4o-scripted-1m-medium", "ok", 1_000_000, 20, &pricing.Price{ID: uuid.New(), Tiers: []pricing.Tier{{Input: 5000, Output: 20000}}})
	add("gpt-4o-scripted-nousage-medium", "no-usage", 128000, 30, w.price)
	add("gpt-4o-scripted-client-medium", "fail400", 128000, 40, w.price)
	add("gpt-4o-scripted-slow-medium", "slow", 128000, 50, w.price)
	add("gpt-4o-scripted-unpriced-medium", "ok", 128000, 60, nil)

	w.vendor = &store.Vendor{ID: uuid.New(), Name: "acme", Status: "active"}
	w.project = &store.Project{ID: uuid.New(), VendorID: w.vendor.ID, Name: "default", Status: "active"}
	keyBudget := func(limit int64) store.Budget {
		return store.Budget{ID: uuid.New(), VendorID: w.vendor.ID, ScopeType: "key", Kind: "monthly", Timezone: "UTC", AnchorDay: 1, LimitNanoUSD: &limit, OnExceed: "block", Enforcement: "available", Status: "active"}
	}
	projLimit := int64(100 * pricing.NanoPerUSD)
	projBudget := store.Budget{ID: uuid.New(), VendorID: w.vendor.ID, ScopeType: "project", Kind: "lifetime", LimitNanoUSD: &projLimit, OnExceed: "alert", Enforcement: "available", Status: "active"}
	mk := func(secret, name string, primaryDep *catalog.Deployment, policy string, mutate func(k *store.Key)) *catalog.KeyConfig {
		k := &store.Key{ID: uuid.New(), ProjectID: w.project.ID, VendorID: w.vendor.ID, KeyPrefix: "sk-ll-test", Name: name, Status: "active", PrimaryDeploymentID: primaryDep.ID,
			FallbackPolicy: []byte(policy), ResponseFormat: "openai", RequestOverrides: []byte(`{}`)}
		if mutate != nil {
			mutate(k)
		}
		kc := &catalog.KeyConfig{Key: k, Project: w.project, Vendor: w.vendor, Budgets: []store.Budget{keyBudget(10 * pricing.NanoPerUSD), projBudget}, Policy: catalog.ParsePolicy(k.FallbackPolicy), LoadedAt: time.Now()}
		w.keys[secret] = kc
		return kc
	}
	defaultPolicy := `{"max_attempts":3,"filters":{"same_model":true,"effort":"any"},"order_by":["priority"]}`
	mk(secretMain, "main", primary, defaultPolicy, nil)
	tiny := mk(secretTiny, "tiny", primary, defaultPolicy, nil)
	tiny.Budgets = []store.Budget{keyBudget(1000)} // 0.000001 USD: any request exceeds it
	mk(secretRPM, "rpm", primary, defaultPolicy, func(k *store.Key) { k.RPMLimit = 1 })
	mk(secretDead, "revoked", primary, defaultPolicy, func(k *store.Key) { k.Status = "revoked" })
	past := time.Now().Add(-time.Hour)
	mk(secretOld, "expired", primary, defaultPolicy, func(k *store.Key) { k.ExpiresAt = &past })
	mk(secretNoFB, "nofallback", w.deps["gpt-4o-scripted-fail-medium"], `{"triggers":[]}`, nil)
	mk(secretLimit, "allowlist", primary, defaultPolicy, func(k *store.Key) {
		k.AllowedDeployments = []uuid.UUID{primary.ID, w.deps["gpt-4o-scripted-1m-medium"].ID}
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// A pool that never connects: key misses and TouchKey fail fast, by design.
	pool, err := pgxpool.New(ctx, "postgres://nobody@127.0.0.1:1/none?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	db := &store.DB{Pool: pool}

	loader := catalog.NewLoader(db, rdb, nil, time.Hour, time.Hour)
	loader.Prime(w.snap, w.keys)

	bus := events.NewChannelBus(1000, t.TempDir())
	bus.Subscribe("test", w.events.handle)
	go bus.Run(ctx)

	cfg := &config.Config{MaxInflight: maxInflight, ReservationTTL: 660 * time.Second, DegradedTolerancePct: 5}
	reg := adapters.Registry{"scripted": w.adapter}
	g := New(cfg, loader, w.enf, ratelimit.New(rdb), w.breakers, reg, bus, db)
	w.srv = httptest.NewServer(g.Router())
	t.Cleanup(w.srv.Close)
	return w
}

func (w *world) do(method, path, secret string, body any, headers map[string]string) (*http.Response, []byte) {
	w.t.Helper()
	var rd io.Reader
	if body != nil {
		switch b := body.(type) {
		case string:
			rd = strings.NewReader(b)
		default:
			j, _ := json.Marshal(b)
			rd = bytes.NewReader(j)
		}
	}
	req, _ := http.NewRequest(method, w.srv.URL+path, rd)
	if secret != "" {
		req.Header.Set("Authorization", "Bearer "+secret)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := w.srv.Client().Do(req)
	if err != nil {
		w.t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, b
}

func chatBody(text string) map[string]any {
	return map[string]any{"messages": []map[string]any{{"role": "user", "content": text}}}
}

func errorOf(t *testing.T, b []byte) (typ, code string) {
	t.Helper()
	var out struct {
		Error struct{ Type, Code string } `json:"error"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("not an error body: %s", b)
	}
	return out.Error.Type, out.Error.Code
}

func (w *world) budgetField(kc *catalog.KeyConfig, scopeType, field string) int64 {
	w.t.Helper()
	for _, s := range budget.Resolve(kc.Budgets, time.Now()) {
		if s.Budget.ScopeType == scopeType {
			v := w.mr.HGet(s.RedisKey(), field)
			if v == "" {
				return 0
			}
			n, _ := strconv.ParseInt(v, 10, 64)
			return n
		}
	}
	w.t.Fatalf("no %s scope", scopeType)
	return 0
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met")
}

// ---- operational endpoints (PLAN §6.1)

func TestOperationalEndpoints(t *testing.T) {
	w := newWorld(t, 10)
	if resp, b := w.do("GET", "/healthz", "", nil, nil); resp.StatusCode != 200 || string(b) != "ok" {
		t.Fatalf("healthz %d %s", resp.StatusCode, b)
	}
	if resp, _ := w.do("GET", "/readyz", "", nil, nil); resp.StatusCode != 200 {
		t.Fatalf("readyz %d", resp.StatusCode)
	}
	resp, b := w.do("GET", "/metrics", "", nil, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("metrics %d", resp.StatusCode)
	}
	// PLAN §11 metric names must be registered.
	for _, m := range []string{"gateway_inflight", "gateway_enforcement_degraded", "gateway_reservation_expired_total", "redis_op_duration_seconds"} {
		if !bytes.Contains(b, []byte(m)) {
			t.Errorf("metric %s missing from /metrics", m)
		}
	}
}

// ---- virtual key resolution (keys are the product; vendor auth is out of scope)

func TestKeyChecks(t *testing.T) {
	w := newWorld(t, 10)
	cases := []struct {
		name      string
		secret    string
		headers   map[string]string
		status    int
		typ, code string
	}{
		{"missing", "", nil, 401, "invalid_key", "missing_api_key"},
		{"revoked", secretDead, nil, 401, "key_revoked", "key_revoked"},
		{"expired", secretOld, nil, 401, "key_revoked", "key_expired"},
	}
	for _, c := range cases {
		resp, b := w.do("POST", "/v1/chat/completions", c.secret, chatBody("hi"), c.headers)
		typ, code := errorOf(t, b)
		if resp.StatusCode != c.status || typ != c.typ || code != c.code {
			t.Errorf("%s: %d %s/%s", c.name, resp.StatusCode, typ, code)
		}
	}
	// Unknown key with Postgres unreachable: 503, not a false 401 (PLAN §4.2).
	resp, b := w.do("POST", "/v1/chat/completions", "sk-ll-unknown", chatBody("hi"), nil)
	if typ, code := errorOf(t, b); resp.StatusCode != 503 || typ != "server_error" || code != "key_lookup_unavailable" {
		t.Fatalf("unknown key while db down: %d %s", resp.StatusCode, b)
	}
	// x-api-key is accepted for Anthropic SDK compatibility.
	req, _ := http.NewRequest("GET", w.srv.URL+"/v1/gateway/key", nil)
	req.Header.Set("x-api-key", secretMain)
	r2, err := w.srv.Client().Do(req)
	if err != nil || r2.StatusCode != 200 {
		t.Fatalf("x-api-key: %v %d", err, r2.StatusCode)
	}
	r2.Body.Close()
}

// ---- GET /v1/models and /v1/gateway/key

func TestModelsAndKeyInfo(t *testing.T) {
	w := newWorld(t, 10)
	resp, b := w.do("GET", "/v1/models", secretMain, nil, nil)
	var models struct {
		Object string `json:"object"`
		Data   []struct {
			ID      string         `json:"id"`
			Model   string         `json:"model"`
			Primary bool           `json:"primary"`
			Price   map[string]any `json:"price"`
			Context int            `json:"context_window"`
		} `json:"data"`
	}
	if err := json.Unmarshal(b, &models); err != nil || resp.StatusCode != 200 || models.Object != "list" {
		t.Fatalf("%d %s", resp.StatusCode, b)
	}
	primaries := 0
	for _, m := range models.Data {
		if m.Primary {
			primaries++
			if m.ID != "gpt-4o-scripted-128k-medium" || m.Price["input_per_1m_usd"] != "2.500000000" {
				t.Fatalf("primary row: %+v", m)
			}
		}
	}
	if primaries != 1 || len(models.Data) != len(w.snap.Deployments) {
		t.Fatalf("expected one primary and every active deployment: %+v", models.Data)
	}
	// Allow-listed key sees only its deployments.
	_, b = w.do("GET", "/v1/models", secretLimit, nil, nil)
	json.Unmarshal(b, &models)
	if len(models.Data) != 2 {
		t.Fatalf("allow-list must filter /v1/models: %d", len(models.Data))
	}

	resp, b = w.do("GET", "/v1/gateway/key", secretMain, nil, nil)
	var info map[string]any
	if err := json.Unmarshal(b, &info); err != nil || resp.StatusCode != 200 {
		t.Fatalf("%d %s", resp.StatusCode, b)
	}
	if info["name"] != "main" || info["primary_deployment"] != "gpt-4o-scripted-128k-medium" || info["project"] != "default" || info["enforcement_degraded"] != false {
		t.Fatalf("key info: %v", info)
	}
	budgets := info["budgets"].([]any)
	if len(budgets) != 2 {
		t.Fatalf("both scopes must be listed: %v", budgets)
	}
	kb := budgets[0].(map[string]any)
	if kb["scope_type"] != "key" || kb["limit_usd"] != "10.000000000" || kb["remaining_usd"] != "10.000000000" || kb["period_end"] == nil {
		t.Fatalf("key budget row: %v", kb)
	}
}

// ---- POST /v1/chat/completions, happy path and headers (PLAN §6.1 conventions)

func TestChatNonStreaming(t *testing.T) {
	w := newWorld(t, 10)
	resp, b := w.do("POST", "/v1/chat/completions", secretMain, chatBody("hello"), nil)
	if resp.StatusCode != 200 {
		t.Fatalf("%d %s", resp.StatusCode, b)
	}
	var out struct {
		ID      string `json:"id"`
		Model   string `json:"model"`
		Choices []struct {
			Message      struct{ Role, Content string } `json:"message"`
			FinishReason string                         `json:"finish_reason"`
		} `json:"choices"`
		Usage struct {
			PromptTokens int64 `json:"prompt_tokens"`
		} `json:"usage"`
	}
	json.Unmarshal(b, &out)
	if !strings.HasPrefix(out.ID, "req_") || out.Model != "gpt-4o-scripted-128k-medium" || out.Choices[0].Message.Content != "ok from ok" || out.Choices[0].FinishReason != "stop" {
		t.Fatalf("body: %s", b)
	}
	h := resp.Header
	if h.Get("X-Request-Id") != out.ID || h.Get("X-Served-Deployment") != "gpt-4o-scripted-128k-medium" || h.Get("X-Fallback-Used") != "false" {
		t.Fatalf("headers: %v", h)
	}
	// cost = 100 prompt * 2500 + 20 completion * 10000 (scripted usage)
	wantCost := int64(100*2500 + 20*10000)
	if h.Get("X-Cost-Nano-Usd") != strconv.FormatInt(wantCost, 10) {
		t.Fatalf("X-Cost-Nano-Usd %s want %d", h.Get("X-Cost-Nano-Usd"), wantCost)
	}
	if h.Get("X-Budget-Remaining-Nano-Usd") == "" {
		t.Fatal("X-Budget-Remaining-Nano-Usd missing")
	}
	kc := w.keys[secretMain]
	eventually(t, func() bool {
		return w.budgetField(kc, "key", "spent") == wantCost && w.budgetField(kc, "key", "reserved") == 0
	})
	if w.budgetField(kc, "project", "spent") != wantCost {
		t.Fatal("every scope in the chain must be settled")
	}
	eventually(t, func() bool { return len(w.events.all()) == 1 })
	ev := w.events.all()[0]
	if ev.RequestID != out.ID || ev.StatusCode != 200 || ev.AttemptNo != 1 || ev.AttemptsTotal != 1 || ev.CostNanoUSD != wantCost || ev.UsageSource != "provider" || ev.PriceID != w.price.ID.String() || ev.Enforcement != "redis" || len(ev.Budgets) != 2 {
		t.Fatalf("usage event: %+v", ev)
	}
	if ev.EstimatedCostNanoUSD <= 0 || ev.PromptTokens != 100 || ev.CompletionTokens != 20 {
		t.Fatalf("event tokens/estimate: %+v", ev)
	}
	// A second request is a new request id and adds to spent.
	resp2, _ := w.do("POST", "/v1/chat/completions", secretMain, chatBody("again"), nil)
	if resp2.Header.Get("X-Request-Id") == out.ID {
		t.Fatal("request ids must be unique")
	}
	eventually(t, func() bool { return w.budgetField(kc, "key", "spent") == 2*wantCost })
}

func TestChatStreaming(t *testing.T) {
	w := newWorld(t, 10)
	body := chatBody("stream please")
	body["stream"] = true
	req, _ := http.NewRequest("POST", w.srv.URL+"/v1/chat/completions", bytes.NewReader(mustJSON(body)))
	req.Header.Set("Authorization", "Bearer "+secretMain)
	resp, err := w.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") || resp.Header.Get("X-Served-Deployment") == "" {
		t.Fatalf("%d %v", resp.StatusCode, resp.Header)
	}
	var frames []string
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		if strings.HasPrefix(sc.Text(), "data: ") {
			frames = append(frames, strings.TrimPrefix(sc.Text(), "data: "))
		}
	}
	if len(frames) < 4 || frames[len(frames)-1] != "[DONE]" {
		t.Fatalf("frames: %v", frames)
	}
	var first map[string]any
	json.Unmarshal([]byte(frames[0]), &first)
	delta := first["choices"].([]any)[0].(map[string]any)["delta"].(map[string]any)
	if delta["role"] != "assistant" || delta["content"] != "ok " {
		t.Fatalf("first chunk: %v", first)
	}
	var usageChunk map[string]any
	json.Unmarshal([]byte(frames[len(frames)-2]), &usageChunk)
	if usageChunk["usage"] == nil {
		t.Fatalf("usage chunk must precede [DONE]: %s", frames[len(frames)-2])
	}
	text := ""
	for _, f := range frames[:len(frames)-1] {
		var c map[string]any
		json.Unmarshal([]byte(f), &c)
		for _, ch := range c["choices"].([]any) {
			if d, ok := ch.(map[string]any)["delta"].(map[string]any); ok {
				if s, ok := d["content"].(string); ok {
					text += s
				}
			}
		}
	}
	if text != "ok from ok" {
		t.Fatalf("streamed text %q", text)
	}
	kc := w.keys[secretMain]
	eventually(t, func() bool { return w.budgetField(kc, "key", "spent") == 100*2500+20*10000 })
}

func mustJSON(v any) []byte { b, _ := json.Marshal(v); return b }

// ---- model resolution (PLAN §6.1 `model` conventions)

func TestModelResolution(t *testing.T) {
	w := newWorld(t, 10)
	for _, model := range []string{"", "gpt-4o", "gpt-4o-scripted-128k-medium"} {
		body := chatBody("hi")
		if model != "" {
			body["model"] = model
		}
		resp, _ := w.do("POST", "/v1/chat/completions", secretMain, body, nil)
		if resp.StatusCode != 200 || resp.Header.Get("X-Served-Deployment") != "gpt-4o-scripted-128k-medium" {
			t.Fatalf("model %q: %d served %s", model, resp.StatusCode, resp.Header.Get("X-Served-Deployment"))
		}
	}
	body := chatBody("hi")
	body["model"] = "gpt-4o-scripted-1m-medium"
	resp, _ := w.do("POST", "/v1/chat/completions", secretMain, body, nil)
	if resp.Header.Get("X-Served-Deployment") != "gpt-4o-scripted-1m-medium" {
		t.Fatal("explicit deployment slug must be honoured")
	}
	body["model"] = "no-such-model"
	resp, b := w.do("POST", "/v1/chat/completions", secretMain, body, nil)
	if _, code := errorOf(t, b); resp.StatusCode != 404 || code != "model_not_found" {
		t.Fatalf("%d %s", resp.StatusCode, b)
	}
	body["model"] = "gpt-4o-scripted-slow-medium"
	resp, b = w.do("POST", "/v1/chat/completions", secretLimit, body, nil)
	if _, code := errorOf(t, b); resp.StatusCode != 403 || code != "model_not_allowed" {
		t.Fatalf("allow-list: %d %s", resp.StatusCode, b)
	}
	body["model"] = "gpt-4o-scripted-unpriced-medium"
	resp, b = w.do("POST", "/v1/chat/completions", secretMain, body, nil)
	if typ, code := errorOf(t, b); resp.StatusCode != 503 || typ != "no_healthy_deployment" || code != "unpriced_deployment" {
		t.Fatalf("unpriced: %d %s", resp.StatusCode, b)
	}
}

func TestRequestValidationAndFormats(t *testing.T) {
	w := newWorld(t, 10)
	resp, b := w.do("POST", "/v1/chat/completions", secretMain, `{"messages":[]}`, nil)
	if typ, _ := errorOf(t, b); resp.StatusCode != 400 || typ != "invalid_request_error" {
		t.Fatalf("empty messages: %d %s", resp.StatusCode, b)
	}
	resp, b = w.do("POST", "/v1/chat/completions", secretMain, `{bad json`, nil)
	if resp.StatusCode != 400 {
		t.Fatalf("bad json: %d %s", resp.StatusCode, b)
	}
	resp, b = w.do("POST", "/v1/chat/completions", secretMain, "", nil)
	if resp.StatusCode != 400 {
		t.Fatalf("empty body: %d %s", resp.StatusCode, b)
	}
	// Only the openai response format exists in the MVP (PLAN §9).
	resp, b = w.do("POST", "/v1/chat/completions", secretMain, chatBody("hi"), map[string]string{"X-Response-Format": "anthropic"})
	if _, code := errorOf(t, b); resp.StatusCode != 400 || code != "unsupported_response_format" {
		t.Fatalf("response format: %d %s", resp.StatusCode, b)
	}
	// Rejections before any provider call still log an event, and never charge.
	kc := w.keys[secretMain]
	time.Sleep(600 * time.Millisecond)
	if w.budgetField(kc, "key", "spent") != 0 {
		t.Fatal("rejected requests must not be billed")
	}
}

// ---- budgets (PLAN §8.2) and rate limits

func TestBudgetExceeded(t *testing.T) {
	w := newWorld(t, 10)
	resp, b := w.do("POST", "/v1/chat/completions", secretTiny, chatBody("hi"), nil)
	typ, code := errorOf(t, b)
	if resp.StatusCode != 429 || typ != "budget_exceeded" || code != "budget_exceeded" || resp.Header.Get("X-Budget-Denied-Scope") != "key" {
		t.Fatalf("%d %s %v", resp.StatusCode, b, resp.Header)
	}
	if got := w.adapter.seen(); len(got) != 0 {
		t.Fatal("a blocked request must never reach the provider")
	}
	eventually(t, func() bool { return len(w.events.all()) == 1 })
	ev := w.events.all()[0]
	if ev.StatusCode != 429 || ev.ErrorClass != "budget_exceeded" || ev.DeniedBudgetID == nil || *ev.DeniedBudgetID != w.keys[secretTiny].Budgets[0].ID {
		t.Fatalf("rejection event: %+v", ev)
	}
	// The chain is enforced: exhaust the shared project budget, main key is blocked too... unless on_exceed=alert (it is).
	kc := w.keys[secretMain]
	for _, s := range budget.Resolve(kc.Budgets, time.Now()) {
		if s.Budget.ScopeType == "project" {
			w.mr.HSet(s.RedisKey(), "spent", strconv.FormatInt(*s.Budget.LimitNanoUSD+1, 10))
		}
	}
	resp, _ = w.do("POST", "/v1/chat/completions", secretMain, chatBody("hi"), nil)
	if resp.StatusCode != 200 {
		t.Fatal("alert-mode project budget must let the request through")
	}
	// Flip it to block: now the key is denied by the project scope.
	for i := range kc.Budgets {
		if kc.Budgets[i].ScopeType == "project" {
			kc.Budgets[i].OnExceed = "block"
		}
	}
	resp, _ = w.do("POST", "/v1/chat/completions", secretMain, chatBody("hi"), nil)
	if resp.StatusCode != 429 || resp.Header.Get("X-Budget-Denied-Scope") != "project" {
		t.Fatalf("project scope must block: %d %s", resp.StatusCode, resp.Header.Get("X-Budget-Denied-Scope"))
	}
}

func TestBudgetRemainingHeaderIsMostRestrictiveScope(t *testing.T) {
	w := newWorld(t, 10)
	resp, _ := w.do("POST", "/v1/chat/completions", secretMain, chatBody("hi"), nil)
	rem, _ := strconv.ParseInt(resp.Header.Get("X-Budget-Remaining-Nano-Usd"), 10, 64)
	if rem != 10*pricing.NanoPerUSD { // key: $10, project: $100 -> key wins; read before settle
		t.Fatalf("remaining %d", rem)
	}
}

func TestRateLimit(t *testing.T) {
	w := newWorld(t, 10)
	if resp, _ := w.do("POST", "/v1/chat/completions", secretRPM, chatBody("hi"), nil); resp.StatusCode != 200 {
		t.Fatal("first request within rpm")
	}
	resp, b := w.do("POST", "/v1/chat/completions", secretRPM, chatBody("hi"), nil)
	typ, code := errorOf(t, b)
	if resp.StatusCode != 429 || typ != "rate_limited" || code != "rate_limited_rpm" || resp.Header.Get("Retry-After") != "60" {
		t.Fatalf("%d %s %v", resp.StatusCode, b, resp.Header)
	}
	kc := w.keys[secretRPM]
	if w.budgetField(kc, "key", "reserved") != 0 {
		t.Fatal("rate-limited request must not hold a reservation")
	}
}

// ---- fallback (PLAN §8.1, §7 step 7)

func TestFallbackOnRetriableError(t *testing.T) {
	w := newWorld(t, 10)
	body := chatBody("hi")
	body["model"] = "gpt-4o-scripted-fail-medium"
	resp, b := w.do("POST", "/v1/chat/completions", secretMain, body, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("%d %s", resp.StatusCode, b)
	}
	if resp.Header.Get("X-Fallback-Used") != "true" || resp.Header.Get("X-Served-Deployment") == "gpt-4o-scripted-fail-medium" {
		t.Fatalf("headers: %v", resp.Header)
	}
	calls := w.adapter.seen()
	if len(calls) < 2 || calls[0] != "fail500" {
		t.Fatalf("calls: %v", calls)
	}
	eventually(t, func() bool { return len(w.events.all()) == len(calls) })
	evs := w.events.all()
	if evs[0].StatusCode != 502 || evs[0].ErrorClass != "http_5xx" || evs[0].AttemptNo != 1 || evs[0].FallbackUsed {
		t.Fatalf("first attempt event: %+v", evs[0])
	}
	last := evs[len(evs)-1]
	if last.StatusCode != 200 || !last.FallbackUsed || last.FallbackReason != "http_5xx" || last.AttemptsTotal != len(calls) {
		t.Fatalf("final attempt event: %+v", last)
	}
	// Failed attempt without usage is not billed; the serving attempt is.
	if evs[0].CostNanoUSD != 0 || last.CostNanoUSD == 0 {
		t.Fatalf("billing per attempt: %d %d", evs[0].CostNanoUSD, last.CostNanoUSD)
	}
}

func TestFailedAttemptWithUsageIsCharged(t *testing.T) {
	// PLAN §5.2: every provider attempt that produced usage is charged.
	w := newWorld(t, 10)
	body := chatBody("hi")
	body["model"] = "gpt-4o-scripted-partial-medium"
	resp, _ := w.do("POST", "/v1/chat/completions", secretMain, body, nil)
	if resp.StatusCode != 200 || resp.Header.Get("X-Fallback-Used") != "true" {
		t.Fatalf("%d %v", resp.StatusCode, resp.Header)
	}
	partialCost := int64(100*2500 + 5*10000)
	okCost := int64(100*2500 + 20*10000)
	kc := w.keys[secretMain]
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && w.budgetField(kc, "key", "spent") != partialCost+okCost {
		time.Sleep(20 * time.Millisecond)
	}
	if got := w.budgetField(kc, "key", "spent"); got != partialCost+okCost {
		t.Fatalf("spent %d want %d (partial %d + ok %d); calls=%v events=%d", got, partialCost+okCost, partialCost, okCost, w.adapter.seen(), len(w.events.all()))
	}
	eventually(t, func() bool { return len(w.events.all()) == len(w.adapter.seen()) })
	if ev := w.events.all()[0]; ev.CostNanoUSD != partialCost || ev.StatusCode != 502 {
		t.Fatalf("partial attempt: %+v", ev)
	}
}

func TestNoFallbackOnClientError(t *testing.T) {
	w := newWorld(t, 10)
	body := chatBody("hi")
	body["model"] = "gpt-4o-scripted-client-medium"
	resp, b := w.do("POST", "/v1/chat/completions", secretMain, body, nil)
	if typ, code := errorOf(t, b); resp.StatusCode != 400 || typ != "invalid_request_error" || code != "bad_param" {
		t.Fatalf("%d %s", resp.StatusCode, b)
	}
	if calls := w.adapter.seen(); len(calls) != 1 {
		t.Fatalf("client errors must not fall back: %v", calls)
	}
}

func TestFallbackCanBeDisabled(t *testing.T) {
	w := newWorld(t, 10)
	body := chatBody("hi")
	body["model"] = "gpt-4o-scripted-fail-medium"
	resp, b := w.do("POST", "/v1/chat/completions", secretMain, body, map[string]string{"X-Fallback": "off"})
	if typ, _ := errorOf(t, b); resp.StatusCode != 502 || typ != "upstream_error" {
		t.Fatalf("X-Fallback: off: %d %s", resp.StatusCode, b)
	}
	if len(w.adapter.seen()) != 1 {
		t.Fatal("exactly one attempt with fallback off")
	}
	// Policy with no triggers behaves the same.
	resp, _ = w.do("POST", "/v1/chat/completions", secretNoFB, chatBody("hi"), nil)
	if resp.StatusCode != 502 || len(w.adapter.seen()) != 2 {
		t.Fatalf("empty triggers: %d calls=%v", resp.StatusCode, w.adapter.seen())
	}
	// Reservation released even on failure.
	kc := w.keys[secretMain]
	eventually(t, func() bool { return w.budgetField(kc, "key", "reserved") == 0 })
}

func TestMaxAttemptsCapsChain(t *testing.T) {
	w := newWorld(t, 10)
	kc := w.keys[secretMain]
	kc.Policy = catalog.ParsePolicy([]byte(`{"max_attempts":1,"filters":{"same_model":true,"effort":"any"}}`))
	body := chatBody("hi")
	body["model"] = "gpt-4o-scripted-fail-medium"
	resp, _ := w.do("POST", "/v1/chat/completions", secretMain, body, nil)
	if resp.StatusCode != 502 || len(w.adapter.seen()) != 1 {
		t.Fatalf("max_attempts=1: %d calls=%v", resp.StatusCode, w.adapter.seen())
	}
}

func TestContextFallbackAndTooLarge(t *testing.T) {
	w := newWorld(t, 10)
	big := int64(1000 * pricing.NanoPerUSD) // a 2M-token reservation is ~$20; keep budget out of the way
	w.keys[secretMain].Budgets[0].LimitNanoUSD = &big
	// 200k max_tokens cannot fit the 128k primary: skip it, serve from the 1M deployment.
	body := chatBody("hi")
	body["max_tokens"] = 200000
	resp, b := w.do("POST", "/v1/chat/completions", secretMain, body, nil)
	if resp.StatusCode != 200 || resp.Header.Get("X-Served-Deployment") != "gpt-4o-scripted-1m-medium" || resp.Header.Get("X-Fallback-Used") != "true" {
		t.Fatalf("%d %s %v", resp.StatusCode, b, resp.Header)
	}
	if calls := w.adapter.seen(); len(calls) != 1 {
		t.Fatalf("primary must be skipped without a call: %v", calls)
	}
	eventually(t, func() bool { return len(w.events.all()) == 1 })
	if ev := w.events.all()[0]; ev.FallbackReason != "context_too_small" {
		t.Fatalf("fallback reason: %+v", ev)
	}
	// Nothing fits 2M tokens.
	body["max_tokens"] = 2_000_000
	resp, b = w.do("POST", "/v1/chat/completions", secretMain, body, nil)
	if typ, code := errorOf(t, b); resp.StatusCode != 400 || typ != "invalid_request_error" || code != "context_too_large" {
		t.Fatalf("too large: %d %s", resp.StatusCode, b)
	}
}

func TestBreakerSkipsPrimary(t *testing.T) {
	w := newWorld(t, 10)
	kc := w.keys[secretMain]
	primary := w.snap.ByID[kc.Key.PrimaryDeploymentID]
	for i := 0; i < 5; i++ {
		w.breakers.Record(primary.ID, primary.Slug, false)
	}
	resp, _ := w.do("POST", "/v1/chat/completions", secretMain, chatBody("hi"), nil)
	if resp.StatusCode != 200 || resp.Header.Get("X-Served-Deployment") == primary.Slug || resp.Header.Get("X-Fallback-Used") != "true" {
		t.Fatalf("%d %v", resp.StatusCode, resp.Header)
	}
	eventually(t, func() bool { return len(w.events.all()) >= 1 })
	if ev := w.events.all()[0]; ev.FallbackReason != "breaker_open" {
		t.Fatalf("reason: %+v", ev)
	}
	// Once no healthy deployment remains: 503 no_healthy_deployment.
	for _, d := range w.snap.Deployments {
		for i := 0; i < 5; i++ {
			w.breakers.Record(d.ID, d.Slug, false)
		}
	}
	resp, b := w.do("POST", "/v1/chat/completions", secretMain, chatBody("hi"), nil)
	if typ, _ := errorOf(t, b); resp.StatusCode != 503 || typ != "no_healthy_deployment" {
		t.Fatalf("all open: %d %s", resp.StatusCode, b)
	}
}

// ---- usage source fallback (PLAN §5.2)

func TestEstimatedUsageWhenProviderSendsNone(t *testing.T) {
	w := newWorld(t, 10)
	body := chatBody("hello there")
	body["model"] = "gpt-4o-scripted-nousage-medium"
	resp, _ := w.do("POST", "/v1/chat/completions", secretMain, body, nil)
	if resp.StatusCode != 200 {
		t.Fatal(resp.StatusCode)
	}
	cost, _ := strconv.ParseInt(resp.Header.Get("X-Cost-Nano-Usd"), 10, 64)
	if cost <= 0 {
		t.Fatal("estimated usage must still be priced")
	}
	eventually(t, func() bool { return len(w.events.all()) == 1 })
	ev := w.events.all()[0]
	if ev.UsageSource != "estimated" || ev.PromptTokens <= 0 || ev.CompletionTokens <= 0 || ev.CostNanoUSD != cost {
		t.Fatalf("event: %+v", ev)
	}
	kc := w.keys[secretMain]
	eventually(t, func() bool { return w.budgetField(kc, "key", "spent") == cost })
}

// ---- POST /v1/gateway/estimate

func TestEstimate(t *testing.T) {
	w := newWorld(t, 10)
	body := chatBody("estimate me")
	body["max_tokens"] = 100
	resp, b := w.do("POST", "/v1/gateway/estimate", secretMain, body, nil)
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil || resp.StatusCode != 200 {
		t.Fatalf("%d %s", resp.StatusCode, b)
	}
	prompt := int64(out["prompt_tokens_estimate"].(float64))
	if prompt <= 0 || out["max_output_tokens"].(float64) != 100 || out["fits_requested_deployment"] != true || out["allowed"] != true || out["would_serve"] != "gpt-4o-scripted-128k-medium" {
		t.Fatalf("estimate: %v", out)
	}
	if int64(out["estimated_cost_nano_usd"].(float64)) != w.price.Estimate(prompt, 100) {
		t.Fatalf("cost must equal Price.Estimate: %v", out)
	}
	if len(w.adapter.seen()) != 0 {
		t.Fatal("estimate must not call a provider")
	}
	body["max_tokens"] = 200000
	_, b = w.do("POST", "/v1/gateway/estimate", secretMain, body, nil)
	json.Unmarshal(b, &out)
	if out["fits_requested_deployment"] != false || out["would_serve"] != "gpt-4o-scripted-1m-medium" || out["estimated_cost_on_served_nano_usd"] == nil {
		t.Fatalf("context overflow estimate: %v", out)
	}
	_, b = w.do("POST", "/v1/gateway/estimate", secretTiny, chatBody("x"), nil)
	json.Unmarshal(b, &out)
	if out["allowed"] != false || out["budgets"].([]any)[0].(map[string]any)["allows_request"] != false {
		t.Fatalf("tiny budget must report not allowed: %v", out)
	}
}

// ---- POST /v1/embeddings

func TestEmbeddings(t *testing.T) {
	w := newWorld(t, 10)
	resp, b := w.do("POST", "/v1/embeddings", secretMain, map[string]any{"input": []string{"alpha", "beta"}}, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("%d %s", resp.StatusCode, b)
	}
	var out struct {
		Object string `json:"object"`
		Data   []struct {
			Embedding []float32 `json:"embedding"`
		} `json:"data"`
		Usage struct {
			PromptTokens int64 `json:"prompt_tokens"`
		} `json:"usage"`
	}
	json.Unmarshal(b, &out)
	if out.Object != "list" || len(out.Data) != 2 || out.Usage.PromptTokens <= 0 {
		t.Fatalf("body: %s", b)
	}
	cost, _ := strconv.ParseInt(resp.Header.Get("X-Cost-Nano-Usd"), 10, 64)
	if cost != out.Usage.PromptTokens*2500 {
		t.Fatalf("embedding cost %d for %d tokens", cost, out.Usage.PromptTokens)
	}
	kc := w.keys[secretMain]
	eventually(t, func() bool {
		return w.budgetField(kc, "key", "spent") == cost && w.budgetField(kc, "key", "reserved") == 0
	})
	resp, b = w.do("POST", "/v1/embeddings", secretTiny, map[string]any{"input": "x"}, nil)
	if typ, _ := errorOf(t, b); resp.StatusCode != 429 || typ != "budget_exceeded" {
		t.Fatalf("embeddings budget: %d %s", resp.StatusCode, b)
	}
	resp, _ = w.do("POST", "/v1/embeddings", secretMain, `{"input":[]}`, nil)
	if resp.StatusCode != 400 {
		t.Fatal("empty input")
	}
}

// ---- backpressure (PLAN §10)

func TestInflightLimit(t *testing.T) {
	w := newWorld(t, 1)
	body := chatBody("hi")
	body["model"] = "gpt-4o-scripted-slow-medium"
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.do("POST", "/v1/chat/completions", secretMain, body, nil)
	}()
	time.Sleep(100 * time.Millisecond)
	resp, b := w.do("POST", "/v1/chat/completions", secretMain, chatBody("hi"), nil)
	if _, code := errorOf(t, b); resp.StatusCode != 503 || code != "overloaded" || resp.Header.Get("Retry-After") != "1" {
		t.Fatalf("at capacity: %d %s %v", resp.StatusCode, b, resp.Header)
	}
	<-done
	if resp, _ := w.do("POST", "/v1/chat/completions", secretMain, chatBody("hi"), nil); resp.StatusCode != 200 {
		t.Fatal("slot must be released after the slow request finishes")
	}
}

// ---- degraded enforcement (PLAN §4.2)

func TestRedisDownDegradesInsteadOfFailing(t *testing.T) {
	w := newWorld(t, 10)
	if resp, _ := w.do("POST", "/v1/chat/completions", secretMain, chatBody("warm"), nil); resp.StatusCode != 200 {
		t.Fatal("warm-up")
	}
	w.mr.Close()
	resp, b := w.do("POST", "/v1/chat/completions", secretMain, chatBody("hi"), nil)
	if resp.StatusCode != 200 {
		t.Fatalf("availability first: %d %s", resp.StatusCode, b)
	}
	if resp.Header.Get("X-Budget-Enforcement") != "degraded" {
		t.Fatalf("degraded header missing: %v", resp.Header)
	}
	eventually(t, func() bool {
		for _, ev := range w.events.all() {
			if ev.Enforcement == "local" && ev.StatusCode == 200 {
				return true
			}
		}
		return false
	})
	_, b = w.do("GET", "/v1/gateway/key", secretMain, nil, nil)
	var info map[string]any
	json.Unmarshal(b, &info)
	if info["enforcement_degraded"] != true {
		t.Fatalf("key info must expose degraded mode: %v", info["enforcement_degraded"])
	}
	// Strict budgets fail closed with 503.
	kc := w.keys[secretMain]
	kc.Budgets[0].Enforcement = "strict"
	resp, b = w.do("POST", "/v1/chat/completions", secretMain, chatBody("hi"), nil)
	if _, code := errorOf(t, b); resp.StatusCode != 503 || code != "budget_unavailable" {
		t.Fatalf("strict: %d %s", resp.StatusCode, b)
	}
}

func TestRequestOverridesCapMaxTokens(t *testing.T) {
	w := newWorld(t, 10)
	kc := w.keys[secretMain]
	kc.Key.RequestOverrides = []byte(`{"max_tokens_cap":50}`)
	body := chatBody("hi")
	body["max_tokens"] = 4000
	_, b := w.do("POST", "/v1/gateway/estimate", secretMain, body, nil)
	var out map[string]any
	json.Unmarshal(b, &out)
	if out["max_output_tokens"].(float64) != 50 {
		t.Fatalf("override cap not applied: %v", out["max_output_tokens"])
	}
}
