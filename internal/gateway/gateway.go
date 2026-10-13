// Package gateway is the data plane: the HTTP surface vendor apps call with
// a virtual key. Nothing here touches Postgres on the request path except an
// occasional last_used_at touch.
package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rzp/litellm-lite/internal/adapters"
	"github.com/rzp/litellm-lite/internal/budget"
	"github.com/rzp/litellm-lite/internal/canonical"
	"github.com/rzp/litellm-lite/internal/catalog"
	"github.com/rzp/litellm-lite/internal/config"
	"github.com/rzp/litellm-lite/internal/events"
	"github.com/rzp/litellm-lite/internal/formats"
	"github.com/rzp/litellm-lite/internal/metrics"
	"github.com/rzp/litellm-lite/internal/pricing"
	"github.com/rzp/litellm-lite/internal/ratelimit"
	"github.com/rzp/litellm-lite/internal/router"
	"github.com/rzp/litellm-lite/internal/store"
)

type Gateway struct {
	cfg      *config.Config
	loader   *catalog.Loader
	enforcer *budget.Enforcer
	limiter  *ratelimit.Limiter
	breakers *router.Breakers
	adapters adapters.Registry
	bus      events.Bus
	db       *store.DB
	tk       *pricing.Tokenizer
	inflight chan struct{}
}

func New(cfg *config.Config, loader *catalog.Loader, enf *budget.Enforcer, lim *ratelimit.Limiter, br *router.Breakers, reg adapters.Registry, bus events.Bus, db *store.DB) *Gateway {
	return &Gateway{cfg: cfg, loader: loader, enforcer: enf, limiter: lim, breakers: br, adapters: reg, bus: bus, db: db,
		tk: pricing.Default(), inflight: make(chan struct{}, cfg.MaxInflight)}
}

func (g *Gateway) Router() http.Handler {
	r := chi.NewRouter()
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	r.Get("/readyz", g.ready)
	r.Handle("/metrics", promhttp.Handler())
	r.Group(func(r chi.Router) {
		r.Use(g.auth)
		r.Post("/v1/chat/completions", g.chat)
		r.Post("/v1/embeddings", g.embeddings)
		r.Get("/v1/models", g.models)
		r.Post("/v1/gateway/estimate", g.estimate)
		r.Get("/v1/gateway/key", g.keyInfo)
	})
	return r
}

func (g *Gateway) ready(w http.ResponseWriter, _ *http.Request) {
	if g.loader.Snapshot() == nil {
		http.Error(w, "config not loaded", 503)
		return
	}
	w.Write([]byte("ok"))
}

type ctxKey int

const keyCtx ctxKey = 1

func keyFrom(r *http.Request) *catalog.KeyConfig {
	return r.Context().Value(keyCtx).(*catalog.KeyConfig)
}

func (g *Gateway) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secret := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer"))
		if secret == "" {
			secret = r.Header.Get("x-api-key")
		}
		if secret == "" {
			writeError(w, 401, "invalid_key", "missing_api_key", "provide the virtual key as Authorization: Bearer <key>")
			return
		}
		kc, err := g.loader.ResolveKey(r.Context(), secret)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				writeError(w, 401, "invalid_key", "invalid_api_key", "unknown API key")
			} else {
				writeError(w, 503, "server_error", "key_lookup_unavailable", "key lookup unavailable: "+err.Error())
			}
			return
		}
		now := time.Now()
		switch {
		case kc.Key.Status != "active":
			writeError(w, 401, "key_revoked", "key_revoked", "this key has been revoked")
			return
		case kc.Key.ExpiresAt != nil && now.After(*kc.Key.ExpiresAt):
			writeError(w, 401, "key_revoked", "key_expired", "this key has expired")
			return
		case kc.Project.Status != "active" || kc.Vendor.Status != "active":
			writeError(w, 403, "invalid_key", "project_inactive", "the project or vendor is not active")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), keyCtx, kc)))
	})
}

func writeError(w http.ResponseWriter, status int, typ, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(formats.EncodeOpenAIError(&formats.APIError{Status: status, Type: typ, Code: code, Message: msg}))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func newRequestID() string {
	var b [12]byte
	rand.Read(b[:])
	return "req_" + hex.EncodeToString(b[:])
}

// resolveDeployment maps the client's `model` to a deployment the key may use.
func (g *Gateway) resolveDeployment(snap *catalog.Snapshot, kc *catalog.KeyConfig, model string) (*catalog.Deployment, *formats.APIError) {
	primary := snap.ByID[kc.Key.PrimaryDeploymentID]
	if primary == nil {
		return nil, &formats.APIError{Status: 503, Type: "no_healthy_deployment", Code: "primary_missing", Message: "the key's primary deployment no longer exists"}
	}
	if model == "" || model == primary.Slug || (primary.Model != nil && model == primary.Model.Slug) {
		return primary, nil
	}
	allowed := func(d *catalog.Deployment) bool {
		if len(kc.Key.AllowedDeployments) == 0 {
			return true
		}
		for _, id := range kc.Key.AllowedDeployments {
			if id == d.ID {
				return true
			}
		}
		return false
	}
	if d := snap.BySlug[model]; d != nil {
		if !allowed(d) {
			return nil, &formats.APIError{Status: 403, Type: "invalid_request_error", Code: "model_not_allowed", Message: "deployment " + model + " is not allowed for this key"}
		}
		return d, nil
	}
	if ds := snap.ByModelSlug[model]; len(ds) > 0 {
		cands := []*catalog.Deployment{}
		for _, d := range ds {
			if d.Status == "active" && d.Price != nil && allowed(d) {
				cands = append(cands, d)
			}
		}
		if len(cands) > 0 {
			sort.Slice(cands, func(i, j int) bool { return cands[i].Priority < cands[j].Priority })
			return cands[0], nil
		}
		return nil, &formats.APIError{Status: 403, Type: "invalid_request_error", Code: "model_not_allowed", Message: "no allowed deployment serves model " + model}
	}
	return nil, &formats.APIError{Status: 404, Type: "invalid_request_error", Code: "model_not_found", Message: "unknown model or deployment: " + model}
}

// ---- GET /v1/models

func (g *Gateway) models(w http.ResponseWriter, r *http.Request) {
	kc := keyFrom(r)
	snap := g.loader.Snapshot()
	allowed := map[uuid.UUID]bool{}
	for _, id := range kc.Key.AllowedDeployments {
		allowed[id] = true
	}
	data := []map[string]any{}
	for _, d := range snap.Deployments {
		if d.Status != "active" || (len(allowed) > 0 && !allowed[d.ID]) {
			continue
		}
		row := map[string]any{
			"id": d.Slug, "object": "model", "owned_by": d.Provider.Slug, "created": d.CreatedAt.Unix(),
			"model": d.Model.Slug, "provider": d.Provider.Slug, "context_window": d.ContextWindow, "max_output_tokens": d.MaxOutputTokens,
			"effort": d.Effort, "capabilities": d.Capabilities, "primary": d.ID == kc.Key.PrimaryDeploymentID,
		}
		if d.Price != nil {
			t := d.Price.Tier(0)
			row["price"] = map[string]any{"input_per_1m_usd": pricing.USD(t.Input * 1_000_000), "output_per_1m_usd": pricing.USD(t.Output * 1_000_000), "cached_input_per_1m_usd": pricing.USD(t.CachedInput * 1_000_000)}
		}
		data = append(data, row)
	}
	writeJSON(w, 200, map[string]any{"object": "list", "data": data})
}

// ---- GET /v1/gateway/key

func (g *Gateway) budgetsView(ctx context.Context, kc *catalog.KeyConfig) []map[string]any {
	scopes := budget.Resolve(kc.Budgets, time.Now())
	rems, err := g.enforcer.Remaining(ctx, scopes)
	out := []map[string]any{}
	for i, s := range scopes {
		row := map[string]any{"budget_id": s.Budget.ID, "scope_type": s.Budget.ScopeType, "kind": s.Budget.Kind, "on_exceed": s.Budget.OnExceed,
			"period_start": s.PeriodStart, "period_end": s.PeriodEnd}
		if s.Budget.LimitNanoUSD != nil {
			row["limit_nano_usd"] = *s.Budget.LimitNanoUSD
			row["limit_usd"] = pricing.USD(*s.Budget.LimitNanoUSD)
		}
		if s.Budget.LimitTokens != nil {
			row["limit_tokens"] = *s.Budget.LimitTokens
		}
		if err == nil {
			row["spent_nano_usd"] = rems[i].Spent
			row["spent_usd"] = pricing.USD(rems[i].Spent)
			row["reserved_nano_usd"] = rems[i].Reserved
			row["spent_tokens"] = rems[i].Tokens
			if rems[i].RemainingNS != nil {
				row["remaining_nano_usd"] = *rems[i].RemainingNS
				row["remaining_usd"] = pricing.USD(*rems[i].RemainingNS)
			}
		} else {
			row["live_counters"] = "unavailable"
		}
		out = append(out, row)
	}
	return out
}

func (g *Gateway) keyInfo(w http.ResponseWriter, r *http.Request) {
	kc := keyFrom(r)
	snap := g.loader.Snapshot()
	primary := snap.ByID[kc.Key.PrimaryDeploymentID]
	resp := map[string]any{
		"key_id": kc.Key.ID, "name": kc.Key.Name, "key_prefix": kc.Key.KeyPrefix, "project_id": kc.Project.ID, "project": kc.Project.Name, "vendor_id": kc.Vendor.ID,
		"response_format": kc.Key.ResponseFormat, "rpm_limit": kc.Key.RPMLimit, "tpm_limit": kc.Key.TPMLimit, "expires_at": kc.Key.ExpiresAt,
		"fallback_policy": kc.Policy, "budgets": g.budgetsView(r.Context(), kc), "enforcement_degraded": g.enforcer.Degraded(),
	}
	if primary != nil {
		resp["primary_deployment"] = primary.Slug
	}
	writeJSON(w, 200, resp)
}

// ---- POST /v1/gateway/estimate

type plan struct {
	dep          *catalog.Deployment
	req          *canonical.Request
	promptTokens int64
	maxOut       int64
	needed       int64
	estimate     int64
}

func (g *Gateway) planRequest(snap *catalog.Snapshot, kc *catalog.KeyConfig, req *canonical.Request) (*plan, *formats.APIError) {
	dep, aerr := g.resolveDeployment(snap, kc, req.Model)
	if aerr != nil {
		return nil, aerr
	}
	if dep.Price == nil {
		return nil, &formats.APIError{Status: 503, Type: "no_healthy_deployment", Code: "unpriced_deployment", Message: "deployment " + dep.Slug + " has no price and cannot be served"}
	}
	applyOverrides(kc.Key.RequestOverrides, req)
	p := &plan{dep: dep, req: req}
	p.promptTokens = g.tk.CountRequest(dep.Model.Tokenizer, dep.Model.TokenizerMultiplier, req)
	p.maxOut = int64(req.MaxTokens)
	if p.maxOut == 0 {
		p.maxOut = int64(dep.MaxOutputTokens)
	}
	p.needed = p.promptTokens + p.maxOut
	p.estimate = dep.Price.Estimate(p.promptTokens, p.maxOut)
	return p, nil
}

func applyOverrides(raw json.RawMessage, req *canonical.Request) {
	if len(raw) == 0 {
		return
	}
	var o struct {
		MaxTokensCap int      `json:"max_tokens_cap"`
		Temperature  *float64 `json:"temperature"`
	}
	if json.Unmarshal(raw, &o) != nil {
		return
	}
	if o.MaxTokensCap > 0 && (req.MaxTokens == 0 || req.MaxTokens > o.MaxTokensCap) {
		req.MaxTokens = o.MaxTokensCap
	}
	if o.Temperature != nil {
		req.Temperature = o.Temperature
	}
}

func (g *Gateway) estimate(w http.ResponseWriter, r *http.Request) {
	kc := keyFrom(r)
	body, err := readBody(r)
	if err != nil {
		writeError(w, 400, "invalid_request_error", "bad_body", err.Error())
		return
	}
	req, err := formats.DecodeOpenAIChat(body)
	if err != nil {
		writeError(w, 400, "invalid_request_error", "invalid_request", err.Error())
		return
	}
	snap := g.loader.Snapshot()
	p, aerr := g.planRequest(snap, kc, req)
	if aerr != nil {
		writeError(w, aerr.Status, aerr.Type, aerr.Code, aerr.Message)
		return
	}
	fits := p.needed <= int64(p.dep.ContextWindow)
	would := p.dep
	var skips []router.Skip
	if !fits {
		cands, s := router.Candidates(snap, p.dep, kc.Policy, p.needed, kc.Key.AllowedDeployments, nil)
		skips = s
		if len(cands) > 0 {
			would = cands[0]
		} else {
			would = nil
		}
	}
	scopes := budget.Resolve(kc.Budgets, time.Now())
	rems, _ := g.enforcer.Remaining(r.Context(), scopes)
	allowed := would != nil
	budgets := []map[string]any{}
	for i, s := range scopes {
		row := map[string]any{"budget_id": s.Budget.ID, "scope_type": s.Budget.ScopeType, "kind": s.Budget.Kind}
		if rems != nil && s.Budget.LimitNanoUSD != nil {
			ok := rems[i].Spent+rems[i].Reserved+p.estimate <= *s.Budget.LimitNanoUSD || s.Budget.OnExceed != "block"
			row["limit_nano_usd"] = *s.Budget.LimitNanoUSD
			row["spent_nano_usd"] = rems[i].Spent
			row["reserved_nano_usd"] = rems[i].Reserved
			row["allows_request"] = ok
			if !ok {
				allowed = false
			}
		}
		budgets = append(budgets, row)
	}
	resp := map[string]any{
		"requested_deployment": p.dep.Slug, "prompt_tokens_estimate": p.promptTokens, "max_output_tokens": p.maxOut, "needed_context": p.needed,
		"fits_requested_deployment": fits, "estimated_cost_nano_usd": p.estimate, "estimated_cost_usd": pricing.USD(p.estimate),
		"allowed": allowed, "budgets": budgets,
	}
	if would != nil {
		resp["would_serve"] = would.Slug
		if would != p.dep {
			resp["estimated_cost_on_served_nano_usd"] = would.Price.Estimate(p.promptTokens, p.maxOut)
		}
	} else {
		resp["reason"] = "no deployment in the fallback chain fits the required context"
		sk := []map[string]any{}
		for _, s := range skips {
			sk = append(sk, map[string]any{"deployment": s.Deployment.Slug, "reason": s.Reason})
		}
		resp["skipped"] = sk
	}
	writeJSON(w, 200, resp)
}

func readBody(r *http.Request) ([]byte, error) {
	b, err := readAllLimit(r, 10<<20)
	if err != nil {
		return nil, err
	}
	if len(b) == 0 {
		return nil, errors.New("empty body")
	}
	return b, nil
}

func readAllLimit(r *http.Request, n int64) ([]byte, error) {
	lr := http.MaxBytesReader(nil, r.Body, n)
	buf := make([]byte, 0, 4096)
	tmp := make([]byte, 32*1024)
	for {
		k, err := lr.Read(tmp)
		buf = append(buf, tmp[:k]...)
		if err != nil {
			if err.Error() == "EOF" {
				return buf, nil
			}
			return nil, err
		}
	}
}

func clientIP(r *http.Request) string {
	if ip := r.Header.Get("X-Forwarded-For"); ip != "" {
		return strings.TrimSpace(strings.Split(ip, ",")[0])
	}
	host := r.RemoteAddr
	if i := strings.LastIndex(host, ":"); i > 0 {
		host = host[:i]
	}
	return host
}

func (g *Gateway) acquire(w http.ResponseWriter) bool {
	select {
	case g.inflight <- struct{}{}:
		metrics.Inflight.Inc()
		return true
	default:
		w.Header().Set("Retry-After", "1")
		writeError(w, 503, "server_error", "overloaded", "gateway at capacity, retry shortly")
		return false
	}
}

func (g *Gateway) release() {
	<-g.inflight
	metrics.Inflight.Dec()
}
