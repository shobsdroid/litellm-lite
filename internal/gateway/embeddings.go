package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/rzp/litellm-lite/internal/adapters"
	"github.com/rzp/litellm-lite/internal/budget"
	"github.com/rzp/litellm-lite/internal/canonical"
	"github.com/rzp/litellm-lite/internal/catalog"
	"github.com/rzp/litellm-lite/internal/events"
	"github.com/rzp/litellm-lite/internal/formats"
	"github.com/rzp/litellm-lite/internal/metrics"
)

// embeddings is POST /v1/embeddings. Single attempt, no fallback (embeddings
// from a different model are not interchangeable).
func (g *Gateway) embeddings(w http.ResponseWriter, r *http.Request) {
	if !g.acquire(w) {
		return
	}
	defer g.release()
	start := time.Now()
	kc := keyFrom(r)
	rid := newRequestID()
	w.Header().Set("X-Request-Id", rid)
	body, err := readBody(r)
	if err != nil {
		writeError(w, 400, "invalid_request_error", "bad_body", err.Error())
		return
	}
	ereq, err := formats.DecodeOpenAIEmbeddings(body)
	if err != nil {
		writeError(w, 400, "invalid_request_error", "invalid_request", err.Error())
		return
	}
	snap := g.loader.Snapshot()
	dep, aerr := g.resolveDeployment(snap, kc, ereq.Model)
	if aerr != nil {
		writeError(w, aerr.Status, aerr.Type, aerr.Code, aerr.Message)
		return
	}
	if dep.Price == nil {
		writeError(w, 503, "no_healthy_deployment", "unpriced_deployment", "deployment has no price")
		return
	}
	var promptTokens int64
	for _, s := range ereq.Input {
		promptTokens += g.tk.CountText(dep.Model.Tokenizer, s)
	}
	estimate := dep.Price.Estimate(promptTokens, 0)
	ev := baseEmbeddingEvent(r, kc, rid, dep, start, estimate)

	if rl := g.limiter.Allow(r.Context(), "key:"+kc.Key.ID.String(), kc.Key.RPMLimit, kc.Key.TPMLimit, promptTokens); !rl.Allowed {
		g.reject(ev, 429, "rate_limited", "rate_limited_"+rl.Kind, nil)
		writeError(w, 429, "rate_limited", "rate_limited_"+rl.Kind, "key "+rl.Kind+" limit exceeded")
		return
	}
	scopes := budget.Resolve(kc.Budgets, start)
	ev.Budgets = charges(scopes)
	dec, err := g.enforcer.Reserve(r.Context(), rid, scopes, estimate, promptTokens)
	if err != nil {
		writeError(w, 503, "server_error", "budget_unavailable", err.Error())
		return
	}
	ev.Enforcement = dec.Enforcement
	if !dec.Allowed {
		id := dec.DeniedBy.Budget.ID
		g.reject(ev, 429, "budget_exceeded", "budget_exceeded", &id)
		writeError(w, 429, "budget_exceeded", "budget_exceeded", "the "+dec.DeniedBy.Budget.ScopeType+" budget is exhausted")
		return
	}
	ctx := context.WithoutCancel(r.Context())
	defer g.enforcer.Release(ctx, rid)
	headers := map[string]string{}
	_ = json.Unmarshal(dep.Provider.DefaultHeaders, &headers)
	call := adapters.EmbedCall{Req: ereq, ProviderModel: dep.ProviderModelName, BaseURL: dep.Provider.BaseURL, APIKey: snap.Credentials[dep.ProviderID], Headers: headers}
	cctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	resp, cerr := g.adapters[dep.Provider.Dialect].Embed(cctx, call)
	ev.AttemptNo, ev.AttemptsTotal = 1, 1
	ev.ServedDeploymentID, ev.ServedDeploymentSlug, ev.ProviderSlug, ev.PriceID = dep.ID.String(), dep.Slug, dep.Provider.Slug, dep.Price.ID.String()
	ev.LatencyMS = time.Since(start).Milliseconds()
	ev.UpstreamLatencyMS = ev.LatencyMS
	if cerr != nil {
		ae, ok := cerr.(*adapters.Error)
		if !ok {
			ae = adapters.Classify(cerr, 0, nil)
		}
		g.breakers.Record(dep.ID, dep.Slug, ae.Class == adapters.ClassClient)
		ev.StatusCode, ev.ErrorClass, ev.ErrorCode = ae.Status, ae.Class, ae.Code
		metrics.Requests.WithLabelValues(kc.Project.ID.String(), dep.Slug, dep.Provider.Slug, strconv.Itoa(ae.Status), ae.Class).Inc()
		g.bus.Publish(&ev)
		writeError(w, ae.Status, "upstream_error", ae.Code, ae.Message)
		return
	}
	g.breakers.Record(dep.ID, dep.Slug, true)
	usage := resp.Usage
	if usage.Source != "provider" {
		usage = canonical.Usage{PromptTokens: promptTokens, Source: "estimated"}
	}
	cost := dep.Price.CostFor(usage)
	g.enforcer.Settle(ctx, rid, 1, scopes, cost, usage.Total())
	ev.StatusCode, ev.PromptTokens, ev.CostNanoUSD, ev.UsageSource = 200, usage.PromptTokens, cost, usage.Source
	metrics.Requests.WithLabelValues(kc.Project.ID.String(), dep.Slug, dep.Provider.Slug, "200", "").Inc()
	metrics.Cost.WithLabelValues(kc.Project.ID.String(), dep.Slug).Add(float64(cost))
	g.bus.Publish(&ev)
	w.Header().Set("X-Served-Deployment", dep.Slug)
	w.Header().Set("X-Cost-Nano-Usd", strconv.FormatInt(cost, 10))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	w.Write(formats.EncodeOpenAIEmbeddings(dep.Slug, resp))
}

func baseEmbeddingEvent(r *http.Request, kc *catalog.KeyConfig, rid string, dep *catalog.Deployment, start time.Time, estimate int64) events.UsageEvent {
	return events.UsageEvent{TS: start, RequestID: rid, TraceID: rid, VendorID: kc.Vendor.ID, ProjectID: kc.Project.ID, KeyID: kc.Key.ID,
		IngressFormat: "openai_embeddings", ResponseFormat: "openai", ModelSlug: dep.Model.Slug, RequestedDeploymentID: dep.ID.String(),
		EstimatedCostNanoUSD: estimate, ClientIP: clientIP(r), UserAgent: r.UserAgent(), Enforcement: "redis"}
}
