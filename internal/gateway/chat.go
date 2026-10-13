package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/rzp/litellm-lite/internal/adapters"
	"github.com/rzp/litellm-lite/internal/budget"
	"github.com/rzp/litellm-lite/internal/canonical"
	"github.com/rzp/litellm-lite/internal/catalog"
	"github.com/rzp/litellm-lite/internal/events"
	"github.com/rzp/litellm-lite/internal/formats"
	"github.com/rzp/litellm-lite/internal/metrics"
	"github.com/rzp/litellm-lite/internal/pricing"
	"github.com/rzp/litellm-lite/internal/router"
)

type attempt struct {
	dep      *catalog.Deployment
	usage    canonical.Usage
	cost     int64
	status   int
	class    string
	code     string
	reason   string // why this attempt happened (fallback reason), "" for primary
	started  time.Time
	ttft     time.Duration
	finished time.Time
}

// chat is POST /v1/chat/completions.
func (g *Gateway) chat(w http.ResponseWriter, r *http.Request) {
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
	req, err := formats.DecodeOpenAIChat(body)
	if err != nil {
		writeError(w, 400, "invalid_request_error", "invalid_request", err.Error())
		return
	}
	respFormat := kc.Key.ResponseFormat
	if f := r.Header.Get("X-Response-Format"); f != "" {
		respFormat = f
	}
	if respFormat != "openai" {
		writeError(w, 400, "invalid_request_error", "unsupported_response_format", "only the openai response format is available in this build")
		return
	}
	snap := g.loader.Snapshot()
	p, aerr := g.planRequest(snap, kc, req)
	if aerr != nil {
		writeError(w, aerr.Status, aerr.Type, aerr.Code, aerr.Message)
		return
	}

	base := g.baseEvent(r, kc, rid, p, start)
	base.Stream = req.Stream

	// Rate limit, then budget reserve. One Redis round trip each.
	if rl := g.limiter.Allow(r.Context(), "key:"+kc.Key.ID.String(), kc.Key.RPMLimit, kc.Key.TPMLimit, p.needed); !rl.Allowed {
		metrics.RateLimitRejections.WithLabelValues(kc.Project.ID.String(), rl.Kind).Inc()
		w.Header().Set("Retry-After", "60")
		g.reject(base, 429, "rate_limited", "rate_limited_"+rl.Kind, nil)
		writeError(w, 429, "rate_limited", "rate_limited_"+rl.Kind, "key "+rl.Kind+" limit exceeded")
		return
	}
	scopes := budget.Resolve(kc.Budgets, start)
	base.Budgets = charges(scopes)
	dec, err := g.enforcer.Reserve(r.Context(), rid, scopes, p.estimate, p.needed)
	if err != nil {
		g.reject(base, 503, "budget_unavailable", "strict_budget_unenforceable", nil)
		writeError(w, 503, "server_error", "budget_unavailable", err.Error())
		return
	}
	base.Enforcement = dec.Enforcement
	if !dec.Allowed {
		metrics.BudgetRejections.WithLabelValues(kc.Project.ID.String(), dec.DeniedBy.Budget.ScopeType).Inc()
		id := dec.DeniedBy.Budget.ID
		g.reject(base, 429, "budget_exceeded", "budget_exceeded", &id)
		w.Header().Set("X-Budget-Denied-Scope", dec.DeniedBy.Budget.ScopeType)
		writeError(w, 429, "budget_exceeded", "budget_exceeded", "the "+dec.DeniedBy.Budget.ScopeType+" "+dec.DeniedBy.Budget.Kind+" budget is exhausted")
		return
	}
	defer g.enforcer.Release(context.WithoutCancel(r.Context()), rid)
	setBudgetHeaders(w, dec)

	// Candidate chain: primary first, then policy-driven fallbacks.
	chain := []*catalog.Deployment{p.dep}
	fallbacks, _ := router.Candidates(snap, p.dep, kc.Policy, p.needed, kc.Key.AllowedDeployments, g.breakers)
	chain = append(chain, fallbacks...)
	if r.Header.Get("X-Fallback") == "off" {
		chain = chain[:1]
	}

	st := &streamState{w: w, rid: rid, model: req.Model}
	if st.model == "" {
		st.model = p.dep.Slug
	}
	var attempts []*attempt
	var final *canonical.Response
	var lastErr *adapters.Error
	reason := ""
	upstreamCtx := context.WithoutCancel(r.Context()) // keep draining upstream for usage if the client leaves

	for i, dep := range chain {
		if len(attempts) >= kc.Policy.MaxAttempts {
			break
		}
		if skip := g.precheck(upstreamCtx, dep, p, i == 0); skip != "" {
			if i == 0 {
				reason = skip
			}
			continue
		}
		at := &attempt{dep: dep, reason: reason, started: time.Now()}
		attempts = append(attempts, at)
		call := g.call(snap, dep, req, p)
		st.begin(at)
		resp, cerr := g.adapters[dep.Provider.Dialect].Complete(upstreamCtx, call, st.emit)
		at.finished = time.Now()
		if resp != nil {
			at.usage = g.finalUsage(dep, p, resp)
			at.cost = dep.Price.CostFor(at.usage)
			if at.usage.Source != "" {
				g.enforcer.Settle(upstreamCtx, rid, len(attempts), scopes, at.cost, at.usage.Total())
			}
		}
		if cerr == nil {
			at.status = 200
			g.breakers.Record(dep.ID, dep.Slug, true)
			final = resp
			break
		}
		ae, ok := cerr.(*adapters.Error)
		if !ok {
			ae = adapters.Classify(cerr, 0, nil)
		}
		lastErr = ae
		at.status, at.class, at.code = ae.Status, ae.Class, ae.Code
		g.breakers.Record(dep.ID, dep.Slug, !(ae.Class == adapters.ClassClient || ae.Class == adapters.ClassContentFilter || ae.Class == adapters.ClassContextLength))
		if st.committed || !kc.Policy.Triggered(ae.Class) {
			break
		}
		reason = ae.Class
		if i+1 < len(chain) {
			metrics.Fallback.WithLabelValues(dep.Slug, chain[i+1].Slug, ae.Class).Inc()
		}
	}

	g.finish(w, st, req, p, base, attempts, final, lastErr, start)
}

// precheck skips a deployment without calling it. Returns the skip reason.
func (g *Gateway) precheck(ctx context.Context, dep *catalog.Deployment, p *plan, primary bool) string {
	if dep.Price == nil || dep.Status != "active" {
		return "inactive"
	}
	if p.needed > int64(dep.ContextWindow) {
		return "context_too_small"
	}
	if primary && !g.breakers.Allow(dep.ID) {
		return "breaker_open"
	}
	if dep.RPMLimit > 0 || dep.TPMLimit > 0 {
		if rl := g.limiter.Allow(ctx, "dep:"+dep.ID.String(), dep.RPMLimit, dep.TPMLimit, p.needed); !rl.Allowed {
			return "provider_quota"
		}
	}
	return ""
}

func (g *Gateway) call(snap *catalog.Snapshot, dep *catalog.Deployment, req *canonical.Request, p *plan) adapters.Call {
	headers := map[string]string{}
	if len(dep.Provider.DefaultHeaders) > 0 {
		_ = json.Unmarshal(dep.Provider.DefaultHeaders, &headers)
	}
	effort := dep.Effort
	if req.Effort != "" {
		effort = req.Effort
	}
	return adapters.Call{Req: req, ProviderModel: dep.ProviderModelName, ProviderSlug: dep.Provider.Slug, BaseURL: dep.Provider.BaseURL, APIKey: snap.Credentials[dep.ProviderID],
		Headers: headers, MaxOutputTokens: int(p.maxOut), Effort: effort, TTFTTimeout: 30 * time.Second, TotalTimeout: 600 * time.Second}
}

// finalUsage returns provider usage, or an estimate when the provider sent none.
func (g *Gateway) finalUsage(dep *catalog.Deployment, p *plan, resp *canonical.Response) canonical.Usage {
	u := resp.Usage
	if u.Source == "provider" {
		metrics.UsageSource.WithLabelValues("provider").Inc()
		if p.promptTokens > 0 {
			metrics.EstimateRatio.WithLabelValues(dep.Model.Slug).Observe(float64(u.PromptTokens) / float64(p.promptTokens))
		}
		return u
	}
	if resp.Content == "" && len(resp.ToolCalls) == 0 && u.PromptTokens == 0 {
		return canonical.Usage{} // nothing produced, nothing billed
	}
	metrics.UsageSource.WithLabelValues("estimated").Inc()
	out := g.tk.CountText(dep.Model.Tokenizer, resp.Content)
	for _, tc := range resp.ToolCalls {
		out += g.tk.CountText(dep.Model.Tokenizer, tc.Name+tc.Arguments)
	}
	return canonical.Usage{PromptTokens: p.promptTokens, CompletionTokens: out, Source: "estimated"}
}

// finish writes the response (if not already streamed) and emits events.
func (g *Gateway) finish(w http.ResponseWriter, st *streamState, req *canonical.Request, p *plan, base events.UsageEvent, attempts []*attempt, final *canonical.Response, lastErr *adapters.Error, start time.Time) {
	served := ""
	status := 200
	if final != nil {
		last := attempts[len(attempts)-1]
		served = last.dep.Slug
		w.Header().Set("X-Served-Deployment", served)
		w.Header().Set("X-Fallback-Used", strconv.FormatBool(len(attempts) > 1 || last.reason != ""))
		if st.committed {
			st.end()
		} else if req.Stream {
			st.flushAll(last)
			st.end()
		} else {
			w.Header().Set("X-Cost-Nano-Usd", strconv.FormatInt(last.cost, 10))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			w.Write(formats.EncodeOpenAIChat(st.rid, st.model, final))
		}
	} else {
		if lastErr == nil {
			lastErr = &adapters.Error{Status: 400, Class: "context_too_large", Code: "context_too_large", Message: "no deployment in the fallback chain fits the required context of " + strconv.FormatInt(p.needed, 10) + " tokens"}
			if len(attempts) == 0 && p.needed <= int64(p.dep.ContextWindow) {
				lastErr = &adapters.Error{Status: 503, Class: "no_healthy_deployment", Code: "no_healthy_deployment", Message: "no healthy deployment available"}
			}
		}
		status = lastErr.Status
		if status == 499 {
			status = 400
		}
		typ := "upstream_error"
		switch lastErr.Class {
		case adapters.ClassClient, adapters.ClassContextLength, "context_too_large":
			typ = "invalid_request_error"
		case adapters.ClassContentFilter:
			typ = "content_filter"
		case "no_healthy_deployment":
			typ = "no_healthy_deployment"
		}
		if st.committed {
			st.abort(lastErr)
		} else {
			writeError(w, status, typ, lastErr.Code, lastErr.Message)
		}
	}
	metrics.Duration.WithLabelValues(served).Observe(time.Since(start).Seconds())

	// One event per attempt; a request with zero attempts still logs once.
	if len(attempts) == 0 {
		ev := base
		ev.StatusCode, ev.ErrorClass, ev.ErrorCode = status, lastErr.Class, lastErr.Code
		ev.LatencyMS = time.Since(start).Milliseconds()
		metrics.Requests.WithLabelValues(base.ProjectID.String(), "", "", strconv.Itoa(status), ev.ErrorClass).Inc()
		g.bus.Publish(&ev)
		return
	}
	for i, at := range attempts {
		ev := base
		ev.AttemptNo, ev.AttemptsTotal = i+1, len(attempts)
		ev.ServedDeploymentID, ev.ServedDeploymentSlug, ev.ProviderSlug = at.dep.ID.String(), at.dep.Slug, at.dep.Provider.Slug
		ev.FallbackUsed, ev.FallbackReason = at.reason != "", at.reason
		ev.StatusCode, ev.ErrorClass, ev.ErrorCode = at.status, at.class, at.code
		ev.PromptTokens, ev.CachedTokens, ev.CacheWriteTokens = at.usage.PromptTokens, at.usage.CachedTokens, at.usage.CacheWriteTokens
		ev.CompletionTokens, ev.ReasoningTokens = at.usage.CompletionTokens, at.usage.ReasoningTokens
		ev.UsageSource, ev.CostNanoUSD = at.usage.Source, at.cost
		ev.PriceID = at.dep.Price.ID.String()
		ev.UpstreamLatencyMS = at.finished.Sub(at.started).Milliseconds()
		ev.TTFTMS = at.ttft.Milliseconds()
		ev.LatencyMS = time.Since(start).Milliseconds()
		if i == len(attempts)-1 && final == nil {
			ev.StatusCode = status
		}
		metrics.Requests.WithLabelValues(base.ProjectID.String(), at.dep.Slug, at.dep.Provider.Slug, strconv.Itoa(ev.StatusCode), ev.ErrorClass).Inc()
		metrics.Tokens.WithLabelValues(at.dep.Slug, "prompt").Add(float64(at.usage.PromptTokens))
		metrics.Tokens.WithLabelValues(at.dep.Slug, "completion").Add(float64(at.usage.CompletionTokens))
		metrics.Cost.WithLabelValues(base.ProjectID.String(), at.dep.Slug).Add(float64(at.cost))
		if at.ttft > 0 {
			metrics.TTFT.WithLabelValues(at.dep.Slug).Observe(at.ttft.Seconds())
		}
		g.bus.Publish(&ev)
	}
	g.db.TouchKey(context.Background(), base.KeyID)
}

func (g *Gateway) baseEvent(r *http.Request, kc *catalog.KeyConfig, rid string, p *plan, start time.Time) events.UsageEvent {
	return events.UsageEvent{TS: start, RequestID: rid, TraceID: rid, VendorID: kc.Vendor.ID, ProjectID: kc.Project.ID, KeyID: kc.Key.ID,
		IngressFormat: "openai", ResponseFormat: kc.Key.ResponseFormat, ModelSlug: p.dep.Model.Slug, RequestedDeploymentID: p.dep.ID.String(),
		EstimatedCostNanoUSD: p.estimate, ClientIP: clientIP(r), UserAgent: r.UserAgent(), Enforcement: "redis"}
}

// reject logs a gateway-side rejection (no provider attempt).
func (g *Gateway) reject(base events.UsageEvent, status int, class, code string, deniedBudget *uuid.UUID) {
	ev := base
	ev.StatusCode, ev.ErrorClass, ev.ErrorCode, ev.DeniedBudgetID = status, class, code, deniedBudget
	ev.LatencyMS = time.Since(base.TS).Milliseconds()
	metrics.Requests.WithLabelValues(base.ProjectID.String(), "", "", strconv.Itoa(status), class).Inc()
	g.bus.Publish(&ev)
}

func charges(scopes []budget.Scope) []events.BudgetCharge {
	out := make([]events.BudgetCharge, len(scopes))
	for i, s := range scopes {
		out[i] = events.BudgetCharge{BudgetID: s.Budget.ID, VendorID: s.Budget.VendorID, ScopeType: s.Budget.ScopeType, PeriodStart: s.PeriodStart, PeriodEnd: s.PeriodEnd}
	}
	return out
}

func setBudgetHeaders(w http.ResponseWriter, dec budget.Decision) {
	var min *int64
	for _, r := range dec.Remaining {
		if r.RemainingNS != nil && (min == nil || *r.RemainingNS < *min) {
			v := *r.RemainingNS
			min = &v
		}
	}
	if min != nil {
		w.Header().Set("X-Budget-Remaining-Nano-Usd", strconv.FormatInt(*min, 10))
		w.Header().Set("X-Budget-Remaining-Usd", pricing.USD(*min))
	}
	if dec.Enforcement == "local" {
		w.Header().Set("X-Budget-Enforcement", "degraded")
	}
}

// ---- streaming state

// streamState buffers events until the first content token so a failed
// attempt can fall back silently. After commit, bytes go straight through.
type streamState struct {
	w          http.ResponseWriter
	rid, model string
	enc        *formats.StreamEncoder
	buf        []canonical.Event
	committed  bool
	clientGone bool
	cur        *attempt
}

func (s *streamState) begin(at *attempt) {
	s.buf = s.buf[:0]
	s.cur = at
	s.enc = formats.NewOpenAIStreamEncoder(s.rid, s.model)
}

func (s *streamState) emit(ev canonical.Event) error {
	if s.cur != nil && s.cur.ttft == 0 && ev.IsContent() {
		s.cur.ttft = time.Since(s.cur.started)
	}
	if s.clientGone {
		return nil // keep draining upstream for the usage block
	}
	if !s.committed {
		if !ev.IsContent() {
			s.buf = append(s.buf, ev)
			return nil
		}
		s.commit()
		for _, b := range s.buf {
			s.write(s.enc.Encode(b))
		}
		s.buf = s.buf[:0]
	}
	s.write(s.enc.Encode(ev))
	return nil
}

func (s *streamState) commit() {
	h := s.w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Served-Deployment", s.cur.dep.Slug)
	h.Set("X-Fallback-Used", strconv.FormatBool(s.cur.reason != ""))
	s.w.WriteHeader(200)
	s.committed = true
}

func (s *streamState) write(b []byte) {
	if len(b) == 0 || s.clientGone {
		return
	}
	if _, err := s.w.Write(b); err != nil {
		s.clientGone = true
		return
	}
	if f, ok := s.w.(http.Flusher); ok {
		f.Flush()
	}
}

// flushAll handles a stream that completed with no content (e.g. empty answer).
func (s *streamState) flushAll(at *attempt) {
	s.cur = at
	s.commit()
	for _, b := range s.buf {
		s.write(s.enc.Encode(b))
	}
}

func (s *streamState) end() { s.write(s.enc.Done()) }

func (s *streamState) abort(e *adapters.Error) {
	b := formats.EncodeOpenAIError(&formats.APIError{Status: e.Status, Type: "upstream_error", Code: e.Code, Message: e.Message})
	s.write(append(append([]byte("data: "), b...), '\n', '\n'))
	s.end()
}
