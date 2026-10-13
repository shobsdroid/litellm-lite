// Package controlplane is the admin API. Unauthenticated in the MVP: the
// vendor comes from X-Vendor-Id (defaulting to the only vendor when there is
// exactly one). The identity middleware is the single seam where auth plugs in.
package controlplane

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rzp/litellm-lite/internal/analytics"
	"github.com/rzp/litellm-lite/internal/budget"
	"github.com/rzp/litellm-lite/internal/catalog"
	"github.com/rzp/litellm-lite/internal/pricing"
	"github.com/rzp/litellm-lite/internal/router"
	"github.com/rzp/litellm-lite/internal/secrets"
	"github.com/rzp/litellm-lite/internal/store"
	"github.com/rzp/litellm-lite/internal/webhooks"
)

type Server struct {
	db       *store.DB
	ch       *analytics.Store
	keyring  secrets.Keyring
	enforcer *budget.Enforcer
	breakers *router.Breakers
	loader   *catalog.Loader
}

func New(db *store.DB, ch *analytics.Store, kr secrets.Keyring, enf *budget.Enforcer, br *router.Breakers, loader *catalog.Loader) *Server {
	return &Server{db: db, ch: ch, keyring: kr, enforcer: enf, breakers: br, loader: loader}
}

type ctxKey int

const vendorCtx ctxKey = 1

func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	r.Handle("/metrics", promhttp.Handler())
	r.Route("/v1", func(r chi.Router) {
		r.Use(s.identity)
		r.Get("/vendors", s.listVendors)
		r.Post("/vendors", s.createVendor)
		r.Get("/vendors/{id}", s.getVendor)
		r.Patch("/vendors/{id}", s.patchVendor)

		r.Get("/providers", s.listProviders)
		r.Post("/providers", s.createProvider)
		r.Get("/providers/{id}", s.getProvider)
		r.Patch("/providers/{id}", s.patchProvider)
		r.Delete("/providers/{id}", s.deleteProvider)
		r.Get("/providers/{id}/credentials", s.listCredentials)
		r.Post("/providers/{id}/credentials", s.createCredential)
		r.Post("/providers/{id}/credentials/{cid}/revoke", s.revokeCredential)

		r.Get("/models", s.listModels)
		r.Post("/models", s.createModel)
		r.Get("/models/{id}", s.getModel)
		r.Patch("/models/{id}", s.patchModel)

		r.Get("/deployments", s.listDeployments)
		r.Post("/deployments", s.createDeployment)
		r.Get("/deployments/{id}", s.getDeployment)
		r.Patch("/deployments/{id}", s.patchDeployment)
		r.Delete("/deployments/{id}", s.deleteDeployment)
		r.Get("/deployments/{id}/prices", s.listPrices)
		r.Post("/deployments/{id}/prices", s.createPrice)
		r.Get("/deployments/{id}/health", s.deploymentHealth)

		r.Get("/projects", s.listProjects)
		r.Post("/projects", s.createProject)
		r.Get("/projects/{id}", s.getProject)
		r.Patch("/projects/{id}", s.patchProject)
		r.Delete("/projects/{id}", s.deleteProject)

		r.Get("/keys", s.listKeys)
		r.Post("/keys", s.createKey)
		r.Get("/keys/{id}", s.getKey)
		r.Patch("/keys/{id}", s.patchKey)
		r.Post("/keys/{id}/rotate", s.rotateKey)
		r.Post("/keys/{id}/revoke", s.revokeKey)
		r.Get("/keys/{id}/usage", s.keyUsage)

		r.Get("/budgets", s.listBudgets)
		r.Post("/budgets", s.createBudget)
		r.Get("/budgets/{id}", s.getBudget)
		r.Patch("/budgets/{id}", s.patchBudget)
		r.Delete("/budgets/{id}", s.deleteBudget)
		r.Get("/budgets/{id}/periods", s.budgetPeriods)
		r.Get("/budgets/{id}/remaining", s.budgetRemaining)

		r.Get("/webhooks", s.listWebhooks)
		r.Post("/webhooks", s.createWebhook)
		r.Get("/webhooks/{id}", s.getWebhook)
		r.Patch("/webhooks/{id}", s.patchWebhook)
		r.Delete("/webhooks/{id}", s.deleteWebhook)
		r.Post("/webhooks/{id}/test", s.testWebhook)
		r.Get("/webhooks/{id}/deliveries", s.listDeliveries)
		r.Post("/webhook-deliveries/{id}/replay", s.replayDelivery)

		r.Get("/analytics/usage", s.analyticsUsage)
		r.Get("/analytics/requests", s.analyticsRequests)
		r.Get("/analytics/requests/{rid}", s.analyticsRequest)
		r.Get("/analytics/budgets/remaining", s.analyticsBudgetsRemaining)
		r.Get("/analytics/fallbacks", s.analyticsFallbacks)
		r.Get("/audit", s.audit)
	})
	return r
}

// ---- helpers

func (s *Server) identity(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var vid uuid.UUID
		if h := r.Header.Get("X-Vendor-Id"); h != "" {
			id, err := uuid.Parse(h)
			if err != nil {
				fail(w, 400, "bad_vendor_id", "X-Vendor-Id must be a UUID")
				return
			}
			vid = id
		} else if vs, err := s.db.ListVendors(r.Context()); err == nil && len(vs) == 1 {
			vid = vs[0].ID
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), vendorCtx, vid)))
	})
}

func vendorOf(r *http.Request) (uuid.UUID, bool) {
	v, _ := r.Context().Value(vendorCtx).(uuid.UUID)
	return v, v != uuid.Nil
}

func requireVendor(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	v, ok := vendorOf(r)
	if !ok {
		fail(w, 400, "vendor_required", "set X-Vendor-Id (more than one vendor exists)")
	}
	return v, ok
}

func fail(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": code, "message": msg}})
}

func ok(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func handleErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		fail(w, 404, "not_found", "not found")
	case errors.Is(err, store.ErrVersionConflict):
		fail(w, 409, "version_conflict", "the resource changed; re-read and retry with the new version")
	default:
		fail(w, 500, "internal", err.Error())
	}
}

func decode(r *http.Request, v any) error {
	d := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
	return d.Decode(v)
}

func pathID(r *http.Request, name string) (uuid.UUID, error) {
	return uuid.Parse(chi.URLParam(r, name))
}

// applyPatch merges a JSON patch over the existing entity and honours If-Match.
func applyPatch(r *http.Request, entity any, version *int32) error {
	body, err := readAll(r)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, entity); err != nil {
		return err
	}
	if m := r.Header.Get("If-Match"); m != "" {
		v, err := strconv.Atoi(strings.Trim(m, `"`))
		if err != nil {
			return errors.New("If-Match must be the integer version")
		}
		*version = int32(v)
	}
	return nil
}

func readAll(r *http.Request) ([]byte, error) {
	var buf []byte
	tmp := make([]byte, 32*1024)
	lr := http.MaxBytesReader(nil, r.Body, 1<<20)
	for {
		n, err := lr.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if err != nil {
			if err.Error() == "EOF" {
				return buf, nil
			}
			return nil, err
		}
	}
}

func (s *Server) write(ctx context.Context, vendorID *uuid.UUID, action, entity string, id uuid.UUID, before, after any, fn func(tx pgx.Tx) error) error {
	return s.db.Tx(ctx, func(tx pgx.Tx) error {
		if err := fn(tx); err != nil {
			return err
		}
		if err := store.Audit(ctx, tx, vendorID, "local", action, entity, &id, before, after); err != nil {
			return err
		}
		return store.Outbox(ctx, tx, entity, id, action)
	})
}

func jsonOr(raw json.RawMessage, def string) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage(def)
	}
	return raw
}

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// ---- vendors (platform scope)

func (s *Server) listVendors(w http.ResponseWriter, r *http.Request) {
	vs, err := s.db.ListVendors(r.Context())
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, map[string]any{"data": vs})
}

func (s *Server) createVendor(w http.ResponseWriter, r *http.Request) {
	var v store.Vendor
	if err := decode(r, &v); err != nil || v.Name == "" {
		fail(w, 400, "invalid", "name is required")
		return
	}
	v.Status, v.Settings = orDefault(v.Status, "active"), jsonOr(v.Settings, "{}")
	var out *store.Vendor
	err := s.db.Tx(r.Context(), func(tx pgx.Tx) error {
		var err error
		out, err = s.db.InsertVendor(r.Context(), tx, &v)
		if err != nil {
			return err
		}
		return store.Audit(r.Context(), tx, &out.ID, "local", "create", "vendor", &out.ID, nil, out)
	})
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 201, out)
}

func (s *Server) getVendor(w http.ResponseWriter, r *http.Request) {
	id, _ := pathID(r, "id")
	v, err := s.db.GetVendor(r.Context(), s.db.Pool, id)
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, v)
}

func (s *Server) patchVendor(w http.ResponseWriter, r *http.Request) {
	id, _ := pathID(r, "id")
	cur, err := s.db.GetVendor(r.Context(), s.db.Pool, id)
	if err != nil {
		handleErr(w, err)
		return
	}
	before := *cur
	if err := applyPatch(r, cur, &cur.Version); err != nil {
		fail(w, 400, "invalid", err.Error())
		return
	}
	cur.ID = id
	var out *store.Vendor
	err = s.write(r.Context(), &id, "update", "vendor", id, before, nil, func(tx pgx.Tx) error {
		var err error
		out, err = s.db.UpdateVendor(r.Context(), tx, cur)
		return err
	})
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, out)
}

// ---- providers

func (s *Server) listProviders(w http.ResponseWriter, r *http.Request) {
	ps, err := s.db.ListProviders(r.Context(), s.db.Pool)
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, map[string]any{"data": ps})
}

func validDialect(d string) bool {
	switch d {
	case "openai_chat", "anthropic_messages", "gemini_generate", "mock":
		return true
	}
	return false
}

func (s *Server) createProvider(w http.ResponseWriter, r *http.Request) {
	var p store.Provider
	var in struct {
		store.Provider
		APIKey string `json:"api_key"`
	}
	if err := decode(r, &in); err != nil {
		fail(w, 400, "invalid", err.Error())
		return
	}
	p = in.Provider
	if p.Slug == "" || !validDialect(p.Dialect) {
		fail(w, 400, "invalid", "slug and a valid dialect (openai_chat|anthropic_messages|gemini_generate|mock) are required")
		return
	}
	p.Status, p.DefaultHeaders, p.DisplayName = orDefault(p.Status, "active"), jsonOr(p.DefaultHeaders, "{}"), orDefault(p.DisplayName, p.Slug)
	var out *store.Provider
	err := s.db.Tx(r.Context(), func(tx pgx.Tx) error {
		var err error
		if out, err = s.db.InsertProvider(r.Context(), tx, &p); err != nil {
			return err
		}
		if in.APIKey != "" {
			ct, kid, err := s.keyring.Encrypt([]byte(in.APIKey))
			if err != nil {
				return err
			}
			if _, err := s.db.InsertCredential(r.Context(), tx, out.ID, ct, kid); err != nil {
				return err
			}
		}
		if err := store.Audit(r.Context(), tx, nil, "local", "create", "provider", &out.ID, nil, out); err != nil {
			return err
		}
		return store.Outbox(r.Context(), tx, "provider", out.ID, "create")
	})
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 201, out)
}

func (s *Server) getProvider(w http.ResponseWriter, r *http.Request) {
	id, _ := pathID(r, "id")
	p, err := s.db.GetProvider(r.Context(), s.db.Pool, id)
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, p)
}

func (s *Server) patchProvider(w http.ResponseWriter, r *http.Request) {
	id, _ := pathID(r, "id")
	cur, err := s.db.GetProvider(r.Context(), s.db.Pool, id)
	if err != nil {
		handleErr(w, err)
		return
	}
	before := *cur
	if err := applyPatch(r, cur, &cur.Version); err != nil {
		fail(w, 400, "invalid", err.Error())
		return
	}
	cur.ID = id
	if !validDialect(cur.Dialect) {
		fail(w, 400, "invalid", "invalid dialect")
		return
	}
	var out *store.Provider
	err = s.write(r.Context(), nil, "update", "provider", id, before, nil, func(tx pgx.Tx) error {
		var err error
		out, err = s.db.UpdateProvider(r.Context(), tx, cur)
		return err
	})
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, out)
}

func (s *Server) deleteProvider(w http.ResponseWriter, r *http.Request) {
	id, _ := pathID(r, "id")
	err := s.write(r.Context(), nil, "delete", "provider", id, nil, nil, func(tx pgx.Tx) error {
		return s.db.SoftDeleteProvider(r.Context(), tx, id)
	})
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			handleErr(w, err)
		} else {
			fail(w, 409, "in_use", err.Error())
		}
		return
	}
	w.WriteHeader(204)
}

func (s *Server) listCredentials(w http.ResponseWriter, r *http.Request) {
	id, _ := pathID(r, "id")
	cs, err := s.db.ListCredentials(r.Context(), s.db.Pool, id)
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, map[string]any{"data": cs})
}

func (s *Server) createCredential(w http.ResponseWriter, r *http.Request) {
	id, _ := pathID(r, "id")
	var in struct {
		APIKey string `json:"api_key"`
	}
	if err := decode(r, &in); err != nil || in.APIKey == "" {
		fail(w, 400, "invalid", "api_key is required")
		return
	}
	ct, kid, err := s.keyring.Encrypt([]byte(in.APIKey))
	if err != nil {
		handleErr(w, err)
		return
	}
	var out *store.ProviderCredential
	err = s.write(r.Context(), nil, "create", "provider_credential", id, nil, nil, func(tx pgx.Tx) error {
		var err error
		out, err = s.db.InsertCredential(r.Context(), tx, id, ct, kid)
		return err
	})
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 201, out)
}

func (s *Server) revokeCredential(w http.ResponseWriter, r *http.Request) {
	id, _ := pathID(r, "id")
	cid, _ := pathID(r, "cid")
	err := s.write(r.Context(), nil, "revoke", "provider_credential", cid, nil, nil, func(tx pgx.Tx) error {
		return s.db.RevokeCredential(r.Context(), tx, id, cid)
	})
	if err != nil {
		handleErr(w, err)
		return
	}
	w.WriteHeader(204)
}

// ---- models

func (s *Server) listModels(w http.ResponseWriter, r *http.Request) {
	ms, err := s.db.ListModels(r.Context(), s.db.Pool)
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, map[string]any{"data": ms})
}

func (s *Server) createModel(w http.ResponseWriter, r *http.Request) {
	var m store.Model
	if err := decode(r, &m); err != nil || m.Slug == "" {
		fail(w, 400, "invalid", "slug is required")
		return
	}
	m.DisplayName, m.Status, m.Tokenizer = orDefault(m.DisplayName, m.Slug), orDefault(m.Status, "active"), orDefault(m.Tokenizer, "o200k_base")
	m.Capabilities = jsonOr(m.Capabilities, `["streaming"]`)
	if m.TokenizerMultiplier == 0 {
		m.TokenizerMultiplier = 1
	}
	if len(m.SupportedEfforts) == 0 {
		m.SupportedEfforts = []string{"none"}
	}
	var out *store.Model
	err := s.write(r.Context(), nil, "create", "model", uuid.Nil, nil, nil, func(tx pgx.Tx) error {
		var err error
		out, err = s.db.InsertModel(r.Context(), tx, &m)
		return err
	})
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 201, out)
}

func (s *Server) getModel(w http.ResponseWriter, r *http.Request) {
	id, _ := pathID(r, "id")
	m, err := s.db.GetModel(r.Context(), s.db.Pool, id)
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, m)
}

func (s *Server) patchModel(w http.ResponseWriter, r *http.Request) {
	id, _ := pathID(r, "id")
	cur, err := s.db.GetModel(r.Context(), s.db.Pool, id)
	if err != nil {
		handleErr(w, err)
		return
	}
	before := *cur
	if err := applyPatch(r, cur, &cur.Version); err != nil {
		fail(w, 400, "invalid", err.Error())
		return
	}
	cur.ID = id
	var out *store.Model
	err = s.write(r.Context(), nil, "update", "model", id, before, nil, func(tx pgx.Tx) error {
		var err error
		out, err = s.db.UpdateModel(r.Context(), tx, cur)
		return err
	})
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, out)
}

// ---- deployments

func DeploymentSlug(model, provider string, ctxWindow int32, effort string) string {
	ctx := fmt.Sprintf("%dk", ctxWindow/1000)
	if ctxWindow >= 1_000_000 {
		ctx = fmt.Sprintf("%dm", ctxWindow/1_000_000)
	}
	return fmt.Sprintf("%s-%s-%s-%s", model, provider, ctx, orDefault(effort, "none"))
}

func (s *Server) listDeployments(w http.ResponseWriter, r *http.Request) {
	ds, err := s.db.ListDeployments(r.Context(), s.db.Pool)
	if err != nil {
		handleErr(w, err)
		return
	}
	q := r.URL.Query()
	out := []store.Deployment{}
	for _, d := range ds {
		if v := q.Get("model_id"); v != "" && d.ModelID.String() != v {
			continue
		}
		if v := q.Get("provider_id"); v != "" && d.ProviderID.String() != v {
			continue
		}
		if v := q.Get("status"); v != "" && d.Status != v {
			continue
		}
		out = append(out, d)
	}
	ok(w, 200, map[string]any{"data": out})
}

type deploymentIn struct {
	store.Deployment
	Model    string          `json:"model"`    // model slug (alternative to model_id)
	Provider string          `json:"provider"` // provider slug (alternative to provider_id)
	Tiers    json.RawMessage `json:"tiers"`    // optional initial price
}

func (s *Server) createDeployment(w http.ResponseWriter, r *http.Request) {
	var in deploymentIn
	if err := decode(r, &in); err != nil {
		fail(w, 400, "invalid", err.Error())
		return
	}
	d := in.Deployment
	ctx := r.Context()
	if in.Model != "" {
		m, err := s.db.GetModelBySlug(ctx, s.db.Pool, in.Model)
		if err != nil {
			fail(w, 400, "invalid", "unknown model "+in.Model)
			return
		}
		d.ModelID = m.ID
	}
	if in.Provider != "" {
		p, err := s.db.GetProviderBySlug(ctx, s.db.Pool, in.Provider)
		if err != nil {
			fail(w, 400, "invalid", "unknown provider "+in.Provider)
			return
		}
		d.ProviderID = p.ID
	}
	m, err1 := s.db.GetModel(ctx, s.db.Pool, d.ModelID)
	p, err2 := s.db.GetProvider(ctx, s.db.Pool, d.ProviderID)
	if err1 != nil || err2 != nil || d.ContextWindow <= 0 {
		fail(w, 400, "invalid", "model, provider and context_window are required")
		return
	}
	d.Effort, d.Status, d.ProviderModelName = orDefault(d.Effort, "none"), orDefault(d.Status, "active"), orDefault(d.ProviderModelName, m.Slug)
	if d.MaxOutputTokens == 0 {
		d.MaxOutputTokens = 4096
	}
	if d.Priority == 0 {
		d.Priority = 100
	}
	if d.Weight == 0 {
		d.Weight = 1
	}
	d.Slug = DeploymentSlug(m.Slug, p.Slug, d.ContextWindow, d.Effort)
	if len(in.Tiers) > 0 {
		if _, err := pricing.ParseTiers(in.Tiers); err != nil {
			fail(w, 400, "invalid", "tiers: "+err.Error())
			return
		}
	}
	var out *store.Deployment
	err := s.db.Tx(ctx, func(tx pgx.Tx) error {
		var err error
		if out, err = s.db.InsertDeployment(ctx, tx, &d); err != nil {
			return err
		}
		if len(in.Tiers) > 0 {
			if _, err := s.db.InsertPrice(ctx, tx, &store.DeploymentPrice{DeploymentID: out.ID, Tiers: in.Tiers, Source: "api", CreatedBy: "local"}); err != nil {
				return err
			}
		}
		if err := store.Audit(ctx, tx, nil, "local", "create", "deployment", &out.ID, nil, out); err != nil {
			return err
		}
		return store.Outbox(ctx, tx, "deployment", out.ID, "create")
	})
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 201, out)
}

func (s *Server) getDeployment(w http.ResponseWriter, r *http.Request) {
	id, _ := pathID(r, "id")
	d, err := s.db.GetDeployment(r.Context(), s.db.Pool, id)
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, d)
}

func (s *Server) patchDeployment(w http.ResponseWriter, r *http.Request) {
	id, _ := pathID(r, "id")
	cur, err := s.db.GetDeployment(r.Context(), s.db.Pool, id)
	if err != nil {
		handleErr(w, err)
		return
	}
	before := *cur
	if err := applyPatch(r, cur, &cur.Version); err != nil {
		fail(w, 400, "invalid", err.Error())
		return
	}
	cur.ID, cur.ModelID, cur.ProviderID = id, before.ModelID, before.ProviderID
	m, _ := s.db.GetModel(r.Context(), s.db.Pool, cur.ModelID)
	p, _ := s.db.GetProvider(r.Context(), s.db.Pool, cur.ProviderID)
	cur.Slug = DeploymentSlug(m.Slug, p.Slug, cur.ContextWindow, cur.Effort)
	var out *store.Deployment
	err = s.write(r.Context(), nil, "update", "deployment", id, before, nil, func(tx pgx.Tx) error {
		var err error
		out, err = s.db.UpdateDeployment(r.Context(), tx, cur)
		return err
	})
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, out)
}

func (s *Server) deleteDeployment(w http.ResponseWriter, r *http.Request) {
	id, _ := pathID(r, "id")
	err := s.write(r.Context(), nil, "delete", "deployment", id, nil, nil, func(tx pgx.Tx) error {
		return s.db.SoftDeleteDeployment(r.Context(), tx, id)
	})
	if err != nil {
		fail(w, 409, "in_use", err.Error())
		return
	}
	w.WriteHeader(204)
}

func (s *Server) listPrices(w http.ResponseWriter, r *http.Request) {
	id, _ := pathID(r, "id")
	ps, err := s.db.ListPrices(r.Context(), s.db.Pool, id)
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, map[string]any{"data": ps})
}

func (s *Server) createPrice(w http.ResponseWriter, r *http.Request) {
	id, _ := pathID(r, "id")
	var p store.DeploymentPrice
	if err := decode(r, &p); err != nil {
		fail(w, 400, "invalid", err.Error())
		return
	}
	if _, err := pricing.ParseTiers(p.Tiers); err != nil {
		fail(w, 400, "invalid", "tiers: "+err.Error())
		return
	}
	p.DeploymentID, p.Source, p.CreatedBy = id, orDefault(p.Source, "api"), "local"
	var out *store.DeploymentPrice
	err := s.write(r.Context(), nil, "create", "deployment_price", id, nil, nil, func(tx pgx.Tx) error {
		var err error
		out, err = s.db.InsertPrice(r.Context(), tx, &p)
		return err
	})
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 201, out)
}

func (s *Server) deploymentHealth(w http.ResponseWriter, r *http.Request) {
	id, _ := pathID(r, "id")
	d, err := s.db.GetDeployment(r.Context(), s.db.Pool, id)
	if err != nil {
		handleErr(w, err)
		return
	}
	resp := map[string]any{"deployment": d.Slug, "breaker": s.breakers.State(id)}
	if h, err := s.ch.DeploymentHealth(r.Context(), d.Slug); err == nil {
		resp["last_15m"] = h
	}
	ok(w, 200, resp)
}

// ---- projects

func (s *Server) listProjects(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	ps, err := s.db.ListProjects(r.Context(), vid)
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, map[string]any{"data": ps})
}

func (s *Server) createProject(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	var p store.Project
	if err := decode(r, &p); err != nil || p.Name == "" {
		fail(w, 400, "invalid", "name is required")
		return
	}
	p.VendorID, p.Status, p.Metadata = vid, orDefault(p.Status, "active"), jsonOr(p.Metadata, "{}")
	var out *store.Project
	err := s.db.Tx(r.Context(), func(tx pgx.Tx) error {
		var err error
		if out, err = s.db.InsertProject(r.Context(), tx, &p); err != nil {
			return err
		}
		if err := store.Audit(r.Context(), tx, &vid, "local", "create", "project", &out.ID, nil, out); err != nil {
			return err
		}
		return store.Outbox(r.Context(), tx, "project", out.ID, "create")
	})
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 201, out)
}

func (s *Server) getProject(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	id, _ := pathID(r, "id")
	p, err := s.db.GetProject(r.Context(), s.db.Pool, vid, id)
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, p)
}

func (s *Server) patchProject(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	id, _ := pathID(r, "id")
	cur, err := s.db.GetProject(r.Context(), s.db.Pool, vid, id)
	if err != nil {
		handleErr(w, err)
		return
	}
	before := *cur
	if err := applyPatch(r, cur, &cur.Version); err != nil {
		fail(w, 400, "invalid", err.Error())
		return
	}
	cur.ID, cur.VendorID = id, vid
	var out *store.Project
	err = s.write(r.Context(), &vid, "update", "project", id, before, nil, func(tx pgx.Tx) error {
		var err error
		out, err = s.db.UpdateProject(r.Context(), tx, cur)
		return err
	})
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, out)
}

func (s *Server) deleteProject(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	id, _ := pathID(r, "id")
	if _, err := s.db.GetProject(r.Context(), s.db.Pool, vid, id); err != nil {
		handleErr(w, err)
		return
	}
	err := s.write(r.Context(), &vid, "delete", "project", id, nil, nil, func(tx pgx.Tx) error {
		return s.db.SoftDeleteProject(r.Context(), tx, id)
	})
	if err != nil {
		handleErr(w, err)
		return
	}
	w.WriteHeader(204)
}

// ---- keys

func newSecret(prefix string) (secret string, hash []byte, shown string) {
	var b [32]byte
	rand.Read(b[:])
	secret = "sk-ll-" + prefix + "-" + strings.TrimRight(base64.RawURLEncoding.EncodeToString(b[:]), "=")
	h := catalog.HashKey(secret)
	return secret, h[:], secret[:len("sk-ll-")+len(prefix)+5]
}

func (s *Server) listKeys(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	var pid *uuid.UUID
	if v := r.URL.Query().Get("project_id"); v != "" {
		id, err := uuid.Parse(v)
		if err != nil {
			fail(w, 400, "invalid", "project_id must be a UUID")
			return
		}
		pid = &id
	}
	ks, err := s.db.ListKeys(r.Context(), vid, pid, r.URL.Query().Get("status"))
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, map[string]any{"data": ks})
}

type keyIn struct {
	store.Key
	PrimaryDeployment string `json:"primary_deployment"` // slug alternative
	Budget            *struct {
		Kind         string `json:"kind"`
		LimitUSD     string `json:"limit_usd"`
		LimitNanoUSD *int64 `json:"limit_nano_usd"`
		OnExceed     string `json:"on_exceed"`
	} `json:"budget"`
}

func parseUSD(s string) (int64, error) {
	s = strings.TrimSpace(s)
	whole, frac, _ := strings.Cut(s, ".")
	if whole == "" {
		whole = "0"
	}
	w, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return 0, err
	}
	for len(frac) < 9 {
		frac += "0"
	}
	f, err := strconv.ParseInt(frac[:9], 10, 64)
	if err != nil {
		return 0, err
	}
	return w*pricing.NanoPerUSD + f, nil
}

func (s *Server) createKey(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	var in keyIn
	if err := decode(r, &in); err != nil {
		fail(w, 400, "invalid", err.Error())
		return
	}
	k := in.Key
	ctx := r.Context()
	proj, err := s.db.GetProject(ctx, s.db.Pool, vid, k.ProjectID)
	if err != nil {
		fail(w, 400, "invalid", "project_id is required and must belong to the vendor")
		return
	}
	if in.PrimaryDeployment != "" {
		d, err := s.db.GetDeploymentBySlug(ctx, s.db.Pool, in.PrimaryDeployment)
		if err != nil {
			fail(w, 400, "invalid", "unknown deployment "+in.PrimaryDeployment)
			return
		}
		k.PrimaryDeploymentID = d.ID
	}
	if _, err := s.db.GetDeployment(ctx, s.db.Pool, k.PrimaryDeploymentID); err != nil {
		fail(w, 400, "invalid", "primary_deployment (slug) or primary_deployment_id is required")
		return
	}
	prefix := strings.ToLower(strings.Map(func(c rune) rune {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			return c
		}
		return -1
	}, proj.Name))
	if len(prefix) > 8 {
		prefix = prefix[:8]
	}
	if prefix == "" {
		prefix = "proj"
	}
	secret, hash, shown := newSecret(prefix)
	k.VendorID, k.KeyHash, k.KeyPrefix = vid, hash, shown
	k.Name, k.Status, k.ResponseFormat = orDefault(k.Name, "default"), "active", orDefault(k.ResponseFormat, "openai")
	k.FallbackPolicy, k.RequestOverrides = jsonOr(k.FallbackPolicy, "{}"), jsonOr(k.RequestOverrides, "{}")
	if k.AllowedDeployments == nil {
		k.AllowedDeployments = []uuid.UUID{}
	}
	var out *store.Key
	err = s.db.Tx(ctx, func(tx pgx.Tx) error {
		var err error
		if out, err = s.db.InsertKey(ctx, tx, &k); err != nil {
			return err
		}
		if in.Budget != nil {
			b := store.Budget{VendorID: vid, ScopeType: "key", ScopeID: out.ID, Kind: orDefault(in.Budget.Kind, "monthly"), OnExceed: orDefault(in.Budget.OnExceed, "block"),
				Timezone: "UTC", AnchorDay: 1, AlertThresholds: []int32{50, 80, 100}, Enforcement: "available", Status: "active"}
			if in.Budget.LimitNanoUSD != nil {
				b.LimitNanoUSD = in.Budget.LimitNanoUSD
			} else if in.Budget.LimitUSD != "" {
				n, err := parseUSD(in.Budget.LimitUSD)
				if err != nil {
					return fmt.Errorf("budget.limit_usd: %w", err)
				}
				b.LimitNanoUSD = &n
			}
			if _, err := s.db.InsertBudget(ctx, tx, &b); err != nil {
				return err
			}
		}
		if err := store.Audit(ctx, tx, &vid, "local", "create", "key", &out.ID, nil, out); err != nil {
			return err
		}
		return store.Outbox(ctx, tx, "key", out.ID, "create")
	})
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 201, map[string]any{"key": out, "secret": secret, "note": "the secret is shown once"})
}

func (s *Server) getKey(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	id, _ := pathID(r, "id")
	k, err := s.db.GetKey(r.Context(), s.db.Pool, vid, id)
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, k)
}

func (s *Server) patchKey(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	id, _ := pathID(r, "id")
	cur, err := s.db.GetKey(r.Context(), s.db.Pool, vid, id)
	if err != nil {
		handleErr(w, err)
		return
	}
	before := *cur
	var in keyIn
	in.Key = *cur
	if err := applyPatch(r, &in, &in.Version); err != nil {
		fail(w, 400, "invalid", err.Error())
		return
	}
	k := in.Key
	if in.PrimaryDeployment != "" {
		d, err := s.db.GetDeploymentBySlug(r.Context(), s.db.Pool, in.PrimaryDeployment)
		if err != nil {
			fail(w, 400, "invalid", "unknown deployment "+in.PrimaryDeployment)
			return
		}
		k.PrimaryDeploymentID = d.ID
	}
	k.ID, k.VendorID, k.ProjectID, k.KeyHash, k.KeyPrefix = id, vid, before.ProjectID, before.KeyHash, before.KeyPrefix
	var out *store.Key
	err = s.write(r.Context(), &vid, "update", "key", id, before, nil, func(tx pgx.Tx) error {
		var err error
		out, err = s.db.UpdateKey(r.Context(), tx, &k)
		return err
	})
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, out)
}

func (s *Server) rotateKey(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	id, _ := pathID(r, "id")
	cur, err := s.db.GetKey(r.Context(), s.db.Pool, vid, id)
	if err != nil {
		handleErr(w, err)
		return
	}
	prefix := strings.TrimSuffix(strings.TrimPrefix(cur.KeyPrefix, "sk-ll-"), cur.KeyPrefix[len(cur.KeyPrefix)-5:])
	secret, hash, shown := newSecret(prefix)
	before := *cur
	cur.KeyHash, cur.KeyPrefix = hash, shown
	var out *store.Key
	err = s.write(r.Context(), &vid, "rotate", "key", id, before, nil, func(tx pgx.Tx) error {
		var err error
		out, err = s.db.UpdateKey(r.Context(), tx, cur)
		return err
	})
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, map[string]any{"key": out, "secret": secret, "note": "the previous secret stops working immediately in the MVP"})
}

func (s *Server) revokeKey(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	id, _ := pathID(r, "id")
	cur, err := s.db.GetKey(r.Context(), s.db.Pool, vid, id)
	if err != nil {
		handleErr(w, err)
		return
	}
	before := *cur
	cur.Status = "revoked"
	var out *store.Key
	err = s.write(r.Context(), &vid, "revoke", "key", id, before, nil, func(tx pgx.Tx) error {
		var err error
		out, err = s.db.UpdateKey(r.Context(), tx, cur)
		return err
	})
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, out)
}

func (s *Server) keyUsage(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	id, _ := pathID(r, "id")
	from, to, gran := timeRange(r)
	rows, err := s.ch.Usage(r.Context(), analytics.UsageQuery{VendorID: vid, KeyID: &id, GroupBy: "deployment", From: from, To: to, Granularity: gran})
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, map[string]any{"data": rows})
}

func timeRange(r *http.Request) (from, to time.Time, gran string) {
	q := r.URL.Query()
	to = time.Now().Add(time.Hour)
	from = time.Now().Add(-24 * time.Hour)
	if v := q.Get("from"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			from = t
		}
	}
	if v := q.Get("to"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			to = t
		}
	}
	gran = orDefault(q.Get("granularity"), "hour")
	return
}

// ---- budgets

func (s *Server) listBudgets(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	var sid *uuid.UUID
	if v := r.URL.Query().Get("scope_id"); v != "" {
		id, err := uuid.Parse(v)
		if err == nil {
			sid = &id
		}
	}
	bs, err := s.db.ListBudgets(r.Context(), vid, r.URL.Query().Get("scope_type"), sid)
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, map[string]any{"data": bs})
}

type budgetIn struct {
	store.Budget
	LimitUSD string `json:"limit_usd"`
}

func (s *Server) createBudget(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	var in budgetIn
	if err := decode(r, &in); err != nil {
		fail(w, 400, "invalid", err.Error())
		return
	}
	b := in.Budget
	if in.LimitUSD != "" {
		n, err := parseUSD(in.LimitUSD)
		if err != nil {
			fail(w, 400, "invalid", "limit_usd: "+err.Error())
			return
		}
		b.LimitNanoUSD = &n
	}
	switch b.ScopeType {
	case "key", "project", "vendor":
	default:
		fail(w, 400, "invalid", "scope_type must be key|project|vendor")
		return
	}
	if b.Kind != "monthly" && b.Kind != "lifetime" {
		fail(w, 400, "invalid", "kind must be monthly|lifetime")
		return
	}
	if b.LimitNanoUSD == nil && b.LimitTokens == nil {
		fail(w, 400, "invalid", "limit_usd, limit_nano_usd or limit_tokens is required")
		return
	}
	// scope must belong to the vendor
	ctx := r.Context()
	var scopeErr error
	switch b.ScopeType {
	case "key":
		_, scopeErr = s.db.GetKey(ctx, s.db.Pool, vid, b.ScopeID)
	case "project":
		_, scopeErr = s.db.GetProject(ctx, s.db.Pool, vid, b.ScopeID)
	case "vendor":
		if b.ScopeID != vid {
			scopeErr = store.ErrNotFound
		}
	}
	if scopeErr != nil {
		fail(w, 400, "invalid", "scope_id does not belong to this vendor")
		return
	}
	b.VendorID, b.Timezone, b.OnExceed, b.Enforcement, b.Status = vid, orDefault(b.Timezone, "UTC"), orDefault(b.OnExceed, "block"), orDefault(b.Enforcement, "available"), "active"
	if b.AnchorDay == 0 {
		b.AnchorDay = 1
	}
	if len(b.AlertThresholds) == 0 {
		b.AlertThresholds = []int32{50, 80, 100}
	}
	var out *store.Budget
	err := s.db.Tx(ctx, func(tx pgx.Tx) error {
		var err error
		if out, err = s.db.InsertBudget(ctx, tx, &b); err != nil {
			return err
		}
		if err := store.Audit(ctx, tx, &vid, "local", "create", "budget", &out.ID, nil, out); err != nil {
			return err
		}
		return store.Outbox(ctx, tx, "budget", out.ID, "create")
	})
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 201, out)
}

func (s *Server) getBudget(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	id, _ := pathID(r, "id")
	b, err := s.db.GetBudget(r.Context(), s.db.Pool, vid, id)
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, b)
}

func (s *Server) patchBudget(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	id, _ := pathID(r, "id")
	cur, err := s.db.GetBudget(r.Context(), s.db.Pool, vid, id)
	if err != nil {
		handleErr(w, err)
		return
	}
	before := *cur
	var in budgetIn
	in.Budget = *cur
	if err := applyPatch(r, &in, &in.Version); err != nil {
		fail(w, 400, "invalid", err.Error())
		return
	}
	b := in.Budget
	if in.LimitUSD != "" {
		n, err := parseUSD(in.LimitUSD)
		if err != nil {
			fail(w, 400, "invalid", "limit_usd: "+err.Error())
			return
		}
		b.LimitNanoUSD = &n
	}
	b.ID, b.VendorID, b.ScopeType, b.ScopeID = id, vid, before.ScopeType, before.ScopeID
	var out *store.Budget
	err = s.write(r.Context(), &vid, "update", "budget", id, before, nil, func(tx pgx.Tx) error {
		var err error
		out, err = s.db.UpdateBudget(r.Context(), tx, &b)
		return err
	})
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, out)
}

func (s *Server) deleteBudget(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	id, _ := pathID(r, "id")
	cur, err := s.db.GetBudget(r.Context(), s.db.Pool, vid, id)
	if err != nil {
		handleErr(w, err)
		return
	}
	before := *cur
	cur.Status = "deleted"
	err = s.write(r.Context(), &vid, "delete", "budget", id, before, nil, func(tx pgx.Tx) error {
		_, err := s.db.UpdateBudget(r.Context(), tx, cur)
		return err
	})
	if err != nil {
		handleErr(w, err)
		return
	}
	w.WriteHeader(204)
}

func (s *Server) budgetPeriods(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	id, _ := pathID(r, "id")
	if _, err := s.db.GetBudget(r.Context(), s.db.Pool, vid, id); err != nil {
		handleErr(w, err)
		return
	}
	ps, err := s.db.ListBudgetPeriods(r.Context(), id)
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, map[string]any{"data": ps})
}

func (s *Server) remainingView(ctx context.Context, bs []store.Budget) []map[string]any {
	scopes := budget.Resolve(bs, time.Now())
	rems, err := s.enforcer.Remaining(ctx, scopes)
	out := []map[string]any{}
	for i, sc := range scopes {
		row := map[string]any{"budget_id": sc.Budget.ID, "scope_type": sc.Budget.ScopeType, "scope_id": sc.Budget.ScopeID, "kind": sc.Budget.Kind,
			"period_start": sc.PeriodStart, "period_end": sc.PeriodEnd, "on_exceed": sc.Budget.OnExceed}
		if sc.Budget.LimitNanoUSD != nil {
			row["limit_nano_usd"], row["limit_usd"] = *sc.Budget.LimitNanoUSD, pricing.USD(*sc.Budget.LimitNanoUSD)
		}
		if err == nil {
			row["spent_nano_usd"], row["spent_usd"], row["reserved_nano_usd"], row["spent_tokens"] = rems[i].Spent, pricing.USD(rems[i].Spent), rems[i].Reserved, rems[i].Tokens
			if rems[i].RemainingNS != nil {
				row["remaining_nano_usd"], row["remaining_usd"] = *rems[i].RemainingNS, pricing.USD(*rems[i].RemainingNS)
			}
		} else {
			row["live_counters"] = "unavailable"
		}
		out = append(out, row)
	}
	return out
}

func (s *Server) budgetRemaining(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	id, _ := pathID(r, "id")
	b, err := s.db.GetBudget(r.Context(), s.db.Pool, vid, id)
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, s.remainingView(r.Context(), []store.Budget{*b})[0])
}

// ---- webhooks

func (s *Server) listWebhooks(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	ws, err := s.db.ListWebhooks(r.Context(), vid)
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, map[string]any{"data": ws})
}

func (s *Server) createWebhook(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	var in struct {
		store.WebhookEndpoint
		Secret string `json:"secret"`
	}
	if err := decode(r, &in); err != nil || in.URL == "" {
		fail(w, 400, "invalid", "url is required")
		return
	}
	if in.Secret == "" {
		var b [24]byte
		rand.Read(b[:])
		in.Secret = "whsec_" + base64.RawURLEncoding.EncodeToString(b[:])
	}
	ct, _, err := s.keyring.Encrypt([]byte(in.Secret))
	if err != nil {
		handleErr(w, err)
		return
	}
	ep := in.WebhookEndpoint
	ep.VendorID, ep.SecretCiphertext, ep.Status = vid, ct, "active"
	if len(ep.Events) == 0 {
		ep.Events = []string{"*"}
	}
	var out *store.WebhookEndpoint
	err = s.write(r.Context(), &vid, "create", "webhook", uuid.Nil, nil, nil, func(tx pgx.Tx) error {
		var err error
		out, err = s.db.InsertWebhook(r.Context(), tx, &ep)
		return err
	})
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 201, map[string]any{"webhook": out, "secret": in.Secret, "note": "the secret is shown once; verify X-Signature with HMAC-SHA256 over '<t>.<body>'"})
}

func (s *Server) getWebhook(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	id, _ := pathID(r, "id")
	ep, err := s.db.GetWebhook(r.Context(), s.db.Pool, vid, id)
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, ep)
}

func (s *Server) patchWebhook(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	id, _ := pathID(r, "id")
	cur, err := s.db.GetWebhook(r.Context(), s.db.Pool, vid, id)
	if err != nil {
		handleErr(w, err)
		return
	}
	before := *cur
	if err := applyPatch(r, cur, &cur.Version); err != nil {
		fail(w, 400, "invalid", err.Error())
		return
	}
	cur.ID, cur.VendorID = id, vid
	var out *store.WebhookEndpoint
	err = s.write(r.Context(), &vid, "update", "webhook", id, before, nil, func(tx pgx.Tx) error {
		var err error
		out, err = s.db.UpdateWebhook(r.Context(), tx, cur)
		return err
	})
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, out)
}

func (s *Server) deleteWebhook(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	id, _ := pathID(r, "id")
	cur, err := s.db.GetWebhook(r.Context(), s.db.Pool, vid, id)
	if err != nil {
		handleErr(w, err)
		return
	}
	before := *cur
	cur.Status = "deleted"
	err = s.write(r.Context(), &vid, "delete", "webhook", id, before, nil, func(tx pgx.Tx) error {
		_, err := s.db.UpdateWebhook(r.Context(), tx, cur)
		return err
	})
	if err != nil {
		handleErr(w, err)
		return
	}
	w.WriteHeader(204)
}

func (s *Server) testWebhook(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	id, _ := pathID(r, "id")
	ep, err := s.db.GetWebhook(r.Context(), s.db.Pool, vid, id)
	if err != nil {
		handleErr(w, err)
		return
	}
	eventID := "ping:" + uuid.NewString()
	body, _ := json.Marshal(map[string]any{"id": eventID, "type": "ping", "created_at": time.Now().UTC(), "data": map[string]any{"webhook_id": ep.ID}})
	if err := s.db.EnqueueDelivery(r.Context(), s.db.Pool, ep.ID, eventID, "ping", body); err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 202, map[string]any{"event_id": eventID, "status": "queued"})
}

func (s *Server) listDeliveries(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	id, _ := pathID(r, "id")
	if _, err := s.db.GetWebhook(r.Context(), s.db.Pool, vid, id); err != nil {
		handleErr(w, err)
		return
	}
	ds, err := s.db.ListDeliveries(r.Context(), id, r.URL.Query().Get("status"))
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, map[string]any{"data": ds})
}

func (s *Server) replayDelivery(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	id, _ := pathID(r, "id")
	if err := s.db.ReplayDelivery(r.Context(), vid, id); err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 202, map[string]any{"status": "queued"})
}

// ---- analytics and audit

func optUUID(r *http.Request, name string) *uuid.UUID {
	if v := r.URL.Query().Get(name); v != "" {
		if id, err := uuid.Parse(v); err == nil {
			return &id
		}
	}
	return nil
}

func (s *Server) analyticsUsage(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	from, to, gran := timeRange(r)
	rows, err := s.ch.Usage(r.Context(), analytics.UsageQuery{VendorID: vid, ProjectID: optUUID(r, "project_id"), KeyID: optUUID(r, "key_id"),
		GroupBy: r.URL.Query().Get("group_by"), From: from, To: to, Granularity: gran})
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, map[string]any{"data": rows})
}

func (s *Server) analyticsRequests(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	from, to, _ := timeRange(r)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	rows, err := s.ch.Requests(r.Context(), analytics.RequestsQuery{VendorID: vid, ProjectID: optUUID(r, "project_id"), KeyID: optUUID(r, "key_id"),
		Status: r.URL.Query().Get("status"), ErrorClass: r.URL.Query().Get("error_class"), From: from, To: to, Limit: limit})
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, map[string]any{"data": rows})
}

func (s *Server) analyticsRequest(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	rows, err := s.ch.Request(r.Context(), vid, chi.URLParam(r, "rid"))
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, map[string]any{"request_id": chi.URLParam(r, "rid"), "attempts": rows})
}

func (s *Server) analyticsBudgetsRemaining(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	bs, err := s.db.ListBudgets(r.Context(), vid, r.URL.Query().Get("scope_type"), optUUID(r, "scope_id"))
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, map[string]any{"data": s.remainingView(r.Context(), bs)})
}

func (s *Server) analyticsFallbacks(w http.ResponseWriter, r *http.Request) {
	vid, okv := requireVendor(w, r)
	if !okv {
		return
	}
	from, to, _ := timeRange(r)
	rows, err := s.ch.Fallbacks(r.Context(), vid, from, to)
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, map[string]any{"data": rows})
}

func (s *Server) audit(w http.ResponseWriter, r *http.Request) {
	var vid *uuid.UUID
	if v, okv := vendorOf(r); okv {
		vid = &v
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.ListAudit(r.Context(), vid, r.URL.Query().Get("entity_type"), optUUID(r, "entity_id"), limit)
	if err != nil {
		handleErr(w, err)
		return
	}
	ok(w, 200, map[string]any{"data": rows})
}

var _ = webhooks.Sign
