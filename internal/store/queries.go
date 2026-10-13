package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var ErrNotFound = errors.New("not found")
var ErrVersionConflict = errors.New("version conflict")

func one[T any](ctx context.Context, q Querier, sql string, args ...any) (*T, error) {
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	v, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByNameLax[T])
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return v, err
}

func many[T any](ctx context.Context, q Querier, sql string, args ...any) ([]T, error) {
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	out, err := pgx.CollectRows(rows, pgx.RowToStructByNameLax[T])
	if out == nil {
		out = []T{}
	}
	return out, err
}

// Audit writes an audit row. before/after are marshalled from any value (nil allowed).
func Audit(ctx context.Context, q Querier, vendorID *uuid.UUID, actor, action, entityType string, entityID *uuid.UUID, before, after any) error {
	var b, a []byte
	if before != nil {
		b, _ = json.Marshal(before)
	}
	if after != nil {
		a, _ = json.Marshal(after)
	}
	_, err := q.Exec(ctx, `INSERT INTO audit_logs(vendor_id, actor, action, entity_type, entity_id, before, after) VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		vendorID, actor, action, entityType, entityID, nullJSON(b), nullJSON(a))
	return err
}

func nullJSON(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return json.RawMessage(b)
}

// Outbox records a config change to be published to gateway caches after commit.
func Outbox(ctx context.Context, q Querier, entityType string, entityID uuid.UUID, op string) error {
	_, err := q.Exec(ctx, `INSERT INTO config_outbox(entity_type, entity_id, op) VALUES ($1,$2,$3)`, entityType, entityID, op)
	return err
}

func (d *DB) DrainOutbox(ctx context.Context, limit int) ([]ConfigChange, error) {
	var out []ConfigChange
	err := d.Tx(ctx, func(tx pgx.Tx) error {
		rows, err := many[ConfigChange](ctx, tx, `DELETE FROM config_outbox WHERE id IN (SELECT id FROM config_outbox ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED) RETURNING id, entity_type, entity_id, op`, limit)
		out = rows
		return err
	})
	return out, err
}

// ---- vendors

func (d *DB) ListVendors(ctx context.Context) ([]Vendor, error) {
	return many[Vendor](ctx, d.Pool, `SELECT * FROM vendors ORDER BY created_at`)
}
func (d *DB) GetVendor(ctx context.Context, q Querier, id uuid.UUID) (*Vendor, error) {
	return one[Vendor](ctx, q, `SELECT * FROM vendors WHERE id=$1`, id)
}
func (d *DB) InsertVendor(ctx context.Context, q Querier, v *Vendor) (*Vendor, error) {
	return one[Vendor](ctx, q, `INSERT INTO vendors(name, status, settings) VALUES ($1,$2,$3) RETURNING *`, v.Name, v.Status, v.Settings)
}
func (d *DB) UpdateVendor(ctx context.Context, q Querier, v *Vendor) (*Vendor, error) {
	return versioned[Vendor](ctx, q, `UPDATE vendors SET name=$3, status=$4, settings=$5, version=version+1, updated_at=now() WHERE id=$1 AND version=$2 RETURNING *`,
		v.ID, v.Version, v.Name, v.Status, v.Settings)
}

func versioned[T any](ctx context.Context, q Querier, sql string, args ...any) (*T, error) {
	v, err := one[T](ctx, q, sql, args...)
	if errors.Is(err, ErrNotFound) {
		return nil, ErrVersionConflict
	}
	return v, err
}

// ---- providers

func (d *DB) ListProviders(ctx context.Context, q Querier) ([]Provider, error) {
	return many[Provider](ctx, q, `SELECT * FROM providers WHERE deleted_at IS NULL ORDER BY slug`)
}
func (d *DB) GetProvider(ctx context.Context, q Querier, id uuid.UUID) (*Provider, error) {
	return one[Provider](ctx, q, `SELECT * FROM providers WHERE id=$1 AND deleted_at IS NULL`, id)
}
func (d *DB) GetProviderBySlug(ctx context.Context, q Querier, slug string) (*Provider, error) {
	return one[Provider](ctx, q, `SELECT * FROM providers WHERE slug=$1 AND deleted_at IS NULL`, slug)
}
func (d *DB) InsertProvider(ctx context.Context, q Querier, p *Provider) (*Provider, error) {
	return one[Provider](ctx, q, `INSERT INTO providers(slug, display_name, dialect, base_url, region, status, default_headers, rpm_limit, tpm_limit)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *`, p.Slug, p.DisplayName, p.Dialect, p.BaseURL, p.Region, p.Status, p.DefaultHeaders, p.RPMLimit, p.TPMLimit)
}
func (d *DB) UpdateProvider(ctx context.Context, q Querier, p *Provider) (*Provider, error) {
	return versioned[Provider](ctx, q, `UPDATE providers SET slug=$3, display_name=$4, dialect=$5, base_url=$6, region=$7, status=$8, default_headers=$9, rpm_limit=$10, tpm_limit=$11, version=version+1, updated_at=now()
		WHERE id=$1 AND version=$2 AND deleted_at IS NULL RETURNING *`, p.ID, p.Version, p.Slug, p.DisplayName, p.Dialect, p.BaseURL, p.Region, p.Status, p.DefaultHeaders, p.RPMLimit, p.TPMLimit)
}
func (d *DB) SoftDeleteProvider(ctx context.Context, q Querier, id uuid.UUID) error {
	var n int
	if err := q.QueryRow(ctx, `SELECT count(*) FROM model_deployments WHERE provider_id=$1 AND deleted_at IS NULL`, id).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return fmt.Errorf("provider has %d active deployments", n)
	}
	_, err := q.Exec(ctx, `UPDATE providers SET deleted_at=now(), status='deleted', version=version+1 WHERE id=$1`, id)
	return err
}

func (d *DB) ListCredentials(ctx context.Context, q Querier, providerID uuid.UUID) ([]ProviderCredential, error) {
	return many[ProviderCredential](ctx, q, `SELECT * FROM provider_credentials WHERE provider_id=$1 ORDER BY version DESC`, providerID)
}
func (d *DB) ActiveCredentials(ctx context.Context, q Querier) ([]ProviderCredential, error) {
	return many[ProviderCredential](ctx, q, `SELECT DISTINCT ON (provider_id) * FROM provider_credentials WHERE status='active' ORDER BY provider_id, version DESC`)
}
func (d *DB) InsertCredential(ctx context.Context, q Querier, providerID uuid.UUID, ciphertext []byte, keyID string) (*ProviderCredential, error) {
	return one[ProviderCredential](ctx, q, `INSERT INTO provider_credentials(provider_id, version, ciphertext, key_id)
		VALUES ($1, (SELECT coalesce(max(version),0)+1 FROM provider_credentials WHERE provider_id=$1), $2, $3) RETURNING *`, providerID, ciphertext, keyID)
}
func (d *DB) RevokeCredential(ctx context.Context, q Querier, providerID, id uuid.UUID) error {
	tag, err := q.Exec(ctx, `UPDATE provider_credentials SET status='revoked', rotated_at=now() WHERE id=$1 AND provider_id=$2`, id, providerID)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

// ---- models

func (d *DB) ListModels(ctx context.Context, q Querier) ([]Model, error) {
	return many[Model](ctx, q, `SELECT * FROM models ORDER BY slug`)
}
func (d *DB) GetModel(ctx context.Context, q Querier, id uuid.UUID) (*Model, error) {
	return one[Model](ctx, q, `SELECT * FROM models WHERE id=$1`, id)
}
func (d *DB) GetModelBySlug(ctx context.Context, q Querier, slug string) (*Model, error) {
	return one[Model](ctx, q, `SELECT * FROM models WHERE slug=$1`, slug)
}
func (d *DB) InsertModel(ctx context.Context, q Querier, m *Model) (*Model, error) {
	return one[Model](ctx, q, `INSERT INTO models(slug, display_name, family, tokenizer, tokenizer_multiplier, capabilities, supported_efforts, status)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`, m.Slug, m.DisplayName, m.Family, m.Tokenizer, m.TokenizerMultiplier, m.Capabilities, m.SupportedEfforts, m.Status)
}
func (d *DB) UpdateModel(ctx context.Context, q Querier, m *Model) (*Model, error) {
	return versioned[Model](ctx, q, `UPDATE models SET slug=$3, display_name=$4, family=$5, tokenizer=$6, tokenizer_multiplier=$7, capabilities=$8, supported_efforts=$9, status=$10, version=version+1, updated_at=now()
		WHERE id=$1 AND version=$2 RETURNING *`, m.ID, m.Version, m.Slug, m.DisplayName, m.Family, m.Tokenizer, m.TokenizerMultiplier, m.Capabilities, m.SupportedEfforts, m.Status)
}

// ---- deployments

func (d *DB) ListDeployments(ctx context.Context, q Querier) ([]Deployment, error) {
	return many[Deployment](ctx, q, `SELECT * FROM model_deployments WHERE deleted_at IS NULL ORDER BY slug`)
}
func (d *DB) GetDeployment(ctx context.Context, q Querier, id uuid.UUID) (*Deployment, error) {
	return one[Deployment](ctx, q, `SELECT * FROM model_deployments WHERE id=$1 AND deleted_at IS NULL`, id)
}
func (d *DB) GetDeploymentBySlug(ctx context.Context, q Querier, slug string) (*Deployment, error) {
	return one[Deployment](ctx, q, `SELECT * FROM model_deployments WHERE slug=$1 AND deleted_at IS NULL`, slug)
}
func (d *DB) InsertDeployment(ctx context.Context, q Querier, dep *Deployment) (*Deployment, error) {
	return one[Deployment](ctx, q, `INSERT INTO model_deployments(model_id, provider_id, slug, provider_model_name, context_window, max_output_tokens, effort, priority, weight, rpm_limit, tpm_limit, status)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING *`,
		dep.ModelID, dep.ProviderID, dep.Slug, dep.ProviderModelName, dep.ContextWindow, dep.MaxOutputTokens, dep.Effort, dep.Priority, dep.Weight, dep.RPMLimit, dep.TPMLimit, dep.Status)
}
func (d *DB) UpdateDeployment(ctx context.Context, q Querier, dep *Deployment) (*Deployment, error) {
	return versioned[Deployment](ctx, q, `UPDATE model_deployments SET slug=$3, provider_model_name=$4, context_window=$5, max_output_tokens=$6, effort=$7, priority=$8, weight=$9, rpm_limit=$10, tpm_limit=$11, status=$12, version=version+1, updated_at=now()
		WHERE id=$1 AND version=$2 AND deleted_at IS NULL RETURNING *`,
		dep.ID, dep.Version, dep.Slug, dep.ProviderModelName, dep.ContextWindow, dep.MaxOutputTokens, dep.Effort, dep.Priority, dep.Weight, dep.RPMLimit, dep.TPMLimit, dep.Status)
}
func (d *DB) SoftDeleteDeployment(ctx context.Context, q Querier, id uuid.UUID) error {
	var n int
	if err := q.QueryRow(ctx, `SELECT count(*) FROM keys WHERE primary_deployment_id=$1 AND status='active'`, id).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return fmt.Errorf("deployment is the primary of %d active keys", n)
	}
	_, err := q.Exec(ctx, `UPDATE model_deployments SET deleted_at=now(), status='deleted', version=version+1 WHERE id=$1`, id)
	return err
}

func (d *DB) ListPrices(ctx context.Context, q Querier, deploymentID uuid.UUID) ([]DeploymentPrice, error) {
	return many[DeploymentPrice](ctx, q, `SELECT * FROM deployment_prices WHERE deployment_id=$1 ORDER BY effective_from DESC`, deploymentID)
}
func (d *DB) CurrentPrices(ctx context.Context, q Querier) ([]DeploymentPrice, error) {
	return many[DeploymentPrice](ctx, q, `SELECT DISTINCT ON (deployment_id) * FROM deployment_prices WHERE effective_from <= now() ORDER BY deployment_id, effective_from DESC`)
}
func (d *DB) InsertPrice(ctx context.Context, q Querier, p *DeploymentPrice) (*DeploymentPrice, error) {
	if p.EffectiveFrom.IsZero() {
		p.EffectiveFrom = time.Now()
	}
	return one[DeploymentPrice](ctx, q, `INSERT INTO deployment_prices(deployment_id, effective_from, tiers, verified, source, created_by) VALUES ($1,$2,$3,$4,$5,$6) RETURNING *`,
		p.DeploymentID, p.EffectiveFrom, p.Tiers, p.Verified, p.Source, p.CreatedBy)
}

// ---- projects

func (d *DB) ListProjects(ctx context.Context, vendorID uuid.UUID) ([]Project, error) {
	return many[Project](ctx, d.Pool, `SELECT * FROM projects WHERE vendor_id=$1 AND deleted_at IS NULL ORDER BY created_at`, vendorID)
}
func (d *DB) GetProject(ctx context.Context, q Querier, vendorID, id uuid.UUID) (*Project, error) {
	return one[Project](ctx, q, `SELECT * FROM projects WHERE id=$1 AND vendor_id=$2 AND deleted_at IS NULL`, id, vendorID)
}
func (d *DB) InsertProject(ctx context.Context, q Querier, p *Project) (*Project, error) {
	return one[Project](ctx, q, `INSERT INTO projects(vendor_id, name, description, metadata, status) VALUES ($1,$2,$3,$4,$5) RETURNING *`, p.VendorID, p.Name, p.Description, p.Metadata, p.Status)
}
func (d *DB) UpdateProject(ctx context.Context, q Querier, p *Project) (*Project, error) {
	return versioned[Project](ctx, q, `UPDATE projects SET name=$3, description=$4, metadata=$5, status=$6, version=version+1, updated_at=now() WHERE id=$1 AND version=$2 AND deleted_at IS NULL RETURNING *`,
		p.ID, p.Version, p.Name, p.Description, p.Metadata, p.Status)
}
func (d *DB) SoftDeleteProject(ctx context.Context, q Querier, id uuid.UUID) error {
	if _, err := q.Exec(ctx, `UPDATE keys SET status='revoked', version=version+1, updated_at=now() WHERE project_id=$1 AND status='active'`, id); err != nil {
		return err
	}
	_, err := q.Exec(ctx, `UPDATE projects SET deleted_at=now(), status='deleted', version=version+1 WHERE id=$1`, id)
	return err
}

// ---- keys

func (d *DB) ListKeys(ctx context.Context, vendorID uuid.UUID, projectID *uuid.UUID, status string) ([]Key, error) {
	return many[Key](ctx, d.Pool, `SELECT * FROM keys WHERE vendor_id=$1 AND ($2::uuid IS NULL OR project_id=$2) AND ($3='' OR status=$3) ORDER BY created_at`, vendorID, projectID, status)
}
func (d *DB) GetKey(ctx context.Context, q Querier, vendorID, id uuid.UUID) (*Key, error) {
	return one[Key](ctx, q, `SELECT * FROM keys WHERE id=$1 AND vendor_id=$2`, id, vendorID)
}
func (d *DB) GetKeyByID(ctx context.Context, q Querier, id uuid.UUID) (*Key, error) {
	return one[Key](ctx, q, `SELECT * FROM keys WHERE id=$1`, id)
}
func (d *DB) GetKeyByHash(ctx context.Context, q Querier, hash []byte) (*Key, error) {
	return one[Key](ctx, q, `SELECT * FROM keys WHERE key_hash=$1`, hash)
}
func (d *DB) InsertKey(ctx context.Context, q Querier, k *Key) (*Key, error) {
	return one[Key](ctx, q, `INSERT INTO keys(project_id, vendor_id, key_hash, key_prefix, name, status, primary_deployment_id, fallback_policy, response_format, request_overrides, allowed_deployments, rpm_limit, tpm_limit, payload_logging, expires_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) RETURNING *`,
		k.ProjectID, k.VendorID, k.KeyHash, k.KeyPrefix, k.Name, k.Status, k.PrimaryDeploymentID, k.FallbackPolicy, k.ResponseFormat, k.RequestOverrides, k.AllowedDeployments, k.RPMLimit, k.TPMLimit, k.PayloadLogging, k.ExpiresAt)
}
func (d *DB) UpdateKey(ctx context.Context, q Querier, k *Key) (*Key, error) {
	return versioned[Key](ctx, q, `UPDATE keys SET key_hash=$3, key_prefix=$4, name=$5, status=$6, primary_deployment_id=$7, fallback_policy=$8, response_format=$9, request_overrides=$10, allowed_deployments=$11, rpm_limit=$12, tpm_limit=$13, payload_logging=$14, expires_at=$15, version=version+1, updated_at=now()
		WHERE id=$1 AND version=$2 RETURNING *`,
		k.ID, k.Version, k.KeyHash, k.KeyPrefix, k.Name, k.Status, k.PrimaryDeploymentID, k.FallbackPolicy, k.ResponseFormat, k.RequestOverrides, k.AllowedDeployments, k.RPMLimit, k.TPMLimit, k.PayloadLogging, k.ExpiresAt)
}
func (d *DB) TouchKey(ctx context.Context, id uuid.UUID) {
	_, _ = d.Pool.Exec(ctx, `UPDATE keys SET last_used_at=now() WHERE id=$1 AND (last_used_at IS NULL OR last_used_at < now() - interval '1 minute')`, id)
}

// ---- budgets

func (d *DB) ListBudgets(ctx context.Context, vendorID uuid.UUID, scopeType string, scopeID *uuid.UUID) ([]Budget, error) {
	return many[Budget](ctx, d.Pool, `SELECT * FROM budgets WHERE vendor_id=$1 AND ($2='' OR scope_type=$2) AND ($3::uuid IS NULL OR scope_id=$3) AND status<>'deleted' ORDER BY created_at`, vendorID, scopeType, scopeID)
}
func (d *DB) BudgetsForScopes(ctx context.Context, q Querier, scopeIDs []uuid.UUID) ([]Budget, error) {
	return many[Budget](ctx, q, `SELECT * FROM budgets WHERE scope_id = ANY($1) AND status='active'`, scopeIDs)
}
func (d *DB) GetBudget(ctx context.Context, q Querier, vendorID, id uuid.UUID) (*Budget, error) {
	return one[Budget](ctx, q, `SELECT * FROM budgets WHERE id=$1 AND vendor_id=$2 AND status<>'deleted'`, id, vendorID)
}
func (d *DB) InsertBudget(ctx context.Context, q Querier, b *Budget) (*Budget, error) {
	return one[Budget](ctx, q, `INSERT INTO budgets(vendor_id, scope_type, scope_id, kind, limit_nano_usd, limit_tokens, timezone, anchor_day, on_exceed, alert_thresholds, enforcement, status)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING *`,
		b.VendorID, b.ScopeType, b.ScopeID, b.Kind, b.LimitNanoUSD, b.LimitTokens, b.Timezone, b.AnchorDay, b.OnExceed, b.AlertThresholds, b.Enforcement, b.Status)
}
func (d *DB) UpdateBudget(ctx context.Context, q Querier, b *Budget) (*Budget, error) {
	return versioned[Budget](ctx, q, `UPDATE budgets SET kind=$3, limit_nano_usd=$4, limit_tokens=$5, timezone=$6, anchor_day=$7, on_exceed=$8, alert_thresholds=$9, enforcement=$10, status=$11, version=version+1, updated_at=now()
		WHERE id=$1 AND version=$2 RETURNING *`, b.ID, b.Version, b.Kind, b.LimitNanoUSD, b.LimitTokens, b.Timezone, b.AnchorDay, b.OnExceed, b.AlertThresholds, b.Enforcement, b.Status)
}
func (d *DB) ListBudgetPeriods(ctx context.Context, budgetID uuid.UUID) ([]BudgetPeriod, error) {
	return many[BudgetPeriod](ctx, d.Pool, `SELECT * FROM budget_periods WHERE budget_id=$1 ORDER BY period_start DESC`, budgetID)
}
func (d *DB) GetBudgetPeriod(ctx context.Context, q Querier, budgetID uuid.UUID, start time.Time) (*BudgetPeriod, error) {
	return one[BudgetPeriod](ctx, q, `SELECT * FROM budget_periods WHERE budget_id=$1 AND period_start=$2`, budgetID, start)
}

// AddBudgetSpend is the batched, idempotent-by-window aggregate upsert used by the ledger.
func (d *DB) AddBudgetSpend(ctx context.Context, q Querier, budgetID uuid.UUID, start, end time.Time, nano, tokens, requests int64) (*BudgetPeriod, error) {
	return one[BudgetPeriod](ctx, q, `INSERT INTO budget_periods(budget_id, period_start, period_end, spent_nano_usd, spent_tokens, request_count)
		VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (budget_id, period_start) DO UPDATE SET spent_nano_usd = budget_periods.spent_nano_usd + EXCLUDED.spent_nano_usd,
		  spent_tokens = budget_periods.spent_tokens + EXCLUDED.spent_tokens, request_count = budget_periods.request_count + EXCLUDED.request_count, updated_at=now()
		RETURNING *`, budgetID, start, end, nano, tokens, requests)
}
func (d *DB) MarkThreshold(ctx context.Context, q Querier, budgetID uuid.UUID, start time.Time, pct int32, exceeded bool) error {
	_, err := q.Exec(ctx, `UPDATE budget_periods SET thresholds_fired = array_append(thresholds_fired, $3), exceeded_at = CASE WHEN $4 AND exceeded_at IS NULL THEN now() ELSE exceeded_at END WHERE budget_id=$1 AND period_start=$2 AND NOT ($3 = ANY(thresholds_fired))`, budgetID, start, pct, exceeded)
	return err
}

// ---- webhooks

func (d *DB) ListWebhooks(ctx context.Context, vendorID uuid.UUID) ([]WebhookEndpoint, error) {
	return many[WebhookEndpoint](ctx, d.Pool, `SELECT * FROM webhook_endpoints WHERE vendor_id=$1 AND status<>'deleted' ORDER BY created_at`, vendorID)
}
func (d *DB) ActiveWebhooksForVendor(ctx context.Context, q Querier, vendorID uuid.UUID, event string) ([]WebhookEndpoint, error) {
	return many[WebhookEndpoint](ctx, q, `SELECT * FROM webhook_endpoints WHERE vendor_id=$1 AND status='active' AND ($2 = ANY(events) OR '*' = ANY(events))`, vendorID, event)
}
func (d *DB) GetWebhook(ctx context.Context, q Querier, vendorID, id uuid.UUID) (*WebhookEndpoint, error) {
	return one[WebhookEndpoint](ctx, q, `SELECT * FROM webhook_endpoints WHERE id=$1 AND vendor_id=$2 AND status<>'deleted'`, id, vendorID)
}
func (d *DB) InsertWebhook(ctx context.Context, q Querier, w *WebhookEndpoint) (*WebhookEndpoint, error) {
	return one[WebhookEndpoint](ctx, q, `INSERT INTO webhook_endpoints(vendor_id, url, secret_ciphertext, events, status) VALUES ($1,$2,$3,$4,$5) RETURNING *`, w.VendorID, w.URL, w.SecretCiphertext, w.Events, w.Status)
}
func (d *DB) UpdateWebhook(ctx context.Context, q Querier, w *WebhookEndpoint) (*WebhookEndpoint, error) {
	return versioned[WebhookEndpoint](ctx, q, `UPDATE webhook_endpoints SET url=$3, events=$4, status=$5, version=version+1, updated_at=now() WHERE id=$1 AND version=$2 RETURNING *`, w.ID, w.Version, w.URL, w.Events, w.Status)
}
func (d *DB) EnqueueDelivery(ctx context.Context, q Querier, endpointID uuid.UUID, eventID, eventType string, payload []byte) error {
	_, err := q.Exec(ctx, `INSERT INTO webhook_deliveries(endpoint_id, event_id, event_type, payload) VALUES ($1,$2,$3,$4) ON CONFLICT (endpoint_id, event_id) DO NOTHING`, endpointID, eventID, eventType, json.RawMessage(payload))
	return err
}
func (d *DB) ListDeliveries(ctx context.Context, endpointID uuid.UUID, status string) ([]WebhookDelivery, error) {
	return many[WebhookDelivery](ctx, d.Pool, `SELECT * FROM webhook_deliveries WHERE endpoint_id=$1 AND ($2='' OR status=$2) ORDER BY created_at DESC LIMIT 200`, endpointID, status)
}
func (d *DB) ReplayDelivery(ctx context.Context, vendorID, id uuid.UUID) error {
	tag, err := d.Pool.Exec(ctx, `UPDATE webhook_deliveries d SET status='pending', next_attempt_at=now(), attempts=0 FROM webhook_endpoints e WHERE d.endpoint_id=e.id AND d.id=$1 AND e.vendor_id=$2`, id, vendorID)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}
func (d *DB) ClaimDeliveries(ctx context.Context, tx pgx.Tx, limit int) ([]WebhookDelivery, error) {
	return many[WebhookDelivery](ctx, tx, `SELECT * FROM webhook_deliveries WHERE status='pending' AND next_attempt_at <= now() ORDER BY next_attempt_at LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
}
func (d *DB) FinishDelivery(ctx context.Context, q Querier, id uuid.UUID, status string, attempts int32, next time.Time, code *int32, errText *string) error {
	_, err := q.Exec(ctx, `UPDATE webhook_deliveries SET status=$2, attempts=$3, next_attempt_at=$4, last_status_code=$5, last_error=$6, updated_at=now() WHERE id=$1`, id, status, attempts, next, code, errText)
	return err
}
func (d *DB) GetWebhookByID(ctx context.Context, q Querier, id uuid.UUID) (*WebhookEndpoint, error) {
	return one[WebhookEndpoint](ctx, q, `SELECT * FROM webhook_endpoints WHERE id=$1`, id)
}

// ---- audit

func (d *DB) ListAudit(ctx context.Context, vendorID *uuid.UUID, entityType string, entityID *uuid.UUID, limit int) ([]AuditLog, error) {
	return many[AuditLog](ctx, d.Pool, `SELECT * FROM audit_logs WHERE ($1::uuid IS NULL OR vendor_id=$1) AND ($2='' OR entity_type=$2) AND ($3::uuid IS NULL OR entity_id=$3) ORDER BY id DESC LIMIT $4`, vendorID, entityType, entityID, limit)
}
