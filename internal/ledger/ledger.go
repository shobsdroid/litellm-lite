// Package ledger is the usage worker: it aggregates usage events into
// durable budget_periods rows (batched per budget per window), detects
// thresholds and exceedance, and enqueues webhook deliveries in the same
// transaction.
package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rzp/litellm-lite/internal/events"
	"github.com/rzp/litellm-lite/internal/pricing"
	"github.com/rzp/litellm-lite/internal/store"
)

type Ledger struct {
	db *store.DB
}

func New(db *store.DB) *Ledger { return &Ledger{db: db} }

type agg struct {
	charge   events.BudgetCharge
	nano     int64
	tokens   int64
	requests int64
}

// Handler aggregates one bus batch and applies it in one transaction.
func (l *Ledger) Handler() events.Handler {
	return func(ctx context.Context, batch []*events.UsageEvent) {
		aggs := map[uuid.UUID]*agg{}
		denied := map[uuid.UUID]*events.UsageEvent{}
		for _, e := range batch {
			for _, c := range e.Budgets {
				a := aggs[c.BudgetID]
				if a == nil {
					a = &agg{charge: c}
					aggs[c.BudgetID] = a
				}
				a.nano += e.CostNanoUSD
				a.tokens += e.PromptTokens + e.CompletionTokens
				if e.AttemptNo == e.AttemptsTotal {
					a.requests++
				}
			}
			if e.DeniedBudgetID != nil {
				denied[*e.DeniedBudgetID] = e
			}
		}
		if len(aggs) == 0 && len(denied) == 0 {
			return
		}
		err := l.db.Tx(ctx, func(tx pgx.Tx) error {
			for id, e := range denied {
				if err := l.denied(ctx, tx, id, e); err != nil {
					return err
				}
			}
			for _, a := range aggs {
				p, err := l.db.AddBudgetSpend(ctx, tx, a.charge.BudgetID, a.charge.PeriodStart, a.charge.PeriodEnd, a.nano, a.tokens, a.requests)
				if err != nil {
					return err
				}
				if err := l.checkThresholds(ctx, tx, a.charge, p); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			slog.Error("ledger apply failed", "err", err)
		}
	}
}

// denied fires budget.exceeded once per budget per period when the gateway
// blocked a request. Dedup is the (endpoint, event_id) unique constraint.
func (l *Ledger) denied(ctx context.Context, tx pgx.Tx, budgetID uuid.UUID, e *events.UsageEvent) error {
	b, err := one[store.Budget](ctx, tx, `SELECT * FROM budgets WHERE id=$1`, budgetID)
	if err != nil {
		return nil
	}
	var charge *events.BudgetCharge
	for i := range e.Budgets {
		if e.Budgets[i].BudgetID == budgetID {
			charge = &e.Budgets[i]
		}
	}
	if charge == nil {
		return nil
	}
	if _, err := l.db.AddBudgetSpend(ctx, tx, budgetID, charge.PeriodStart, charge.PeriodEnd, 0, 0, 0); err != nil {
		return err
	}
	if err := l.db.MarkThreshold(ctx, tx, budgetID, charge.PeriodStart, 100, true); err != nil {
		return err
	}
	payload := map[string]any{
		"budget_id": b.ID, "scope_type": b.ScopeType, "scope_id": b.ScopeID, "kind": b.Kind, "threshold_pct": 100,
		"period_start": charge.PeriodStart, "period_end": charge.PeriodEnd, "on_exceed": b.OnExceed,
		"blocked_request_id": e.RequestID, "key_id": e.KeyID, "project_id": e.ProjectID,
	}
	if b.LimitNanoUSD != nil {
		payload["limit_nano_usd"] = *b.LimitNanoUSD
		payload["limit_usd"] = pricing.USD(*b.LimitNanoUSD)
	}
	return Enqueue(ctx, l.db, tx, b.VendorID, "budget.exceeded", fmt.Sprintf("%s:%d:denied", b.ID, charge.PeriodStart.Unix()), payload)
}

func (l *Ledger) checkThresholds(ctx context.Context, tx pgx.Tx, c events.BudgetCharge, p *store.BudgetPeriod) error {
	b, err := one[store.Budget](ctx, tx, `SELECT * FROM budgets WHERE id=$1`, c.BudgetID)
	if err != nil {
		return nil // budget deleted; nothing to alert
	}
	if b.LimitNanoUSD == nil || *b.LimitNanoUSD <= 0 {
		return nil
	}
	pct := p.SpentNanoUSD * 100 / *b.LimitNanoUSD
	for _, th := range b.AlertThresholds {
		if pct < int64(th) || has(p.ThresholdsFired, th) {
			continue
		}
		if err := l.db.MarkThreshold(ctx, tx, b.ID, p.PeriodStart, th, th >= 100); err != nil {
			return err
		}
		evType := "budget.threshold"
		if th >= 100 {
			evType = "budget.exceeded"
		}
		payload := map[string]any{
			"budget_id": b.ID, "scope_type": b.ScopeType, "scope_id": b.ScopeID, "kind": b.Kind,
			"threshold_pct": th, "spent_nano_usd": p.SpentNanoUSD, "spent_usd": pricing.USD(p.SpentNanoUSD),
			"limit_nano_usd": *b.LimitNanoUSD, "limit_usd": pricing.USD(*b.LimitNanoUSD),
			"period_start": p.PeriodStart, "period_end": p.PeriodEnd, "on_exceed": b.OnExceed,
		}
		eventID := fmt.Sprintf("%s:%d:%d", b.ID, p.PeriodStart.Unix(), th)
		if err := Enqueue(ctx, l.db, tx, b.VendorID, evType, eventID, payload); err != nil {
			return err
		}
		p.ThresholdsFired = append(p.ThresholdsFired, th)
	}
	return nil
}

// Enqueue fans an event out to every endpoint of the vendor subscribed to it.
func Enqueue(ctx context.Context, db *store.DB, tx pgx.Tx, vendorID uuid.UUID, evType, eventID string, payload map[string]any) error {
	eps, err := db.ActiveWebhooksForVendor(ctx, tx, vendorID, evType)
	if err != nil {
		return err
	}
	if len(eps) == 0 {
		return nil
	}
	body, _ := json.Marshal(map[string]any{"id": eventID, "type": evType, "created_at": time.Now().UTC(), "data": payload})
	for _, ep := range eps {
		if err := db.EnqueueDelivery(ctx, tx, ep.ID, eventID, evType, body); err != nil {
			return err
		}
	}
	return nil
}

func has(xs []int32, x int32) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func one[T any](ctx context.Context, tx pgx.Tx, sql string, args ...any) (*T, error) {
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByNameLax[T])
}
