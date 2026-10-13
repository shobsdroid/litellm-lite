// Package analytics writes request logs to ClickHouse in batches and serves
// the analytics read API from the raw table and its rollups.
package analytics

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/rzp/litellm-lite/internal/events"
)

type Store struct {
	conn driver.Conn
}

func Open(ctx context.Context, addr, db string) (*Store, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr}, Auth: clickhouse.Auth{Database: db, Username: "default"},
		Settings:    clickhouse.Settings{"async_insert": 1, "wait_for_async_insert": 0},
		DialTimeout: 5 * time.Second, MaxOpenConns: 8, MaxIdleConns: 4,
	})
	if err != nil {
		return nil, err
	}
	// Database may not exist yet: connect to default, create, reconnect.
	if err := conn.Ping(ctx); err != nil {
		boot, err2 := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}, Auth: clickhouse.Auth{Database: "default", Username: "default"}})
		if err2 != nil {
			return nil, err
		}
		if err2 = boot.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+db); err2 != nil {
			return nil, fmt.Errorf("clickhouse: %w", err2)
		}
		_ = boot.Close()
		if err := conn.Ping(ctx); err != nil {
			return nil, fmt.Errorf("clickhouse ping: %w", err)
		}
	}
	return &Store{conn: conn}, nil
}

func (s *Store) Migrate(ctx context.Context, files embed.FS, dir string) error {
	entries, err := fs.ReadDir(files, dir)
	if err != nil {
		return err
	}
	names := []string{}
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	for _, n := range names {
		body, _ := fs.ReadFile(files, dir+"/"+n)
		for _, stmt := range strings.Split(string(body), ";") {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}
			if err := s.conn.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("clickhouse migration %s: %w", n, err)
			}
		}
	}
	return nil
}

// Write inserts a batch of usage events. At-least-once; the table dedups.
func (s *Store) Write(ctx context.Context, batch []*events.UsageEvent) error {
	b, err := s.conn.PrepareBatch(ctx, `INSERT INTO request_logs (ts, request_id, trace_id, vendor_id, project_id, key_id, ingress_format, response_format, model_slug,
		requested_deployment_id, served_deployment_id, served_deployment_slug, provider_slug, attempt_no, attempts_total, fallback_used, fallback_reason, status_code, error_class, error_code, stream,
		prompt_tokens, cached_tokens, cache_write_tokens, completion_tokens, reasoning_tokens, price_id, cost_nano_usd, estimated_cost_nano_usd, usage_source, enforcement,
		latency_ms, ttft_ms, upstream_latency_ms, client_ip, user_agent, payload_ref)`)
	if err != nil {
		return err
	}
	for _, e := range batch {
		if err := b.Append(e.TS, e.RequestID, e.TraceID, e.VendorID.String(), e.ProjectID.String(), e.KeyID.String(), e.IngressFormat, e.ResponseFormat, e.ModelSlug,
			e.RequestedDeploymentID, e.ServedDeploymentID, e.ServedDeploymentSlug, e.ProviderSlug, uint8(e.AttemptNo), uint8(e.AttemptsTotal), b2u(e.FallbackUsed), e.FallbackReason, uint16(e.StatusCode), e.ErrorClass, e.ErrorCode, b2u(e.Stream),
			uint32(e.PromptTokens), uint32(e.CachedTokens), uint32(e.CacheWriteTokens), uint32(e.CompletionTokens), uint32(e.ReasoningTokens), e.PriceID, e.CostNanoUSD, e.EstimatedCostNanoUSD, e.UsageSource, e.Enforcement,
			uint32(e.LatencyMS), uint32(e.TTFTMS), uint32(e.UpstreamLatencyMS), e.ClientIP, e.UserAgent, e.PayloadRef); err != nil {
			return err
		}
	}
	return b.Send()
}

func b2u(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}

// Subscribe returns a bus handler that writes batches, retrying in place.
func (s *Store) Handler() events.Handler {
	return func(ctx context.Context, batch []*events.UsageEvent) {
		for attempt := 0; attempt < 3; attempt++ {
			if err := s.Write(ctx, batch); err == nil {
				return
			} else if attempt == 2 {
				slog.Error("clickhouse write failed; batch dropped", "n", len(batch), "err", err)
			} else {
				time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
			}
		}
	}
}

// ---- queries

type UsageRow struct {
	Bucket           time.Time `json:"bucket"`
	Group            string    `json:"group"`
	Requests         uint64    `json:"requests"`
	Errors           uint64    `json:"errors"`
	Fallbacks        uint64    `json:"fallbacks"`
	PromptTokens     uint64    `json:"prompt_tokens"`
	CachedTokens     uint64    `json:"cached_tokens"`
	CompletionTokens uint64    `json:"completion_tokens"`
	ReasoningTokens  uint64    `json:"reasoning_tokens"`
	CostNanoUSD      int64     `json:"cost_nano_usd"`
}

type UsageQuery struct {
	VendorID    uuid.UUID
	ProjectID   *uuid.UUID
	KeyID       *uuid.UUID
	GroupBy     string // key | project | deployment | provider | none
	From, To    time.Time
	Granularity string // hour | day
}

func (s *Store) Usage(ctx context.Context, q UsageQuery) ([]UsageRow, error) {
	groupCol := "''"
	switch q.GroupBy {
	case "key":
		groupCol = "key_id"
	case "project":
		groupCol = "project_id"
	case "deployment":
		groupCol = "served_deployment_slug"
	case "provider":
		groupCol = "provider_slug"
	}
	bucket := "toStartOfHour(hour)"
	if q.Granularity == "day" {
		bucket = "toStartOfDay(hour)"
	}
	where := "vendor_id = ? AND hour >= ? AND hour < ?"
	args := []any{q.VendorID.String(), q.From, q.To}
	if q.ProjectID != nil {
		where += " AND project_id = ?"
		args = append(args, q.ProjectID.String())
	}
	if q.KeyID != nil {
		where += " AND key_id = ?"
		args = append(args, q.KeyID.String())
	}
	sql := fmt.Sprintf(`SELECT %s AS bucket, %s AS grp, sum(requests), sum(errors), sum(fallbacks), sum(prompt_tokens), sum(cached_tokens), sum(completion_tokens), sum(reasoning_tokens), sum(cost_nano_usd)
		FROM usage_hourly WHERE %s GROUP BY bucket, grp ORDER BY bucket, grp`, bucket, groupCol, where)
	rows, err := s.conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []UsageRow{}
	for rows.Next() {
		var r UsageRow
		if err := rows.Scan(&r.Bucket, &r.Group, &r.Requests, &r.Errors, &r.Fallbacks, &r.PromptTokens, &r.CachedTokens, &r.CompletionTokens, &r.ReasoningTokens, &r.CostNanoUSD); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

type RequestRow struct {
	TS                   time.Time `json:"ts"`
	RequestID            string    `json:"request_id"`
	ProjectID            string    `json:"project_id"`
	KeyID                string    `json:"key_id"`
	ModelSlug            string    `json:"model_slug"`
	ServedDeploymentSlug string    `json:"served_deployment_slug"`
	ProviderSlug         string    `json:"provider_slug"`
	AttemptNo            uint8     `json:"attempt_no"`
	AttemptsTotal        uint8     `json:"attempts_total"`
	FallbackUsed         uint8     `json:"fallback_used"`
	FallbackReason       string    `json:"fallback_reason"`
	StatusCode           uint16    `json:"status_code"`
	ErrorClass           string    `json:"error_class"`
	ErrorCode            string    `json:"error_code"`
	Stream               uint8     `json:"stream"`
	PromptTokens         uint32    `json:"prompt_tokens"`
	CachedTokens         uint32    `json:"cached_tokens"`
	CompletionTokens     uint32    `json:"completion_tokens"`
	ReasoningTokens      uint32    `json:"reasoning_tokens"`
	PriceID              string    `json:"price_id"`
	CostNanoUSD          int64     `json:"cost_nano_usd"`
	EstimatedCostNanoUSD int64     `json:"estimated_cost_nano_usd"`
	UsageSource          string    `json:"usage_source"`
	Enforcement          string    `json:"enforcement"`
	LatencyMS            uint32    `json:"latency_ms"`
	TTFTMS               uint32    `json:"ttft_ms"`
}

const requestCols = `ts, request_id, project_id, key_id, model_slug, served_deployment_slug, provider_slug, attempt_no, attempts_total, fallback_used, fallback_reason, status_code, error_class, error_code, stream,
	prompt_tokens, cached_tokens, completion_tokens, reasoning_tokens, price_id, cost_nano_usd, estimated_cost_nano_usd, usage_source, enforcement, latency_ms, ttft_ms`

func scanRequests(rows driver.Rows) ([]RequestRow, error) {
	defer rows.Close()
	out := []RequestRow{}
	for rows.Next() {
		var r RequestRow
		if err := rows.Scan(&r.TS, &r.RequestID, &r.ProjectID, &r.KeyID, &r.ModelSlug, &r.ServedDeploymentSlug, &r.ProviderSlug, &r.AttemptNo, &r.AttemptsTotal, &r.FallbackUsed, &r.FallbackReason, &r.StatusCode, &r.ErrorClass, &r.ErrorCode, &r.Stream,
			&r.PromptTokens, &r.CachedTokens, &r.CompletionTokens, &r.ReasoningTokens, &r.PriceID, &r.CostNanoUSD, &r.EstimatedCostNanoUSD, &r.UsageSource, &r.Enforcement, &r.LatencyMS, &r.TTFTMS); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

type RequestsQuery struct {
	VendorID   uuid.UUID
	ProjectID  *uuid.UUID
	KeyID      *uuid.UUID
	Status     string // "" | ok | error
	ErrorClass string
	From, To   time.Time
	Limit      int
}

func (s *Store) Requests(ctx context.Context, q RequestsQuery) ([]RequestRow, error) {
	where := "vendor_id = ? AND ts >= ? AND ts < ?"
	args := []any{q.VendorID.String(), q.From, q.To}
	if q.ProjectID != nil {
		where += " AND project_id = ?"
		args = append(args, q.ProjectID.String())
	}
	if q.KeyID != nil {
		where += " AND key_id = ?"
		args = append(args, q.KeyID.String())
	}
	switch q.Status {
	case "ok":
		where += " AND status_code < 400"
	case "error":
		where += " AND status_code >= 400"
	}
	if q.ErrorClass != "" {
		where += " AND error_class = ?"
		args = append(args, q.ErrorClass)
	}
	if q.Limit <= 0 || q.Limit > 1000 {
		q.Limit = 100
	}
	args = append(args, q.Limit)
	rows, err := s.conn.Query(ctx, fmt.Sprintf(`SELECT %s FROM request_logs FINAL WHERE %s ORDER BY ts DESC LIMIT ?`, requestCols, where), args...)
	if err != nil {
		return nil, err
	}
	return scanRequests(rows)
}

func (s *Store) Request(ctx context.Context, vendorID uuid.UUID, requestID string) ([]RequestRow, error) {
	rows, err := s.conn.Query(ctx, fmt.Sprintf(`SELECT %s FROM request_logs FINAL WHERE vendor_id = ? AND request_id = ? ORDER BY attempt_no`, requestCols), vendorID.String(), requestID)
	if err != nil {
		return nil, err
	}
	return scanRequests(rows)
}

type FallbackRow struct {
	ServedDeploymentSlug string `json:"served_deployment_slug"`
	FallbackReason       string `json:"fallback_reason"`
	Count                uint64 `json:"count"`
}

func (s *Store) Fallbacks(ctx context.Context, vendorID uuid.UUID, from, to time.Time) ([]FallbackRow, error) {
	rows, err := s.conn.Query(ctx, `SELECT served_deployment_slug, fallback_reason, count() FROM request_logs FINAL WHERE vendor_id = ? AND ts >= ? AND ts < ? AND fallback_used = 1 GROUP BY served_deployment_slug, fallback_reason ORDER BY count() DESC`, vendorID.String(), from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []FallbackRow{}
	for rows.Next() {
		var r FallbackRow
		if err := rows.Scan(&r.ServedDeploymentSlug, &r.FallbackReason, &r.Count); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// DeploymentHealth summarizes the last 15 minutes for the control plane.
type DeploymentHealth struct {
	Requests uint64  `json:"requests"`
	Errors   uint64  `json:"errors"`
	P50MS    float64 `json:"p50_ms"`
	P99MS    float64 `json:"p99_ms"`
}

func (s *Store) DeploymentHealth(ctx context.Context, slug string) (*DeploymentHealth, error) {
	var h DeploymentHealth
	row := s.conn.QueryRow(ctx, `SELECT count(), countIf(status_code >= 400), quantile(0.5)(latency_ms), quantile(0.99)(latency_ms) FROM request_logs WHERE served_deployment_slug = ? AND ts >= now() - INTERVAL 15 MINUTE`, slug)
	if err := row.Scan(&h.Requests, &h.Errors, &h.P50MS, &h.P99MS); err != nil {
		return nil, err
	}
	return &h, nil
}
