// Package metrics defines the Prometheus series. Labels are intentionally
// limited to low-cardinality dimensions; per-key numbers live in ClickHouse.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	Requests = promauto.NewCounterVec(prometheus.CounterOpts{Name: "gateway_requests_total"},
		[]string{"project", "deployment", "provider", "status", "error_class"})
	Duration = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "gateway_request_duration_seconds",
		Buckets: []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120}}, []string{"deployment"})
	TTFT = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "gateway_ttft_seconds",
		Buckets: []float64{.05, .1, .25, .5, 1, 2, 5, 10, 30}}, []string{"deployment"})
	Inflight = promauto.NewGauge(prometheus.GaugeOpts{Name: "gateway_inflight"})
	Tokens   = promauto.NewCounterVec(prometheus.CounterOpts{Name: "gateway_tokens_total"}, []string{"deployment", "type"})
	Cost     = promauto.NewCounterVec(prometheus.CounterOpts{Name: "gateway_cost_nano_usd_total"}, []string{"project", "deployment"})
	Fallback = promauto.NewCounterVec(prometheus.CounterOpts{Name: "gateway_fallback_total"}, []string{"from", "to", "reason"})
	Breaker  = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "gateway_breaker_state"}, []string{"deployment"})

	BudgetRejections    = promauto.NewCounterVec(prometheus.CounterOpts{Name: "gateway_budget_rejections_total"}, []string{"project", "scope"})
	RateLimitRejections = promauto.NewCounterVec(prometheus.CounterOpts{Name: "gateway_ratelimit_rejections_total"}, []string{"project", "kind"})
	EnforcementDegraded = promauto.NewGauge(prometheus.GaugeOpts{Name: "gateway_enforcement_degraded"})
	ReservationExpired  = promauto.NewCounter(prometheus.CounterOpts{Name: "gateway_reservation_expired_total"})

	EstimateRatio = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "pricing_estimate_ratio",
		Buckets: []float64{.5, .7, .8, .9, 1, 1.1, 1.25, 1.5, 2, 3}}, []string{"model"})
	UsageSource = promauto.NewCounterVec(prometheus.CounterOpts{Name: "pricing_usage_source_total"}, []string{"source"})

	RedisLatency = promauto.NewHistogram(prometheus.HistogramOpts{Name: "redis_op_duration_seconds",
		Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1}})
	EventsSpilled   = promauto.NewCounter(prometheus.CounterOpts{Name: "events_spilled_total"})
	EventsPublished = promauto.NewCounter(prometheus.CounterOpts{Name: "events_published_total"})
	ConfigCacheHits = promauto.NewCounterVec(prometheus.CounterOpts{Name: "config_key_cache_total"}, []string{"result"})
)
