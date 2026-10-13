CREATE TABLE IF NOT EXISTS request_logs (
  ts DateTime64(3),
  request_id String,
  trace_id String,
  vendor_id String,
  project_id String,
  key_id String,
  ingress_format LowCardinality(String),
  response_format LowCardinality(String),
  model_slug LowCardinality(String),
  requested_deployment_id String,
  served_deployment_id String,
  served_deployment_slug LowCardinality(String),
  provider_slug LowCardinality(String),
  attempt_no UInt8,
  attempts_total UInt8,
  fallback_used UInt8,
  fallback_reason LowCardinality(String),
  status_code UInt16,
  error_class LowCardinality(String),
  error_code LowCardinality(String),
  stream UInt8,
  prompt_tokens UInt32,
  cached_tokens UInt32,
  cache_write_tokens UInt32,
  completion_tokens UInt32,
  reasoning_tokens UInt32,
  price_id String,
  cost_nano_usd Int64,
  estimated_cost_nano_usd Int64,
  usage_source LowCardinality(String),
  enforcement LowCardinality(String),
  latency_ms UInt32,
  ttft_ms UInt32,
  upstream_latency_ms UInt32,
  client_ip String,
  user_agent String,
  payload_ref String
) ENGINE = ReplacingMergeTree(ts)
PARTITION BY toYYYYMMDD(ts)
ORDER BY (vendor_id, project_id, key_id, request_id, attempt_no)
TTL toDateTime(ts) + INTERVAL 90 DAY;

CREATE TABLE IF NOT EXISTS usage_hourly (
  hour DateTime,
  vendor_id String,
  project_id String,
  key_id String,
  served_deployment_slug LowCardinality(String),
  provider_slug LowCardinality(String),
  requests UInt64,
  errors UInt64,
  fallbacks UInt64,
  prompt_tokens UInt64,
  cached_tokens UInt64,
  completion_tokens UInt64,
  reasoning_tokens UInt64,
  cost_nano_usd Int64
) ENGINE = SummingMergeTree
ORDER BY (vendor_id, project_id, key_id, served_deployment_slug, provider_slug, hour);

CREATE MATERIALIZED VIEW IF NOT EXISTS usage_hourly_mv TO usage_hourly AS
SELECT
  toStartOfHour(ts) AS hour,
  vendor_id, project_id, key_id, served_deployment_slug, provider_slug,
  count() AS requests,
  countIf(status_code >= 400) AS errors,
  countIf(fallback_used = 1 AND attempt_no = attempts_total) AS fallbacks,
  sum(prompt_tokens) AS prompt_tokens,
  sum(cached_tokens) AS cached_tokens,
  sum(completion_tokens) AS completion_tokens,
  sum(reasoning_tokens) AS reasoning_tokens,
  sum(cost_nano_usd) AS cost_nano_usd
FROM request_logs
GROUP BY hour, vendor_id, project_id, key_id, served_deployment_slug, provider_slug;
