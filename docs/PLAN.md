# litellm-lite: Architecture and Delivery Plan

Status: v3 (2026-10-13). Local MVP implemented and verified; see section 15 for the assumptions taken and section 16 for what was verified.

### Changes in v3

- The local MVP is built. Section 15 lists every assumption taken to get it
  running locally and what changes on the way to production. Section 16
  records what was verified end to end.
- Migrations moved under `cmd/litellm-lite/migrations` so the binary embeds
  them; `litellm-lite migrate` needs no files on disk.

### Changes in v2

- New section 4: consistency and atomicity posture. Every write is listed with
  where it is made atomic and what happens when a dependency is down.
  Availability wins over consistency on the hot path.
- New section 5: the pricing engine. Integer nano-USD math, provider-reported
  usage as the source of truth, tokenizer-based reservation, per-attempt
  charging, client-disconnect handling, reconciliation.
- New section 9: local MVP scope. One binary, three containers, no Kafka, no
  KMS, no UI. Interfaces kept so the scale-out pieces slot in later.
- Section 6: API surface rewritten. Gateway endpoints and the full control
  plane endpoint list are now explicit for review.
- Phases re-cut so Phase 1 is the local MVP.
- Vendor auth (login, users, JWTs, API tokens) is deferred out of the MVP.
  The control plane runs unauthenticated locally and takes the vendor from a
  header. Virtual keys on the gateway stay, since they are the product.

## 1. What we are building

An LLM gateway that sits between vendor applications and LLM providers. A vendor
creates projects, issues virtual keys, and points each key at a model deployment
with a fallback policy, a response format and budgets. The gateway authenticates
the key, enforces budgets and rate limits, routes to the provider, translates the
response into the requested format, and records usage. Target: 10,000 requests
per second sustained, with streaming. The budget gate and the accuracy of the
cost it enforces are the product.

### Glossary

| Term | Meaning |
|---|---|
| Vendor | A tenant. Owns projects, users, webhooks. Logs in to the console. |
| Project | Grouping of keys under a vendor. Carries the vendor ID for authorization and data scoping. |
| Provider | An LLM company or API endpoint (OpenAI, Anthropic, Google Gemini, Moonshot/Kimi, Azure OpenAI, Bedrock, OpenRouter...). |
| Model | A canonical model (gpt-4o, claude-sonnet-4-5, gemini-2.5-pro, kimi-k2). Provider-independent. |
| Deployment | A concrete serving of a model: `<model>-<provider>-<context>-<effort>`. A model has many deployments. This is what keys point at. |
| Virtual key | The secret a vendor app sends us. Resolves to a project, a primary deployment, a fallback policy, a response format, and budgets. |
| Budget | A spend cap (USD and/or tokens) at key, project or vendor scope. Monthly recurring or lifetime. |

## 2. Services

Logical services below. One Go repo. In the MVP all of them run in a single
process (`litellm-lite serve`); at scale they split into two deployables
(gateway, control plane) plus a worker. The split is enforced at the package
level from day one so it costs nothing later.

### 2.1 Gateway (data plane, hot path)

- Parse ingress into the canonical request, authenticate key, rate limit,
  budget check and reserve, select deployment, call provider adapter, stream
  back in the key's response format, settle budget, emit usage event.
- Stateless. Holds a config cache in memory. Never queries Postgres on the hot
  path.
- Provider adapters: one per API dialect, not per provider. Dialects:
  `openai_chat`, `openai_responses`, `anthropic_messages`, `gemini_generate`,
  `bedrock_converse`. Moonshot/Kimi, DeepSeek, Groq, Together and OpenRouter
  are `openai_chat` with a different base URL.
- A `mock` dialect that returns canned or generated responses with realistic
  usage numbers, for local load tests without spending money.

### 2.2 Control plane (admin API)

- CRUD for vendors, vendor users, providers, provider credentials, models,
  deployments, deployment prices, projects, keys, budgets, webhook endpoints.
- Analytics read API backed by ClickHouse.
- Vendor login and auth are deferred (section 8.3). In the MVP every control
  plane call carries `X-Vendor-Id`; platform-admin routes carry
  `X-Platform-Admin: true`. Both are trusted as-is on localhost.
- Publishes config-change notifications so gateway caches invalidate.

### 2.3 Usage ledger worker

- Consumes usage events, batches inserts into ClickHouse, maintains durable
  budget aggregates in Postgres, detects thresholds, writes webhook outbox rows.
- Hourly reconciliation of Redis counters against the ledger.

### 2.4 Webhook dispatcher worker

- Polls the Postgres outbox, signs and delivers, retries with backoff,
  dead-letters.

### 2.5 Scheduler

- Monthly budget period rollover, key expiry, provider health probes, price
  table refresh, retention enforcement.

```
 vendor app ---> [ L7 LB / Envoy ] ---> [ Gateway pods (N, stateless) ] ---> OpenAI / Anthropic /
                                         | in-memory config, adapters |      Gemini / Moonshot ...
                                         +--------+----------+--------+
                              budget / rate limit |          | usage events (async)
                                                  v          v
                                           [ Redis ]    [ event bus: in-proc channel (MVP) / Kafka (scale) ]
                                               ^               |
                                               |               v
                       config invalidation     |      [ Ledger worker ]  ---> ClickHouse (request logs, rollups)
                       (pub/sub)               |      [ Webhook worker ] ---> vendor webhook URLs
                                               |               |
 vendor console ---> [ Control plane API ] ----+---------> [ Postgres ] (system of record, ledger, outbox)
```

## 3. Data sources

| Store | Role | MVP | Scale |
|---|---|---|---|
| PostgreSQL 16 | System of record for control-plane entities, budget aggregates, webhook outbox, audit log. Never on the hot path. | single container | primary + replicas |
| Redis 7 | Budget counters, rate-limit buckets, provider quota buckets, breaker state, key config L2 cache, invalidation pub/sub, idempotency, settle dedup. | single container | cluster, 3 masters + replicas |
| Event bus | Usage events from gateway to workers. | in-process buffered channel with disk spill | Kafka or Redpanda |
| ClickHouse | Request logs, token usage, cost, latency, hourly and daily rollups. | single container | cluster |
| Object storage | Opt-in raw payload logging. | local filesystem | S3 |
| Secrets | Encryption key for provider credentials and webhook secrets. | AES-256-GCM key from env | KMS or Vault envelope encryption |
| Observability | Metrics, traces, logs. | `/metrics` endpoint, stdout JSON | OTel to Prometheus, Tempo, Loki, Grafana |

### 3.1 Core schema (Postgres)

```
vendors(id, name, status, settings jsonb, version, created_at, updated_at)
-- deferred past MVP:
-- vendor_users(id, vendor_id, email, password_hash, role, mfa_secret, created_at)
-- vendor_api_tokens(id, vendor_id, token_hash, name, scopes, expires_at)

providers(id, slug, display_name, dialect, base_url, region, status, default_headers jsonb,
          rpm_limit, tpm_limit, version, created_at, updated_at)
provider_credentials(id, provider_id, version, ciphertext, key_id, status, rotated_at)

models(id, slug, display_name, family, tokenizer, capabilities jsonb, supported_efforts text[], status)
model_deployments(id, model_id, provider_id, provider_model_name, context_window, max_output_tokens,
                  effort, priority, weight, rpm_limit, tpm_limit, status, version,
                  slug generated as '<model>-<provider>-<context>-<effort>',
                  unique(model_id, provider_id, context_window, effort))
deployment_prices(id, deployment_id, effective_from, tiers jsonb, verified bool, source, created_by)
    -- tiers: [{up_to_prompt_tokens, input, cached_input, cache_write_5m, cache_write_1h,
    --          output, reasoning}]  all in nano-USD per token, int64

projects(id, vendor_id, name, description, metadata jsonb, status, version, created_at)
keys(id, project_id, vendor_id, key_hash, key_prefix, name, status, primary_deployment_id,
     fallback_policy jsonb, response_format, request_overrides jsonb, allowed_deployments uuid[],
     rpm_limit, tpm_limit, payload_logging bool, expires_at, version, created_at, last_used_at)

budgets(id, vendor_id, scope_type enum(key,project,vendor), scope_id, kind enum(monthly,lifetime),
        limit_nano_usd, limit_tokens, timezone, anchor_day, on_exceed enum(block,alert),
        alert_thresholds int[], enforcement enum(available,strict), status, version)
budget_periods(budget_id, period_start, period_end, spent_nano_usd, spent_tokens, request_count,
               exceeded_at, updated_at, primary key(budget_id, period_start))

event_cursor(consumer, partition, position)   -- advanced in the same tx as budget_periods

webhook_endpoints(id, vendor_id, url, secret_ciphertext, events text[], status, version)
webhook_deliveries(id, endpoint_id, event_id unique, event_type, payload jsonb, status, attempts,
                   next_attempt_at, last_status_code, last_error)

config_outbox(id, entity_type, entity_id, op, created_at)   -- drained to pub/sub after commit
audit_logs(id, vendor_id, actor_id, action, entity_type, entity_id, before jsonb, after jsonb, at)
```

### 3.2 Request log (ClickHouse)

```
request_logs(ts, request_id, trace_id, vendor_id, project_id, key_id,
             ingress_format, response_format, model_slug, requested_deployment_id,
             served_deployment_id, provider_slug, attempt_no, attempts_total,
             fallback_used, fallback_reason, status_code, error_class, error_code, stream,
             prompt_tokens, cached_tokens, cache_write_tokens, completion_tokens, reasoning_tokens,
             price_id, cost_nano_usd, estimated_cost_nano_usd,
             latency_ms, ttft_ms, upstream_latency_ms, client_ip, user_agent, payload_ref)
  ENGINE ReplacingMergeTree(ts) ORDER BY (vendor_id, project_id, key_id, request_id, attempt_no)
  PARTITION BY toYYYYMMDD(ts) TTL ts + INTERVAL 90 DAY

usage_hourly, usage_daily   (materialized views keyed by vendor/project/key/deployment)  -- kept forever
```

One row per provider attempt, not per request. `ReplacingMergeTree` keyed on
`request_id, attempt_no` makes delivery at-least-once safe.

## 4. Consistency and atomicity

Posture: **availability first on the hot path, atomicity on every write.** The
gateway must keep serving when Postgres, ClickHouse or the event bus is
unreachable. Redis is the only synchronous dependency, and even that degrades
rather than fails. Money is never lost silently: every degraded mode is logged,
metered and reconciled.

### 4.1 Where each write is made atomic

| Write | Mechanism | Notes |
|---|---|---|
| Budget check + reserve | One Redis Lua script over the whole scope chain | Reads `spent` and `reserved` for key, project and vendor budgets (monthly and lifetime), rejects if any would exceed, otherwise increments every `reserved` in the same script. No partial reservations. |
| Budget settle | One Redis Lua script | Replaces the reservation with actual cost on every budget in the chain. Idempotent: the script does `SETNX settled:{request_id}:{attempt}` with 24h TTL and no-ops on replay. |
| Reservation expiry | Reservation stored as a sorted-set member with a deadline | A sweeper in each pod releases reservations past their deadline (request max duration + 60s) so a crashed pod cannot pin spend. |
| Rate limit | Redis Lua sliding window | Same script family, one round trip. |
| Create or update any control-plane entity | Single Postgres transaction | Entity row + audit row + `config_outbox` row commit together. Pub/sub publish happens after commit from the outbox drainer, so a cache invalidation can never precede the data it refers to. |
| Create key with budgets | Single transaction | Key, its budgets, initial `budget_periods` rows, audit, outbox. |
| Concurrent edits | Optimistic concurrency | Every mutable entity has a `version` column. `UPDATE ... WHERE id = $1 AND version = $2`; zero rows affected returns HTTP 409. No row locks held across requests. |
| Budget aggregates in Postgres | Batched upsert per (budget, period) per window | The ledger worker aggregates events in memory in 1s windows and issues one `INSERT ... ON CONFLICT DO UPDATE SET spent = spent + EXCLUDED.spent` per budget per window. 10k events/s become tens of upserts/s, which avoids hot-row lock contention. |
| Exactly-once aggregation | Event cursor in the same transaction | The worker writes its consumed position to `event_cursor` in the same transaction as the aggregate upserts. Crash and replay reprocesses only what was not committed. |
| Request logs | ClickHouse async batched insert, at-least-once | Duplicates collapse via `ReplacingMergeTree`. Queries use `FINAL` or the rollups. |
| Webhook dispatch | `SELECT ... FOR UPDATE SKIP LOCKED` | Many dispatcher workers, no double delivery, no lock waits. `event_id` is unique so a retried ledger window cannot create a second delivery. |
| Monthly rollover | Idempotent insert | `INSERT ... ON CONFLICT DO NOTHING` on `(budget_id, period_start)`. |
| Usage event emission | Buffered channel, then event bus, then disk spill | Never blocks the response. The buffer flushes to the bus in batches; if the bus is down, batches append to a local spill file that is replayed when it returns. |

Postgres runs at `READ COMMITTED`. Nothing needs `SERIALIZABLE` because every
cross-row invariant is handled by single-statement upserts, Lua scripts or
optimistic versioning.

### 4.2 Behaviour when a dependency is down

| Dependency down | Gateway behaviour | What is lost |
|---|---|---|
| Postgres | Keeps serving from in-memory and Redis config caches. Unknown keys cannot be resolved and get 401 until it returns. Control plane returns 503. | Nothing on the hot path. Config changes are delayed. |
| Redis | **Degraded enforcement.** Each pod enforces budgets and rate limits from a local copy seeded by the last successful read, with a configurable overrun tolerance (default 5% of the limit). Metric `gateway_enforcement_degraded = 1`, alert fires, every request logged with `enforcement = local`. Budgets marked `enforcement = strict` fail closed with 503 instead. | Possible overrun up to the tolerance, fully recorded and reconciled. |
| Event bus | Spill to local disk, replay on recovery. | Analytics lag. Budget counters in Redis are unaffected because settle is synchronous. |
| ClickHouse | Ledger worker buffers to disk, continues Postgres aggregate writes. | Analytics lag. |
| Provider | Fallback chain, then an error in the client's format. | Nothing. |

Default `enforcement = available` is a decision to confirm (section 14). It is
the right default for a product that must stay up, and overruns are bounded,
logged and visible, but a vendor who prefers hard stops can opt into `strict`
per budget.

## 5. Pricing engine

This is the gate. The requirements are accuracy to the provider's invoice and
less than a millisecond of overhead per request.

### 5.1 Representation

- All money is `int64` **nano-USD** (1 USD = 1,000,000,000). No floats anywhere
  in the cost path. Range is 9.2 billion USD, enough.
- Prices are stored per token, not per million, so `cost = tokens * price` is a
  single integer multiply with no division and no rounding. $2.50 per 1M input
  tokens is stored as `2500` nano-USD per token.
- Token types priced separately: `input` (uncached), `cached_input` (cache
  read), `cache_write_5m`, `cache_write_1h` (Anthropic), `output`, `reasoning`.
  Providers that bill reasoning as output (OpenAI) have `reasoning = output` in
  the price row. Image and audio tokens count as input.
- **Tiers** for long-context pricing (Gemini 2.5 Pro bills differently above
  200k prompt tokens). A price row holds an ordered list of tiers by
  `up_to_prompt_tokens`; the tier is chosen once per attempt from the prompt
  token count.
- Price rows are versioned by `effective_from`. The row effective at request
  time is pinned on the log line as `price_id`, so repricing history is
  replayable and an audit can show exactly which price produced which cost.
- The price table is seeded from a checked-in `prices.yaml` (initially derived
  from LiteLLM's public `model_prices_and_context_window.json`, then verified
  by hand against provider pricing pages; `verified` is a flag on the row).
  Platform admins update it through the API; every change is audited.

### 5.2 Source of truth for token counts

Provider-reported usage, always. Our tokenizers are for estimation only.

- Streaming requests always ask the provider for usage: OpenAI-dialect requests
  get `stream_options.include_usage = true` injected; Anthropic reports usage
  in `message_start` and `message_delta`; Gemini reports `usageMetadata` on the
  final chunk. The adapter extracts a normalized `Usage` struct per attempt.
- If a provider returns no usage (rare, some OpenAI-compatible hosts), the
  adapter falls back to tokenizing the request and the concatenated response
  with the model's tokenizer and marks the log line `usage_source = estimated`.
- **Every provider attempt that produced usage is charged**, including a failed
  attempt that streamed partial output before dying. The provider bills us for
  it, so the vendor is billed for it. Each attempt is its own log row and its
  own settle call.
- **Client disconnect mid-stream.** The provider keeps generating and billing.
  The gateway keeps draining the upstream stream in the background (bounded by
  the deployment's total timeout) to receive the final usage block, and settles
  from it. If the drain fails, settle from the tokenized partial output and
  flag `usage_source = estimated`.

### 5.3 Reservation estimate

The reserve-then-settle flow (section 7) needs a pre-request cost estimate that
is cheap and slightly pessimistic.

- `estimated_input = tokenize(prompt)` using the model's tokenizer family.
  OpenAI models: `tiktoken` (`o200k_base` / `cl100k_base`), pure Go, roughly a
  microsecond per token. Anthropic and Gemini have no public Go tokenizer, so
  they use `o200k_base` with a per-model calibration multiplier (stored on
  `models.tokenizer`, default 1.15) that the reconciliation job tunes from
  observed actual-vs-estimated ratios.
- `estimated_output = max_tokens` if the client set it, else the deployment's
  `max_output_tokens`. Pessimistic by design: a reservation that is too small
  lets concurrent requests overshoot the cap, a reservation that is too large
  only briefly reduces headroom.
- `estimate = tier(estimated_input).input * estimated_input + tier.output *
  estimated_output`. Integer math on in-memory data, no I/O.
- The same tokenizer path backs `POST /v1/tokens/count` so vendors can ask
  "what will this cost and will it be allowed" before sending.

### 5.4 Speed budget

| Step | Target |
|---|---|
| Tokenize a 4k-token prompt for the estimate | < 300 us |
| Compute cost from usage (tier lookup + 6 multiplies) | < 1 us |
| Reserve (one Lua call) | < 1 ms, p99 < 3 ms |
| Settle (one Lua call, off the response path) | not on the client's critical path |

Prices and tokenizer tables live in the config snapshot in memory. There is no
lookup, cache miss or network call in the cost computation itself.

### 5.5 Accuracy controls

- Golden tests: fixture usage objects captured from each provider (streaming
  and non-streaming, with cache hits, with reasoning, with tool calls, with
  tier crossover) and the expected nano-USD cost. These run on every commit.
- Invariant checks in the ledger worker: `cost == sum(tokens_by_type *
  price_by_type)` recomputed from the log row; a mismatch is a bug and pages.
- Daily reconciliation (Phase 3) against provider usage APIs where they exist
  (OpenAI Usage API, Anthropic admin usage endpoint). Report per deployment:
  our billed tokens vs theirs, our cost vs theirs. Drift above 0.5% alerts.
- Calibration report for estimators: distribution of `actual / estimated`
  input tokens per model, used to tune the multiplier and to catch tokenizer
  drift when providers change models.

## 6. API surface

Two listeners. The gateway is what vendor apps call with a virtual key. The
control plane is what the console and automation call. In the MVP it is
unauthenticated and identifies the vendor from a header; vendor login comes
later. Both are versioned under `/v1`.

### 6.1 Gateway (data plane), default port 8080

Compatibility endpoints match the request and response shapes of the SDKs
vendors already use. The point is drop-in: change `base_url` and the key,
nothing else.

| Method and path | Compatible with | Phase |
|---|---|---|
| `POST /v1/chat/completions` | OpenAI Chat Completions (streaming and non-streaming, tools, JSON mode, vision) | MVP |
| `POST /v1/embeddings` | OpenAI Embeddings | MVP |
| `GET /v1/models` | OpenAI model list; returns the deployments the calling key may use, with context window, effort and prices | MVP |
| `POST /v1/responses` | OpenAI Responses API | Phase 2 |
| `POST /v1/messages` | Anthropic Messages (also accepts `x-api-key` and `anthropic-version` headers) | Phase 2 |
| `POST /v1/messages/count_tokens` | Anthropic token counting | Phase 2 |

Gateway-native endpoints, under their own prefix so they never collide with a
provider's future paths:

| Method and path | Purpose | Phase |
|---|---|---|
| `POST /v1/gateway/estimate` | Body is any supported request shape. Returns estimated tokens, estimated cost, the deployment that would serve it, and whether every budget in the chain would allow it. Does not call a provider. | MVP |
| `GET /v1/gateway/key` | The calling key's name, project, primary and fallback deployments, response format, rate limits, and remaining budget per scope with period end. | MVP |
| `GET /v1/gateway/usage?from&to&granularity` | Usage and cost history for the calling key. | Phase 2 |

Operational:

| Path | Purpose |
|---|---|
| `GET /healthz` | Process alive. |
| `GET /readyz` | Config loaded, Redis reachable (or degraded mode engaged). |
| `GET /metrics` | Prometheus exposition. |

Conventions on the gateway:

- Auth: `Authorization: Bearer sk-ll-...`. `x-api-key` is also accepted for
  Anthropic SDK compatibility.
- `model` in the body may be a deployment slug
  (`gpt-4o-openai-128k-medium`), a model slug (`gpt-4o`, resolved to the
  key's primary if it matches, else to the best allowed deployment of that
  model), or omitted, which means the key's primary.
- Request headers: `X-Response-Format: openai|anthropic|passthrough` overrides
  the key default; `X-Fallback: off` disables fallback for the request;
  `Idempotency-Key` enables replay protection.
- Response headers: `X-Request-Id`, `X-Served-Deployment`, `X-Fallback-Used`,
  `X-Cost-Nano-Usd`, `X-Budget-Remaining-Nano-Usd` (most restrictive scope),
  `X-RateLimit-Remaining-Requests`, `X-RateLimit-Remaining-Tokens`.
- Errors are returned in the response format's error shape. Gateway-specific
  conditions use `type`: `budget_exceeded` (429), `rate_limited` (429),
  `context_too_large` (400, no deployment in the chain fits),
  `no_healthy_deployment` (503), `invalid_key` (401), `key_revoked` (401).
- Gemini's native `:generateContent` path is intentionally not exposed. Gemini
  models are served through the OpenAI or Anthropic shapes.

### 6.2 Control plane, default port 8081, prefix `/v1`

All routes are flat, filtered by query parameter, scoped to one vendor. List
endpoints paginate with `?limit&cursor`. Writes take `If-Match: <version>` and
return 409 on mismatch.

Caller identity (MVP): `X-Vendor-Id: <uuid>` scopes every vendor route;
`X-Platform-Admin: true` unlocks catalog writes. Both headers are trusted
without verification and the listener binds to localhost only. The middleware
that reads them is the single seam where real auth plugs in later.

Auth and identity (deferred, kept here so the shape is agreed)

```
POST   /v1/auth/login                 email + password -> access + refresh tokens
POST   /v1/auth/refresh
POST   /v1/auth/logout
GET    /v1/auth/me
GET|POST|PATCH|DELETE /v1/users[/{id}]
GET|POST|DELETE       /v1/api-tokens[/{id}]
```

Platform catalog (platform admin only for writes; vendors may read)

```
GET    /v1/vendors                                   (platform admin)
POST   /v1/vendors
GET    /v1/vendors/{id}
PATCH  /v1/vendors/{id}

GET    /v1/providers
POST   /v1/providers
GET    /v1/providers/{id}
PATCH  /v1/providers/{id}
DELETE /v1/providers/{id}                            soft delete; refused while deployments reference it
POST   /v1/providers/{id}/credentials                new version; old stays valid until rotated out
GET    /v1/providers/{id}/credentials                metadata only, never plaintext
POST   /v1/providers/{id}/credentials/{cid}/revoke
POST   /v1/providers/{id}/probe                      live health check

GET    /v1/models
POST   /v1/models
GET    /v1/models/{id}
PATCH  /v1/models/{id}

GET    /v1/deployments?model_id&provider_id&status
POST   /v1/deployments
GET    /v1/deployments/{id}
PATCH  /v1/deployments/{id}
DELETE /v1/deployments/{id}                          soft delete; refused while keys reference it as primary
GET    /v1/deployments/{id}/prices                   history
POST   /v1/deployments/{id}/prices                   new row with effective_from
GET    /v1/deployments/{id}/health                   breaker state, error rate, p50/p99
```

Vendor resources

```
GET    /v1/projects
POST   /v1/projects
GET    /v1/projects/{id}
PATCH  /v1/projects/{id}
DELETE /v1/projects/{id}                             soft delete; revokes its keys

GET    /v1/keys?project_id&status
POST   /v1/keys                                      body carries project_id; plaintext returned once
GET    /v1/keys/{id}
PATCH  /v1/keys/{id}                                 name, primary, fallback_policy, response_format, limits
POST   /v1/keys/{id}/rotate                          new secret, old valid for a grace window
POST   /v1/keys/{id}/revoke
GET    /v1/keys/{id}/usage?from&to&granularity

GET    /v1/budgets?scope_type&scope_id
POST   /v1/budgets
GET    /v1/budgets/{id}
PATCH  /v1/budgets/{id}
DELETE /v1/budgets/{id}
GET    /v1/budgets/{id}/periods                       spent per period, exceeded_at
GET    /v1/budgets/{id}/remaining                     live from Redis

GET    /v1/webhooks
POST   /v1/webhooks
GET    /v1/webhooks/{id}
PATCH  /v1/webhooks/{id}
DELETE /v1/webhooks/{id}
POST   /v1/webhooks/{id}/test                         sends a signed ping
GET    /v1/webhooks/{id}/deliveries?status
POST   /v1/webhook-deliveries/{id}/replay
```

Analytics and audit

```
GET    /v1/analytics/usage?group_by=key|project|deployment|provider&from&to&granularity=hour|day
GET    /v1/analytics/requests?key_id&project_id&status&error_class&from&to&cursor
GET    /v1/analytics/requests/{request_id}            every attempt for one request
GET    /v1/analytics/budgets/remaining?scope_type&scope_id
GET    /v1/analytics/fallbacks?from&to                 rate by deployment and reason
GET    /v1/audit?entity_type&entity_id&actor_id&from&to
```

Operational: `GET /healthz`, `GET /readyz`, `GET /metrics`, and
`GET /v1/openapi.json`.

## 7. Hot-path request flow

1. **Parse.** Decode the ingress body into the canonical request. Reject
   malformed requests before any I/O.
2. **Authenticate.** `sha256(bearer)` looked up in the in-process LRU; miss
   falls through to Redis, then Postgres. Unknown keys are negative-cached for
   60s. Check key, project and vendor status and expiry.
3. **Estimate.** Tokenize the prompt, derive `needed_context = prompt +
   max_tokens` and the reservation estimate (section 5.3).
4. **Rate limit and reserve.** One pipelined Redis round trip runs the
   rate-limit script and the budget check-and-reserve script. Any rejection
   returns 429 in the client's error shape, is logged with the matching
   `error_class`, and enqueues a `budget.exceeded` or `key.rate_limited` event
   (deduplicated per scope per period).
5. **Select deployment.** Start with the key's primary. Skip to fallback if
   `needed_context > context_window`, the breaker is open, or the provider
   quota bucket is empty.
6. **Call provider.** Translate, attach the decrypted credential, send with
   HTTP/2 keep-alive. Timeouts: connect 5s, time-to-first-token 30s, total up
   to 600s, all per deployment.
7. **Fallback on failure.** Retriable: connection error, timeout, HTTP 429,
   HTTP 5xx, provider `context_length_exceeded`. Non-retriable: client-caused
   400, content refusals, 401 from our own credentials (page on-call). For
   streaming, fallback is only possible before the first content token is
   forwarded; the gateway buffers until then, then commits.
8. **Translate response.** Stream through the response-format encoder.
   Normalize usage, finish reasons, tool calls, errors. Set response headers.
9. **Settle.** Per attempt that produced usage: compute cost, run the settle
   script, emit the usage event through the buffered channel. None of this
   blocks the client; the final chunk is already sent.

## 8. Fallback policy, budgets, auth

### 8.1 Fallback policy

Stored per key as JSON. Explicit candidates are tried first, then filtered
candidates.

```json
{
  "max_attempts": 3,
  "triggers": ["timeout", "connection_error", "http_5xx", "http_429", "context_length_exceeded"],
  "explicit": ["claude-sonnet-4-5-anthropic-200k-medium"],
  "filters": {
    "min_context": "auto",
    "same_model": false,
    "same_family": true,
    "providers_allow": ["anthropic", "google", "openai"],
    "providers_deny": [],
    "effort": "gte",
    "max_input_price_nano_usd": 5000,
    "require_capabilities": ["tools", "streaming"]
  },
  "order_by": ["same_model", "price", "p50_latency"]
}
```

`min_context: "auto"` resolves to `prompt_tokens + max_tokens` for the current
request and is the primary filter. Candidates with an open breaker or an empty
provider quota bucket are excluded before ordering. Every attempt is logged
with its reason.

### 8.2 Budgets

- Scopes: key, project, vendor. All budgets in the chain are enforced.
- Kinds: monthly recurring (calendar month in the budget's timezone, or an
  anchor day) and lifetime. Both can coexist on one scope.
- Units: nano-USD and/or tokens.
- Hot-path state: Redis hash `budget:{budget_id}:{period_start}` with fields
  `spent`, `reserved`, `tokens`; TTL to period end plus 7 days. Lifetime
  budgets use period `lifetime` with no TTL.
- `on_exceed = block` returns 429 `budget_exceeded`; `alert` lets the request
  through and fires the webhook. Thresholds default 50, 80, 100.
- `enforcement = available | strict` governs Redis-down behaviour (section 4.2).
- Durability and reconciliation per section 4.1.

### 8.3 Auth and tenancy

- **Vendor auth is deferred.** MVP control plane trusts `X-Vendor-Id` and
  `X-Platform-Admin` on localhost. The later design: email + argon2id
  password, optional TOTP, JWT access tokens (15 min) with server-side
  refresh tokens, roles owner / admin / developer / viewer, and a separate
  platform-admin role for the catalog. Only the identity middleware changes.
- **Tenancy scoping is not deferred.** Every query is scoped by `vendor_id`
  from day one, so turning auth on later changes where the vendor ID comes
  from, not how it is used.
- Virtual keys: `sk-ll-<project_prefix>-<base62 random 32 bytes>`. Only the
  SHA-256 is stored.
- Every row that a vendor can see carries `vendor_id`; keys denormalize it so
  the hot path never joins. Postgres row-level security as a second guard.
- Provider credentials encrypted with AES-256-GCM. MVP key from env; scale
  uses KMS envelope encryption. Versioned so rotation is a new row.

## 9. Local MVP scope

Goal: a developer runs `make up` and within a minute has a gateway that
authenticates keys, enforces budgets with accurate pricing, falls back across
providers, and records usage, all on a laptop.

**Runs as:** one Go binary, `litellm-lite serve`, hosting the gateway on 8080,
the control plane on 8081, and the workers as goroutines. The event bus is an
in-process buffered channel with disk spill. Three containers via
docker-compose: Postgres, Redis, ClickHouse. A `.env` carries provider API
keys and the credential encryption key.

**In:**

- Control plane: vendors, providers, credentials, models, deployments,
  prices, projects, keys, budgets, webhooks, analytics, audit. Header-based
  vendor identity, no login. Full endpoint list in 6.2.
- Gateway: `/v1/chat/completions`, `/v1/embeddings`, `/v1/models`,
  `/v1/gateway/estimate`, `/v1/gateway/key`. OpenAI response format only.
- Adapters: `openai_chat` (OpenAI, Moonshot/Kimi), `anthropic_messages`,
  `gemini_generate`, `mock`.
- Pricing engine complete per section 5, including golden tests.
- Budgets with reserve and settle, monthly and lifetime, all three scopes,
  degraded mode.
- Fallback with filters, circuit breakers, per-key rate limits.
- Request logs to ClickHouse, hourly rollups, analytics endpoints.
- Webhooks with outbox, signing, retries.
- `/metrics` and structured stdout logs.
- `make seed`: one vendor, one project, the four providers, a starter price
  table, a handful of deployments, one key with a monthly budget.
- `make load`: `k6` or `vegeta` script against the `mock` provider to measure
  per-core throughput and per-request Redis latency on the laptop. The number
  this produces is the basis for the Phase 3 capacity plan.

**Out, with the interface that will hold it:**

| Deferred | Interface in the MVP |
|---|---|
| Kafka | `events.Bus` with `ChannelBus` implementation; `KafkaBus` later |
| KMS | `secrets.Keyring` with `EnvKeyring`; `KMSKeyring` later |
| Object storage for payloads | `blobs.Store` with `FSStore`; `S3Store` later |
| OTel traces, Grafana | `/metrics` only; spans added behind a flag |
| Admin UI | OpenAPI spec and `httpie` examples |
| Anthropic and Responses ingress, response formats other than OpenAI | canonical model already carries everything needed |
| Vendor login, users, API tokens, RBAC | `auth.Identity` middleware with `HeaderIdentity`; `JWTIdentity` later |
| SSO, multi-region, BYO provider credentials | none required |

## 10. Concurrency and the 10k rps target

The work per request is tiny; the wait per request is long. The design holds
many idle connections cheaply.

| Quantity | Value |
|---|---|
| Target throughput | 10,000 rps |
| Assumed mean request duration (streaming) | 5 s |
| Concurrent in-flight requests | ~50,000 |
| Memory per in-flight streaming request (Go) | ~100 KB |
| Fleet memory for in-flight state | ~5 GB |
| Redis ops per request | 2 round trips (reserve pipeline, settle) |
| Redis ops per second | ~40,000 |
| Event bus messages per second | 10,000 at ~1 KB |

- **Go.** Goroutines make 50k concurrent streams cheap; the HTTP/2 client and
  the library ecosystem are mature. Python would need roughly 10x the pods.
- **Stateless gateway pods.** 20 pods at 500 rps each (2 vCPU, 4 GB) with 2x
  headroom. The load balancer must allow 600s idle streams with no buffering.
- **No database on the hot path.** Catalog fully in memory; keys in a bounded
  LRU with Redis as L2. Invalidation via the config outbox and pub/sub, with
  a 30s poll of a `config_version` counter as the safety net.
- **Atomic Redis scripts** for budgets and rate limits (section 4.1).
- **Async after the response.** Buffered channel, batching producer, disk spill.
- **Backpressure.** Per-pod in-flight semaphore (503 + `Retry-After`), per-key
  RPM and TPM, per-deployment token buckets that mirror provider quotas.
- **Circuit breakers per deployment**, local sliding window, state mirrored to
  Redis.
- **Connection management.** HTTP/2 multiplexing to providers that support it,
  large keep-alive pools otherwise. Raise fd limits, `SO_REUSEPORT`,
  `GOMAXPROCS` from the CPU quota.
- **Graceful shutdown.** Stop accepting, drain streams up to 120s, flush the
  event buffer, exit. Rolling deploys with `maxSurge`.

## 11. Observability

**Metrics.** Labels limited to project, deployment, provider, status and
error_class; never key_id, to avoid series explosion. Per-key numbers come
from ClickHouse.

- `gateway_requests_total`, `gateway_request_duration_seconds`,
  `gateway_ttft_seconds`, `gateway_inflight`
- `gateway_tokens_total{type}`, `gateway_cost_nano_usd_total`
- `gateway_fallback_total{from,to,reason}`, `gateway_breaker_state`
- `gateway_budget_rejections_total`, `gateway_ratelimit_rejections_total`
- `gateway_enforcement_degraded`, `gateway_reservation_expired_total`
- `pricing_estimate_ratio` histogram (actual / estimated input tokens, by model)
- `pricing_usage_source_total{source=provider|estimated}`
- Redis latency, event bus lag and spill count, config cache hit rate

**Traces.** One span per request, child span per attempt. Trace ID returned as
`X-Request-Id` and stored on the ClickHouse row.

**Logs.** Request logs in ClickHouse (metadata only), application logs as
stdout JSON, audit logs in Postgres.

**Vendor-facing.** Usage and cost over time per key, project and deployment;
remaining budget per scope with period end; error and fallback rates; latency
percentiles. Remaining budget is served from Redis for freshness.

**Payload logging.** Off by default. Per-key opt-in to the blob store with
retention and redaction rules.

## 12. Webhooks

- Events: `budget.threshold`, `budget.exceeded`, `key.rate_limited`,
  `key.expiring`, `provider.degraded`, `provider.recovered`, `errors.spike`
  (windowed), `request.failed` (opt-in per key).
- Delivery: outbox row written in the ledger transaction. `X-Signature:
  t=<ts>,v1=<hmac-sha256>`. Retries at 1m, 5m, 30m, 2h, 6h, 24h, then
  dead-letter. Replayable from the console. Stable `event_id` for dedup.

## 13. Repository layout and phases

```
cmd/litellm-lite            single binary: serve, migrate, seed, load; embeds migrations/
internal/canonical          request/response canonical model
internal/adapters           openai_chat, anthropic_messages, gemini_generate, mock
internal/formats            ingress decoders and egress encoders
internal/router             deployment selection, fallback, breakers
internal/pricing            price tiers, tokenizers, cost computation, golden tests
internal/budget             Lua scripts, reserve/settle, reservation sweeper, reconciliation
internal/ratelimit
internal/config             snapshot, LRU, outbox drainer, invalidation
internal/store              sqlc-generated Postgres access
internal/events             Bus interface, ChannelBus, disk spill
internal/analytics          ClickHouse writer and query layer
internal/webhooks
internal/auth               Identity middleware; HeaderIdentity in MVP
internal/secrets            Keyring interface, EnvKeyring
deploy/compose
prices.yaml
```

**Phase 0: foundation (week 1).** Repo, CI, compose with Postgres, Redis,
ClickHouse, migrations, OpenAPI skeleton, canonical model, pricing engine
with golden tests (built first because everything else depends on it).

**Phase 1: local MVP (weeks 2 to 5).** Everything in section 9. Exit
criteria: `make up && make seed && make load` passes; golden pricing tests
pass; a real request to each of the four providers prices to the cent against
the provider's dashboard; budget exceeded blocks and fires a webhook; killing
Redis mid-load engages degraded mode and recovery reconciles.

**Phase 2: formats, auth, polish (weeks 6 to 7).** Vendor login, users,
API tokens and RBAC behind the identity middleware. Responses and Anthropic
ingress, Anthropic response format, idempotency keys, payload logging, usage
endpoint on the gateway.

**Phase 3: scale (weeks 8 to 10).** Split into deployables, `KafkaBus`,
`KMSKeyring`, `S3Store`, OTel, Helm, load test to 10k rps on a staging
cluster, daily provider reconciliation, runbooks.

**Phase 4: ongoing.** Admin console, SSO, BYO provider credentials,
response caching, guardrails, multi-region.

## 14. Assumptions to confirm

1. **Go** as the implementation language.
2. **Provider credentials are platform-owned.** Vendors use our provider
   accounts and pay through budgets.
3. **"Remaining tokens per key and per token"** is read as remaining budget per
   key and per project (and per vendor).
4. **Budgets are USD by default**, with token caps as an option.
5. **Error webhooks are aggregated** by default; per-request webhooks are opt-in.
6. **Budget-exceeded returns HTTP 429** with an OpenAI-shaped error body.
7. **Redis-down default is `available`** (bounded local enforcement, logged
   and reconciled), with `strict` opt-in per budget. Confirm this matches
   "availability is more important".
8. **Failed provider attempts that produced usage are charged** to the vendor,
   because the provider charges us.
9. **Gemini is served only through OpenAI and Anthropic shapes**, not its
   native path.
10. **MVP control plane is unauthenticated on localhost** with the vendor
    taken from `X-Vendor-Id`. Virtual keys on the gateway are still real.

## 15. Assumptions taken for the local build, and how they change

Each row is a shortcut taken so the MVP runs on a laptop with `make up &&
make serve`. The right-hand column is the production change, and the seam in
the code where it happens.

| Area | Local assumption | Production change | Seam |
|---|---|---|---|
| Auth | No vendor login. Control plane trusts `X-Vendor-Id` (defaults to the only vendor) and binds to localhost. | JWT or session identity, RBAC, platform-admin role. | `controlplane.identity` middleware. |
| Process model | One binary runs gateway, control plane and all workers as goroutines. | Three deployables (gateway, control plane, worker) from the same packages. | `cmd/litellm-lite/main.go` `serve()`; packages already separated. |
| Event bus | In-process channel (64k buffer) with disk spill to `data/spill`. | Kafka or Redpanda with the same `events.Bus` interface; consumer offsets in `event_cursor`. | `events.Bus`. |
| Exactly-once ledger | At-least-once: a crash between the ClickHouse write and the Postgres upsert can double-count one batch in `budget_periods`. Redis counters are unaffected (settle is idempotent). | Kafka offsets committed in the ledger transaction; hourly reconciliation from ClickHouse corrects any drift. | `ledger.Handler`, `event_cursor` table (exists, unused). |
| Redis | Single instance, AOF persistence. If Redis loses data, counters restart from zero plus whatever was accrued locally during the outage. | Redis Cluster; a rebuild job that reloads `spent` from `budget_periods` on startup or on detected loss. | `budget.Enforcer` plus a new `rebuild` routine reading `store.GetBudgetPeriod`. |
| Degraded enforcement | Default `available`: when Redis is down each pod enforces from its last known counters with 5% tolerance and pushes accrued spend back on recovery. | Same, but per-pod tolerance must be divided by pod count, or `strict` used for money-critical budgets. | `budget.Enforcer.reserveLocal`, `DEGRADED_TOLERANCE_PCT`. |
| Secrets | AES-256-GCM key from `ENCRYPTION_KEY` in `.env`. Seed uses an all-zero key. | KMS envelope encryption; the ciphertext row already stores `key_id` for rotation. | `secrets.Keyring`. |
| Key cache | Unbounded-ish map cleared wholesale past 100k entries; whole cache invalidated on any key, project, budget or vendor change. | Proper LRU with per-entry invalidation; Redis as L2. | `catalog.Loader.ResolveKey` / `InvalidateKeys`. |
| Config reload | Full snapshot reload every 10s plus pub/sub invalidation. | Fine at any realistic catalog size; keep. | `catalog.Loader.Run`. |
| Rate limits | Fixed one-minute windows; fail open when Redis is down. | Sliding window or GCRA if burst smoothing matters. | `ratelimit` Lua script. |
| Reservation estimate | `max_tokens` from the request, else the deployment's `max_output_tokens`. Pessimistic by design: small budgets reject requests that omit `max_tokens`. | Keep, and add a per-key `reservation_output_tokens` override. | `gateway.planRequest`. |
| Tokenizers | `o200k_base` for everything, with a per-model multiplier for Anthropic and Gemini. | Calibrate multipliers from `pricing_estimate_ratio`; optionally call provider count-token APIs for large prompts. | `models.tokenizer_multiplier`. |
| Prices | `prices.yaml` seeded once, `verified=false`. | Review against invoices, set `verified`, run the daily provider reconciliation. | `deployment_prices`, `POST /v1/deployments/{id}/prices`. |
| Response formats | OpenAI only; `anthropic` and `passthrough` return 400. | Add encoders in `formats` and the `/v1/messages` ingress. | `formats`, `gateway.chat`. |
| Embeddings | OpenAI-dialect and mock only. No fallback. | Gemini embeddings adapter. | `adapters.Adapter.Embed`. |
| Streaming disconnects | Upstream is drained to completion (bounded by the 600s total timeout) to capture usage. | Keep; cap the drain budget per deployment. | `gateway.streamState.write`. |
| Timeouts | TTFT 30s, total 600s, hard-coded. | Per-deployment columns. | `gateway.call`. |
| Circuit breakers | Per-pod, in memory. | Mirror state to Redis so pods converge. | `router.Breakers`. |
| Key rotation | Old secret stops immediately. | Grace window with two valid hashes. | `keys.key_hash` becomes a list. |
| Webhooks | Retries at 5s, 10s, 15s... (`FastRetry`) so local tests finish quickly. | Backoff schedule 1m to 24h (already in code behind the flag). | `webhooks.Dispatcher.FastRetry`. |
| Observability | `/metrics` only; `X-Request-Id` doubles as trace id. | OTel traces and Grafana dashboards. | `metrics` package, add OTel spans. |
| Payload logging | Not implemented. | Blob store with retention and redaction. | `keys.payload_logging` column exists. |
| ClickHouse | Single container, raw rows kept 90 days. | Cluster with replication; rollups kept forever. | `migrations/clickhouse`. |
| Load test | `litellm-lite load` against the mock provider on one laptop. | k6 against a staging cluster with real providers behind a sandbox. | `cmd/litellm-lite/load.go`. |

## 16. Verified locally

All of the following were run against the seeded local stack on 2026-10-13.

- Non-streaming and streaming chat completions through the OpenAI format,
  with exact cost headers. 11 prompt tokens at $1/M and 26 completion tokens
  at $4/M priced to 115,000 nano-USD.
- Fallback on an upstream 500 to a second provider, both non-streaming and
  streaming (the stream is buffered until the first content token, so the
  client sees a clean 200 from the fallback). Non-retriable 400s stop the
  chain. `X-Fallback: off` disables it.
- Context-window fallback: a 9k-token prompt on an 8k primary was served by
  the 128k deployment, and `/v1/gateway/estimate` predicted the same.
- Budgets: a $0.0006 key budget allowed five $0.000115 requests and blocked
  the sixth with 429 and `X-Budget-Denied-Scope: key`; reservations were
  released cleanly. The ledger recorded the period, thresholds and
  `exceeded_at`; a signed `budget.exceeded` webhook was delivered.
- Analytics: usage by deployment from the ClickHouse rollup, error listing,
  per-request attempt history, fallback reasons, audit log, deployment health
  with breaker state.
- Redis outage: with Redis stopped, requests were served with
  `X-Budget-Enforcement: degraded`, `gateway_enforcement_degraded` went to 1,
  and on restart the accrued spend was pushed back and the gauge returned to 0.
- Load on one laptop against the mock provider: 500 rps non-streaming for 15s
  with p99 2.2 ms and zero errors; 1,500 rps streaming for 10s with p99 2.0 ms
  and zero errors; no event spills; Redis p99 under 2.5 ms.
