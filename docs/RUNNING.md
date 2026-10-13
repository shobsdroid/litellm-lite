# litellm-lite: how to run it

litellm-lite is an LLM gateway written in Go: virtual keys, budgets enforced with exact
per-token pricing, provider fallback, usage analytics and webhooks, behind an
OpenAI-compatible API. It ships as a single binary plus three containers
(Postgres, Redis, ClickHouse). Architecture and assumptions are in `docs/PLAN.md`.

## Prerequisites

- Go 1.27+ (`go.mod` pins 1.27.1; a newer Go toolchain auto-downloads it)
- Docker with the compose plugin
- Ports 5433, 6380, 9001, 8124 (containers) and 8080, 8081 (the binary) free

No `.env` is needed. Every setting has a local default. Copy `.env.example` to `.env`
only if you want to change ports or add real provider keys.

## Bring it up (two commands)

```bash
make up        # starts containers, applies schema, seeds a vendor/project/keys
make serve     # gateway on :8080, control plane on :8081
```

`make up` writes two virtual keys to `data/`:

| File | Budget | Routing |
|---|---|---|
| `data/seed-key` | $10/month on the key, $100 lifetime on the project | mock provider, falls back to two other mock deployments |
| `data/seed-key-tiny-budget` | $0.0005/month | mock provider; handy for watching a budget run out |

The mock provider needs no credentials, so everything below works offline.
To use real providers, set `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `GEMINI_API_KEY` or
`MOONSHOT_API_KEY` in `.env` and re-run `make seed`. With `OPENAI_API_KEY` set, seed
also writes `data/seed-key-openai`.

## Try it (data plane, :8080)

```bash
KEY=$(cat data/seed-key)

# OpenAI-compatible chat. "model" is optional; the key's primary deployment is used.
curl -s localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"hello"}],"max_tokens":50}'

# Streaming
curl -N localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"hello"}],"stream":true}'

# Cost estimate and budget check, no provider call
curl -s localhost:8080/v1/gateway/estimate \
  -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"hello"}],"max_tokens":50}'

# Key config and remaining budget per scope
curl -s localhost:8080/v1/gateway/key -H "Authorization: Bearer $KEY"

# Deployments this key may use, with prices
curl -s localhost:8080/v1/models -H "Authorization: Bearer $KEY"
```

Every response carries `X-Request-Id`, `X-Served-Deployment`, `X-Fallback-Used`,
`X-Cost-Nano-Usd` (non-streaming) and `X-Budget-Remaining-Usd`.

### Exercising fallback and budgets without spending money

The mock provider reacts to hints in the last user message:

```
[fail:500] [fail:429] [fail:timeout] [fail:context]   fail the attempt
[fail:500@mock]      fail only provider "mock", so the fallback deployment takes over
[latency:250]        sleep 250 ms
[output:120]         produce 120 completion tokens
```

Example: send `hello [fail:500@mock]` and check `X-Fallback-Used` and
`X-Served-Deployment` in the response. Send a few requests with the tiny-budget key
and watch it start returning budget-exceeded errors. A prompt larger than 8k tokens
on the seed key is routed to the 128k deployment by the context filter.

## Control plane (admin API, :8081)

Unauthenticated and bound to localhost. The vendor is taken from `X-Vendor-Id`;
with the single seeded vendor the header can be omitted.

```bash
curl -s localhost:8081/v1/projects
curl -s localhost:8081/v1/keys
curl -s localhost:8081/v1/budgets
curl -s localhost:8081/v1/budgets/<id>/remaining
curl -s 'localhost:8081/v1/analytics/usage?group_by=deployment'
curl -s 'localhost:8081/v1/analytics/requests?status=error'
curl -s 'localhost:8081/v1/analytics/requests/<request id>'
curl -s 'localhost:8081/v1/analytics/fallbacks'
curl -s localhost:8081/v1/audit
```

Create a key:

```bash
curl -s localhost:8081/v1/keys -H 'Content-Type: application/json' -d '{
  "project_id": "<project id from /v1/projects>", "name": "app-1",
  "primary_deployment": "mock-large-mock-128k-none",
  "fallback_policy": {"max_attempts": 3, "filters": {"min_context": "auto", "same_family": true}},
  "budget": {"kind": "monthly", "limit_usd": "25.00"}}'
```

Register a webhook:

```bash
curl -s localhost:8081/v1/webhooks -H 'Content-Type: application/json' \
  -d '{"url":"https://example.test/hook","events":["budget.exceeded","budget.threshold"]}'
```

Writes accept `If-Match: <version>` and return 409 on a stale version.
Also available: `/v1/providers` (+ `/credentials`), `/v1/models`, `/v1/deployments`
(+ `/prices`, `/health`), `/v1/webhooks` (+ `/test`, `/deliveries`, replay).
Full route list is in `docs/PLAN.md` section 6.

## Tests

Tests need no Docker and no `.env`. Redis-backed packages run the real Lua
scripts against an in-process miniredis, and the gateway suite boots the whole
data plane behind `httptest` with a scripted provider adapter.

```bash
make test                        # same as: go test ./...
go test -short ./...             # skips the slow circuit-breaker timing tests
go test -v -run TestGoldenCosts ./internal/pricing/   # one test, verbose
go test -race ./internal/gateway/ ./internal/budget/   # race detector on the hot paths
```

The full run takes about 20 seconds; most of that is timing-based tests in
`internal/budget` and `internal/router`.

What is covered, by package:

| Package | What the tests check |
|---|---|
| `internal/gateway` | End-to-end through the HTTP router: auth and key checks, non-streaming and streaming chat, model resolution, request validation, budget-exceeded and most-restrictive-scope header, rate limits, fallback on retriable errors, no fallback on client errors, max-attempts cap, context-size fallback, breaker skipping the primary, estimated usage when the provider sends none, in-flight limit, Redis-down degraded mode |
| `internal/pricing` | Golden cost table (exact nano-USD for known token counts), tier selection, per-token-type cost, exactness at scale, estimate is never below actual, tokenizer counts and fallback |
| `internal/budget` | Reserve/settle/release Lua scripts: whole-chain reservation, no partial reservation on deny, token limits and alert mode, idempotent settle per attempt, reservation sweeper, degraded mode with bounded overrun and flush on recovery, billing-period math (calendar month, anchor day, timezone, lifetime) |
| `internal/router` | Circuit breaker open/half-open/decay, fallback candidate selection: price ordering, explicit list, context filter, provider and family filters, effort, price cap, capabilities, health and allow-list |
| `internal/adapters` | OpenAI, Anthropic, Gemini and mock dialects: body translation, non-streaming and streaming parsing, stop/finish reasons, upstream error classification, retriability, every MVP dialect is registered |
| `internal/formats` | OpenAI ingress/egress: chat and embeddings decode/encode, multimodal and tool calls, malformed input, SSE stream encoder |
| `internal/ratelimit` | RPM and TPM windows, TPM rollback of the request count, key expiry, fail-open without Redis |
| `internal/events` | In-process bus: batching by size and interval, disk spill on full buffer and replay, flush on shutdown |
| `internal/catalog` | Fallback-policy parsing and defaults, min-context, key hashing, capability checks, snapshot swap, scope chain on key config |
| `internal/secrets` | AES keyring round trip, fresh nonces, tamper and wrong-key detection |
| `internal/webhooks` | Signature format and secret/body binding, retry backoff schedule |
| `internal/config` | Defaults, env overrides, bad encryption key |

`internal/controlplane`, `internal/store` and `internal/ledger` have no unit
tests; they are exercised by hand through the curl examples above.

## Load test

```bash
make load                        # 500 rps for 20 s against the mock provider
RPS=1500 DURATION=10s CONC=1024 make load
```

Prometheus metrics are on `/metrics` of both servers. Health: `/healthz` and `/readyz`.

## Tear down

```bash
make down      # stop containers, keep volumes
make clean     # stop containers and delete bin/ and data/
```

## Layout

```
cmd/litellm-lite       serve | migrate | seed | load; embeds SQL migrations
internal/gateway       data plane: auth, planning, attempt loop, streaming fallback
internal/controlplane  admin API
internal/adapters      openai_chat, anthropic_messages, gemini_generate, mock
internal/formats       OpenAI ingress/egress
internal/pricing       nano-USD cost engine, tokenizer, golden tests
internal/budget        Redis Lua reserve/settle/release, degraded mode, reservation sweeper
internal/router        fallback candidates, circuit breakers
internal/catalog       in-memory config snapshot, key cache, invalidation
internal/events        in-process bus with disk spill
internal/analytics     ClickHouse writer and queries
internal/ledger        budget aggregates, thresholds, webhook outbox
internal/webhooks      signed delivery with backoff
internal/store         Postgres queries
prices.yaml            seed catalog and price table
```
