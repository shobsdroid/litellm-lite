# litellm-lite

An LLM gateway: virtual keys, budgets enforced with exact pricing, provider
fallback, usage analytics and webhooks, behind an OpenAI-compatible API.
Local MVP. See `docs/PLAN.md` for the architecture and the assumptions taken.

## Run it

Requirements: Go 1.27+, Docker. Step-by-step walkthrough in `docs/RUNNING.md`.

```bash
make up        # containers (Postgres, Redis, ClickHouse), schema, seed data
make serve     # gateway on :8080, control plane on :8081
```

`make seed` writes two virtual keys to `data/`:

| File | Primary deployment | Budget |
|---|---|---|
| `data/seed-key` | `mock-small-mock-8k-none`, falls back to `mock-b` and `mock-large` | $10 / month on the key, $100 lifetime on the project |
| `data/seed-key-tiny-budget` | `mock-small-mock-8k-none` | $0.0005 / month (for watching a budget run out) |

Set `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `GEMINI_API_KEY` or
`MOONSHOT_API_KEY` in `.env` and re-run `make seed` to enable real providers.
With `OPENAI_API_KEY` set, seed also writes `data/seed-key-openai`.

## Try it

```bash
KEY=$(cat data/seed-key)

# OpenAI-compatible chat (model may be a deployment slug, a model slug, or omitted for the key's primary)
curl -s localhost:8080/v1/chat/completions -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"hello"}],"max_tokens":50}'

# streaming
curl -N localhost:8080/v1/chat/completions -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"hello"}],"stream":true}'

# what would this cost, and would the budget allow it (no provider call)
curl -s localhost:8080/v1/gateway/estimate -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"hello"}],"max_tokens":50}'

# the key's config and remaining budget per scope
curl -s localhost:8080/v1/gateway/key -H "Authorization: Bearer $KEY"

# deployments this key may use, with prices
curl -s localhost:8080/v1/models -H "Authorization: Bearer $KEY"
```

Every response carries `X-Request-Id`, `X-Served-Deployment`,
`X-Fallback-Used`, `X-Cost-Nano-Usd` (non-streaming) and
`X-Budget-Remaining-Usd`.

The `mock` provider reacts to hints in the last user message, so fallback and
budgets can be exercised without spending money:

```
[fail:500] [fail:429] [fail:timeout] [fail:context]   fail the attempt
[fail:500@mock]                                        fail only on provider "mock" (so mock-b takes over)
[latency:250]                                          sleep 250ms
[output:120]                                           produce 120 completion tokens
```

A prompt larger than 8k tokens on the seed key is routed to the 128k
deployment by the context filter.

## Control plane

Unauthenticated, bound to localhost. The vendor is taken from `X-Vendor-Id`;
with a single vendor (the seed) the header can be omitted.

```bash
curl -s localhost:8081/v1/projects
curl -s localhost:8081/v1/keys
curl -s localhost:8081/v1/keys -H 'Content-Type: application/json' -d '{
  "project_id": "<project id>", "name": "app-1", "primary_deployment": "mock-large-mock-128k-none",
  "fallback_policy": {"max_attempts": 3, "filters": {"min_context": "auto", "same_family": true}},
  "budget": {"kind": "monthly", "limit_usd": "25.00"}}'
curl -s localhost:8081/v1/budgets
curl -s localhost:8081/v1/budgets/<id>/remaining
curl -s localhost:8081/v1/webhooks -H 'Content-Type: application/json' -d '{"url":"https://example.test/hook","events":["budget.exceeded","budget.threshold"]}'
curl -s 'localhost:8081/v1/analytics/usage?group_by=deployment'
curl -s 'localhost:8081/v1/analytics/requests?status=error'
curl -s 'localhost:8081/v1/analytics/requests/<request id>'
curl -s 'localhost:8081/v1/analytics/fallbacks'
curl -s 'localhost:8081/v1/audit'
```

Writes accept `If-Match: <version>` and return 409 on a stale version.
Platform catalog routes: `/v1/providers` (with `/credentials`), `/v1/models`,
`/v1/deployments` (with `/prices` and `/health`). Full list in `docs/PLAN.md`
section 6.

## Load test

```bash
make load                      # 500 rps for 20s against the mock provider
RPS=1500 DURATION=10s make load
```

## Tests

```bash
make test                      # includes the pricing golden tests
```

## Layout

```
cmd/litellm-lite       serve | migrate | seed | load; embeds migrations/
internal/gateway       data plane: auth, planning, attempt loop, streaming fallback
internal/controlplane  admin API
internal/adapters      openai_chat, anthropic_messages, gemini_generate, mock
internal/formats       OpenAI ingress/egress
internal/pricing       nano-USD cost engine, tokenizer, golden tests
internal/budget        Lua reserve/settle/release, degraded mode, reservation sweeper
internal/router        fallback candidates, circuit breakers
internal/catalog       in-memory config snapshot, key cache, invalidation
internal/events        in-process bus with disk spill
internal/analytics     ClickHouse writer and queries
internal/ledger        budget aggregates, thresholds, webhook outbox
internal/webhooks      signed delivery with backoff
internal/store         Postgres queries
prices.yaml            seed catalog and price table
```
