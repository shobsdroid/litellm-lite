CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE vendors (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL,
  status text NOT NULL DEFAULT 'active',
  settings jsonb NOT NULL DEFAULT '{}',
  version int NOT NULL DEFAULT 1,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE providers (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  slug text NOT NULL UNIQUE,
  display_name text NOT NULL,
  dialect text NOT NULL,              -- openai_chat | anthropic_messages | gemini_generate | mock
  base_url text NOT NULL DEFAULT '',
  region text NOT NULL DEFAULT '',
  status text NOT NULL DEFAULT 'active',
  default_headers jsonb NOT NULL DEFAULT '{}',
  rpm_limit int NOT NULL DEFAULT 0,
  tpm_limit bigint NOT NULL DEFAULT 0,
  version int NOT NULL DEFAULT 1,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  deleted_at timestamptz
);

CREATE TABLE provider_credentials (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  provider_id uuid NOT NULL REFERENCES providers(id),
  version int NOT NULL,
  ciphertext bytea NOT NULL,
  key_id text NOT NULL,
  status text NOT NULL DEFAULT 'active',   -- active | revoked
  created_at timestamptz NOT NULL DEFAULT now(),
  rotated_at timestamptz,
  UNIQUE (provider_id, version)
);

CREATE TABLE models (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  slug text NOT NULL UNIQUE,
  display_name text NOT NULL,
  family text NOT NULL DEFAULT '',
  tokenizer text NOT NULL DEFAULT 'o200k_base',
  tokenizer_multiplier numeric(6,3) NOT NULL DEFAULT 1.0,
  capabilities jsonb NOT NULL DEFAULT '[]',
  supported_efforts text[] NOT NULL DEFAULT '{none}',
  status text NOT NULL DEFAULT 'active',
  version int NOT NULL DEFAULT 1,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE model_deployments (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  model_id uuid NOT NULL REFERENCES models(id),
  provider_id uuid NOT NULL REFERENCES providers(id),
  slug text NOT NULL UNIQUE,            -- <model>-<provider>-<context>-<effort>, computed by the app
  provider_model_name text NOT NULL,
  context_window int NOT NULL,
  max_output_tokens int NOT NULL DEFAULT 4096,
  effort text NOT NULL DEFAULT 'none',
  priority int NOT NULL DEFAULT 100,
  weight int NOT NULL DEFAULT 1,
  rpm_limit int NOT NULL DEFAULT 0,
  tpm_limit bigint NOT NULL DEFAULT 0,
  status text NOT NULL DEFAULT 'active',
  version int NOT NULL DEFAULT 1,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  deleted_at timestamptz,
  UNIQUE (model_id, provider_id, context_window, effort)
);

-- tiers: [{"up_to_prompt_tokens": 200000, "input": 1250, "cached_input": 125, "cache_write_5m": 0,
--          "cache_write_1h": 0, "output": 10000, "reasoning": 10000}]  nano-USD per token
CREATE TABLE deployment_prices (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  deployment_id uuid NOT NULL REFERENCES model_deployments(id),
  effective_from timestamptz NOT NULL DEFAULT now(),
  tiers jsonb NOT NULL,
  verified boolean NOT NULL DEFAULT false,
  source text NOT NULL DEFAULT '',
  created_by text NOT NULL DEFAULT '',
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON deployment_prices (deployment_id, effective_from DESC);

CREATE TABLE projects (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  vendor_id uuid NOT NULL REFERENCES vendors(id),
  name text NOT NULL,
  description text NOT NULL DEFAULT '',
  metadata jsonb NOT NULL DEFAULT '{}',
  status text NOT NULL DEFAULT 'active',
  version int NOT NULL DEFAULT 1,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  deleted_at timestamptz
);
CREATE INDEX ON projects (vendor_id);

CREATE TABLE keys (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id uuid NOT NULL REFERENCES projects(id),
  vendor_id uuid NOT NULL REFERENCES vendors(id),
  key_hash bytea NOT NULL UNIQUE,
  key_prefix text NOT NULL,
  name text NOT NULL,
  status text NOT NULL DEFAULT 'active',   -- active | revoked
  primary_deployment_id uuid NOT NULL REFERENCES model_deployments(id),
  fallback_policy jsonb NOT NULL DEFAULT '{}',
  response_format text NOT NULL DEFAULT 'openai',
  request_overrides jsonb NOT NULL DEFAULT '{}',
  allowed_deployments uuid[] NOT NULL DEFAULT '{}',
  rpm_limit int NOT NULL DEFAULT 0,
  tpm_limit bigint NOT NULL DEFAULT 0,
  payload_logging boolean NOT NULL DEFAULT false,
  expires_at timestamptz,
  version int NOT NULL DEFAULT 1,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  last_used_at timestamptz
);
CREATE INDEX ON keys (project_id);
CREATE INDEX ON keys (vendor_id);

CREATE TABLE budgets (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  vendor_id uuid NOT NULL REFERENCES vendors(id),
  scope_type text NOT NULL,              -- key | project | vendor
  scope_id uuid NOT NULL,
  kind text NOT NULL,                    -- monthly | lifetime
  limit_nano_usd bigint,
  limit_tokens bigint,
  timezone text NOT NULL DEFAULT 'UTC',
  anchor_day int NOT NULL DEFAULT 1,
  on_exceed text NOT NULL DEFAULT 'block',      -- block | alert
  alert_thresholds int[] NOT NULL DEFAULT '{50,80,100}',
  enforcement text NOT NULL DEFAULT 'available', -- available | strict
  status text NOT NULL DEFAULT 'active',
  version int NOT NULL DEFAULT 1,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON budgets (scope_type, scope_id);
CREATE INDEX ON budgets (vendor_id);

CREATE TABLE budget_periods (
  budget_id uuid NOT NULL REFERENCES budgets(id),
  period_start timestamptz NOT NULL,
  period_end timestamptz NOT NULL,
  spent_nano_usd bigint NOT NULL DEFAULT 0,
  spent_tokens bigint NOT NULL DEFAULT 0,
  request_count bigint NOT NULL DEFAULT 0,
  thresholds_fired int[] NOT NULL DEFAULT '{}',
  exceeded_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (budget_id, period_start)
);

CREATE TABLE event_cursor (
  consumer text NOT NULL,
  partition int NOT NULL DEFAULT 0,
  position bigint NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, partition)
);

CREATE TABLE webhook_endpoints (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  vendor_id uuid NOT NULL REFERENCES vendors(id),
  url text NOT NULL,
  secret_ciphertext bytea NOT NULL,
  events text[] NOT NULL DEFAULT '{}',
  status text NOT NULL DEFAULT 'active',
  version int NOT NULL DEFAULT 1,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON webhook_endpoints (vendor_id);

CREATE TABLE webhook_deliveries (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  endpoint_id uuid NOT NULL REFERENCES webhook_endpoints(id),
  event_id text NOT NULL,
  event_type text NOT NULL,
  payload jsonb NOT NULL,
  status text NOT NULL DEFAULT 'pending',   -- pending | delivered | dead
  attempts int NOT NULL DEFAULT 0,
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  last_status_code int,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (endpoint_id, event_id)
);
CREATE INDEX ON webhook_deliveries (status, next_attempt_at);

CREATE TABLE config_outbox (
  id bigserial PRIMARY KEY,
  entity_type text NOT NULL,
  entity_id uuid NOT NULL,
  op text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE audit_logs (
  id bigserial PRIMARY KEY,
  vendor_id uuid,
  actor text NOT NULL DEFAULT '',
  action text NOT NULL,
  entity_type text NOT NULL,
  entity_id uuid,
  before jsonb,
  after jsonb,
  at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON audit_logs (entity_type, entity_id);
CREATE INDEX ON audit_logs (vendor_id, at DESC);
