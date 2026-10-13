package store

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Vendor struct {
	ID        uuid.UUID       `db:"id" json:"id"`
	Name      string          `db:"name" json:"name"`
	Status    string          `db:"status" json:"status"`
	Settings  json.RawMessage `db:"settings" json:"settings"`
	Version   int32           `db:"version" json:"version"`
	CreatedAt time.Time       `db:"created_at" json:"created_at"`
	UpdatedAt time.Time       `db:"updated_at" json:"updated_at"`
}

type Provider struct {
	ID             uuid.UUID       `db:"id" json:"id"`
	Slug           string          `db:"slug" json:"slug"`
	DisplayName    string          `db:"display_name" json:"display_name"`
	Dialect        string          `db:"dialect" json:"dialect"`
	BaseURL        string          `db:"base_url" json:"base_url"`
	Region         string          `db:"region" json:"region"`
	Status         string          `db:"status" json:"status"`
	DefaultHeaders json.RawMessage `db:"default_headers" json:"default_headers"`
	RPMLimit       int32           `db:"rpm_limit" json:"rpm_limit"`
	TPMLimit       int64           `db:"tpm_limit" json:"tpm_limit"`
	Version        int32           `db:"version" json:"version"`
	CreatedAt      time.Time       `db:"created_at" json:"created_at"`
	UpdatedAt      time.Time       `db:"updated_at" json:"updated_at"`
	DeletedAt      *time.Time      `db:"deleted_at" json:"deleted_at,omitempty"`
}

type ProviderCredential struct {
	ID         uuid.UUID  `db:"id" json:"id"`
	ProviderID uuid.UUID  `db:"provider_id" json:"provider_id"`
	Version    int32      `db:"version" json:"version"`
	Ciphertext []byte     `db:"ciphertext" json:"-"`
	KeyID      string     `db:"key_id" json:"key_id"`
	Status     string     `db:"status" json:"status"`
	CreatedAt  time.Time  `db:"created_at" json:"created_at"`
	RotatedAt  *time.Time `db:"rotated_at" json:"rotated_at,omitempty"`
}

type Model struct {
	ID                  uuid.UUID       `db:"id" json:"id"`
	Slug                string          `db:"slug" json:"slug"`
	DisplayName         string          `db:"display_name" json:"display_name"`
	Family              string          `db:"family" json:"family"`
	Tokenizer           string          `db:"tokenizer" json:"tokenizer"`
	TokenizerMultiplier float64         `db:"tokenizer_multiplier" json:"tokenizer_multiplier"`
	Capabilities        json.RawMessage `db:"capabilities" json:"capabilities"`
	SupportedEfforts    []string        `db:"supported_efforts" json:"supported_efforts"`
	Status              string          `db:"status" json:"status"`
	Version             int32           `db:"version" json:"version"`
	CreatedAt           time.Time       `db:"created_at" json:"created_at"`
	UpdatedAt           time.Time       `db:"updated_at" json:"updated_at"`
}

type Deployment struct {
	ID                uuid.UUID  `db:"id" json:"id"`
	ModelID           uuid.UUID  `db:"model_id" json:"model_id"`
	ProviderID        uuid.UUID  `db:"provider_id" json:"provider_id"`
	Slug              string     `db:"slug" json:"slug"`
	ProviderModelName string     `db:"provider_model_name" json:"provider_model_name"`
	ContextWindow     int32      `db:"context_window" json:"context_window"`
	MaxOutputTokens   int32      `db:"max_output_tokens" json:"max_output_tokens"`
	Effort            string     `db:"effort" json:"effort"`
	Priority          int32      `db:"priority" json:"priority"`
	Weight            int32      `db:"weight" json:"weight"`
	RPMLimit          int32      `db:"rpm_limit" json:"rpm_limit"`
	TPMLimit          int64      `db:"tpm_limit" json:"tpm_limit"`
	Status            string     `db:"status" json:"status"`
	Version           int32      `db:"version" json:"version"`
	CreatedAt         time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt         time.Time  `db:"updated_at" json:"updated_at"`
	DeletedAt         *time.Time `db:"deleted_at" json:"deleted_at,omitempty"`
}

type DeploymentPrice struct {
	ID            uuid.UUID       `db:"id" json:"id"`
	DeploymentID  uuid.UUID       `db:"deployment_id" json:"deployment_id"`
	EffectiveFrom time.Time       `db:"effective_from" json:"effective_from"`
	Tiers         json.RawMessage `db:"tiers" json:"tiers"`
	Verified      bool            `db:"verified" json:"verified"`
	Source        string          `db:"source" json:"source"`
	CreatedBy     string          `db:"created_by" json:"created_by"`
	CreatedAt     time.Time       `db:"created_at" json:"created_at"`
}

type Project struct {
	ID          uuid.UUID       `db:"id" json:"id"`
	VendorID    uuid.UUID       `db:"vendor_id" json:"vendor_id"`
	Name        string          `db:"name" json:"name"`
	Description string          `db:"description" json:"description"`
	Metadata    json.RawMessage `db:"metadata" json:"metadata"`
	Status      string          `db:"status" json:"status"`
	Version     int32           `db:"version" json:"version"`
	CreatedAt   time.Time       `db:"created_at" json:"created_at"`
	UpdatedAt   time.Time       `db:"updated_at" json:"updated_at"`
	DeletedAt   *time.Time      `db:"deleted_at" json:"deleted_at,omitempty"`
}

type Key struct {
	ID                  uuid.UUID       `db:"id" json:"id"`
	ProjectID           uuid.UUID       `db:"project_id" json:"project_id"`
	VendorID            uuid.UUID       `db:"vendor_id" json:"vendor_id"`
	KeyHash             []byte          `db:"key_hash" json:"-"`
	KeyPrefix           string          `db:"key_prefix" json:"key_prefix"`
	Name                string          `db:"name" json:"name"`
	Status              string          `db:"status" json:"status"`
	PrimaryDeploymentID uuid.UUID       `db:"primary_deployment_id" json:"primary_deployment_id"`
	FallbackPolicy      json.RawMessage `db:"fallback_policy" json:"fallback_policy"`
	ResponseFormat      string          `db:"response_format" json:"response_format"`
	RequestOverrides    json.RawMessage `db:"request_overrides" json:"request_overrides"`
	AllowedDeployments  []uuid.UUID     `db:"allowed_deployments" json:"allowed_deployments"`
	RPMLimit            int32           `db:"rpm_limit" json:"rpm_limit"`
	TPMLimit            int64           `db:"tpm_limit" json:"tpm_limit"`
	PayloadLogging      bool            `db:"payload_logging" json:"payload_logging"`
	ExpiresAt           *time.Time      `db:"expires_at" json:"expires_at,omitempty"`
	Version             int32           `db:"version" json:"version"`
	CreatedAt           time.Time       `db:"created_at" json:"created_at"`
	UpdatedAt           time.Time       `db:"updated_at" json:"updated_at"`
	LastUsedAt          *time.Time      `db:"last_used_at" json:"last_used_at,omitempty"`
}

type Budget struct {
	ID              uuid.UUID `db:"id" json:"id"`
	VendorID        uuid.UUID `db:"vendor_id" json:"vendor_id"`
	ScopeType       string    `db:"scope_type" json:"scope_type"`
	ScopeID         uuid.UUID `db:"scope_id" json:"scope_id"`
	Kind            string    `db:"kind" json:"kind"`
	LimitNanoUSD    *int64    `db:"limit_nano_usd" json:"limit_nano_usd,omitempty"`
	LimitTokens     *int64    `db:"limit_tokens" json:"limit_tokens,omitempty"`
	Timezone        string    `db:"timezone" json:"timezone"`
	AnchorDay       int32     `db:"anchor_day" json:"anchor_day"`
	OnExceed        string    `db:"on_exceed" json:"on_exceed"`
	AlertThresholds []int32   `db:"alert_thresholds" json:"alert_thresholds"`
	Enforcement     string    `db:"enforcement" json:"enforcement"`
	Status          string    `db:"status" json:"status"`
	Version         int32     `db:"version" json:"version"`
	CreatedAt       time.Time `db:"created_at" json:"created_at"`
	UpdatedAt       time.Time `db:"updated_at" json:"updated_at"`
}

type BudgetPeriod struct {
	BudgetID        uuid.UUID  `db:"budget_id" json:"budget_id"`
	PeriodStart     time.Time  `db:"period_start" json:"period_start"`
	PeriodEnd       time.Time  `db:"period_end" json:"period_end"`
	SpentNanoUSD    int64      `db:"spent_nano_usd" json:"spent_nano_usd"`
	SpentTokens     int64      `db:"spent_tokens" json:"spent_tokens"`
	RequestCount    int64      `db:"request_count" json:"request_count"`
	ThresholdsFired []int32    `db:"thresholds_fired" json:"thresholds_fired"`
	ExceededAt      *time.Time `db:"exceeded_at" json:"exceeded_at,omitempty"`
	UpdatedAt       time.Time  `db:"updated_at" json:"updated_at"`
}

type WebhookEndpoint struct {
	ID               uuid.UUID `db:"id" json:"id"`
	VendorID         uuid.UUID `db:"vendor_id" json:"vendor_id"`
	URL              string    `db:"url" json:"url"`
	SecretCiphertext []byte    `db:"secret_ciphertext" json:"-"`
	Events           []string  `db:"events" json:"events"`
	Status           string    `db:"status" json:"status"`
	Version          int32     `db:"version" json:"version"`
	CreatedAt        time.Time `db:"created_at" json:"created_at"`
	UpdatedAt        time.Time `db:"updated_at" json:"updated_at"`
}

type WebhookDelivery struct {
	ID             uuid.UUID       `db:"id" json:"id"`
	EndpointID     uuid.UUID       `db:"endpoint_id" json:"endpoint_id"`
	EventID        string          `db:"event_id" json:"event_id"`
	EventType      string          `db:"event_type" json:"event_type"`
	Payload        json.RawMessage `db:"payload" json:"payload"`
	Status         string          `db:"status" json:"status"`
	Attempts       int32           `db:"attempts" json:"attempts"`
	NextAttemptAt  time.Time       `db:"next_attempt_at" json:"next_attempt_at"`
	LastStatusCode *int32          `db:"last_status_code" json:"last_status_code,omitempty"`
	LastError      *string         `db:"last_error" json:"last_error,omitempty"`
	CreatedAt      time.Time       `db:"created_at" json:"created_at"`
	UpdatedAt      time.Time       `db:"updated_at" json:"updated_at"`
}

type AuditLog struct {
	ID         int64           `db:"id" json:"id"`
	VendorID   *uuid.UUID      `db:"vendor_id" json:"vendor_id,omitempty"`
	Actor      string          `db:"actor" json:"actor"`
	Action     string          `db:"action" json:"action"`
	EntityType string          `db:"entity_type" json:"entity_type"`
	EntityID   *uuid.UUID      `db:"entity_id" json:"entity_id,omitempty"`
	Before     json.RawMessage `db:"before" json:"before,omitempty"`
	After      json.RawMessage `db:"after" json:"after,omitempty"`
	At         time.Time       `db:"at" json:"at"`
}

type ConfigChange struct {
	ID         int64     `db:"id"`
	EntityType string    `db:"entity_type"`
	EntityID   uuid.UUID `db:"entity_id"`
	Op         string    `db:"op"`
}
