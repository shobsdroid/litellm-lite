package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rzp/litellm-lite/internal/catalog"
	"github.com/rzp/litellm-lite/internal/config"
	"github.com/rzp/litellm-lite/internal/controlplane"
	"github.com/rzp/litellm-lite/internal/pricing"
	"github.com/rzp/litellm-lite/internal/secrets"
	"github.com/rzp/litellm-lite/internal/store"
	"gopkg.in/yaml.v3"
)

type priceIn struct {
	UpToPromptTokens int64   `yaml:"up_to_prompt_tokens"`
	Input            float64 `yaml:"input"`
	CachedInput      float64 `yaml:"cached_input"`
	CacheWrite5m     float64 `yaml:"cache_write_5m"`
	CacheWrite1h     float64 `yaml:"cache_write_1h"`
	Output           float64 `yaml:"output"`
	Reasoning        float64 `yaml:"reasoning"`
}

type seedFile struct {
	Models []struct {
		Slug                string   `yaml:"slug"`
		Family              string   `yaml:"family"`
		Tokenizer           string   `yaml:"tokenizer"`
		TokenizerMultiplier float64  `yaml:"tokenizer_multiplier"`
		Capabilities        []string `yaml:"capabilities"`
		SupportedEfforts    []string `yaml:"supported_efforts"`
	} `yaml:"models"`
	Providers []struct {
		Slug    string `yaml:"slug"`
		Dialect string `yaml:"dialect"`
		BaseURL string `yaml:"base_url"`
		EnvKey  string `yaml:"env_key"`
	} `yaml:"providers"`
	Deployments []struct {
		Model             string    `yaml:"model"`
		Provider          string    `yaml:"provider"`
		ProviderModelName string    `yaml:"provider_model_name"`
		ContextWindow     int32     `yaml:"context_window"`
		MaxOutputTokens   int32     `yaml:"max_output_tokens"`
		Effort            string    `yaml:"effort"`
		Priority          int32     `yaml:"priority"`
		Price             *priceIn  `yaml:"price"`
		Tiers             []priceIn `yaml:"tiers"`
	} `yaml:"deployments"`
}

// perMillionToNanoPerToken: $X per 1M tokens == X * 1000 nano-USD per token.
func perMillionToNanoPerToken(usd float64) int64 { return int64(math.Round(usd * 1000)) }

func toTier(p priceIn) pricing.Tier {
	t := pricing.Tier{UpToPromptTokens: p.UpToPromptTokens, Input: perMillionToNanoPerToken(p.Input), CachedInput: perMillionToNanoPerToken(p.CachedInput),
		CacheWrite5m: perMillionToNanoPerToken(p.CacheWrite5m), CacheWrite1h: perMillionToNanoPerToken(p.CacheWrite1h), Output: perMillionToNanoPerToken(p.Output), Reasoning: perMillionToNanoPerToken(p.Reasoning)}
	if t.Reasoning == 0 {
		t.Reasoning = t.Output
	}
	if t.CachedInput == 0 {
		t.CachedInput = t.Input
	}
	return t
}

func seed(ctx context.Context, cfg *config.Config) error {
	raw, err := os.ReadFile("prices.yaml")
	if err != nil {
		return err
	}
	var sf seedFile
	if err := yaml.Unmarshal(raw, &sf); err != nil {
		return err
	}
	db, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	keyring, err := secrets.NewEnvKeyring(cfg.EncryptionKey)
	if err != nil {
		return err
	}
	q := db.Pool

	for _, m := range sf.Models {
		if _, err := db.GetModelBySlug(ctx, q, m.Slug); err == nil {
			continue
		}
		caps, _ := json.Marshal(m.Capabilities)
		mult := m.TokenizerMultiplier
		if mult == 0 {
			mult = 1
		}
		efforts := m.SupportedEfforts
		if len(efforts) == 0 {
			efforts = []string{"none"}
		}
		if _, err := db.InsertModel(ctx, q, &store.Model{Slug: m.Slug, DisplayName: m.Slug, Family: m.Family, Tokenizer: orDefault(m.Tokenizer, "o200k_base"),
			TokenizerMultiplier: mult, Capabilities: caps, SupportedEfforts: efforts, Status: "active"}); err != nil {
			return err
		}
	}
	for _, p := range sf.Providers {
		prov, err := db.GetProviderBySlug(ctx, q, p.Slug)
		if err != nil {
			prov, err = db.InsertProvider(ctx, q, &store.Provider{Slug: p.Slug, DisplayName: p.Slug, Dialect: p.Dialect, BaseURL: p.BaseURL, Status: "active", DefaultHeaders: json.RawMessage("{}")})
			if err != nil {
				return err
			}
		}
		secret := "mock"
		if p.EnvKey != "" {
			secret = os.Getenv(p.EnvKey)
		}
		creds, _ := db.ListCredentials(ctx, q, prov.ID)
		if secret != "" && len(creds) == 0 {
			ct, kid, err := keyring.Encrypt([]byte(secret))
			if err != nil {
				return err
			}
			if _, err := db.InsertCredential(ctx, q, prov.ID, ct, kid); err != nil {
				return err
			}
		} else if secret == "" && len(creds) == 0 {
			slog.Info("provider seeded without credentials; set "+p.EnvKey+" and re-run seed to enable it", "provider", p.Slug)
		}
	}
	for _, d := range sf.Deployments {
		m, err := db.GetModelBySlug(ctx, q, d.Model)
		if err != nil {
			return fmt.Errorf("deployment references unknown model %s", d.Model)
		}
		p, err := db.GetProviderBySlug(ctx, q, d.Provider)
		if err != nil {
			return fmt.Errorf("deployment references unknown provider %s", d.Provider)
		}
		slug := controlplane.DeploymentSlug(m.Slug, p.Slug, d.ContextWindow, d.Effort)
		dep, err := db.GetDeploymentBySlug(ctx, q, slug)
		if err != nil {
			prio := d.Priority
			if prio == 0 {
				prio = 100
			}
			dep, err = db.InsertDeployment(ctx, q, &store.Deployment{ModelID: m.ID, ProviderID: p.ID, Slug: slug, ProviderModelName: orDefault(d.ProviderModelName, m.Slug),
				ContextWindow: d.ContextWindow, MaxOutputTokens: d.MaxOutputTokens, Effort: orDefault(d.Effort, "none"), Priority: prio, Weight: 1, Status: "active"})
			if err != nil {
				return err
			}
		}
		if prices, _ := db.ListPrices(ctx, q, dep.ID); len(prices) == 0 {
			var tiers []pricing.Tier
			if d.Price != nil {
				tiers = []pricing.Tier{toTier(*d.Price)}
			}
			for _, t := range d.Tiers {
				tiers = append(tiers, toTier(t))
			}
			b, _ := json.Marshal(tiers)
			if _, err := db.InsertPrice(ctx, q, &store.DeploymentPrice{DeploymentID: dep.ID, Tiers: b, Source: "prices.yaml", CreatedBy: "seed"}); err != nil {
				return err
			}
		}
	}

	// Local vendor, project and keys.
	vendors, _ := db.ListVendors(ctx)
	var vendor *store.Vendor
	for i := range vendors {
		if vendors[i].Name == "local-vendor" {
			vendor = &vendors[i]
		}
	}
	if vendor == nil {
		if vendor, err = db.InsertVendor(ctx, q, &store.Vendor{Name: "local-vendor", Status: "active", Settings: json.RawMessage("{}")}); err != nil {
			return err
		}
	}
	projects, _ := db.ListProjects(ctx, vendor.ID)
	var project *store.Project
	for i := range projects {
		if projects[i].Name == "demo" {
			project = &projects[i]
		}
	}
	if project == nil {
		if project, err = db.InsertProject(ctx, q, &store.Project{VendorID: vendor.ID, Name: "demo", Status: "active", Metadata: json.RawMessage("{}")}); err != nil {
			return err
		}
	}
	_ = os.MkdirAll("data", 0o755)
	mkKey := func(file, name, primary string, policy string, budgetUSD string) error {
		path := filepath.Join("data", file)
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		dep, err := db.GetDeploymentBySlug(ctx, q, primary)
		if err != nil {
			return fmt.Errorf("seed key %s: unknown deployment %s", name, primary)
		}
		secret := "sk-ll-demo-" + uuid.NewString()
		h := catalog.HashKey(secret)
		return db.Tx(ctx, func(tx pgx.Tx) error {
			k, err := db.InsertKey(ctx, tx, &store.Key{ProjectID: project.ID, VendorID: vendor.ID, KeyHash: h[:], KeyPrefix: secret[:16], Name: name, Status: "active",
				PrimaryDeploymentID: dep.ID, FallbackPolicy: json.RawMessage(policy), ResponseFormat: "openai", RequestOverrides: json.RawMessage("{}"), AllowedDeployments: []uuid.UUID{}})
			if err != nil {
				return err
			}
			if budgetUSD != "" {
				limit, _ := parseUSD(budgetUSD)
				if _, err := db.InsertBudget(ctx, tx, &store.Budget{VendorID: vendor.ID, ScopeType: "key", ScopeID: k.ID, Kind: "monthly", LimitNanoUSD: &limit, Timezone: "UTC", AnchorDay: 1,
					OnExceed: "block", AlertThresholds: []int32{50, 80, 100}, Enforcement: "available", Status: "active"}); err != nil {
					return err
				}
			}
			if err := store.Outbox(ctx, tx, "key", k.ID, "create"); err != nil {
				return err
			}
			return os.WriteFile(path, []byte(secret), 0o600)
		})
	}
	if err := mkKey("seed-key", "demo-mock", "mock-small-mock-8k-none", `{"max_attempts":3,"filters":{"min_context":"auto","providers_allow":["mock","mock-b"]}}`, "10.00"); err != nil {
		return err
	}
	if err := mkKey("seed-key-tiny-budget", "demo-tiny-budget", "mock-small-mock-8k-none", `{}`, "0.0005"); err != nil {
		return err
	}
	if os.Getenv("OPENAI_API_KEY") != "" {
		if err := mkKey("seed-key-openai", "demo-openai", "gpt-4o-mini-openai-128k-none", `{"max_attempts":2,"filters":{"min_context":"auto","same_family":true}}`, "5.00"); err != nil {
			return err
		}
	}
	// project-level lifetime budget as a second scope in the chain
	if bs, _ := db.ListBudgets(ctx, vendor.ID, "project", &project.ID); len(bs) == 0 {
		limit, _ := parseUSD("100.00")
		if _, err := db.InsertBudget(ctx, q, &store.Budget{VendorID: vendor.ID, ScopeType: "project", ScopeID: project.ID, Kind: "lifetime", LimitNanoUSD: &limit, Timezone: "UTC", AnchorDay: 1,
			OnExceed: "block", AlertThresholds: []int32{50, 80, 100}, Enforcement: "available", Status: "active"}); err != nil {
			return err
		}
	}
	slog.Info("seed complete", "vendor_id", vendor.ID, "project_id", project.ID, "keys", "data/seed-key data/seed-key-tiny-budget")
	fmt.Printf("\nvendor_id=%s\nproject_id=%s\nkey (mock, $10/month):   $(cat data/seed-key)\nkey (mock, $0.0005/month): $(cat data/seed-key-tiny-budget)\n\n", vendor.ID, project.ID)
	return nil
}

func orDefault(s, d string) string {
	if s == "" {
		return d
	}
	return s
}

func parseUSD(s string) (int64, error) {
	var whole, frac int64
	var fs string
	if _, err := fmt.Sscanf(s, "%d.%s", &whole, &fs); err != nil {
		if _, err := fmt.Sscanf(s, "%d", &whole); err != nil {
			return 0, err
		}
	}
	for len(fs) < 9 {
		fs += "0"
	}
	fmt.Sscanf(fs[:9], "%d", &frac)
	return whole*pricing.NanoPerUSD + frac, nil
}
