package catalog

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/rzp/litellm-lite/internal/metrics"
	"github.com/rzp/litellm-lite/internal/pricing"
	"github.com/rzp/litellm-lite/internal/secrets"
	"github.com/rzp/litellm-lite/internal/store"
)

const InvalidationChannel = "config.changed"

type Loader struct {
	db       *store.DB
	rdb      *redis.Client
	keyring  secrets.Keyring
	holder   Holder
	interval time.Duration
	keyTTL   time.Duration

	keysMu  sync.RWMutex
	keys    map[[32]byte]*keyEntry // sha256(secret) -> config
	keyByID map[uuid.UUID][32]byte
	negMu   sync.Mutex
	neg     map[[32]byte]time.Time // unknown keys, negative cache
}

type keyEntry struct {
	cfg     *KeyConfig
	expires time.Time
}

func NewLoader(db *store.DB, rdb *redis.Client, keyring secrets.Keyring, interval, keyTTL time.Duration) *Loader {
	return &Loader{db: db, rdb: rdb, keyring: keyring, interval: interval, keyTTL: keyTTL,
		keys: map[[32]byte]*keyEntry{}, keyByID: map[uuid.UUID][32]byte{}, neg: map[[32]byte]time.Time{}}
}

func (l *Loader) Snapshot() *Snapshot { return l.holder.Load() }

// Load builds a full snapshot from Postgres. The catalog is small, so a full
// reload is cheaper and safer than incremental patching.
func (l *Loader) Load(ctx context.Context) error {
	q := l.db.Pool
	providers, err := l.db.ListProviders(ctx, q)
	if err != nil {
		return err
	}
	models, err := l.db.ListModels(ctx, q)
	if err != nil {
		return err
	}
	deps, err := l.db.ListDeployments(ctx, q)
	if err != nil {
		return err
	}
	prices, err := l.db.CurrentPrices(ctx, q)
	if err != nil {
		return err
	}
	creds, err := l.db.ActiveCredentials(ctx, q)
	if err != nil {
		return err
	}
	s := &Snapshot{LoadedAt: time.Now(), ByID: map[uuid.UUID]*Deployment{}, BySlug: map[string]*Deployment{}, ByModelSlug: map[string][]*Deployment{},
		Providers: map[uuid.UUID]*store.Provider{}, Models: map[uuid.UUID]*store.Model{}, Credentials: map[uuid.UUID]string{}}
	if old := l.holder.Load(); old != nil {
		s.Version = old.Version + 1
	}
	for i := range providers {
		s.Providers[providers[i].ID] = &providers[i]
	}
	for i := range models {
		s.Models[models[i].ID] = &models[i]
	}
	priceByDep := map[uuid.UUID]*pricing.Price{}
	for _, p := range prices {
		tiers, err := pricing.ParseTiers(p.Tiers)
		if err != nil {
			slog.Warn("bad price row skipped", "price_id", p.ID, "err", err)
			continue
		}
		priceByDep[p.DeploymentID] = &pricing.Price{ID: p.ID, DeploymentID: p.DeploymentID, EffectiveFrom: p.EffectiveFrom, Tiers: tiers, Verified: p.Verified, Source: p.Source}
	}
	for _, c := range creds {
		plain, err := l.keyring.Decrypt(c.Ciphertext, c.KeyID)
		if err != nil {
			slog.Error("credential decrypt failed", "provider_id", c.ProviderID, "err", err)
			continue
		}
		s.Credentials[c.ProviderID] = string(plain)
	}
	for i := range deps {
		d := &Deployment{Deployment: deps[i], Model: s.Models[deps[i].ModelID], Provider: s.Providers[deps[i].ProviderID], Price: priceByDep[deps[i].ID]}
		if d.Model != nil {
			_ = json.Unmarshal(d.Model.Capabilities, &d.Capabilities)
		}
		if d.Provider == nil || d.Model == nil {
			continue
		}
		s.ByID[d.ID] = d
		s.BySlug[d.Slug] = d
		s.ByModelSlug[d.Model.Slug] = append(s.ByModelSlug[d.Model.Slug], d)
		s.Deployments = append(s.Deployments, d)
	}
	l.holder.Store(s)
	return nil
}

func HashKey(secret string) [32]byte { return sha256.Sum256([]byte(secret)) }

// ResolveKey returns the config for a virtual key secret, from cache or Postgres.
func (l *Loader) ResolveKey(ctx context.Context, secret string) (*KeyConfig, error) {
	h := HashKey(secret)
	now := time.Now()
	l.keysMu.RLock()
	e := l.keys[h]
	l.keysMu.RUnlock()
	if e != nil && now.Before(e.expires) {
		metrics.ConfigCacheHits.WithLabelValues("hit").Inc()
		return e.cfg, nil
	}
	l.negMu.Lock()
	until, isNeg := l.neg[h]
	l.negMu.Unlock()
	if isNeg && now.Before(until) {
		metrics.ConfigCacheHits.WithLabelValues("negative").Inc()
		return nil, store.ErrNotFound
	}
	metrics.ConfigCacheHits.WithLabelValues("miss").Inc()
	cfg, err := l.loadKey(ctx, h[:])
	if err != nil {
		if err == store.ErrNotFound {
			l.negMu.Lock()
			l.neg[h] = now.Add(60 * time.Second)
			l.negMu.Unlock()
		}
		return nil, err
	}
	l.keysMu.Lock()
	l.keys[h] = &keyEntry{cfg: cfg, expires: now.Add(l.keyTTL)}
	l.keyByID[cfg.Key.ID] = h
	if len(l.keys) > 100000 { // bounded: drop everything rather than track LRU order in the MVP
		l.keys = map[[32]byte]*keyEntry{h: l.keys[h]}
		l.keyByID = map[uuid.UUID][32]byte{cfg.Key.ID: h}
	}
	l.keysMu.Unlock()
	return cfg, nil
}

func (l *Loader) loadKey(ctx context.Context, hash []byte) (*KeyConfig, error) {
	q := l.db.Pool
	k, err := l.db.GetKeyByHash(ctx, q, hash)
	if err != nil {
		return nil, err
	}
	p, err := l.db.GetProject(ctx, q, k.VendorID, k.ProjectID)
	if err != nil {
		return nil, err
	}
	v, err := l.db.GetVendor(ctx, q, k.VendorID)
	if err != nil {
		return nil, err
	}
	budgets, err := l.db.BudgetsForScopes(ctx, q, []uuid.UUID{k.ID, p.ID, v.ID})
	if err != nil {
		return nil, err
	}
	return &KeyConfig{Key: k, Project: p, Vendor: v, Budgets: budgets, Policy: ParsePolicy(k.FallbackPolicy), LoadedAt: time.Now()}, nil
}

// Invalidate drops cached keys. Any key/project/budget/vendor change clears
// the whole key cache: it is cheap to rebuild and avoids subtle staleness.
func (l *Loader) InvalidateKeys() {
	l.keysMu.Lock()
	l.keys = map[[32]byte]*keyEntry{}
	l.keyByID = map[uuid.UUID][32]byte{}
	l.keysMu.Unlock()
	l.negMu.Lock()
	l.neg = map[[32]byte]time.Time{}
	l.negMu.Unlock()
}

// Run periodically reloads the snapshot and listens for invalidations.
func (l *Loader) Run(ctx context.Context) {
	go l.subscribe(ctx)
	t := time.NewTicker(l.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := l.Load(ctx); err != nil {
				slog.Warn("config reload failed; serving previous snapshot", "err", err)
			}
		}
	}
}

func (l *Loader) subscribe(ctx context.Context) {
	for {
		sub := l.rdb.Subscribe(ctx, InvalidationChannel)
		ch := sub.Channel()
		for msg := range ch {
			var c store.ConfigChange
			_ = json.Unmarshal([]byte(msg.Payload), &c)
			switch c.EntityType {
			case "key", "project", "budget", "vendor":
				l.InvalidateKeys()
			default:
				if err := l.Load(ctx); err != nil {
					slog.Warn("config reload on invalidation failed", "err", err)
				}
			}
		}
		_ = sub.Close()
		if ctx.Err() != nil {
			return
		}
		time.Sleep(time.Second)
	}
}

// DrainOutbox publishes committed config changes. Runs in the control plane
// process; the gateway subscribes. In the single-binary MVP both are local.
func DrainOutbox(ctx context.Context, db *store.DB, rdb *redis.Client) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			rows, err := db.DrainOutbox(ctx, 500)
			if err != nil {
				continue
			}
			for _, r := range rows {
				b, _ := json.Marshal(r)
				_ = rdb.Publish(ctx, InvalidationChannel, b).Err()
			}
		}
	}
}
