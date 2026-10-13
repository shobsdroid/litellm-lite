package catalog

import (
	"time"
)

// Prime installs a snapshot and a set of pre-resolved keys without touching
// Postgres. It exists for in-process tests of the gateway (and for any future
// static-config mode); production code paths never call it.
func (l *Loader) Prime(snap *Snapshot, keys map[string]*KeyConfig) {
	l.holder.Store(snap)
	l.keysMu.Lock()
	defer l.keysMu.Unlock()
	for secret, cfg := range keys {
		h := HashKey(secret)
		l.keys[h] = &keyEntry{cfg: cfg, expires: time.Now().Add(100 * 365 * 24 * time.Hour)}
		l.keyByID[cfg.Key.ID] = h
	}
}
