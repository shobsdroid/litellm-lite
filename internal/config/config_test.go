package config

import (
	"strings"
	"testing"
	"time"
)

func TestDefaults(t *testing.T) {
	for _, k := range []string{"DATABASE_URL", "REDIS_ADDR", "GATEWAY_ADDR", "CONTROLPLANE_ADDR", "ENCRYPTION_KEY", "MAX_INFLIGHT", "RESERVATION_TTL_SECONDS", "DEGRADED_TOLERANCE_PCT"} {
		t.Setenv(k, "")
	}
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	// PLAN §6: gateway 8080, control plane 8081, both local.
	if c.GatewayAddr != "127.0.0.1:8080" || c.ControlPlaneAddr != "127.0.0.1:8081" {
		t.Fatalf("addrs %s %s", c.GatewayAddr, c.ControlPlaneAddr)
	}
	if len(c.EncryptionKey) != 32 {
		t.Fatalf("default key length %d", len(c.EncryptionKey))
	}
	// PLAN §4.2: default overrun tolerance 5%.
	if c.DegradedTolerancePct != 5 {
		t.Fatalf("tolerance %d", c.DegradedTolerancePct)
	}
	// PLAN §4.1: reservation deadline = request max duration (600s) + 60s.
	if c.ReservationTTL != 660*time.Second {
		t.Fatalf("reservation ttl %s", c.ReservationTTL)
	}
	if c.MaxInflight <= 0 || c.KeyCacheTTL <= 0 || c.ConfigReloadInterval <= 0 {
		t.Fatalf("hot-path tuning must have positive defaults: %+v", c)
	}
}

func TestEnvOverrides(t *testing.T) {
	t.Setenv("GATEWAY_ADDR", "0.0.0.0:9090")
	t.Setenv("MAX_INFLIGHT", "42")
	t.Setenv("DEGRADED_TOLERANCE_PCT", "10")
	t.Setenv("ENCRYPTION_KEY", strings.Repeat("ab", 32))
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.GatewayAddr != "0.0.0.0:9090" || c.MaxInflight != 42 || c.DegradedTolerancePct != 10 || c.EncryptionKey[0] != 0xab {
		t.Fatalf("overrides not applied: %+v", c)
	}
}

func TestBadEncryptionKey(t *testing.T) {
	for _, v := range []string{"zz", strings.Repeat("a", 63), strings.Repeat("a", 66)} {
		t.Setenv("ENCRYPTION_KEY", v)
		if _, err := Load(); err == nil {
			t.Errorf("key %q should be rejected", v)
		}
	}
	t.Setenv("MAX_INFLIGHT", "not-a-number")
	t.Setenv("ENCRYPTION_KEY", "")
	c, err := Load()
	if err != nil || c.MaxInflight != 20000 {
		t.Fatalf("non-numeric int env must fall back to the default: %v %+v", err, c)
	}
}
