// Package config loads process configuration from the environment.
// Every value has a local default so `make up && make serve` works with no .env.
package config

import (
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL      string
	RedisAddr        string
	ClickHouseAddr   string
	ClickHouseDB     string
	GatewayAddr      string
	ControlPlaneAddr string
	EncryptionKey    []byte
	SpillDir         string
	LogLevel         string

	// Hot-path tuning.
	MaxInflight          int
	ReservationTTL       time.Duration
	ConfigReloadInterval time.Duration
	KeyCacheTTL          time.Duration
	DegradedTolerancePct int
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func Load() (*Config, error) {
	c := &Config{
		DatabaseURL:          env("DATABASE_URL", "postgres://litellm:litellm@localhost:5433/litellm?sslmode=disable"),
		RedisAddr:            env("REDIS_ADDR", "localhost:6380"),
		ClickHouseAddr:       env("CLICKHOUSE_ADDR", "localhost:9001"),
		ClickHouseDB:         env("CLICKHOUSE_DB", "litellm"),
		GatewayAddr:          env("GATEWAY_ADDR", "127.0.0.1:8080"),
		ControlPlaneAddr:     env("CONTROLPLANE_ADDR", "127.0.0.1:8081"),
		SpillDir:             env("SPILL_DIR", "./data/spill"),
		LogLevel:             env("LOG_LEVEL", "info"),
		MaxInflight:          envInt("MAX_INFLIGHT", 20000),
		ReservationTTL:       time.Duration(envInt("RESERVATION_TTL_SECONDS", 660)) * time.Second,
		ConfigReloadInterval: time.Duration(envInt("CONFIG_RELOAD_SECONDS", 10)) * time.Second,
		KeyCacheTTL:          time.Duration(envInt("KEY_CACHE_TTL_SECONDS", 60)) * time.Second,
		DegradedTolerancePct: envInt("DEGRADED_TOLERANCE_PCT", 5),
	}
	keyHex := env("ENCRYPTION_KEY", "0000000000000000000000000000000000000000000000000000000000000000")
	key, err := hex.DecodeString(keyHex)
	if err != nil || len(key) != 32 {
		return nil, fmt.Errorf("ENCRYPTION_KEY must be 64 hex chars (32 bytes)")
	}
	c.EncryptionKey = key
	return c, nil
}
