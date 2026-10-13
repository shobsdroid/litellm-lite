package webhooks

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"
)

// PLAN §12: X-Signature: t=<ts>,v1=<hmac-sha256 over "<ts>.<body>">.
func TestSignFormatAndValue(t *testing.T) {
	body := []byte(`{"event":"budget.exceeded"}`)
	sig := Sign("whsec_abc", 1760000000, body)
	ts, v1, ok := strings.Cut(sig, ",")
	if !ok || ts != "t=1760000000" || !strings.HasPrefix(v1, "v1=") {
		t.Fatalf("bad signature shape: %s", sig)
	}
	mac := hmac.New(sha256.New, []byte("whsec_abc"))
	mac.Write([]byte("1760000000." + string(body)))
	if want := hex.EncodeToString(mac.Sum(nil)); v1 != "v1="+want {
		t.Fatalf("hmac mismatch: %s vs %s", v1, want)
	}
}

func TestSignIsSecretAndBodyBound(t *testing.T) {
	body := []byte(`{}`)
	a := Sign("s1", 1, body)
	if a == Sign("s2", 1, body) {
		t.Fatal("different secrets must sign differently")
	}
	if a == Sign("s1", 1, []byte(`{"x":1}`)) {
		t.Fatal("different bodies must sign differently")
	}
	if a == Sign("s1", 2, body) {
		t.Fatal("timestamp must be part of the signed material (replay protection)")
	}
}

// PLAN §12: retries at 1m, 5m, 30m, 2h, 6h, 24h, then dead-letter.
func TestBackoffSchedule(t *testing.T) {
	want := []time.Duration{time.Minute, 5 * time.Minute, 30 * time.Minute, 2 * time.Hour, 6 * time.Hour, 24 * time.Hour}
	if len(backoff) != len(want) {
		t.Fatalf("backoff has %d steps want %d", len(backoff), len(want))
	}
	for i := range want {
		if backoff[i] != want[i] {
			t.Errorf("step %d: %s want %s", i, backoff[i], want[i])
		}
	}
}
