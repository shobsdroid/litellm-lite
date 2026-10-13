package secrets

import (
	"bytes"
	"crypto/rand"
	"testing"
)

// PLAN §8.3: provider credentials encrypted with AES-256-GCM, versioned key id.

func newKey(t *testing.T) *EnvKeyring {
	t.Helper()
	key := make([]byte, 32)
	rand.Read(key)
	k, err := NewEnvKeyring(key)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func TestRoundTrip(t *testing.T) {
	k := newKey(t)
	ct, id, err := k.Encrypt([]byte("sk-live-secret"))
	if err != nil {
		t.Fatal(err)
	}
	if id != "env-v1" {
		t.Fatalf("key id %q", id)
	}
	if bytes.Contains(ct, []byte("sk-live")) {
		t.Fatal("ciphertext leaks plaintext")
	}
	pt, err := k.Decrypt(ct, id)
	if err != nil || string(pt) != "sk-live-secret" {
		t.Fatalf("decrypt: %q %v", pt, err)
	}
}

func TestNonceIsFresh(t *testing.T) {
	k := newKey(t)
	a, _, _ := k.Encrypt([]byte("same"))
	b, _, _ := k.Encrypt([]byte("same"))
	if bytes.Equal(a, b) {
		t.Fatal("two encryptions of the same plaintext must differ (random nonce)")
	}
}

func TestTamperDetected(t *testing.T) {
	k := newKey(t)
	ct, id, _ := k.Encrypt([]byte("payload"))
	ct[len(ct)-1] ^= 0xff
	if _, err := k.Decrypt(ct, id); err == nil {
		t.Fatal("GCM must reject a modified ciphertext")
	}
}

func TestWrongKeyOrID(t *testing.T) {
	k1, k2 := newKey(t), newKey(t)
	ct, id, _ := k1.Encrypt([]byte("payload"))
	if _, err := k2.Decrypt(ct, id); err == nil {
		t.Fatal("a different key must not decrypt")
	}
	if _, err := k1.Decrypt(ct, "kms-v9"); err == nil {
		t.Fatal("unknown key id must be refused")
	}
	if _, err := k1.Decrypt([]byte{1, 2, 3}, id); err == nil {
		t.Fatal("ciphertext shorter than the nonce must be refused")
	}
}

func TestKeyLength(t *testing.T) {
	if _, err := NewEnvKeyring([]byte("short")); err == nil {
		t.Fatal("non AES key length must error")
	}
	for _, n := range []int{16, 24, 32} {
		if _, err := NewEnvKeyring(make([]byte, n)); err != nil {
			t.Fatalf("%d-byte key: %v", n, err)
		}
	}
}
