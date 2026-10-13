// Package secrets encrypts provider credentials and webhook secrets at rest.
// The MVP uses one AES-256-GCM key from the environment (EnvKeyring).
// Production swaps in a KMS-backed Keyring with the same interface.
package secrets

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
)

type Keyring interface {
	// Encrypt returns ciphertext and the id of the key that produced it.
	Encrypt(plaintext []byte) (ciphertext []byte, keyID string, err error)
	Decrypt(ciphertext []byte, keyID string) ([]byte, error)
}

type EnvKeyring struct {
	aead cipher.AEAD
}

const envKeyID = "env-v1"

func NewEnvKeyring(key []byte) (*EnvKeyring, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &EnvKeyring{aead: aead}, nil
}

func (k *EnvKeyring) Encrypt(plaintext []byte) ([]byte, string, error) {
	nonce := make([]byte, k.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, "", err
	}
	return k.aead.Seal(nonce, nonce, plaintext, nil), envKeyID, nil
}

func (k *EnvKeyring) Decrypt(ciphertext []byte, keyID string) ([]byte, error) {
	if keyID != envKeyID {
		return nil, errors.New("unknown key id " + keyID)
	}
	ns := k.aead.NonceSize()
	if len(ciphertext) < ns {
		return nil, errors.New("ciphertext too short")
	}
	return k.aead.Open(nil, ciphertext[:ns], ciphertext[ns:], nil)
}
