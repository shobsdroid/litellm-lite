// Package webhooks delivers outbox rows to vendor endpoints with HMAC
// signatures and exponential backoff. Workers claim rows with
// FOR UPDATE SKIP LOCKED so several dispatchers never double-deliver.
package webhooks

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rzp/litellm-lite/internal/secrets"
	"github.com/rzp/litellm-lite/internal/store"
)

var backoff = []time.Duration{time.Minute, 5 * time.Minute, 30 * time.Minute, 2 * time.Hour, 6 * time.Hour, 24 * time.Hour}

type Dispatcher struct {
	db      *store.DB
	keyring secrets.Keyring
	client  *http.Client
	// FastRetry shortens backoff for local development.
	FastRetry bool
}

func New(db *store.DB, keyring secrets.Keyring) *Dispatcher {
	return &Dispatcher{db: db, keyring: keyring, client: &http.Client{Timeout: 10 * time.Second}}
}

func Sign(secret string, ts int64, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(ts, 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	return fmt.Sprintf("t=%d,v1=%s", ts, hex.EncodeToString(mac.Sum(nil)))
}

func (d *Dispatcher) Run(ctx context.Context) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for d.tick(ctx) > 0 {
			}
		}
	}
}

func (d *Dispatcher) tick(ctx context.Context) int {
	n := 0
	err := d.db.Tx(ctx, func(tx pgx.Tx) error {
		rows, err := d.db.ClaimDeliveries(ctx, tx, 50)
		if err != nil {
			return err
		}
		n = len(rows)
		for _, r := range rows {
			d.deliver(ctx, tx, &r)
		}
		return nil
	})
	if err != nil {
		slog.Warn("webhook tick failed", "err", err)
		return 0
	}
	return n
}

func (d *Dispatcher) deliver(ctx context.Context, tx pgx.Tx, r *store.WebhookDelivery) {
	ep, err := d.db.GetWebhookByID(ctx, tx, r.EndpointID)
	if err != nil || ep.Status != "active" {
		msg := "endpoint inactive"
		_ = d.db.FinishDelivery(ctx, tx, r.ID, "dead", r.Attempts, time.Now(), nil, &msg)
		return
	}
	secret, err := d.keyring.Decrypt(ep.SecretCiphertext, "env-v1")
	if err != nil {
		msg := "secret decrypt failed"
		_ = d.db.FinishDelivery(ctx, tx, r.ID, "dead", r.Attempts, time.Now(), nil, &msg)
		return
	}
	ts := time.Now().Unix()
	req, _ := http.NewRequestWithContext(ctx, "POST", ep.URL, bytes.NewReader(r.Payload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Signature", Sign(string(secret), ts, r.Payload))
	req.Header.Set("X-Event-Id", r.EventID)
	req.Header.Set("X-Event-Type", r.EventType)
	resp, err := d.client.Do(req)
	attempts := r.Attempts + 1
	var code *int32
	var errText *string
	ok := false
	if err != nil {
		s := err.Error()
		errText = &s
	} else {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		c := int32(resp.StatusCode)
		code = &c
		ok = resp.StatusCode >= 200 && resp.StatusCode < 300
		if !ok {
			s := "non-2xx response"
			errText = &s
		}
	}
	if ok {
		_ = d.db.FinishDelivery(ctx, tx, r.ID, "delivered", attempts, time.Now(), code, nil)
		return
	}
	if int(attempts) > len(backoff) {
		_ = d.db.FinishDelivery(ctx, tx, r.ID, "dead", attempts, time.Now(), code, errText)
		return
	}
	wait := backoff[attempts-1]
	if d.FastRetry {
		wait = time.Duration(attempts) * 5 * time.Second
	}
	_ = d.db.FinishDelivery(ctx, tx, r.ID, "pending", attempts, time.Now().Add(wait), code, errText)
}
