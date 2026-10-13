package pricing

import (
	"encoding/json"
	"sync"

	"github.com/pkoukk/tiktoken-go"
	tiktoken_loader "github.com/pkoukk/tiktoken-go-loader"
	"github.com/rzp/litellm-lite/internal/canonical"
)

// Tokenizer estimates prompt tokens. Provider-reported usage is always the
// source of truth for billing; this only sizes reservations and context checks.
type Tokenizer struct {
	mu   sync.Mutex
	encs map[string]*tiktoken.Tiktoken
}

var defaultTokenizer = &Tokenizer{encs: map[string]*tiktoken.Tiktoken{}}

func init() {
	tiktoken.SetBpeLoader(tiktoken_loader.NewOfflineLoader())
}

func Default() *Tokenizer { return defaultTokenizer }

func (t *Tokenizer) enc(name string) (*tiktoken.Tiktoken, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if e, ok := t.encs[name]; ok {
		return e, nil
	}
	e, err := tiktoken.GetEncoding(name)
	if err != nil {
		return nil, err
	}
	t.encs[name] = e
	return e, nil
}

// Warm loads an encoding eagerly (first load takes ~1s).
func (t *Tokenizer) Warm(name string) error {
	_, err := t.enc(name)
	return err
}

// CountText counts tokens in free text with the named encoding; falls back to len/4.
func (t *Tokenizer) CountText(encoding, s string) int64 {
	e, err := t.enc(encoding)
	if err != nil {
		return int64(len(s)+3) / 4
	}
	return int64(len(e.Encode(s, nil, nil)))
}

// CountRequest estimates prompt tokens for a chat request, including tool
// definitions and a per-message framing overhead, scaled by multiplier
// (calibration for models whose tokenizer we do not have).
func (t *Tokenizer) CountRequest(encoding string, multiplier float64, req *canonical.Request) int64 {
	var n int64
	for _, m := range req.Messages {
		n += 4 // role + separators
		n += t.CountText(encoding, m.Text())
		for _, p := range m.Parts {
			if p.Type == "image_url" {
				n += 1000 // conservative image budget; providers bill by tiles
			}
		}
		for _, tc := range m.ToolCalls {
			n += t.CountText(encoding, tc.Name+tc.Arguments) + 4
		}
	}
	if len(req.Tools) > 0 {
		b, _ := json.Marshal(req.Tools)
		n += t.CountText(encoding, string(b))
	}
	if multiplier <= 0 {
		multiplier = 1
	}
	return int64(float64(n)*multiplier + 0.5)
}
