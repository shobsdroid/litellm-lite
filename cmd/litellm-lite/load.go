package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rzp/litellm-lite/internal/config"
)

// load drives the gateway at a target rate with bounded concurrency and
// reports throughput, latency percentiles and status counts.
func load(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("load", flag.ContinueOnError)
	key := fs.String("key", "", "virtual key")
	rps := fs.Int("rps", 500, "target requests per second")
	dur := fs.Duration("duration", 20*time.Second, "test duration")
	conc := fs.Int("concurrency", 512, "max in-flight requests")
	stream := fs.Bool("stream", false, "use streaming requests")
	target := fs.String("url", "http://"+cfg.GatewayAddr, "gateway base URL")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *key == "" {
		return fmt.Errorf("--key is required")
	}
	body := []byte(fmt.Sprintf(`{"messages":[{"role":"user","content":"load test [output:30]"}],"max_tokens":64,"stream":%v}`, *stream))
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: *conc, MaxIdleConns: *conc}}
	var sent, okc, errc atomic.Int64
	codes := sync.Map{}
	var mu sync.Mutex
	lats := make([]time.Duration, 0, *rps*int(dur.Seconds()))
	sem := make(chan struct{}, *conc)
	ctx, cancel := context.WithTimeout(ctx, *dur)
	defer cancel()
	tick := time.NewTicker(time.Second / time.Duration(*rps))
	defer tick.Stop()
	var wg sync.WaitGroup
	start := time.Now()
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-tick.C:
			select {
			case sem <- struct{}{}:
			default:
				errc.Add(1)
				codes.Store("client_saturated", true)
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				t0 := time.Now()
				req, _ := http.NewRequest("POST", *target+"/v1/chat/completions", bytes.NewReader(body))
				req.Header.Set("Authorization", "Bearer "+*key)
				req.Header.Set("Content-Type", "application/json")
				sent.Add(1)
				resp, err := client.Do(req)
				if err != nil {
					errc.Add(1)
					return
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				d := time.Since(t0)
				mu.Lock()
				lats = append(lats, d)
				mu.Unlock()
				c, _ := codes.LoadOrStore(resp.StatusCode, new(atomic.Int64))
				c.(*atomic.Int64).Add(1)
				if resp.StatusCode == 200 {
					okc.Add(1)
				} else {
					errc.Add(1)
				}
			}()
		}
	}
	wg.Wait()
	elapsed := time.Since(start)
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	pct := func(p float64) time.Duration {
		if len(lats) == 0 {
			return 0
		}
		return lats[int(float64(len(lats)-1)*p)]
	}
	var sb strings.Builder
	codes.Range(func(k, v any) bool {
		if n, ok := v.(*atomic.Int64); ok {
			fmt.Fprintf(&sb, " %v=%d", k, n.Load())
		} else {
			fmt.Fprintf(&sb, " %v", k)
		}
		return true
	})
	fmt.Printf("sent=%d ok=%d err=%d elapsed=%s achieved_rps=%.0f p50=%s p90=%s p99=%s max=%s codes:%s\n",
		sent.Load(), okc.Load(), errc.Load(), elapsed.Round(time.Millisecond), float64(sent.Load())/elapsed.Seconds(), pct(.5), pct(.9), pct(.99), pct(1), sb.String())
	return nil
}
