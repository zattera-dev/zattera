// mesh-demo is a fixture that exercises intra-cluster, cross-node service
// communication on Zattera.
//
// Deployed with several replicas (one per node), each replica exposes:
//
//	GET /whoami  → this replica's identity (JSON). This is what siblings call.
//	GET /healthz → "ok" (the healthcheck).
//	GET /        → fan out to the peer service(s) and report which distinct
//	               replicas answered — the proof that traffic crossed nodes.
//	GET /fanout  → same as /, always JSON.
//
// Who to call is driven entirely by env vars:
//
//	PEERS       comma-separated internal hostnames to fan out to.
//	            Default: "<ZATTERA_APP>.internal" — this service's own internal
//	            DNS name, which the per-node VIP proxy load-balances (P2C, no
//	            affinity) across every healthy replica in the cluster.
//	PEER_COUNT  how many requests to send per peer host (default 6). More calls
//	            → higher chance of touching every replica.
//	PORT        listen port and the port used to reach peers (Zattera injects it).
//	ZATTERA_APP the app name (Zattera injects it); used to derive PEERS.
//	ZATTERA_ENV the environment name (Zattera injects it); reported for context.
//
// Zattera injects no per-replica identity var, so a replica identifies itself by
// its container HOSTNAME. Repeated calls to <app>.internal returning different
// hostnames prove the request reached different replicas — and, since replicas
// spread one-per-node, different nodes.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type identity struct {
	Instance string `json:"instance"` // container hostname (unique per replica)
	App      string `json:"app"`
	Env      string `json:"env"`
}

type fanoutReport struct {
	Summary      string     `json:"summary"`
	Self         identity   `json:"self"`
	PeersQueried []string   `json:"peers_queried"`
	CallsPerPeer int        `json:"calls_per_peer"`
	Reached      []identity `json:"distinct_replicas_reached"`
	ReachedSelf  bool       `json:"reached_self"`
	Errors       []string   `json:"errors,omitempty"`
	TookMs       int64      `json:"took_ms"`
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	port := env("PORT", "8080")
	app := env("ZATTERA_APP", "mesh")
	self := identity{
		Instance: env("HOSTNAME", "unknown"),
		App:      app,
		Env:      env("ZATTERA_ENV", "unknown"),
	}
	peers := splitHosts(env("PEERS", app+".internal"))
	perPeer, err := strconv.Atoi(env("PEER_COUNT", "6"))
	if err != nil || perPeer < 1 {
		perPeer = 6
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("/whoami", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, self)
	})
	fan := func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, doFanout(r.Context(), self, peers, port, perPeer))
	}
	mux.HandleFunc("/fanout", fan)
	mux.HandleFunc("/", fan)

	fmt.Printf("mesh-demo listening on :%s as instance=%s app=%s env=%s peers=%v\n",
		port, self.Instance, self.App, self.Env, peers)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		fmt.Fprintln(os.Stderr, "server error:", err)
		os.Exit(1)
	}
}

// doFanout calls each peer host PORT/whoami perPeer times concurrently and
// aggregates the distinct replica identities that answered.
func doFanout(ctx context.Context, self identity, peers []string, port string, perPeer int) fanoutReport {
	start := time.Now()
	client := &http.Client{Timeout: 4 * time.Second}

	var mu sync.Mutex
	seen := map[string]identity{}
	var errs []string

	var wg sync.WaitGroup
	for _, host := range peers {
		url := "http://" + host + ":" + port + "/whoami"
		for i := 0; i < perPeer; i++ {
			wg.Add(1)
			go func(url string) {
				defer wg.Done()
				id, err := fetchIdentity(ctx, client, url)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					errs = append(errs, err.Error())
					return
				}
				seen[id.Instance] = id
			}(url)
		}
	}
	wg.Wait()

	reached := make([]identity, 0, len(seen))
	_, reachedSelf := seen[self.Instance]
	for _, id := range seen {
		reached = append(reached, id)
	}
	sort.Slice(reached, func(i, j int) bool { return reached[i].Instance < reached[j].Instance })

	return fanoutReport{
		Summary: fmt.Sprintf("I am %s; reached %d distinct replica(s) across %d call(s) to %s",
			self.Instance, len(reached), perPeer*len(peers), strings.Join(peers, ", ")),
		Self:         self,
		PeersQueried: peers,
		CallsPerPeer: perPeer,
		Reached:      reached,
		ReachedSelf:  reachedSelf,
		Errors:       dedupe(errs),
		TookMs:       time.Since(start).Milliseconds(),
	}
}

func fetchIdentity(ctx context.Context, c *http.Client, url string) (identity, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	resp, err := c.Do(req)
	if err != nil {
		return identity{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return identity{}, fmt.Errorf("%s: HTTP %d", url, resp.StatusCode)
	}
	var id identity
	if err := json.NewDecoder(resp.Body).Decode(&id); err != nil {
		return identity{}, fmt.Errorf("%s: %v", url, err)
	}
	return id, nil
}

func splitHosts(s string) []string {
	var out []string
	for _, h := range strings.Split(s, ",") {
		if h = strings.TrimSpace(h); h != "" {
			out = append(out, h)
		}
	}
	return out
}

func dedupe(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
