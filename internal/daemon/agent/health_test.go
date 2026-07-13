package agent

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	crt "github.com/zattera-dev/zattera/internal/daemon/runtime"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/testutil/fakeruntime"
)

const (
	healthy   = zatterav1.InstanceState_INSTANCE_STATE_HEALTHY
	unhealthy = zatterav1.InstanceState_INSTANCE_STATE_UNHEALTHY
)

func TestHealth(t *testing.T) {
	t.Run("state machine: grace tolerated, first pass heals, flap, recovery", func(t *testing.T) {
		cfg := HealthConfig{Interval: 10 * time.Second, Grace: 60 * time.Second, Threshold: 3}
		sm := newHealthSM(cfg)

		// Failures inside the grace window keep the instance RUNNING.
		for _, elapsed := range []time.Duration{10 * time.Second, 20 * time.Second, 50 * time.Second} {
			if st, changed := sm.observe(false, elapsed); changed || st != running {
				t.Fatalf("grace failure should not transition (elapsed %s): st=%v changed=%v", elapsed, st, changed)
			}
		}
		// First pass → HEALTHY.
		if st, changed := sm.observe(true, 55*time.Second); !changed || st != healthy {
			t.Fatalf("first pass should heal: st=%v changed=%v", st, changed)
		}
		// Two fails: still HEALTHY (threshold is 3).
		sm.observe(false, 70*time.Second)
		if st, changed := sm.observe(false, 80*time.Second); changed || st != healthy {
			t.Fatalf("below threshold should stay HEALTHY: st=%v changed=%v", st, changed)
		}
		// Third consecutive fail → UNHEALTHY.
		if st, changed := sm.observe(false, 90*time.Second); !changed || st != unhealthy {
			t.Fatalf("threshold reached should be UNHEALTHY: st=%v changed=%v", st, changed)
		}
		// Recovery on pass.
		if st, changed := sm.observe(true, 100*time.Second); !changed || st != healthy {
			t.Fatalf("pass should recover to HEALTHY: st=%v changed=%v", st, changed)
		}
	})

	t.Run("grace expiry without a pass eventually goes UNHEALTHY", func(t *testing.T) {
		sm := newHealthSM(HealthConfig{Grace: 30 * time.Second, Threshold: 3})
		// Fail through the grace window; only past grace do failures count.
		sm.observe(false, 10*time.Second)
		sm.observe(false, 20*time.Second)
		if _, changed := sm.observe(false, 25*time.Second); changed {
			t.Fatal("should still be starting within grace")
		}
		if st, changed := sm.observe(false, 35*time.Second); !changed || st != unhealthy {
			t.Fatalf("past grace with >=threshold fails should be UNHEALTHY: st=%v changed=%v", st, changed)
		}
	})

	t.Run("no healthcheck reports HEALTHY immediately", func(t *testing.T) {
		rec := &statusRec{latest: map[string]*zatterav1.AssignmentObserved{}}
		mgr := NewManager(context.Background(), ManagerConfig{Clock: clock.NewFake(), Report: rec.sink})
		mgr.Ensure(assign("a1", "h1", run), &zatterav1.ServiceSpec{}, crt.ContainerState{ID: "c1"})
		if rec.state("a1") != healthy {
			t.Fatalf("expected immediate HEALTHY, got %v", rec.state("a1"))
		}
	})

	t.Run("http probe passes on 2xx and fails on 5xx", func(t *testing.T) {
		var code int
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) }))
		defer srv.Close()
		p := httpProbe(srv.URL + "/healthz")

		code = 200
		if err := p(context.Background()); err != nil {
			t.Fatalf("2xx should pass: %v", err)
		}
		code = 503
		if err := p(context.Background()); err == nil {
			t.Fatal("5xx should fail")
		}
	})

	t.Run("tcp probe passes on open port and fails on closed", func(t *testing.T) {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := lis.Addr().String()
		if err := tcpProbe(addr)(context.Background()); err != nil {
			t.Fatalf("open port should pass: %v", err)
		}
		_ = lis.Close()
		if err := tcpProbe(addr)(context.Background()); err == nil {
			t.Fatal("closed port should fail")
		}
	})

	t.Run("exec probe passes when the command exits zero", func(t *testing.T) {
		rt := fakeruntime.New()
		id, _ := rt.CreateContainer(context.Background(), crt.ContainerSpec{Image: "app"})
		_ = rt.StartContainer(context.Background(), id)
		if err := execProbe(rt, id, "true")(context.Background()); err != nil {
			t.Fatalf("exec of a running container should pass: %v", err)
		}
		// A missing container fails.
		if err := execProbe(rt, "nope", "true")(context.Background()); err == nil {
			t.Fatal("exec of an unknown container should fail")
		}
	})

	t.Run("monitor drives transitions on the clock and stops on cancel", func(t *testing.T) {
		clk := clock.NewFake()
		rec := &statusRec{latest: map[string]*zatterav1.AssignmentObserved{}}
		var mu sync.Mutex
		pass := true
		calls := 0
		probe := func(context.Context) error {
			mu.Lock()
			defer mu.Unlock()
			calls++
			if pass {
				return nil
			}
			return errors.New("down")
		}
		setPass := func(v bool) { mu.Lock(); pass = v; mu.Unlock() }
		getCalls := func() int { mu.Lock(); defer mu.Unlock(); return calls }

		mon := &Monitor{
			assignID: "a1",
			probe:    probe,
			cfg:      HealthConfig{Interval: 10 * time.Second, Timeout: time.Second, Grace: 60 * time.Second, Threshold: 3},
			clock:    clk,
			report:   rec.sink,
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go mon.Run(ctx)

		// First probe passes → HEALTHY.
		advanceOneProbe(t, clk, 10*time.Second, getCalls)
		waitState(t, rec, "a1", healthy)

		// Three consecutive fails → UNHEALTHY.
		setPass(false)
		for i := 0; i < 3; i++ {
			advanceOneProbe(t, clk, 10*time.Second, getCalls)
		}
		waitState(t, rec, "a1", unhealthy)

		// Recover.
		setPass(true)
		advanceOneProbe(t, clk, 10*time.Second, getCalls)
		waitState(t, rec, "a1", healthy)

		// Cancel stops probing.
		cancel()
		time.Sleep(10 * time.Millisecond)
		before := getCalls()
		clk.Advance(30 * time.Second)
		time.Sleep(10 * time.Millisecond)
		if getCalls() != before {
			t.Fatal("monitor should stop probing after cancel")
		}
	})

	t.Run("probeTarget selects container IP on linux and host port on darwin", func(t *testing.T) {
		st := crt.ContainerState{
			IPAddress: "172.30.0.5",
			Ports:     []crt.PortBinding{{Name: "http", ContainerPort: 8080, HostPort: 30001}},
		}
		if addr, ok := probeTarget(false, 8080, st, "127.0.0.1"); !ok || addr != "172.30.0.5:8080" {
			t.Fatalf("linux target = %q ok=%v", addr, ok)
		}
		if addr, ok := probeTarget(true, 8080, st, "127.0.0.1"); !ok || addr != "127.0.0.1:30001" {
			t.Fatalf("darwin target = %q ok=%v", addr, ok)
		}
		// No container IP (linux) → not resolvable.
		if _, ok := probeTarget(false, 8080, crt.ContainerState{}, "127.0.0.1"); ok {
			t.Fatal("missing container IP should not resolve")
		}
	})
}

// advanceOneProbe advances the fake clock until the monitor goroutine runs at
// least one more probe. It re-advances on each poll so a not-yet-registered
// ticker (the goroutine may not have called NewTicker yet) can't wedge the
// test.
func advanceOneProbe(t *testing.T, clk *clock.Fake, interval time.Duration, calls func() int) {
	t.Helper()
	before := calls()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		clk.Advance(interval)
		for i := 0; i < 50; i++ {
			if calls() > before {
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}
	t.Fatal("probe did not run after clock advance")
}

func waitState(t *testing.T, rec *statusRec, id string, want zatterav1.InstanceState) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if rec.state(id) == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("state %v not reached for %s (last %v)", want, id, rec.state(id))
}
