package agent

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	crt "github.com/zattera-dev/zattera/internal/daemon/runtime"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// Health check defaults (proto docs).
const (
	defaultProbeInterval  = 10 * time.Second
	defaultProbeTimeout   = 5 * time.Second
	defaultProbeGrace     = 60 * time.Second
	defaultProbeThreshold = 3
)

// HealthConfig is the resolved probe schedule for one instance.
type HealthConfig struct {
	Interval  time.Duration
	Timeout   time.Duration
	Grace     time.Duration
	Threshold int
}

// resolveHealthConfig applies proto defaults over a (possibly nil) HealthCheck.
func resolveHealthConfig(hc *zatterav1.HealthCheck) HealthConfig {
	cfg := HealthConfig{
		Interval:  defaultProbeInterval,
		Timeout:   defaultProbeTimeout,
		Grace:     defaultProbeGrace,
		Threshold: defaultProbeThreshold,
	}
	if hc == nil {
		return cfg
	}
	if d := hc.GetInterval().AsDuration(); d > 0 {
		cfg.Interval = d
	}
	if d := hc.GetTimeout().AsDuration(); d > 0 {
		cfg.Timeout = d
	}
	if d := hc.GetGracePeriod().AsDuration(); d > 0 {
		cfg.Grace = d
	}
	if t := hc.GetUnhealthyThreshold(); t > 0 {
		cfg.Threshold = int(t)
	}
	return cfg
}

// Probe performs one health check. A nil error means the instance passed.
type Probe func(ctx context.Context) error

// httpProbe passes on any 2xx/3xx response.
func httpProbe(url string) Probe {
	return func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode < 200 || resp.StatusCode >= 400 {
			return fmt.Errorf("http status %d", resp.StatusCode)
		}
		return nil
	}
}

// tcpProbe passes if the address accepts a connection.
func tcpProbe(addr string) Probe {
	return func(ctx context.Context) error {
		var d net.Dialer
		conn, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			return err
		}
		return conn.Close()
	}
}

// execProbe passes if the command exits 0 inside the container.
func execProbe(rt crt.ContainerRuntime, containerID string, command string) Probe {
	cmd := []string{"/bin/sh", "-c", command}
	return func(ctx context.Context) error {
		code, err := rt.Exec(ctx, containerID, crt.ExecSpec{Command: cmd}, nil, nil, nil, nil)
		if err != nil {
			return err
		}
		if code != 0 {
			return fmt.Errorf("exec exited %d", code)
		}
		return nil
	}
}

// healthSM is the per-instance health state machine. It is pure: feed it probe
// results with the elapsed time since start and it returns the reported state
// and whether it changed.
type healthSM struct {
	cfg        HealthConfig
	state      zatterav1.InstanceState // last reported
	passedOnce bool
	fails      int
}

func newHealthSM(cfg HealthConfig) *healthSM {
	return &healthSM{cfg: cfg, state: zatterav1.InstanceState_INSTANCE_STATE_RUNNING}
}

// observe folds one probe result into the machine.
func (s *healthSM) observe(pass bool, elapsed time.Duration) (zatterav1.InstanceState, bool) {
	if pass {
		s.fails = 0
		s.passedOnce = true
		if s.state != zatterav1.InstanceState_INSTANCE_STATE_HEALTHY {
			s.state = zatterav1.InstanceState_INSTANCE_STATE_HEALTHY
			return s.state, true
		}
		return s.state, false
	}

	s.fails++
	switch {
	case s.state == zatterav1.InstanceState_INSTANCE_STATE_HEALTHY:
		// Flap out only after the configured consecutive failures.
		if s.fails >= s.cfg.Threshold {
			s.state = zatterav1.InstanceState_INSTANCE_STATE_UNHEALTHY
			return s.state, true
		}
	case !s.passedOnce:
		// Still starting: tolerate failures until the grace window elapses.
		if elapsed >= s.cfg.Grace && s.fails >= s.cfg.Threshold {
			s.state = zatterav1.InstanceState_INSTANCE_STATE_UNHEALTHY
			return s.state, true
		}
	}
	return s.state, false
}

// Monitor probes one instance on the clock and reports state transitions
// (only on change) via the sink.
type Monitor struct {
	assignID string
	probe    Probe
	cfg      HealthConfig
	clock    clock.Clock
	report   StatusSink
	log      *slog.Logger
}

// Run probes until ctx is canceled.
func (m *Monitor) Run(ctx context.Context) {
	start := m.clock.Now()
	sm := newHealthSM(m.cfg)
	tick := m.clock.NewTicker(m.cfg.Interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C():
			pass := m.runProbe(ctx)
			if state, changed := sm.observe(pass, m.clock.Now().Sub(start)); changed {
				m.emit(state)
			}
		}
	}
}

// runProbe runs the probe under a timeout in its own goroutine so a hung probe
// bounds at Timeout instead of stalling the serial loop.
func (m *Monitor) runProbe(ctx context.Context) bool {
	pctx, cancel := context.WithTimeout(ctx, m.cfg.Timeout)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- m.probe(pctx) }()
	select {
	case err := <-done:
		return err == nil
	case <-pctx.Done():
		return false
	}
}

func (m *Monitor) emit(state zatterav1.InstanceState) {
	if m.report == nil {
		return
	}
	m.report(map[string]*zatterav1.AssignmentObserved{
		m.assignID: {State: state, UpdatedAt: timestamppb.New(m.clock.Now())},
	})
}

// Manager owns one Monitor per running assignment. The executor calls Ensure
// when a container reaches RUNNING and Remove when it is torn down.
type Manager struct {
	clock       clock.Clock
	report      StatusSink
	rt          crt.ContainerRuntime
	hostIP      string
	useHostPort bool // probe via the published host port (macOS dev)
	log         *slog.Logger

	mu       sync.Mutex
	monitors map[string]context.CancelFunc
	base     context.Context
}

// ManagerConfig configures the health manager.
type ManagerConfig struct {
	Clock   clock.Clock
	Report  StatusSink
	Runtime crt.ContainerRuntime
	HostIP  string
	Logger  *slog.Logger
}

// NewManager builds a health manager. base bounds every monitor's lifetime.
func NewManager(base context.Context, cfg ManagerConfig) *Manager {
	if cfg.Clock == nil {
		cfg.Clock = clock.Real{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.HostIP == "" {
		cfg.HostIP = "127.0.0.1"
	}
	return &Manager{
		clock:       cfg.Clock,
		report:      cfg.Report,
		rt:          cfg.Runtime,
		hostIP:      cfg.HostIP,
		useHostPort: runtime.GOOS == "darwin",
		log:         cfg.Logger,
		monitors:    map[string]context.CancelFunc{},
		base:        base,
	}
}

// Ensure starts a monitor for the assignment if one is not already running. A
// service without a health check is reported HEALTHY immediately.
func (m *Manager) Ensure(a *zatterav1.Assignment, spec *zatterav1.ServiceSpec, st crt.ContainerState) {
	id := a.GetMeta().GetId()
	m.mu.Lock()
	if _, ok := m.monitors[id]; ok {
		m.mu.Unlock()
		return
	}

	hc := spec.GetHealthcheck()
	if hc == nil {
		// No health check: healthy as soon as it is running.
		m.monitors[id] = func() {} // mark handled; nothing to cancel
		m.mu.Unlock()
		if m.report != nil {
			m.report(map[string]*zatterav1.AssignmentObserved{
				id: {State: zatterav1.InstanceState_INSTANCE_STATE_HEALTHY, ContainerId: st.ID, UpdatedAt: timestamppb.New(m.clock.Now())},
			})
		}
		return
	}

	probe, ok := m.buildProbe(hc, spec, st)
	if !ok {
		m.mu.Unlock()
		m.log.Warn("health: could not resolve a probe target; skipping", "assignment", id)
		return
	}
	ctx, cancel := context.WithCancel(m.base)
	m.monitors[id] = cancel
	m.mu.Unlock()

	mon := &Monitor{assignID: id, probe: probe, cfg: resolveHealthConfig(hc), clock: m.clock, report: m.report, log: m.log}
	go mon.Run(ctx)
}

// Remove stops the monitor for an assignment (idempotent).
func (m *Manager) Remove(assignID string) {
	m.mu.Lock()
	cancel := m.monitors[assignID]
	delete(m.monitors, assignID)
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// Reconcile stops monitors whose assignment is no longer live.
func (m *Manager) Reconcile(live map[string]bool) {
	m.mu.Lock()
	var stale []context.CancelFunc
	for id, cancel := range m.monitors {
		if !live[id] {
			stale = append(stale, cancel)
			delete(m.monitors, id)
		}
	}
	m.mu.Unlock()
	for _, cancel := range stale {
		cancel()
	}
}

// buildProbe resolves the probe for a health check against a container.
func (m *Manager) buildProbe(hc *zatterav1.HealthCheck, spec *zatterav1.ServiceSpec, st crt.ContainerState) (Probe, bool) {
	typ := hc.GetType()
	if typ == zatterav1.HealthCheckType_HEALTH_CHECK_TYPE_UNSPECIFIED {
		// Default: HTTP when the service exposes a port, else TCP.
		if len(spec.GetPorts()) > 0 {
			typ = zatterav1.HealthCheckType_HEALTH_CHECK_TYPE_HTTP
		} else {
			typ = zatterav1.HealthCheckType_HEALTH_CHECK_TYPE_TCP
		}
	}

	if typ == zatterav1.HealthCheckType_HEALTH_CHECK_TYPE_EXEC {
		return execProbe(m.rt, st.ID, hc.GetCommand()), true
	}

	addr, ok := probeTarget(m.useHostPort, healthPort(hc, spec), st, m.hostIP)
	if !ok {
		return nil, false
	}
	switch typ {
	case zatterav1.HealthCheckType_HEALTH_CHECK_TYPE_HTTP:
		path := hc.GetPath()
		if path == "" {
			path = "/healthz"
		}
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}
		return httpProbe("http://" + addr + path), true
	default: // TCP
		return tcpProbe(addr), true
	}
}

// healthPort picks the container port to probe: the health check's explicit
// port, else the first declared service port.
func healthPort(hc *zatterav1.HealthCheck, spec *zatterav1.ServiceSpec) uint32 {
	if p := hc.GetPort(); p > 0 {
		return p
	}
	if ports := spec.GetPorts(); len(ports) > 0 {
		return ports[0].GetContainerPort()
	}
	return 0
}

// probeTarget resolves host:port for HTTP/TCP probes. On Linux the container IP
// on the per-env bridge is reachable from the host netns; on macOS it is not,
// so we probe the published host port instead.
func probeTarget(useHostPort bool, containerPort uint32, st crt.ContainerState, hostIP string) (string, bool) {
	if useHostPort {
		for _, b := range st.Ports {
			if b.ContainerPort == containerPort && b.HostPort != 0 {
				host := b.HostIP
				if host == "" || host == "0.0.0.0" {
					host = hostIP
				}
				return net.JoinHostPort(host, itoa(b.HostPort)), true
			}
		}
		return "", false
	}
	if st.IPAddress == "" || containerPort == 0 {
		return "", false
	}
	return net.JoinHostPort(st.IPAddress, itoa(containerPort)), true
}

func itoa(p uint32) string { return fmt.Sprintf("%d", p) }
