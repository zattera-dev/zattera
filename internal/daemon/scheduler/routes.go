package scheduler

import (
	"context"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"time"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/state"
)

// routeDebounce coalesces a burst of state changes into one rebuild.
const routeDebounce = 200 * time.Millisecond

// RouteBuilder builds a global RouteSnapshot from cluster state and fans it out
// to WatchRoutes subscribers. Every control node builds the same snapshot from
// its replicated state (a pure function of state), so any control node can serve
// the stream — no leader gating needed.
type RouteBuilder struct {
	store  *raftstore.Store
	clk    clock.Clock
	log    *slog.Logger
	domain string // cluster app domain (implicit <app>-<env>.<domain> hosts)

	mu      sync.Mutex
	current *clusterv1.RouteSnapshot
	subs    map[int]chan *clusterv1.RouteSnapshot
	nextID  int
}

// NewRouteBuilder constructs the route builder. domain is cfg.Domain (empty →
// no implicit cluster subdomains).
func NewRouteBuilder(store *raftstore.Store, clk clock.Clock, domain string, log *slog.Logger) *RouteBuilder {
	if log == nil {
		log = slog.Default()
	}
	if clk == nil {
		clk = clock.Real{}
	}
	return &RouteBuilder{
		store: store, clk: clk, log: log, domain: domain,
		current: &clusterv1.RouteSnapshot{},
		subs:    map[int]chan *clusterv1.RouteSnapshot{},
	}
}

// Run watches routing-relevant state and rebuilds (debounced) plus on a 15s
// tick, fanning each snapshot out to subscribers.
func (b *RouteBuilder) Run(ctx context.Context) {
	sub := b.store.State().Watch(state.KindEnvironment, state.KindDomain, state.KindAssignment, state.KindNode, state.KindServiceVIP)
	defer sub.Close()
	tick := b.clk.NewTicker(15 * time.Second)
	defer tick.Stop()

	b.rebuild()
	for {
		select {
		case <-ctx.Done():
			return
		case <-sub.Notify():
			sub.Drain()
			// Debounce: let a burst settle before rebuilding.
			select {
			case <-ctx.Done():
				return
			case <-b.clk.After(routeDebounce):
			}
			b.rebuild()
		case <-tick.C():
			b.rebuild()
		}
	}
}

func (b *RouteBuilder) rebuild() {
	snap := b.build()
	b.mu.Lock()
	b.current = snap
	for _, ch := range b.subs {
		pushLatest(ch, snap)
	}
	b.mu.Unlock()
}

// Current returns the latest snapshot.
func (b *RouteBuilder) Current() *clusterv1.RouteSnapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.current
}

// Subscribe registers a WatchRoutes consumer; Unsubscribe with the returned id.
func (b *RouteBuilder) Subscribe() (int, <-chan *clusterv1.RouteSnapshot) {
	ch := make(chan *clusterv1.RouteSnapshot, 1)
	b.mu.Lock()
	id := b.nextID
	b.nextID++
	b.subs[id] = ch
	b.mu.Unlock()
	return id, ch
}

// Unsubscribe removes a subscriber.
func (b *RouteBuilder) Unsubscribe(id int) {
	b.mu.Lock()
	delete(b.subs, id)
	b.mu.Unlock()
}

// pushLatest delivers snap, dropping any stale queued snapshot (latest wins).
func pushLatest(ch chan *clusterv1.RouteSnapshot, snap *clusterv1.RouteSnapshot) {
	for {
		select {
		case ch <- snap:
			return
		default:
			select {
			case <-ch: // drop the stale one and retry
			default:
				return
			}
		}
	}
}

// build produces the global snapshot from current state.
func (b *RouteBuilder) build() *clusterv1.RouteSnapshot {
	st := b.store.State()
	snap := &clusterv1.RouteSnapshot{Version: st.Version()}
	certHosts := map[string]bool{}

	for _, env := range st.ListEnvironments("", "") {
		app, ok := st.App(env.GetAppId())
		if !ok {
			continue
		}
		envID := env.GetMeta().GetId()
		spec := env.GetService()

		// HTTP routes: explicit domains + the implicit cluster subdomain.
		httpPort := httpPortName(spec)
		for _, dom := range st.ListDomains(env.GetProjectId()) {
			if dom.GetEnvironmentId() != envID {
				continue
			}
			portName := dom.GetPortName()
			if portName == "" {
				portName = httpPort
			}
			snap.HttpRoutes = append(snap.HttpRoutes, &clusterv1.HTTPRoute{
				Hostname:        dom.GetHostname(),
				PathPrefix:      dom.GetPathPrefix(),
				ProjectId:       env.GetProjectId(),
				AppId:           env.GetAppId(),
				EnvironmentId:   envID,
				AppName:         app.GetName(),
				EnvironmentName: env.GetName(),
				Middleware:      dom.GetMiddleware(),
				Endpoints:       b.endpoints(st, env, portName),
				ScaleToZero:     spec.GetScaleToZero(),
				MaxConcurrency:  spec.GetMaxConcurrency(),
				RateLimit:       spec.GetRateLimit(),
				RouteGeneration: env.GetRouteGeneration(),
			})
			certHosts[dom.GetHostname()] = true
		}
		if b.domain != "" {
			host := app.GetName() + "-" + env.GetName() + "." + b.domain
			snap.HttpRoutes = append(snap.HttpRoutes, &clusterv1.HTTPRoute{
				Hostname:        host,
				ProjectId:       env.GetProjectId(),
				AppId:           env.GetAppId(),
				EnvironmentId:   envID,
				AppName:         app.GetName(),
				EnvironmentName: env.GetName(),
				Endpoints:       b.endpoints(st, env, httpPort),
				ScaleToZero:     spec.GetScaleToZero(),
				MaxConcurrency:  spec.GetMaxConcurrency(),
				RateLimit:       spec.GetRateLimit(),
				RouteGeneration: env.GetRouteGeneration(),
			})
			certHosts[host] = true
		}

		// L4 routes: any PortSpec with a public L4 port.
		for _, port := range spec.GetPorts() {
			if port.GetPublicL4Port() == 0 {
				continue
			}
			snap.L4Routes = append(snap.L4Routes, &clusterv1.L4Route{
				PublicPort:    port.GetPublicL4Port(),
				Protocol:      "tcp",
				EnvironmentId: envID,
				Endpoints:     b.endpoints(st, env, port.GetName()),
			})
		}

		// Internal service (VIP) for cross-service calls.
		if vip, ok := st.ServiceVIP(envID); ok {
			var ports []*clusterv1.InternalPort
			for _, port := range spec.GetPorts() {
				ports = append(ports, &clusterv1.InternalPort{
					Name:      port.GetName(),
					Port:      port.GetContainerPort(),
					Protocol:  "tcp",
					Endpoints: b.endpoints(st, env, port.GetName()),
				})
			}
			snap.InternalServices = append(snap.InternalServices, &clusterv1.InternalService{
				Fqdn:          app.GetName() + "." + env.GetName() + "." + projectName(st, env.GetProjectId()) + ".internal.",
				ProjectId:     env.GetProjectId(),
				EnvironmentId: envID,
				Vip:           vip,
				Ports:         ports,
			})
		}
	}

	snap.CertHosts = sortedKeys(certHosts)
	return snap
}

// endpoints returns the HEALTHY endpoints of an environment's ACTIVE release for
// a named port. Only the active release's replicas appear — this is the
// blue/green traffic switch. Addresses are node-reachable (mesh IP + published
// port; 127.0.0.1 single-node).
func (b *RouteBuilder) endpoints(st *state.Store, env *zatterav1.Environment, portName string) []*clusterv1.Endpoint {
	active := env.GetActiveReleaseId()
	if active == "" {
		return nil
	}
	var eps []*clusterv1.Endpoint
	for _, a := range st.ListAssignments(env.GetMeta().GetId()) {
		if a.GetReleaseId() != active || a.GetDesired() != zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN {
			continue
		}
		if a.GetObserved().GetState() != zatterav1.InstanceState_INSTANCE_STATE_HEALTHY {
			continue
		}
		port := a.GetMeshPortBindings()[portName]
		if port == 0 {
			continue
		}
		// Drop endpoints on a node that is gone or DOWN — the proxy could not
		// reach them (the scheduler reschedules the replica separately).
		n, ok := st.Node(a.GetNodeId())
		if !ok || n.GetStatus() == zatterav1.NodeStatus_NODE_STATUS_DOWN || n.GetStatus() == zatterav1.NodeStatus_NODE_STATUS_DRAINED {
			continue
		}
		host := "127.0.0.1"
		if n.GetMeshIp() != "" {
			host = n.GetMeshIp()
		}
		eps = append(eps, &clusterv1.Endpoint{
			AssignmentId: a.GetMeta().GetId(),
			NodeId:       a.GetNodeId(),
			Addr:         host + ":" + strconv.Itoa(int(port)),
			Healthy:      true,
		})
	}
	sort.Slice(eps, func(i, j int) bool { return eps[i].GetAssignmentId() < eps[j].GetAssignmentId() })
	return eps
}

// httpPortName picks the port to route HTTP traffic to: the first HTTP (or
// unspecified) port, else the first declared port, else "http".
func httpPortName(spec *zatterav1.ServiceSpec) string {
	for _, p := range spec.GetPorts() {
		if p.GetProtocol() == zatterav1.Protocol_PROTOCOL_HTTP || p.GetProtocol() == zatterav1.Protocol_PROTOCOL_UNSPECIFIED {
			return p.GetName()
		}
	}
	if len(spec.GetPorts()) > 0 {
		return spec.GetPorts()[0].GetName()
	}
	return "http"
}

func projectName(st *state.Store, projectID string) string {
	if p, ok := st.Project(projectID); ok {
		return p.GetName()
	}
	return projectID
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
