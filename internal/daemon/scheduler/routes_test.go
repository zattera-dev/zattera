package scheduler

import (
	"testing"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/state"
)

func seedRouteEnv(st *state.Store) {
	st.PutProject(&zatterav1.Project{Meta: &zatterav1.Meta{Id: "proj"}, Name: "demo"})
	st.PutApp(&zatterav1.App{Meta: &zatterav1.Meta{Id: "app"}, ProjectId: "proj", Name: "api"})
	spec := &zatterav1.ServiceSpec{Ports: []*zatterav1.PortSpec{{Name: "http", ContainerPort: 8080, Protocol: zatterav1.Protocol_PROTOCOL_HTTP}}}
	st.PutEnvironment(&zatterav1.Environment{
		Meta: &zatterav1.Meta{Id: envID}, ProjectId: "proj", AppId: "app", Name: "production",
		Service: spec, ActiveReleaseId: "relA", RouteGeneration: 1,
	})
	st.PutNode(&zatterav1.Node{Meta: &zatterav1.Meta{Id: "n1"}, Status: zatterav1.NodeStatus_NODE_STATUS_ALIVE, MeshIp: "10.90.0.1"})
}

// healthyAssignment adds a RUN+HEALTHY replica of release rel on node n1.
func healthyAssignment(st *state.Store, id, rel string, port uint32) {
	st.PutAssignment(&zatterav1.Assignment{
		Meta: &zatterav1.Meta{Id: id}, EnvironmentId: envID, NodeId: "n1", ReleaseId: rel,
		Desired:          zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN,
		MeshPortBindings: map[string]uint32{"http": port},
		Observed:         &zatterav1.AssignmentObserved{State: zatterav1.InstanceState_INSTANCE_STATE_HEALTHY},
	})
}

func newBuilder(t *testing.T) (*RouteBuilder, *state.Store) {
	t.Helper()
	rs := raftstore.NewTestStore(t)
	return NewRouteBuilder(rs, clock.NewFake(), "apps.example.com", nil), rs.State()
}

func findRoute(t *testing.T, b *RouteBuilder, host string) *clusterv1.HTTPRoute {
	t.Helper()
	for _, r := range b.build().GetHttpRoutes() {
		if r.GetHostname() == host {
			return r
		}
	}
	return nil
}

func TestRoutesBuild(t *testing.T) {
	b, st := newBuilder(t)
	seedRouteEnv(st)
	healthyAssignment(st, "a1", "relA", 30001)
	st.PutDomain(&zatterav1.Domain{
		Meta: &zatterav1.Meta{Id: "dom1"}, ProjectId: "proj", EnvironmentId: envID, Hostname: "api.example.com",
	})

	snap := b.build()
	if snap.GetVersion() == 0 {
		t.Error("snapshot version should track state version")
	}

	// Explicit domain + implicit cluster subdomain both present.
	custom := findRoute(t, b, "api.example.com")
	sub := findRoute(t, b, "api-production.apps.example.com")
	if custom == nil || sub == nil {
		t.Fatalf("missing routes: custom=%v sub=%v", custom != nil, sub != nil)
	}
	if len(custom.GetEndpoints()) != 1 || custom.GetEndpoints()[0].GetAddr() != "10.90.0.1:30001" {
		t.Fatalf("endpoint addr wrong: %+v", custom.GetEndpoints())
	}
	if custom.GetRouteGeneration() != 1 {
		t.Errorf("route_generation not propagated")
	}
	// cert_hosts covers both hostnames.
	if got := snap.GetCertHosts(); len(got) != 2 {
		t.Errorf("cert_hosts = %v, want 2", got)
	}
}

// TestRoutesRateLimitOnBothRouteKinds pins the reason the rate limit lives on
// ServiceSpec rather than Domain.Middleware: the implicit cluster subdomain is
// built without any Domain, so domain-level config would leave it unprotected
// even though it is internet-exposed (T-107).
func TestRoutesRateLimitOnBothRouteKinds(t *testing.T) {
	b, st := newBuilder(t)
	seedRouteEnv(st)
	healthyAssignment(st, "a1", "relA", 30001)
	st.PutDomain(&zatterav1.Domain{
		Meta: &zatterav1.Meta{Id: "dom1"}, ProjectId: "proj", EnvironmentId: envID, Hostname: "api.example.com",
	})

	env, _ := st.Environment(envID)
	env.GetService().RateLimit = &zatterav1.RateLimit{RequestsPerSecond: 25, Burst: 50}
	st.PutEnvironment(env)

	for _, host := range []string{"api.example.com", "api-production.apps.example.com"} {
		rt := findRoute(t, b, host)
		if rt == nil {
			t.Fatalf("route %q missing", host)
		}
		if rt.GetRateLimit().GetRequestsPerSecond() != 25 || rt.GetRateLimit().GetBurst() != 50 {
			t.Errorf("route %q rate limit = %v, want 25/50", host, rt.GetRateLimit())
		}
	}
}

func TestRoutesPromoteSwapsEndpoints(t *testing.T) {
	b, st := newBuilder(t)
	seedRouteEnv(st)
	healthyAssignment(st, "a1", "relA", 30001)
	healthyAssignment(st, "a2", "relB", 30002) // green, not yet active

	// Before promotion only relA (active) endpoints appear.
	sub := findRoute(t, b, "api-production.apps.example.com")
	if len(sub.GetEndpoints()) != 1 || sub.GetEndpoints()[0].GetAddr() != "10.90.0.1:30001" {
		t.Fatalf("pre-promote endpoints = %+v", sub.GetEndpoints())
	}

	// Promote relB: active_release_id flips → endpoints swap atomically.
	env, _ := st.Environment(envID)
	env.ActiveReleaseId = "relB"
	st.PutEnvironment(env)

	sub = findRoute(t, b, "api-production.apps.example.com")
	if len(sub.GetEndpoints()) != 1 || sub.GetEndpoints()[0].GetAddr() != "10.90.0.1:30002" {
		t.Fatalf("post-promote endpoints = %+v", sub.GetEndpoints())
	}
}

func TestRoutesUnhealthyDropped(t *testing.T) {
	b, st := newBuilder(t)
	seedRouteEnv(st)
	healthyAssignment(st, "a1", "relA", 30001)
	// A second replica that never became HEALTHY.
	st.PutAssignment(&zatterav1.Assignment{
		Meta: &zatterav1.Meta{Id: "a2"}, EnvironmentId: envID, NodeId: "n1", ReleaseId: "relA",
		Desired:          zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN,
		MeshPortBindings: map[string]uint32{"http": 30002},
		Observed:         &zatterav1.AssignmentObserved{State: zatterav1.InstanceState_INSTANCE_STATE_RUNNING},
	})
	sub := findRoute(t, b, "api-production.apps.example.com")
	if len(sub.GetEndpoints()) != 1 {
		t.Fatalf("unhealthy endpoint should be dropped, got %+v", sub.GetEndpoints())
	}
}

func TestRoutesNodeDownDropped(t *testing.T) {
	b, st := newBuilder(t)
	seedRouteEnv(st)
	healthyAssignment(st, "a1", "relA", 30001)

	n, _ := st.Node("n1")
	n.Status = zatterav1.NodeStatus_NODE_STATUS_DOWN
	st.PutNode(n)

	sub := findRoute(t, b, "api-production.apps.example.com")
	if len(sub.GetEndpoints()) != 0 {
		t.Fatalf("endpoints on a DOWN node should be dropped, got %+v", sub.GetEndpoints())
	}
}

func TestRoutesL4AndInternal(t *testing.T) {
	b, st := newBuilder(t)
	seedRouteEnv(st)
	// Add an L4 public port and a service VIP.
	env, _ := st.Environment(envID)
	env.GetService().Ports = append(env.GetService().GetPorts(), &zatterav1.PortSpec{Name: "db", ContainerPort: 5432, Protocol: zatterav1.Protocol_PROTOCOL_TCP, PublicL4Port: 15432})
	st.PutEnvironment(env)
	st.PutAssignment(&zatterav1.Assignment{
		Meta: &zatterav1.Meta{Id: "a1"}, EnvironmentId: envID, NodeId: "n1", ReleaseId: "relA",
		Desired:          zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN,
		MeshPortBindings: map[string]uint32{"http": 30001, "db": 30005},
		Observed:         &zatterav1.AssignmentObserved{State: zatterav1.InstanceState_INSTANCE_STATE_HEALTHY},
	})
	st.SetServiceVIP(envID, "10.97.0.5")

	snap := b.build()
	if len(snap.GetL4Routes()) != 1 || snap.GetL4Routes()[0].GetPublicPort() != 15432 {
		t.Fatalf("L4 route wrong: %+v", snap.GetL4Routes())
	}
	if len(snap.GetL4Routes()[0].GetEndpoints()) != 1 || snap.GetL4Routes()[0].GetEndpoints()[0].GetAddr() != "10.90.0.1:30005" {
		t.Fatalf("L4 endpoint wrong: %+v", snap.GetL4Routes()[0].GetEndpoints())
	}
	if len(snap.GetInternalServices()) != 1 || snap.GetInternalServices()[0].GetVip() != "10.97.0.5" {
		t.Fatalf("internal service wrong: %+v", snap.GetInternalServices())
	}
	if fqdn := snap.GetInternalServices()[0].GetFqdn(); fqdn != "api.production.demo.internal." {
		t.Errorf("fqdn = %q", fqdn)
	}
}
