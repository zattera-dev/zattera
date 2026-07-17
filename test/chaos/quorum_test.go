//go:build chaos

package chaos

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/proxy"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/state"
	"github.com/zattera-dev/zattera/internal/testutil/simcluster"
)

// TestQuorum exercises the cluster's behavior when it loses its raft majority
// (T-68): the control plane must stop accepting writes, yet the data plane
// (proxies serving cached routes, agents holding containers) keeps running, and
// once quorum returns the cluster resumes writing and reconciling. It also
// verifies a write issued around a leader kill survives the handover.
func TestQuorum(t *testing.T) {
	t.Run("writes_blocked_dataplane_survives_on_quorum_loss", testQuorumLossDataPlaneSurvives)
	t.Run("reconciles_after_quorum_restored", testQuorumRestoredReconciles)
	t.Run("envvar_write_and_deploy_survive_leader_kill", testWriteSurvivesFailover)
}

// testQuorumLossDataPlaneSurvives kills two of three controls: writes must fail,
// but the last RouteSnapshot is still served from the proxy's on-disk cache and
// the running assignments (containers) are untouched in surviving state.
func testQuorumLossDataPlaneSurvives(t *testing.T) {
	h := New(t)

	// A proxy has been serving routes: persist the current snapshot to its disk
	// cache via the real RouteClient path, exactly as a live proxy would.
	cachePath := filepath.Join(t.TempDir(), "proxy", "routes.pb")
	snap := sampleSnapshot()
	primeRouteCache(t, cachePath, snap)

	// Record the running assignments before the outage.
	before := runAssignmentIDs(h.leaderState())
	if len(before) == 0 {
		t.Fatal("expected running assignments before quorum loss")
	}

	// Kill two nodes → only one remains, which cannot hold a majority.
	dead := map[string]bool{}
	dead[h.C.KillLeader()] = true          // first leader
	survivorWaitStepdown(t, h)             // let the remaining two re-elect
	dead[h.C.KillLeader()] = true          // second leader → 1 node left
	survivor := onlyLiveNode(t, h.C, dead) // the lone survivor

	// Writes must fail: with no majority the survivor is not (or ceases to be)
	// leader, so an apply is refused rather than silently lost.
	waitNoLeader(t, h, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := survivor.Store.Apply(ctx, withMeta(putNode("w-new"))); !errors.Is(err, raftstore.ErrNotLeader) {
		t.Fatalf("write during quorum loss should be refused with ErrNotLeader, got %v", err)
	}

	// Data plane survives: the survivor's committed assignments are intact (the
	// agent has nothing to reap them; containers keep running).
	after := runAssignmentIDs(survivor.State)
	if !sameSet(before, after) {
		t.Fatalf("running assignments changed during quorum loss: before=%v after=%v", before, after)
	}

	// A proxy restarting with control unreachable still serves the cached routes:
	// a fresh RouteClient loads the last snapshot from disk.
	reloaded := proxy.NewRouteClient(deadDialer{}, "proxy-1", cachePath, nil)
	got := reloaded.Current()
	if got.GetVersion() != snap.GetVersion() || len(got.GetHttpRoutes()) != len(snap.GetHttpRoutes()) {
		t.Fatalf("cached routes not served after quorum loss: got version=%d routes=%d", got.GetVersion(), len(got.GetHttpRoutes()))
	}
	if len(got.GetHttpRoutes()) == 0 || got.GetHttpRoutes()[0].GetHostname() != snap.GetHttpRoutes()[0].GetHostname() {
		t.Fatal("cached route contents lost")
	}
}

// testQuorumRestoredReconciles isolates every node (quorum lost, restorable),
// then heals and asserts the cluster accepts writes and the scheduler reconciles
// a pending scale-up. Partition/heal models a restorable outage the in-process
// harness cannot get from a permanent Kill.
func testQuorumRestoredReconciles(t *testing.T) {
	h := New(t)
	waitRunCount(t, h, 2, 10*time.Second) // steady state: 2 blue replicas

	// Full split — no group holds a majority, so no leader can exist.
	h.C.Partition([]string{"sim-1"}, []string{"sim-2"}, []string{"sim-3"})
	waitNoLeader(t, h, 10*time.Second)
	for _, n := range h.C.Nodes {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := n.Store.Apply(ctx, withMeta(putNode("w-x")))
		cancel()
		if !errors.Is(err, raftstore.ErrNotLeader) {
			t.Fatalf("node %s accepted a write without quorum: %v", n.ID, err)
		}
	}

	// Heal → a leader returns and writes are accepted again.
	h.C.Heal()
	h.C.WaitLeader(10 * time.Second)

	// A scale-up requested after recovery is reconciled by the scheduler.
	env, _ := h.leaderState().Environment(cEnvID)
	env.EffectiveReplicas = 3
	if err := h.C.Apply(withMeta(&clusterv1.Command{Mutation: &clusterv1.Command_PutEnvironment{PutEnvironment: &clusterv1.PutEnvironment{Environment: env}}})); err != nil {
		t.Fatalf("scale-up apply after heal: %v", err)
	}
	waitRunCount(t, h, 3, 15*time.Second)
}

// testWriteSurvivesFailover kills the leader while a deploy is in flight and,
// in the same window, applies an env-var change; after the handover the deploy
// converges consistently and the env-var write is durable.
func testWriteSurvivesFailover(t *testing.T) {
	h := New(t)
	h.allowHealthy() // green will be able to promote
	depID := h.Deploy(t)
	h.waitPhaseAtLeast(t, depID, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_STARTING, 10*time.Second)

	// Kill the leader mid-deploy, then immediately write an env-var change on the
	// new leader (retried across the election by cluster.Apply).
	h.C.KillLeader()
	h.C.WaitLeader(10 * time.Second)
	if err := h.C.Apply(withMeta(&clusterv1.Command{Mutation: &clusterv1.Command_SetEnvVars{SetEnvVars: &clusterv1.SetEnvVars{
		EnvironmentId: cEnvID,
		Set:           map[string]*zatterav1.EncryptedValue{"FEATURE_FLAG": {Ciphertext: []byte("on")}},
	}}})); err != nil {
		t.Fatalf("env-var write during failover: %v", err)
	}

	// The deploy still converges to a consistent terminal state.
	h.waitPhase(t, depID, zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_SUCCEEDED, 20*time.Second)
	h.checkDeploymentConsistent(t, depID)
	h.checkInvariants(t)

	// The env-var write survived the handover.
	vars := h.leaderState().EnvVars(cEnvID)
	if _, ok := vars["FEATURE_FLAG"]; !ok {
		t.Fatal("env-var change issued during failover was lost")
	}
}

// --- helpers --------------------------------------------------------------

// sampleSnapshot is a representative non-empty RouteSnapshot a proxy would cache.
func sampleSnapshot() *clusterv1.RouteSnapshot {
	return &clusterv1.RouteSnapshot{
		Version: 42,
		HttpRoutes: []*clusterv1.HTTPRoute{{
			Hostname:      "app.example.com",
			EnvironmentId: cEnvID,
			Endpoints: []*clusterv1.Endpoint{
				{AssignmentId: "blue-1", NodeId: "w1", Addr: "10.0.0.1:8080", Healthy: true},
			},
		}},
	}
}

// primeRouteCache writes snap to path through the real RouteClient persist path
// (fake dialer streams it once), so the on-disk cache matches production layout.
func primeRouteCache(t *testing.T, path string, snap *clusterv1.RouteSnapshot) {
	t.Helper()
	rc := proxy.NewRouteClient(oneShotDialer{snap: snap}, "proxy-1", path, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rc.Run(ctx)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if rc.Current().GetVersion() == snap.GetVersion() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("route cache not primed")
}

// oneShotDialer yields exactly one snapshot then blocks until canceled.
type oneShotDialer struct{ snap *clusterv1.RouteSnapshot }

func (d oneShotDialer) WatchRoutes(_ context.Context, _ uint64) (proxy.RouteStream, error) {
	return &oneShotStream{snap: d.snap, ch: make(chan struct{})}, nil
}

type oneShotStream struct {
	snap *clusterv1.RouteSnapshot
	sent bool
	ch   chan struct{}
}

func (s *oneShotStream) Recv() (*clusterv1.RouteSnapshot, error) {
	if !s.sent {
		s.sent = true
		return s.snap, nil
	}
	<-s.ch // block forever (control still "up" but idle)
	return nil, context.Canceled
}

// deadDialer always fails to connect: the control plane is unreachable.
type deadDialer struct{}

func (deadDialer) WatchRoutes(_ context.Context, _ uint64) (proxy.RouteStream, error) {
	return nil, context.DeadlineExceeded
}

// runAssignmentIDs returns the set of RUN assignment ids in st (nil-safe).
func runAssignmentIDs(st *state.Store) map[string]bool {
	out := map[string]bool{}
	if st == nil {
		return out
	}
	for _, a := range st.ListAssignments("") {
		if a.GetDesired() == zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN {
			out[a.GetMeta().GetId()] = true
		}
	}
	return out
}

func sameSet(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

// waitRunCount blocks until the env has exactly n RUN assignments.
func waitRunCount(t *testing.T, h *Harness, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		st := h.leaderState()
		if st != nil {
			c := 0
			for _, a := range st.ListAssignments(cEnvID) {
				if a.GetDesired() == zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN {
					c++
				}
			}
			if c == n {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %d RUN assignments", n)
}

// waitNoLeader blocks until the cluster has no leader (quorum lost).
func waitNoLeader(t *testing.T, h *Harness, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if h.C.Leader() == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("a leader still exists; expected quorum loss")
}

// survivorWaitStepdown waits until a leader exists again among the survivors
// after the first kill (so the second kill targets a real leader).
func survivorWaitStepdown(t *testing.T, h *Harness) {
	t.Helper()
	h.C.WaitLeader(10 * time.Second)
}

// onlyLiveNode returns the single node not in dead.
func onlyLiveNode(t *testing.T, c *simcluster.Cluster, dead map[string]bool) *simcluster.Node {
	t.Helper()
	var live *simcluster.Node
	for _, n := range c.Nodes {
		if !dead[n.ID] {
			if live != nil {
				t.Fatal("expected exactly one live node")
			}
			live = n
		}
	}
	if live == nil {
		t.Fatal("no live node found")
	}
	return live
}
