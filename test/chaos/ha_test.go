//go:build chaos

package chaos

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/daemon/ca"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// TestHA exercises the multi-control-node raft core (T-55) over the real mTLS
// TCP transport on loopback ports: growing the quorum with AddVoter, surviving a
// leader kill with continued writes (the in-process stand-in for leader-forward
// — writes always go to whoever currently leads), and removing a follower
// cleanly.
func TestHA(t *testing.T) {
	t.Run("grow: AddVoter enrolls new control nodes over mTLS", testHAGrow)
	t.Run("failover: kill leader, re-elect, writes keep working", testHAFailover)
	t.Run("remove: a follower leaves the quorum cleanly", testHARemoveFollower)
}

// haNode is one in-process control node on a real TLS raft transport.
type haNode struct {
	id    string
	addr  string
	store *raftstore.Store
	trans raft.Transport
}

func (n *haNode) kill() {
	_ = n.store.Shutdown()
	if c, ok := n.trans.(io.Closer); ok {
		_ = c.Close()
	}
}

// newHANode builds one control node. When servers is nil the node starts empty
// (Bootstrap=false) and waits to be AddVoter'd; otherwise it bootstraps that set.
func newHANode(t *testing.T, authority *ca.CA, id, addr string, servers []raft.Server) *haNode {
	t.Helper()
	leaf, err := authority.IssueNode(id, net.ParseIP("127.0.0.1"), ca.NodeCertTTL)
	if err != nil {
		t.Fatalf("issue node cert: %v", err)
	}
	cert, err := leaf.TLSCertificate(authority.CABundlePEM())
	if err != nil {
		t.Fatalf("tls cert: %v", err)
	}
	tr, err := raftstore.NewTLSTransport(addr, addr, cert, authority.Pool(), io.Discard)
	if err != nil {
		t.Fatalf("tls transport %s: %v", id, err)
	}
	st, err := raftstore.New(raftstore.Config{
		NodeID:           id,
		Inmem:            true,
		Bootstrap:        len(servers) > 0,
		BootstrapServers: servers,
		Transport:        tr,
	}, state.New())
	if err != nil {
		t.Fatalf("raftstore.New %s: %v", id, err)
	}
	n := &haNode{id: id, addr: addr, store: st, trans: tr}
	t.Cleanup(n.kill)
	return n
}

// freePort returns a free loopback address.
func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// leaderOf returns the current leader among the live nodes, retrying across
// elections until timeout.
func leaderOf(t *testing.T, timeout time.Duration, nodes ...*haNode) *haNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.store.IsLeader() {
				return n
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader elected in time")
	return nil
}

// putKV applies a key/value on the given leader.
func putKV(t *testing.T, leader *haNode, k, v string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := leader.store.Apply(ctx, &clusterv1.Command{
		RequestId: ids.New(), Actor: "chaos-ha", Time: timestamppb.Now(),
		Mutation: &clusterv1.Command_PutKv{PutKv: &clusterv1.PutKV{Key: k, Value: []byte(v), ExpectedVersion: -1}},
	}); err != nil {
		t.Fatalf("apply %s=%s on leader %s: %v", k, v, leader.id, err)
	}
}

// waitKV blocks until node observes k=v (replication), or fails.
func waitKV(t *testing.T, node *haNode, k, v string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got, _, _, ok := node.store.State().KV(k); ok && string(got) == v {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("node %s never observed %s=%s", node.id, k, v)
}

// configSize returns the number of servers in the leader's committed config.
func configSize(t *testing.T, leader *haNode) int {
	t.Helper()
	f := leader.store.Raft().GetConfiguration()
	if err := f.Error(); err != nil {
		t.Fatalf("get configuration: %v", err)
	}
	return len(f.Configuration().Servers)
}

// --- subtests --------------------------------------------------------------

func testHAGrow(t *testing.T) {
	authority, err := ca.LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatalf("ca: %v", err)
	}
	a1, a2, a3 := freePort(t), freePort(t), freePort(t)

	// n1 bootstraps alone; n2/n3 come up empty and are enrolled via AddVoter.
	n1 := newHANode(t, authority, "n1", a1, []raft.Server{{ID: "n1", Address: raft.ServerAddress(a1)}})
	n2 := newHANode(t, authority, "n2", a2, nil)
	n3 := newHANode(t, authority, "n3", a3, nil)

	leader := leaderOf(t, 10*time.Second, n1)
	if err := leader.store.AddVoter("n2", a2); err != nil {
		t.Fatalf("AddVoter n2: %v", err)
	}
	if err := leader.store.AddVoter("n3", a3); err != nil {
		t.Fatalf("AddVoter n3: %v", err)
	}
	if got := configSize(t, leader); got != 3 {
		t.Fatalf("config size after growth = %d, want 3", got)
	}
	// AddVoter is idempotent: re-adding an existing voter at the same addr is a
	// no-op, not a duplicate.
	if err := leader.store.AddVoter("n2", a2); err != nil {
		t.Fatalf("idempotent AddVoter n2: %v", err)
	}
	if got := configSize(t, leader); got != 3 {
		t.Fatalf("config size after idempotent re-add = %d, want 3", got)
	}

	// A write on the leader replicates to the enrolled followers.
	putKV(t, leader, "grow", "ok")
	waitKV(t, n2, "grow", "ok")
	waitKV(t, n3, "grow", "ok")
}

func testHAFailover(t *testing.T) {
	authority, err := ca.LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatalf("ca: %v", err)
	}
	a1, a2, a3 := freePort(t), freePort(t), freePort(t)
	servers := []raft.Server{
		{ID: "n1", Address: raft.ServerAddress(a1)},
		{ID: "n2", Address: raft.ServerAddress(a2)},
		{ID: "n3", Address: raft.ServerAddress(a3)},
	}
	n1 := newHANode(t, authority, "n1", a1, servers)
	n2 := newHANode(t, authority, "n2", a2, servers)
	n3 := newHANode(t, authority, "n3", a3, servers)
	all := []*haNode{n1, n2, n3}

	leader := leaderOf(t, 10*time.Second, all...)
	putKV(t, leader, "before", "1")

	// Kill the leader. The two survivors must elect a new one and accept writes
	// (a client would leader-forward to it; here we just apply on the new leader).
	leader.kill()
	survivors := make([]*haNode, 0, 2)
	for _, n := range all {
		if n != leader {
			survivors = append(survivors, n)
		}
	}
	newLeader := leaderOf(t, 10*time.Second, survivors...)
	if newLeader == leader {
		t.Fatal("killed node is still reported leader")
	}
	putKV(t, newLeader, "after", "2")

	// The other survivor observes the post-failover write.
	for _, n := range survivors {
		if n != newLeader {
			waitKV(t, n, "after", "2")
		}
	}
}

func testHARemoveFollower(t *testing.T) {
	authority, err := ca.LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatalf("ca: %v", err)
	}
	a1, a2, a3 := freePort(t), freePort(t), freePort(t)
	servers := []raft.Server{
		{ID: "n1", Address: raft.ServerAddress(a1)},
		{ID: "n2", Address: raft.ServerAddress(a2)},
		{ID: "n3", Address: raft.ServerAddress(a3)},
	}
	n1 := newHANode(t, authority, "n1", a1, servers)
	n2 := newHANode(t, authority, "n2", a2, servers)
	n3 := newHANode(t, authority, "n3", a3, servers)
	all := []*haNode{n1, n2, n3}

	leader := leaderOf(t, 10*time.Second, all...)
	// Pick a follower to remove.
	var follower *haNode
	for _, n := range all {
		if n != leader {
			follower = n
			break
		}
	}
	if err := leader.store.RemoveServer(follower.id); err != nil {
		t.Fatalf("RemoveServer %s: %v", follower.id, err)
	}
	if got := configSize(t, leader); got != 2 {
		t.Fatalf("config size after removal = %d, want 2", got)
	}
	// Idempotent: removing an already-absent server is a no-op.
	if err := leader.store.RemoveServer(follower.id); err != nil {
		t.Fatalf("idempotent RemoveServer: %v", err)
	}
	// The two-node quorum still accepts writes and replicates between them.
	putKV(t, leader, "post-remove", "3")
	for _, n := range all {
		if n != follower && n != leader {
			waitKV(t, n, "post-remove", "3")
		}
	}
}
