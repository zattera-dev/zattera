// Package simcluster runs an in-process multi-node Zattera control plane for
// unit and chaos tests: real hashicorp/raft with in-memory transports, real
// FSM/state, fake container runtime, fake clock. No Docker, no network.
//
// It is the backbone of scheduler, HA and failover verification: kill the
// leader, partition nodes, advance time, assert convergence.
package simcluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/state"
	"github.com/zattera-dev/zattera/internal/testutil/fakeruntime"
)

// Node is one simulated control(+worker) node.
type Node struct {
	ID        string
	Store     *raftstore.Store
	State     *state.Store
	Runtime   *fakeruntime.Fake
	Transport *raft.InmemTransport
	killed    bool
}

// Cluster is a set of interconnected nodes sharing one fake clock.
type Cluster struct {
	T     *testing.T
	Nodes []*Node
	Clock *clock.Fake
}

// New builds and boots an n-node cluster and waits for a leader.
func New(t *testing.T, n int) *Cluster {
	t.Helper()
	c := &Cluster{T: t, Clock: clock.NewFake()}

	// Create transports first and fully connect them.
	addrs := make([]raft.ServerAddress, n)
	transports := make([]*raft.InmemTransport, n)
	servers := make([]raft.Server, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("sim-%d", i+1)
		addr, tr := raft.NewInmemTransport(raft.ServerAddress(id))
		addrs[i], transports[i] = addr, tr
		servers[i] = raft.Server{ID: raft.ServerID(id), Address: addr}
	}
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				transports[i].Connect(addrs[j], transports[j])
			}
		}
	}

	for i := 0; i < n; i++ {
		id := string(servers[i].ID)
		st := state.New()
		rs, err := raftstore.New(raftstore.Config{
			NodeID:           id,
			Inmem:            true,
			Bootstrap:        true,
			BootstrapServers: servers,
			Transport:        transports[i],
		}, st)
		if err != nil {
			t.Fatalf("simcluster: node %s: %v", id, err)
		}
		node := &Node{ID: id, Store: rs, State: st, Runtime: fakeruntime.New(), Transport: transports[i]}
		c.Nodes = append(c.Nodes, node)
	}
	t.Cleanup(c.Shutdown)
	c.WaitLeader(10 * time.Second)
	return c
}

// Shutdown stops every live node.
func (c *Cluster) Shutdown() {
	for _, n := range c.Nodes {
		if !n.killed {
			_ = n.Store.Shutdown()
			n.killed = true
		}
	}
}

// Leader returns the current leader node, or nil.
func (c *Cluster) Leader() *Node {
	for _, n := range c.Nodes {
		if !n.killed && n.Store.IsLeader() {
			return n
		}
	}
	return nil
}

// WaitLeader blocks until some live node is leader.
func (c *Cluster) WaitLeader(timeout time.Duration) *Node {
	c.T.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if l := c.Leader(); l != nil {
			return l
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.T.Fatal("simcluster: no leader elected")
	return nil
}

// Apply proposes a command on the current leader, retrying across elections
// until timeout. Business errors are returned immediately.
func (c *Cluster) Apply(cmd *clusterv1.Command) error {
	c.T.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		leader := c.Leader()
		if leader != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err := leader.Store.Apply(ctx, cmd)
			cancel()
			if err == nil || err != raftstore.ErrNotLeader {
				return err
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("simcluster: apply timed out without a stable leader")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Kill shuts a node down abruptly (no dcommission), simulating a crash.
func (c *Cluster) Kill(id string) {
	c.T.Helper()
	for _, n := range c.Nodes {
		if n.ID == id && !n.killed {
			// Disconnect first so peers see it vanish, then shut down.
			n.Transport.DisconnectAll()
			_ = n.Store.Shutdown()
			n.killed = true
			return
		}
	}
	c.T.Fatalf("simcluster: unknown or already killed node %s", id)
}

// KillLeader crashes the current leader and returns its id.
func (c *Cluster) KillLeader() string {
	c.T.Helper()
	l := c.Leader()
	if l == nil {
		c.T.Fatal("simcluster: no leader to kill")
	}
	c.Kill(l.ID)
	return l.ID
}

// Partition splits the cluster into isolated groups of node ids. Nodes within
// a group stay connected; links across groups are severed.
func (c *Cluster) Partition(groups ...[]string) {
	c.T.Helper()
	group := map[string]int{}
	for gi, g := range groups {
		for _, id := range g {
			group[id] = gi
		}
	}
	for _, a := range c.Nodes {
		a.Transport.DisconnectAll()
	}
	for _, a := range c.Nodes {
		for _, b := range c.Nodes {
			if a.ID == b.ID || a.killed || b.killed {
				continue
			}
			if group[a.ID] == group[b.ID] {
				a.Transport.Connect(raft.ServerAddress(b.ID), b.Transport)
			}
		}
	}
}

// Heal reconnects every live node to every other.
func (c *Cluster) Heal() {
	var ids []string
	for _, n := range c.Nodes {
		if !n.killed {
			ids = append(ids, n.ID)
		}
	}
	c.Partition(ids)
}
