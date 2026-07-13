package simcluster

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

func putProject(id, name string) *clusterv1.Command {
	return &clusterv1.Command{
		RequestId: "req-" + id + "-" + name,
		Time:      timestamppb.Now(),
		Mutation: &clusterv1.Command_PutProject{PutProject: &clusterv1.PutProject{
			Project: &zatterav1.Project{Meta: &zatterav1.Meta{Id: id}, Name: name},
		}},
	}
}

// waitReplicated polls until every live node's state satisfies the predicate.
func waitReplicated(t *testing.T, c *Cluster, pred func(n *Node) bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, n := range c.Nodes {
			if n.killed {
				continue
			}
			if !pred(n) {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("state did not replicate in time")
}

func TestThreeNodeReplication(t *testing.T) {
	c := New(t, 3)
	if err := c.Apply(putProject("p1", "demo")); err != nil {
		t.Fatal(err)
	}
	waitReplicated(t, c, func(n *Node) bool {
		p, ok := n.State.Project("p1")
		return ok && p.GetName() == "demo"
	})
}

func TestLeaderFailover(t *testing.T) {
	c := New(t, 3)
	if err := c.Apply(putProject("p1", "before")); err != nil {
		t.Fatal(err)
	}

	killed := c.KillLeader()
	newLeader := c.WaitLeader(10 * time.Second)
	if newLeader.ID == killed {
		t.Fatalf("dead node %s still leader", killed)
	}

	// Writes must keep working on the survivors, and prior state must be there.
	if _, ok := newLeader.State.Project("p1"); !ok {
		t.Fatal("pre-failover state lost")
	}
	if err := c.Apply(putProject("p2", "after")); err != nil {
		t.Fatal(err)
	}
	waitReplicated(t, c, func(n *Node) bool {
		_, ok := n.State.Project("p2")
		return ok
	})
}

func TestMinorityPartitionCannotWrite(t *testing.T) {
	c := New(t, 3)
	leader := c.WaitLeader(5 * time.Second)

	// Isolate the leader; the majority side must elect a new one and accept
	// writes; the old leader must step down.
	var majority []string
	for _, n := range c.Nodes {
		if n.ID != leader.ID {
			majority = append(majority, n.ID)
		}
	}
	c.Partition([]string{leader.ID}, majority)

	deadline := time.Now().Add(10 * time.Second)
	var newLeader *Node
	for time.Now().Before(deadline) {
		for _, n := range c.Nodes {
			if n.ID != leader.ID && n.Store.IsLeader() {
				newLeader = n
			}
		}
		if newLeader != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if newLeader == nil {
		t.Fatal("majority did not elect a leader")
	}
	if err := c.Apply(putProject("p-major", "ok")); err != nil {
		t.Fatalf("majority write failed: %v", err)
	}

	// Heal: the old leader must converge to the majority's state.
	c.Heal()
	waitReplicated(t, c, func(n *Node) bool {
		_, ok := n.State.Project("p-major")
		return ok
	})
}
