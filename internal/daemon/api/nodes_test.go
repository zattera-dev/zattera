package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/ca"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

func putNodeCmd(n *zatterav1.Node) *clusterv1.Command {
	return &clusterv1.Command{Mutation: &clusterv1.Command_PutNode{PutNode: &clusterv1.PutNode{Node: n}}}
}

func newNodeServer(t *testing.T) (*NodeServer, *raftstore.Store, *ca.CA) {
	t.Helper()
	rs := raftstore.NewTestStore(t)
	authority, err := ca.LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return NewNodeServer(rs.State(), rs, clock.Real{}, authority), rs, authority
}

func TestNodesJoinTokenRoundTrip(t *testing.T) {
	ns, _, authority := newNodeServer(t)
	ctx := withIdentity(context.Background(), Identity{UserID: "u1", OrgRole: zatterav1.Role_ROLE_OWNER})

	resp, err := ns.CreateJoinToken(ctx, &zatterav1.CreateJoinTokenRequest{SingleUse: true})
	if err != nil {
		t.Fatalf("create join token: %v", err)
	}
	tok := resp.GetToken()

	// Format: K10<ca-hash-hex>::<secret>.
	if !strings.HasPrefix(tok, joinTokenPrefix) {
		t.Fatalf("token missing prefix: %q", tok)
	}
	body := strings.TrimPrefix(tok, joinTokenPrefix)
	caHash, secret, ok := strings.Cut(body, joinTokenSep)
	if !ok {
		t.Fatalf("token missing separator: %q", tok)
	}
	// CA hash matches sha256(cert.Raw).
	want := sha256.Sum256(authority.Certificate().Raw)
	if caHash != hex.EncodeToString(want[:]) {
		t.Errorf("ca hash mismatch")
	}
	// The stored token's SecretHash matches sha256(secret).
	stored := findJoinToken(t, ns, resp.GetInfo().GetMeta().GetId())
	if stored.GetSecretHash() != HashToken(secret) {
		t.Errorf("stored hash does not match secret")
	}
	if !stored.GetSingleUse() {
		t.Errorf("single_use not persisted")
	}
	// The response must not leak the hash.
	if resp.GetInfo().GetSecretHash() != "" {
		t.Errorf("response leaked secret hash")
	}
	// Default role is worker.
	if r := stored.GetRoles(); len(r) != 1 || r[0] != zatterav1.NodeRole_NODE_ROLE_WORKER {
		t.Errorf("default roles = %v, want [WORKER]", r)
	}
}

func findJoinToken(t *testing.T, ns *NodeServer, id string) *zatterav1.JoinToken {
	t.Helper()
	for _, jt := range ns.store.ListJoinTokens() {
		if jt.GetMeta().GetId() == id {
			return jt
		}
	}
	t.Fatalf("join token %s not found", id)
	return nil
}

func TestNodesListAndGet(t *testing.T) {
	ns, rs, _ := newNodeServer(t)
	// Register a node directly.
	mustApply(t, rs, putNodeCmd(&zatterav1.Node{
		Meta:   metaID("node-a"),
		Name:   "alpha",
		Roles:  []zatterav1.NodeRole{zatterav1.NodeRole_NODE_ROLE_CONTROL},
		Status: zatterav1.NodeStatus_NODE_STATUS_ALIVE,
	}))

	list, err := ns.ListNodes(context.Background(), &emptypb.Empty{})
	if err != nil || len(list.GetNodes()) != 1 {
		t.Fatalf("list nodes: %d %v", len(list.GetNodes()), err)
	}
	got, err := ns.GetNode(context.Background(), &zatterav1.GetNodeRequest{NodeId: "node-a"})
	if err != nil || got.GetName() != "alpha" {
		t.Fatalf("get node: %+v %v", got, err)
	}
	if _, err := ns.GetNode(context.Background(), &zatterav1.GetNodeRequest{NodeId: "missing"}); status.Code(err) != codes.NotFound {
		t.Errorf("missing node code = %v, want NotFound", status.Code(err))
	}
}

func TestNodesSetLabels(t *testing.T) {
	ns, rs, _ := newNodeServer(t)
	mustApply(t, rs, putNodeCmd(&zatterav1.Node{Meta: metaID("node-a"), Name: "alpha", Schedulable: true}))
	ctx := withIdentity(context.Background(), Identity{UserID: "u1", OrgRole: zatterav1.Role_ROLE_ADMIN})

	n, err := ns.SetNodeLabels(ctx, &zatterav1.SetNodeLabelsRequest{
		NodeId: "node-a", Labels: map[string]string{"region": "eu"}, Schedulable: false,
	})
	if err != nil {
		t.Fatalf("set labels: %v", err)
	}
	if n.GetLabels()["region"] != "eu" || n.GetSchedulable() {
		t.Errorf("labels/schedulable not applied: %+v", n)
	}
}

func TestNodesDrainRemove(t *testing.T) {
	ns, rs, _ := newNodeServer(t)
	st := rs.State()
	ctx := context.Background()
	st.PutNode(&zatterav1.Node{
		Meta: &zatterav1.Meta{Id: "w1"}, Name: "worker-1",
		Roles:  []zatterav1.NodeRole{zatterav1.NodeRole_NODE_ROLE_WORKER},
		Status: zatterav1.NodeStatus_NODE_STATUS_ALIVE, Schedulable: true,
	})

	// Drain marks it DRAINING + unschedulable.
	n, err := ns.DrainNode(ctx, &zatterav1.DrainNodeRequest{NodeId: "w1"})
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if n.GetStatus() != zatterav1.NodeStatus_NODE_STATUS_DRAINING || n.GetSchedulable() {
		t.Fatalf("drain should set DRAINING + unschedulable, got %+v", n)
	}

	// Remove refuses until DRAINED (no force).
	if _, err := ns.RemoveNode(ctx, &zatterav1.RemoveNodeRequest{NodeId: "w1"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("remove of non-drained node should be FailedPrecondition, got %v", err)
	}

	// Once DRAINED, remove deletes the node.
	drained, _ := st.Node("w1")
	drained.Status = zatterav1.NodeStatus_NODE_STATUS_DRAINED
	st.PutNode(drained)
	if _, err := ns.RemoveNode(ctx, &zatterav1.RemoveNodeRequest{NodeId: "w1"}); err != nil {
		t.Fatalf("remove drained node: %v", err)
	}
	if _, ok := st.Node("w1"); ok {
		t.Fatal("node should be deleted after remove")
	}

	// Unknown node.
	if _, err := ns.DrainNode(ctx, &zatterav1.DrainNodeRequest{NodeId: "ghost"}); status.Code(err) != codes.NotFound {
		t.Fatalf("drain of unknown node should be NotFound, got %v", err)
	}
}
