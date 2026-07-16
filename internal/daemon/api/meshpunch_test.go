package api

import (
	"context"
	"testing"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/state"
)

func TestRequestPunch(t *testing.T) {
	st := state.New()
	st.PutNode(&zatterav1.Node{Meta: &zatterav1.Meta{Id: "a"}, PublicEndpoints: []string{"198.51.100.1:51820"}})
	st.PutNode(&zatterav1.Node{Meta: &zatterav1.Meta{Id: "b"}, PublicEndpoints: []string{"198.51.100.2:51820"}})
	srv := NewMeshServer(st, nil, clock.NewFake(), nil)

	// a asks to punch with b, but b has no PunchStream → not coordinated.
	resp, err := srv.RequestPunch(nodeCtx("a"), &clusterv1.RequestPunchRequest{TargetNodeId: "b"})
	if err != nil {
		t.Fatalf("request punch: %v", err)
	}
	if resp.GetCoordinated() {
		t.Fatal("should not coordinate with a target that has no punch stream")
	}

	// b registers a punch stream; now a's request coordinates and b's queue
	// receives the command.
	bq, unregister := srv.punch.register("b")
	defer unregister()

	resp, err = srv.RequestPunch(nodeCtx("a"), &clusterv1.RequestPunchRequest{TargetNodeId: "b"})
	if err != nil {
		t.Fatalf("request punch (coordinated): %v", err)
	}
	if !resp.GetCoordinated() {
		t.Fatal("expected coordinated punch once b has a stream")
	}
	if got := resp.GetTargetEndpoints(); len(got) != 1 || got[0] != "198.51.100.2:51820" {
		t.Fatalf("target endpoints = %v", got)
	}
	if resp.GetPunchAt() == nil {
		t.Fatal("punch_at not set")
	}
	select {
	case cmd := <-bq:
		if cmd.GetPeerNodeId() != "a" || len(cmd.GetPeerEndpoints()) != 1 || cmd.GetPeerEndpoints()[0] != "198.51.100.1:51820" {
			t.Fatalf("pushed command wrong: %+v", cmd)
		}
	default:
		t.Fatal("no command pushed to b's queue")
	}

	// Self-punch is rejected.
	if _, err := srv.RequestPunch(nodeCtx("a"), &clusterv1.RequestPunchRequest{TargetNodeId: "a"}); err == nil {
		t.Fatal("self-punch should be rejected")
	}
}

func TestPunchRegistrySupersede(t *testing.T) {
	r := newPunchRegistry()
	old, _ := r.register("n")
	newCh, unregister := r.register("n") // supersedes old
	defer unregister()

	if _, ok := <-old; ok {
		t.Fatal("old stream channel should be closed on supersede")
	}
	if !r.push("n", &clusterv1.PunchCommand{PeerNodeId: "x"}) {
		t.Fatal("push to current stream should succeed")
	}
	select {
	case cmd := <-newCh:
		if cmd.GetPeerNodeId() != "x" {
			t.Fatalf("wrong command: %v", cmd)
		}
	default:
		t.Fatal("current stream did not receive the command")
	}
}

// nodeCtx builds a context carrying a node mTLS identity.
func nodeCtx(nodeID string) context.Context {
	return withIdentity(context.Background(), Identity{NodeID: nodeID})
}
