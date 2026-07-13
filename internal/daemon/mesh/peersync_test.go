package mesh

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/api"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/state"
)

// TestPeerSyncAppliesPushedPeers wires the node-side RunPeerSync to a real
// MeshServer and asserts the pushed PeerSet reaches the mesh Manager.
func TestPeerSyncAppliesPushedPeers(t *testing.T) {
	st := state.New()
	st.PutNode(&zatterav1.Node{
		Meta:               &zatterav1.Meta{Id: "c1"},
		Roles:              []zatterav1.NodeRole{zatterav1.NodeRole_NODE_ROLE_CONTROL},
		MeshIp:             "10.90.0.1",
		WireguardPublicKey: "keyc1",
		PublicEndpoints:    []string{"203.0.113.1:51820"},
	})
	st.PutNode(&zatterav1.Node{
		Meta:  &zatterav1.Meta{Id: "w1"},
		Roles: []zatterav1.NodeRole{zatterav1.NodeRole_NODE_ROLE_WORKER},
	})

	srv := grpc.NewServer()
	clusterv1.RegisterMeshServiceServer(srv, api.NewMeshServer(st, clock.NewFake(), discard()))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	mgr := &recordingManager{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = RunPeerSync(ctx, PeerSyncConfig{
			NodeID:  "w1",
			Manager: mgr,
			Clock:   clock.NewFake(),
			Logger:  discard(),
			Dial: func(context.Context) (grpc.ClientConnInterface, func() error, error) {
				cc, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					return nil, nil, err
				}
				return cc, cc.Close, nil
			},
		})
	}()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if p := mgr.last(); p != nil && len(p.GetPeers()) == 1 && p.GetPeers()[0].GetNodeId() == "c1" {
			if !p.GetHubAndSpoke() {
				t.Fatal("worker peer set should be hub-and-spoke")
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("peer set with control c1 was never applied")
}

type recordingManager struct {
	mu      sync.Mutex
	peers   *clusterv1.PeerSet
	Manager // embed to satisfy the interface; only ApplyPeers is used
}

func (m *recordingManager) ApplyPeers(_ context.Context, p *clusterv1.PeerSet) error {
	m.mu.Lock()
	m.peers = p
	m.mu.Unlock()
	return nil
}

func (m *recordingManager) last() *clusterv1.PeerSet {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.peers
}
