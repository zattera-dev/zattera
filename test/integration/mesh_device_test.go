//go:build integration

package integration

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"log/slog"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/daemon/mesh"
)

// TestWGDevice brings up two real userspace WireGuard devices, peers them over
// loopback, and asserts a genuine WireGuard handshake completes — exercising the
// TUN creation, uapi configuration and encrypted UDP data path. Handshake (not
// tunnel-IP UDP echo) is asserted because both devices share a network
// namespace, where the kernel would deliver tunnel-IP traffic locally without
// traversing the tunnel.
//
// Requires Linux + CAP_NET_ADMIN (the CI privileged container).
func TestWGDevice(t *testing.T) {
	requireNetAdmin(t)

	dir := t.TempDir()
	a := bringUp(t, "zt-a", "10.90.0.1", 51821, filepath.Join(dir, "a.key"))
	b := bringUp(t, "zt-b", "10.90.1.1", 51822, filepath.Join(dir, "b.key"))

	pubA, err := a.PublicKey()
	if err != nil {
		t.Fatalf("pubkey A: %v", err)
	}
	pubB, err := b.PublicKey()
	if err != nil {
		t.Fatalf("pubkey B: %v", err)
	}

	// Peer them over loopback with a 1s keepalive to force a prompt handshake.
	if err := a.ApplyPeers(context.Background(), &clusterv1.PeerSet{Peers: []*clusterv1.Peer{{
		NodeId: "b", WireguardPublicKey: pubB, MeshIp: "10.90.1.1",
		Endpoints: []string{"127.0.0.1:51822"}, PersistentKeepaliveSeconds: 1,
		AllowedIps: []string{"10.90.1.1/32"},
	}}}); err != nil {
		t.Fatalf("A apply peers: %v", err)
	}
	if err := b.ApplyPeers(context.Background(), &clusterv1.PeerSet{Peers: []*clusterv1.Peer{{
		NodeId: "a", WireguardPublicKey: pubA, MeshIp: "10.90.0.1",
		Endpoints: []string{"127.0.0.1:51821"}, PersistentKeepaliveSeconds: 1,
		AllowedIps: []string{"10.90.0.1/32"},
	}}}); err != nil {
		t.Fatalf("B apply peers: %v", err)
	}

	// hexPubB is the key A reports handshakes against.
	kb, err := parseBase64Key(pubB)
	if err != nil {
		t.Fatal(err)
	}
	hexB := hex.EncodeToString(kb)

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		hs, err := a.LastHandshakes()
		if err == nil && hs[hexB] > 0 {
			return // handshake completed — tunnel is live
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("no WireGuard handshake completed within 15s")
}

func bringUp(t *testing.T, ifname, meshIP string, port uint16, keyPath string) *mesh.DeviceManager {
	t.Helper()
	dm := mesh.NewDeviceManager(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	addr := mustAddr(t, meshIP)
	if err := dm.Up(context.Background(), mesh.NodeConfig{
		PrivateKeyPath: keyPath,
		MeshIP:         addr,
		ListenPort:     port,
		InterfaceName:  ifname,
	}); err != nil {
		t.Skipf("mesh: cannot bring up device (need NET_ADMIN): %v", err)
	}
	t.Cleanup(func() { _ = dm.Down(context.Background()) })
	return dm
}

func requireNetAdmin(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skipf("mesh device integration test requires linux, have %s", runtime.GOOS)
	}
}

func mustAddr(t *testing.T, s string) netip.Addr {
	t.Helper()
	a, err := netip.ParseAddr(s)
	if err != nil {
		t.Fatalf("parse addr %s: %v", s, err)
	}
	return a
}

func parseBase64Key(s string) ([]byte, error) { return base64.StdEncoding.DecodeString(s) }
