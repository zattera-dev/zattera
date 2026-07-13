package mesh

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
)

// key returns a deterministic wgtypes.Key with its first byte set, so hex/base64
// encodings are stable for goldens.
func key(b byte) wgtypes.Key {
	var k wgtypes.Key
	k[0] = b
	return k
}

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestBuildUAPIGolden(t *testing.T) {
	cfg := deviceConfig{
		privateKey:   key(0x01),
		listenPort:   51820,
		replacePeers: true,
		peers: []peerConfig{{
			publicKey:        key(0x02),
			endpoint:         "1.2.3.4:51820",
			keepaliveSeconds: 25,
			allowedIPs:       []string{"10.90.0.0/16"},
		}},
	}
	want := "private_key=0100000000000000000000000000000000000000000000000000000000000000\n" +
		"listen_port=51820\n" +
		"replace_peers=true\n" +
		"public_key=0200000000000000000000000000000000000000000000000000000000000000\n" +
		"endpoint=1.2.3.4:51820\n" +
		"persistent_keepalive_interval=25\n" +
		"replace_allowed_ips=true\n" +
		"allowed_ip=10.90.0.0/16\n" +
		"\n"
	if got := buildUAPI(cfg); got != want {
		t.Fatalf("uapi mismatch:\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

func TestBuildUAPIPeerRemoval(t *testing.T) {
	got := buildUAPI(deviceConfig{
		privateKey: key(0x01),
		peers:      []peerConfig{{publicKey: key(0x02), remove: true, allowedIPs: []string{"ignored/32"}}},
	})
	want := "private_key=0100000000000000000000000000000000000000000000000000000000000000\n" +
		"public_key=0200000000000000000000000000000000000000000000000000000000000000\n" +
		"remove=true\n" +
		"\n"
	if got != want {
		t.Fatalf("removal uapi mismatch:\n%s", got)
	}
}

func TestAllowedIPsFor(t *testing.T) {
	// Explicit AllowedIPs win.
	if got := allowedIPsFor(&clusterv1.Peer{AllowedIps: []string{"10.0.0.5/32"}}, true); len(got) != 1 || got[0] != "10.0.0.5/32" {
		t.Fatalf("explicit allowed_ips = %v", got)
	}
	// Hub-and-spoke control peer routes the whole mesh.
	if got := allowedIPsFor(&clusterv1.Peer{IsControl: true, MeshIp: "10.90.0.1"}, true); len(got) != 1 || got[0] != meshCIDR {
		t.Fatalf("hub control allowed_ips = %v", got)
	}
	// Worker peer gets its /32.
	if got := allowedIPsFor(&clusterv1.Peer{MeshIp: "10.90.1.7"}, true); len(got) != 1 || got[0] != "10.90.1.7/32" {
		t.Fatalf("worker allowed_ips = %v", got)
	}
}

func TestBuildPeerConfigs(t *testing.T) {
	peers := &clusterv1.PeerSet{
		HubAndSpoke: true,
		Peers: []*clusterv1.Peer{{
			NodeId:                     "c1",
			WireguardPublicKey:         key(0x02).String(),
			MeshIp:                     "10.90.0.1",
			IsControl:                  true,
			PersistentKeepaliveSeconds: 25,
			Endpoints:                  []string{"1.2.3.4:51820", "5.6.7.8:51820"},
		}},
	}
	pcs, err := buildPeerConfigs(peers)
	if err != nil {
		t.Fatalf("buildPeerConfigs: %v", err)
	}
	if len(pcs) != 1 {
		t.Fatalf("want 1 peer, got %d", len(pcs))
	}
	p := pcs[0]
	if p.publicKey != key(0x02) || p.endpoint != "1.2.3.4:51820" || p.keepaliveSeconds != 25 {
		t.Fatalf("peer config wrong: %+v", p)
	}
	if len(p.allowedIPs) != 1 || p.allowedIPs[0] != meshCIDR {
		t.Fatalf("hub control allowed_ips = %v", p.allowedIPs)
	}

	// A bad public key is a hard error.
	if _, err := buildPeerConfigs(&clusterv1.PeerSet{Peers: []*clusterv1.Peer{{WireguardPublicKey: "not-base64"}}}); err == nil {
		t.Fatal("expected error for invalid public key")
	}
}

// TestApplyPeersRendersUAPI drives ApplyPeers through a fake ipc layer and
// asserts the rendered device configuration.
func TestApplyPeersRendersUAPI(t *testing.T) {
	fake := &fakeIPC{}
	dm := &DeviceManager{
		log:        discard(),
		priv:       key(0x01),
		listenPort: 51820,
		backend:    &userspaceBackend{ipc: fake},
		peerPaths:  map[string]string{},
	}

	peers := &clusterv1.PeerSet{
		HubAndSpoke: true,
		Peers: []*clusterv1.Peer{{
			NodeId:                     "c1",
			WireguardPublicKey:         key(0x02).String(),
			MeshIp:                     "10.90.0.1",
			IsControl:                  true,
			PersistentKeepaliveSeconds: 25,
			Endpoints:                  []string{"1.2.3.4:51820"},
		}},
	}
	if err := dm.ApplyPeers(context.Background(), peers); err != nil {
		t.Fatalf("ApplyPeers: %v", err)
	}
	if len(fake.configs) != 1 {
		t.Fatalf("expected 1 IpcSet call, got %d", len(fake.configs))
	}
	want := "private_key=0100000000000000000000000000000000000000000000000000000000000000\n" +
		"listen_port=51820\n" +
		"replace_peers=true\n" +
		"public_key=0200000000000000000000000000000000000000000000000000000000000000\n" +
		"endpoint=1.2.3.4:51820\n" +
		"persistent_keepalive_interval=25\n" +
		"replace_allowed_ips=true\n" +
		"allowed_ip=10.90.0.0/16\n" +
		"\n"
	if fake.configs[0] != want {
		t.Fatalf("applied uapi mismatch:\n--- got ---\n%s\n--- want ---\n%s", fake.configs[0], want)
	}
	// Status reflects the hub path for the control peer.
	if dm.Status().PeerPaths["c1"] != "direct" {
		t.Fatalf("peer path = %q", dm.Status().PeerPaths["c1"])
	}

	// ApplyPeers before Up is an error.
	if err := (&DeviceManager{log: discard()}).ApplyPeers(context.Background(), peers); err == nil {
		t.Fatal("ApplyPeers should fail when the device is not up")
	}
}

func TestLoadOrCreatePrivateKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "keys", "wg.key")
	k1, err := loadOrCreatePrivateKey(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("key file mode wrong: %v", err)
	}
	k2, err := loadOrCreatePrivateKey(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if k1 != k2 || k1.PublicKey() != k2.PublicKey() {
		t.Fatal("reloaded key should match the persisted one")
	}
}

type fakeIPC struct{ configs []string }

func (f *fakeIPC) IpcSet(c string) error { f.configs = append(f.configs, c); return nil }
