package daemon

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/config"
	"github.com/zattera-dev/zattera/internal/daemon/mesh"
	"github.com/zattera-dev/zattera/internal/daemon/nodeinfo"
	"github.com/zattera-dev/zattera/internal/pkgutil/platform"
	"github.com/zattera-dev/zattera/internal/pkgutil/version"
)

// joinTokenPrefix and separator mirror the control side: K10<ca-hash>::<secret>.
const (
	joinTokenPrefix = "K10"
	joinTokenSep    = "::"
)

// joinResult is the persisted outcome of enrolling with the control plane.
type joinResult struct {
	NodeID          string `json:"node_id"`
	MeshIP          string `json:"mesh_ip"`
	ControlGRPCAddr string `json:"control_grpc_addr"`
	RegistryAddr    string `json:"registry_addr"`
	RegistryUser    string `json:"registry_username"`
	RegistryPass    string `json:"registry_password"`
	MeshEnabled     bool   `json:"mesh_enabled"`

	// On-disk material (not serialized into mesh.json).
	caPEM        []byte               `json:"-"`
	certPEM      []byte               `json:"-"`
	keyPEM       []byte               `json:"-"`
	initialPeers *clusterv1.PeerSet   `json:"-"`
	roles        []zatterav1.NodeRole `json:"-"`
	handover     *controlHandover     `json:"-"`
}

// controlHandover carries the cluster secrets a joining CONTROL node receives so
// it can bring up its own raft + control stack (T-55). Nil for worker joins.
type controlHandover struct {
	dataKey        []byte
	dataKeyVersion uint32
	caKeyPEM       []byte
	raftBindAddr   string
}

// isControl reports whether the control plane assigned this node the control
// role (authoritative over local config).
func (r *joinResult) isControl() bool {
	for _, role := range r.roles {
		if role == zatterav1.NodeRole_NODE_ROLE_CONTROL {
			return true
		}
	}
	return false
}

// runJoin enrolls this node with a control plane: it pins the cluster CA from
// the token, generates a keypair locally, sends a CSR, and persists the signed
// identity under <data-dir>/node/. The private key never leaves the node.
func runJoin(ctx context.Context, cfg config.Config, log *slog.Logger) (*joinResult, error) {
	caHashHex, secret, err := parseJoinToken(cfg.Join.Token)
	if err != nil {
		return nil, err
	}

	key, csrPEM, err := generateCSR()
	if err != nil {
		return nil, err
	}

	creds, err := caPinCreds(caHashHex)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(cfg.Join.Addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("daemon: dial control %s: %w", cfg.Join.Addr, err)
	}
	defer func() { _ = conn.Close() }()

	// Advertise a WireGuard public key so the control plane can register this
	// node in the mesh. Harmless when the cluster runs mesh-disabled. The
	// device (T-19) reuses the same key path so the running key matches.
	var wgPub string
	if k, kerr := mesh.EnsureNodeKey(wgKeyPath(cfg.DataDir)); kerr != nil {
		log.Warn("mesh: could not prepare wireguard key", "err", kerr)
	} else {
		wgPub = k
	}

	capacity := nodeinfo.Detect(cfg.DataDir, log)
	resp, err := clusterv1.NewJoinServiceClient(conn).Join(ctx, &clusterv1.JoinRequest{
		TokenSecret:         secret,
		NodeName:            cfg.NodeName,
		ExistingNodeId:      readNodeID(cfg.DataDir),
		CsrPem:              csrPEM,
		OsArch:              platform.Local(),
		BinaryVersion:       version.Version,
		Capacity:            &zatterav1.ResourceLimits{CpuMillis: capacity.CPUMillis, MemoryMb: capacity.MemoryMB},
		CapacityDiskMb:      capacity.DiskMB,
		WireguardPublicKey:  wgPub,
		WireguardListenPort: uint32(meshListenPort(cfg)),
		PublicEndpoints:     cfg.Mesh.PublicEndpoints,
	})
	if err != nil {
		return nil, fmt.Errorf("daemon: join rpc: %w", err)
	}

	keyPEM, err := marshalECKey(key)
	if err != nil {
		return nil, err
	}
	res := &joinResult{
		NodeID:          resp.GetNodeId(),
		MeshIP:          resp.GetMeshIp(),
		ControlGRPCAddr: resp.GetControlGrpcAddr(),
		RegistryAddr:    resp.GetRegistryAddr(),
		RegistryUser:    resp.GetRegistryUsername(),
		RegistryPass:    resp.GetRegistryPassword(),
		MeshEnabled:     resp.GetMeshEnabled(),
		caPEM:           resp.GetCaCertPem(),
		certPEM:         resp.GetNodeCertPem(),
		keyPEM:          keyPEM,
		initialPeers:    resp.GetInitialPeers(),
		roles:           resp.GetRoles(),
	}
	if len(resp.GetCaKeyPem()) > 0 || resp.GetRaftBindAddr() != "" {
		res.handover = &controlHandover{
			dataKey:        resp.GetDataKey(),
			dataKeyVersion: resp.GetDataKeyVersion(),
			caKeyPEM:       resp.GetCaKeyPem(),
			raftBindAddr:   resp.GetRaftBindAddr(),
		}
	}
	if err := persistJoin(cfg.DataDir, res); err != nil {
		return nil, err
	}
	log.Info("joined cluster", "node", res.NodeID, "mesh_ip", res.MeshIP, "control", res.ControlGRPCAddr)
	return res, nil
}

// parseJoinToken splits K10<ca-hash-hex>::<secret>.
func parseJoinToken(token string) (caHashHex, secret string, err error) {
	if !strings.HasPrefix(token, joinTokenPrefix) {
		return "", "", fmt.Errorf("daemon: malformed join token (missing %s prefix)", joinTokenPrefix)
	}
	body := strings.TrimPrefix(token, joinTokenPrefix)
	i := strings.Index(body, joinTokenSep)
	if i < 0 {
		return "", "", fmt.Errorf("daemon: malformed join token (missing separator)")
	}
	caHashHex, secret = body[:i], body[i+len(joinTokenSep):]
	if caHashHex == "" || secret == "" {
		return "", "", fmt.Errorf("daemon: malformed join token (empty hash or secret)")
	}
	if _, err := hex.DecodeString(caHashHex); err != nil {
		return "", "", fmt.Errorf("daemon: malformed join token CA hash: %w", err)
	}
	return caHashHex, secret, nil
}

// caPinCreds dials with the leaf unverified but asserts the presented chain
// includes a certificate whose SHA-256 matches the token's CA pin (k3s-style).
func caPinCreds(caHashHex string) (credentials.TransportCredentials, error) {
	want, err := hex.DecodeString(caHashHex)
	if err != nil {
		return nil, fmt.Errorf("daemon: bad CA hash in token: %w", err)
	}
	return credentials.NewTLS(&tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true, // replaced by the pin check below
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			for _, raw := range rawCerts {
				sum := sha256.Sum256(raw)
				if bytes.Equal(sum[:], want) {
					return nil
				}
			}
			return fmt.Errorf("daemon: control-plane CA does not match the join token pin")
		},
	}), nil
}

// generateCSR creates a local P-256 key and a PKCS#10 CSR. The server imposes
// the SANs (URI node identity), so the requested subject is nominal.
func generateCSR() (*ecdsa.PrivateKey, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("daemon: generate node key: %w", err)
	}
	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{Subject: pkix.Name{CommonName: "node"}}, key)
	if err != nil {
		return nil, nil, fmt.Errorf("daemon: create csr: %w", err)
	}
	return key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der}), nil
}

func marshalECKey(key *ecdsa.PrivateKey) ([]byte, error) {
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("daemon: marshal node key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}), nil
}

// persistJoin writes the node identity + cluster facts under <data-dir>/node/.
// readNodeID returns this node's id from a prior join (<data-dir>/node/id), or
// "" if it has not joined before. A restarting node sends it so the control
// plane resumes its record instead of registering a duplicate.
func readNodeID(dataDir string) string {
	b, err := os.ReadFile(filepath.Join(dataDir, "node", "id"))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func persistJoin(dataDir string, r *joinResult) error {
	dir := filepath.Join(dataDir, "node")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("daemon: node dir: %w", err)
	}
	files := []struct {
		name string
		data []byte
		mode os.FileMode
	}{
		{"node.crt", r.certPEM, 0o600},
		{"node.key", r.keyPEM, 0o600},
		{"ca.crt", r.caPEM, 0o644},
		{"id", []byte(r.NodeID), 0o600},
	}
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(dir, f.name), f.data, f.mode); err != nil {
			return fmt.Errorf("daemon: write %s: %w", f.name, err)
		}
	}
	meshJSON, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("daemon: marshal mesh.json: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "mesh.json"), meshJSON, 0o600); err != nil {
		return fmt.Errorf("daemon: write mesh.json: %w", err)
	}
	return nil
}
