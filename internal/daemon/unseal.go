package daemon

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/config"
	"github.com/zattera-dev/zattera/internal/daemon/ca"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
)

// dataKeyFile is where an auto-unsealing node caches the cluster data key.
// Deliberately plaintext at 0600 rather than wrapped by another local key:
// anything that could unwrap it would have to live on the same disk, so
// wrapping would buy obscurity, not security, and would misrepresent the
// guarantee. See ADR-0005.
const dataKeyFile = "node/data.key"

// peerUnsealTimeout bounds the whole peer-fetch attempt at startup. Serving
// sealed is better than not serving at all, so this stays short.
const peerUnsealTimeout = 15 * time.Second

// autoUnseal tries to recover the cluster data key for a node that did not just
// bootstrap, then reports the outcome. Ordered cheapest-first: the local cache
// needs no network, the peer fetch needs a reachable unsealed control node.
//
// Never fatal: a sealed node still serves reads, deploys and ingress. It just
// cannot touch secrets, which is why the sealed case is logged loudly rather
// than silently tolerated (T-111).
func autoUnseal(ctx context.Context, cfg config.Config, rs *raftstore.Store, authority *ca.CA, nodeID string, vault *secrets.Vault, log *slog.Logger) {
	if vault.Unsealed() {
		// Freshly bootstrapped: persist the key so the next restart is
		// automatic, unless the operator asked us not to.
		persistDataKey(cfg, vault, log)
		return
	}

	if !cfg.SealedAtRest {
		if err := unsealFromFile(cfg, vault); err == nil {
			log.Info("cluster key unsealed from local key file")
			return
		} else if !errors.Is(err, fs.ErrNotExist) {
			log.Warn("local key file unusable", "err", err)
		}
	}

	if err := unsealFromPeer(ctx, cfg, rs, authority, nodeID, vault, log); err == nil {
		log.Info("cluster key unsealed from a control peer")
		persistDataKey(cfg, vault, log)
		return
	} else if !errors.Is(err, errNoPeer) {
		log.Warn("peer unseal failed", "err", err)
	}

	log.Warn("cluster is SEALED — alerting, env-var writes, backups and volume snapshots are disabled until it is unsealed; run `zattera unseal --passphrase-file <file>`")
}

// persistDataKey caches the data key for the next restart. No-op when the
// operator chose sealed-at-rest, or when the file already matches.
func persistDataKey(cfg config.Config, vault *secrets.Vault, log *slog.Logger) {
	if cfg.SealedAtRest || !vault.Unsealed() {
		return
	}
	path := filepath.Join(cfg.DataDir, dataKeyFile)
	blob, err := proto.Marshal(&zatterav1.EncryptedValue{
		// Reuse EncryptedValue purely as a versioned container: the bytes are
		// the plaintext data key, and the file mode is the protection.
		Ciphertext: vault.DataKey(),
		KeyVersion: vault.KeyVersion(),
	})
	if err != nil {
		log.Warn("cache data key: marshal", "err", err)
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		log.Warn("cache data key: mkdir", "err", err)
		return
	}
	if err := os.WriteFile(path, blob, 0o600); err != nil {
		log.Warn("cache data key: write", "err", err)
	}
}

// unsealFromFile installs the data key cached by a previous run. Returns an
// fs.ErrNotExist-wrapping error when there is no cache, which the caller treats
// as "nothing to do" rather than a failure.
func unsealFromFile(cfg config.Config, vault *secrets.Vault) error {
	blob, err := os.ReadFile(filepath.Join(cfg.DataDir, dataKeyFile))
	if err != nil {
		return err
	}
	var ev zatterav1.EncryptedValue
	if err := proto.Unmarshal(blob, &ev); err != nil {
		return fmt.Errorf("parse key file: %w", err)
	}
	kr, err := secrets.NewKeyring(ev.GetCiphertext(), ev.GetKeyVersion())
	if err != nil {
		return err
	}
	return vault.Install(kr)
}

// errNoPeer means no other control node was reachable — the expected case on a
// single-node cluster, so the caller does not log it as a failure.
var errNoPeer = errors.New("no unsealed control peer available")

// unsealFromPeer asks another control node for the data key over mTLS. The
// node's own certificate (issued when it joined) is the credential; the peer
// additionally checks that this node holds the control role.
//
// This deliberately does NOT reuse JoinService: a join token is single-use and
// the join handler has side effects (mesh IP allocation, cert signing, registry
// credential minting) that must not repeat on every restart.
func unsealFromPeer(ctx context.Context, cfg config.Config, rs *raftstore.Store, authority *ca.CA, nodeID string, vault *secrets.Vault, log *slog.Logger) error {
	peers := controlPeers(rs, nodeID, cfg)
	if len(peers) == 0 {
		return errNoPeer
	}
	ctx, cancel := context.WithTimeout(ctx, peerUnsealTimeout)
	defer cancel()

	lastErr := errNoPeer
	for _, p := range peers {
		conn, err := dialControlPeer(ctx, authority, nodeID, p)
		if err != nil {
			lastErr = err
			continue
		}
		resp, err := clusterv1.NewKeyServiceClient(conn).FetchDataKey(ctx, &clusterv1.FetchDataKeyRequest{})
		_ = conn.Close()
		if err != nil {
			lastErr = err
			log.Debug("peer unseal attempt failed", "peer", p.addr, "err", err)
			continue
		}
		kr, err := secrets.NewKeyring(resp.GetDataKey(), resp.GetDataKeyVersion())
		if err != nil {
			lastErr = err
			continue
		}
		return vault.Install(kr)
	}
	return lastErr
}

// controlPeer is another control node's dialable API address.
type controlPeer struct {
	nodeID string
	host   string // its mesh IP, which is also the SAN on its API cert
	addr   string
}

// controlPeers lists the other control nodes from replicated state. Raft is up
// by the time this runs (autoUnseal is called after WaitForLeader), so the node
// list is authoritative — there is no peer file on disk to consult earlier.
func controlPeers(rs *raftstore.Store, selfID string, cfg config.Config) []controlPeer {
	_, port, err := net.SplitHostPort(cfg.API.Listen)
	if err != nil || port == "" {
		return nil
	}
	var out []controlPeer
	for _, n := range rs.State().ListNodes() {
		if n.GetMeta().GetId() == selfID || !nodeHasControlRole(n) {
			continue
		}
		ip := n.GetMeshIp()
		if ip == "" || n.GetStatus() == zatterav1.NodeStatus_NODE_STATUS_DOWN {
			continue
		}
		out = append(out, controlPeer{nodeID: n.GetMeta().GetId(), host: ip, addr: net.JoinHostPort(ip, port)})
	}
	return out
}

// dialControlPeer opens an mTLS connection to a peer's API, presenting this
// node's own certificate as the credential.
func dialControlPeer(ctx context.Context, authority *ca.CA, nodeID string, p controlPeer) (*grpc.ClientConn, error) {
	creds := credentials.NewTLS(nodeClientTLS(authority, nodeID, p.host))
	conn, err := grpc.NewClient(p.addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, err
	}
	_ = ctx
	return conn, nil
}
