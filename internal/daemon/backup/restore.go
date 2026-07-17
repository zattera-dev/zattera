package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/daemon/volumes"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// RestoreInput configures a restore into a fresh data dir.
type RestoreInput struct {
	ObjectStore volumes.ObjectStore
	Passphrase  string
	DataDir     string // fresh, empty
	NodeID      string // the new single-node cluster's id
}

// Restore rebuilds a fresh single-node cluster from the latest backup: it
// unseals the data key with the passphrase, decrypts the state + CA, marks the
// old nodes DOWN (mesh IPs preserved), and bootstraps a raft store in DataDir so
// a subsequent `zatterad server` comes up with the restored state. It returns
// the backup index (whose volume refs the operator restores as workers rejoin).
func Restore(ctx context.Context, in RestoreInput) (*Index, error) {
	if in.Passphrase == "" {
		return nil, fmt.Errorf("restore: a passphrase is required")
	}
	dir, idx, err := loadIndex(ctx, in.ObjectStore)
	if err != nil {
		return nil, err
	}

	dataKey, keyVersion, err := unsealDataKey(ctx, in.ObjectStore, dir, in.Passphrase)
	if err != nil {
		return nil, err
	}
	sealer, err := secrets.NewSealer(dataKey, keyVersion)
	if err != nil {
		return nil, err
	}

	snap, err := decodeState(ctx, in.ObjectStore, dir, sealer)
	if err != nil {
		return nil, err
	}
	markNodesDown(snap)

	if err := writeCA(ctx, in.ObjectStore, dir, sealer, in.DataDir); err != nil {
		return nil, err
	}
	if err := bootstrapRestored(ctx, in.DataDir, in.NodeID, snap); err != nil {
		return nil, err
	}
	return idx, nil
}

// Verify downloads and decrypts the latest state backup and returns the object
// counts — the weekly restore-test uses it to prove a backup is recoverable.
func Verify(ctx context.Context, store volumes.ObjectStore, passphrase string) (*Index, *state.Store, error) {
	dir, idx, err := loadIndex(ctx, store)
	if err != nil {
		return nil, nil, err
	}
	dataKey, keyVersion, err := unsealDataKey(ctx, store, dir, passphrase)
	if err != nil {
		return nil, nil, err
	}
	sealer, err := secrets.NewSealer(dataKey, keyVersion)
	if err != nil {
		return nil, nil, err
	}
	snap, err := decodeState(ctx, store, dir, sealer)
	if err != nil {
		return nil, nil, err
	}
	st := state.New()
	st.RestoreProto(snap)
	return idx, st, nil
}

func loadIndex(ctx context.Context, store volumes.ObjectStore) (dir string, idx *Index, err error) {
	tsBytes, err := store.Get(ctx, latestKey)
	if err != nil {
		return "", nil, fmt.Errorf("restore: read latest pointer: %w", err)
	}
	ts, err := strconv.ParseInt(strings.TrimSpace(string(tsBytes)), 10, 64)
	if err != nil {
		return "", nil, fmt.Errorf("restore: bad latest pointer: %w", err)
	}
	dir = fmt.Sprintf("%s%d/", backupsDir, ts)
	raw, err := store.Get(ctx, dir+indexObj)
	if err != nil {
		return "", nil, fmt.Errorf("restore: read index: %w", err)
	}
	idx = &Index{}
	if err := json.Unmarshal(raw, idx); err != nil {
		return "", nil, fmt.Errorf("restore: decode index: %w", err)
	}
	return dir, idx, nil
}

func unsealDataKey(ctx context.Context, store volumes.ObjectStore, dir, passphrase string) ([]byte, uint32, error) {
	raw, err := store.Get(ctx, dir+keysObj)
	if err != nil {
		return nil, 0, fmt.Errorf("restore: read key material: %w", err)
	}
	var km zatterav1.ClusterKeyMaterial
	if err := proto.Unmarshal(raw, &km); err != nil {
		return nil, 0, fmt.Errorf("restore: decode key material: %w", err)
	}
	dataKey, err := secrets.UnsealDataKey(&km, passphrase)
	if err != nil {
		return nil, 0, fmt.Errorf("restore: wrong passphrase or corrupt key material: %w", err)
	}
	return dataKey, km.GetKeyVersion(), nil
}

func decodeState(ctx context.Context, store volumes.ObjectStore, dir string, sealer secrets.Sealer) (*clusterv1.Snapshot, error) {
	plain, err := openSealed(ctx, store, dir+stateObj, sealer)
	if err != nil {
		return nil, err
	}
	var snap clusterv1.Snapshot
	if err := proto.Unmarshal(plain, &snap); err != nil {
		return nil, fmt.Errorf("restore: decode state: %w", err)
	}
	return &snap, nil
}

// markNodesDown flags every restored node DOWN (its process is gone) while
// preserving its mesh IP, so a rejoining node reclaims the same address.
func markNodesDown(snap *clusterv1.Snapshot) {
	for _, n := range snap.GetNodes() {
		n.Status = zatterav1.NodeStatus_NODE_STATUS_DOWN
	}
}

func writeCA(ctx context.Context, store volumes.ObjectStore, dir string, sealer secrets.Sealer, dataDir string) error {
	plain, err := openSealed(ctx, store, dir+caObj, sealer)
	if err != nil {
		return err
	}
	var ca caMaterial
	if err := json.Unmarshal(plain, &ca); err != nil {
		return fmt.Errorf("restore: decode ca: %w", err)
	}
	caDir := filepath.Join(dataDir, "ca")
	if err := os.MkdirAll(caDir, 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(caDir, "ca.crt"), ca.Cert, 0o600); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(caDir, "ca.key"), ca.Key, 0o600)
}

// bootstrapRestored writes the restored state into a fresh single-node raft in
// dataDir and snapshots it so the state survives a restart.
func bootstrapRestored(ctx context.Context, dataDir, nodeID string, snap *clusterv1.Snapshot) error {
	raftDir := filepath.Join(dataDir, "raft")
	if err := os.MkdirAll(raftDir, 0o700); err != nil {
		return err
	}
	bind, err := freeLoopbackAddr()
	if err != nil {
		return err
	}
	st := state.New()
	store, err := raftstore.New(raftstore.Config{
		NodeID:    nodeID,
		DataDir:   raftDir,
		BindAddr:  bind,
		Bootstrap: true,
	}, st)
	if err != nil {
		return fmt.Errorf("restore: bootstrap raft: %w", err)
	}
	defer func() { _ = store.Shutdown() }()

	wctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := store.WaitForLeader(wctx); err != nil {
		return fmt.Errorf("restore: raft did not elect self: %w", err)
	}
	// Load the backed-up state into the FSM out-of-band, then apply a marker
	// command through raft: this advances the applied index past the bootstrap
	// entry so ForceSnapshot has "something new" to capture — the snapshot then
	// contains the full restored state, which survives the next restart.
	store.State().RestoreProto(snap)
	actx, acancel := context.WithTimeout(ctx, 10*time.Second)
	defer acancel()
	if err := store.Apply(actx, &clusterv1.Command{
		RequestId: ids.New(),
		Actor:     "system:restore",
		Time:      timestamppb.Now(),
		Mutation: &clusterv1.Command_PutKv{PutKv: &clusterv1.PutKV{
			Key:             "backup/restored-at",
			Value:           []byte(strconv.FormatInt(time.Now().Unix(), 10)),
			ExpectedVersion: -1,
		}},
	}); err != nil {
		return fmt.Errorf("restore: apply marker: %w", err)
	}
	if err := store.ForceSnapshot(); err != nil {
		return fmt.Errorf("restore: persist snapshot: %w", err)
	}
	return nil
}

func openSealed(ctx context.Context, store volumes.ObjectStore, key string, sealer secrets.Sealer) ([]byte, error) {
	raw, err := store.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("restore: read %s: %w", key, err)
	}
	var ev zatterav1.EncryptedValue
	if err := proto.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("restore: decode %s: %w", key, err)
	}
	plain, err := sealer.Open(&ev)
	if err != nil {
		return nil, fmt.Errorf("restore: decrypt %s: %w", key, err)
	}
	return plain, nil
}

func freeLoopbackAddr() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr, nil
}
