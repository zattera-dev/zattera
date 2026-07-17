// Package backup implements full-platform disaster recovery (spec §3.11, T-66):
// a leader backs up the raft state, the cluster CA material and the sealed data
// key to the same S3 object store the volume snapshots use; `zatterad restore`
// rebuilds a fresh single-node cluster from the latest backup.
//
// Layout under the store prefix:
//
//	backups/<ts>/state.pb.enc  encrypted (data key) marshaled state Snapshot
//	backups/<ts>/ca.pb.enc     encrypted (data key) CA cert + key PEM
//	backups/<ts>/keys.pb       ClusterKeyMaterial (data key sealed by passphrase)
//	backups/<ts>/index.json    plaintext index (pointers + volume snapshot refs)
//	backups/latest             plaintext "<ts>" pointer
package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/daemon/volumes"
	"github.com/zattera-dev/zattera/internal/state"
)

const (
	latestKey  = "backups/latest"
	indexKind  = "full"
	indexVer   = 1
	stateObj   = "state.pb.enc"
	caObj      = "ca.pb.enc"
	keysObj    = "keys.pb"
	indexObj   = "index.json"
	backupsDir = "backups/"
)

// Index is the plaintext manifest of one full backup. It holds no secrets — only
// object pointers and the volume snapshots to restore.
type Index struct {
	Version       int         `json:"version"`
	Kind          string      `json:"kind"`
	TimestampUnix int64       `json:"timestamp_unix"`
	KeyVersion    uint32      `json:"key_version"`
	NodeIDs       []string    `json:"node_ids"`
	Volumes       []VolumeRef `json:"volumes"`
}

// VolumeRef points at the latest snapshot to restore for a volume.
type VolumeRef struct {
	VolumeID      string `json:"volume_id"`
	EnvironmentID string `json:"environment_id"`
	Name          string `json:"name"`
	NodeID        string `json:"node_id"`
	ManifestKey   string `json:"manifest_key"` // "" when the volume has no snapshot
}

// Input configures a backup run.
type Input struct {
	Store       *state.Store
	ObjectStore volumes.ObjectStore
	Sealer      secrets.Sealer // built from the cluster data key (state/CA crypto)
	// KeyMaterial is the cluster's data key already sealed under the recovery
	// passphrase (from state) — stored verbatim so restore unseals with that same
	// passphrase. No fresh passphrase is needed at backup time.
	KeyMaterial *zatterav1.ClusterKeyMaterial
	CACertPEM   []byte
	CAKeyPEM    []byte
	Now         time.Time
}

type caMaterial struct {
	Cert []byte `json:"cert"`
	Key  []byte `json:"key"`
}

// Backup writes a full backup and returns its index. It never mutates cluster
// state (the caller records a BackupRecord).
func Backup(ctx context.Context, in Input) (*Index, error) {
	if in.KeyMaterial == nil {
		return nil, fmt.Errorf("backup: cluster key material is required")
	}
	ts := in.Now.Unix()
	dir := fmt.Sprintf("%s%d/", backupsDir, ts)

	// State snapshot, encrypted with the data key.
	snap := in.Store.SnapshotProto(0)
	stateBytes, err := proto.Marshal(snap)
	if err != nil {
		return nil, fmt.Errorf("backup: marshal state: %w", err)
	}
	if err := in.putSealed(ctx, dir+stateObj, stateBytes); err != nil {
		return nil, err
	}

	// CA material, encrypted with the data key (certs stay valid post-restore).
	caBytes, err := json.Marshal(caMaterial{Cert: in.CACertPEM, Key: in.CAKeyPEM})
	if err != nil {
		return nil, err
	}
	if err := in.putSealed(ctx, dir+caObj, caBytes); err != nil {
		return nil, err
	}

	// The cluster's sealed data key (passphrase-protected) — the only way back in.
	kmBytes, err := proto.Marshal(in.KeyMaterial)
	if err != nil {
		return nil, err
	}
	if err := in.ObjectStore.Put(ctx, dir+keysObj, kmBytes); err != nil {
		return nil, err
	}

	idx := in.buildIndex(ts)
	idxBytes, err := json.Marshal(idx)
	if err != nil {
		return nil, err
	}
	if err := in.ObjectStore.Put(ctx, dir+indexObj, idxBytes); err != nil {
		return nil, err
	}
	if err := in.ObjectStore.Put(ctx, latestKey, []byte(strconv.FormatInt(ts, 10))); err != nil {
		return nil, err
	}
	return idx, nil
}

// buildIndex records node ids and each volume's latest completed snapshot.
func (in Input) buildIndex(ts int64) *Index {
	idx := &Index{Version: indexVer, Kind: indexKind, TimestampUnix: ts, KeyVersion: in.KeyMaterial.GetKeyVersion()}
	for _, n := range in.Store.ListNodes() {
		idx.NodeIDs = append(idx.NodeIDs, n.GetMeta().GetId())
	}
	for _, v := range in.Store.ListVolumes("") {
		ref := VolumeRef{VolumeID: v.GetMeta().GetId(), EnvironmentID: v.GetEnvironmentId(), Name: v.GetName(), NodeID: v.GetNodeId()}
		ref.ManifestKey = latestSnapshotKey(in.Store, v.GetMeta().GetId())
		idx.Volumes = append(idx.Volumes, ref)
	}
	return idx
}

// latestSnapshotKey returns the manifest key of the volume's newest completed
// snapshot, or "".
func latestSnapshotKey(st *state.Store, volumeID string) string {
	var best *zatterav1.VolumeSnapshot
	for _, s := range st.ListVolumeSnapshots(volumeID) {
		if s.GetStatus() != zatterav1.SnapshotStatus_SNAPSHOT_STATUS_COMPLETE {
			continue
		}
		if best == nil || s.GetMeta().GetCreatedAt().AsTime().After(best.GetMeta().GetCreatedAt().AsTime()) {
			best = s
		}
	}
	return best.GetManifestKey()
}

// putSealed encrypts data with the data-key sealer and stores it.
func (in Input) putSealed(ctx context.Context, key string, data []byte) error {
	ev, err := in.Sealer.Seal(data)
	if err != nil {
		return fmt.Errorf("backup: seal %s: %w", key, err)
	}
	blob, err := proto.Marshal(ev)
	if err != nil {
		return err
	}
	return in.ObjectStore.Put(ctx, key, blob)
}
