// Package state holds the full replicated cluster state in memory (ADR-0004).
//
// The Store is the Raft FSM's state: plain maps of proto messages keyed by
// ULID, a handful of secondary indexes on hot paths, one RWMutex, and a watch
// hub that lets control loops (scheduler, route builder, cert manager) react
// to changes.
//
// Rules:
//   - Mutating methods are called ONLY from FSM apply handlers
//     (internal/daemon/raftstore). Everything else reads.
//   - Read methods return deep clones; callers may mutate results freely.
//   - Collections are small (thousands of objects): list methods do linear
//     scans with filters unless a path is genuinely hot. Hot-path indexes:
//     tokens by hash, users by email, domains by hostname, assignments by
//     node. Do not add more indexes without a measured need.
package state

import (
	"sync"

	"google.golang.org/protobuf/proto"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	internalv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
)

// Kind identifies a resource collection for watch subscriptions.
type Kind string

const (
	KindOrg            Kind = "org"
	KindUser           Kind = "user"
	KindProject        Kind = "project"
	KindProjectMember  Kind = "project_member"
	KindApp            Kind = "app"
	KindEnvironment    Kind = "environment"
	KindEnvVar         Kind = "env_var"
	KindRelease        Kind = "release"
	KindDeployment     Kind = "deployment"
	KindBuild          Kind = "build"
	KindNode           Kind = "node"
	KindJoinToken      Kind = "join_token"
	KindAssignment     Kind = "assignment"
	KindToken          Kind = "token"
	KindDomain         Kind = "domain"
	KindKV             Kind = "kv"
	KindDNSProvider    Kind = "dns_provider"
	KindVolume         Kind = "volume"
	KindVolumeSnapshot Kind = "volume_snapshot"
	KindBackup         Kind = "backup"
	KindNetworkAlloc   Kind = "network_alloc"
	KindServiceVIP     Kind = "service_vip"
	KindJob            Kind = "job"
	KindAlertRule      Kind = "alert_rule"
	KindNotifyChannel  Kind = "notify_channel"
	KindEvent          Kind = "event"
	KindClusterKey     Kind = "cluster_key"
)

// Ring capacities for append-only histories held in state. Older entries are
// dropped; long-term retention belongs to backups/log sinks, not Raft state.
const (
	auditRingCap   = 50_000
	eventRingCap   = 10_000
	appliedRingCap = 8_192
)

type kvEntry struct {
	value     []byte
	version   int64
	expiresAt int64 // unix ms, 0 = none
}

// Store is the in-memory cluster state. Zero value is not usable; call New.
type Store struct {
	mu sync.RWMutex

	// version increments on every mutation; used to version route snapshots
	// and to cheaply detect "anything changed".
	version uint64

	org        *zatterav1.Org
	clusterKey *zatterav1.ClusterKeyMaterial

	users        map[string]*zatterav1.User
	usersByEmail map[string]string // email → user id

	projects       map[string]*zatterav1.Project
	projectMembers map[string]map[string]*zatterav1.ProjectMember // project id → user id → member

	apps         map[string]*zatterav1.App
	environments map[string]*zatterav1.Environment
	envVars      map[string]map[string]*zatterav1.EncryptedValue // env id → key → value

	releases    map[string]*zatterav1.Release
	deployments map[string]*zatterav1.Deployment
	builds      map[string]*zatterav1.Build

	nodes      map[string]*zatterav1.Node
	joinTokens map[string]*zatterav1.JoinToken

	assignments       map[string]*zatterav1.Assignment
	assignmentsByNode map[string]map[string]struct{} // node id → assignment ids

	tokens       map[string]*zatterav1.Token
	tokensByHash map[string]string // secret hash → token id

	domains           map[string]*zatterav1.Domain
	domainsByHostname map[string]string // hostname → domain id

	kv map[string]kvEntry

	dnsProviders    map[string]*zatterav1.DNSProviderConfig
	volumes         map[string]*zatterav1.Volume
	volumeSnapshots map[string]*zatterav1.VolumeSnapshot

	backupConfig  *zatterav1.BackupConfig
	backupRecords map[string]*zatterav1.BackupRecord

	networkAllocs map[string]*internalv1.NetworkAllocation // key: project/env/node
	serviceVIPs   map[string]string                        // environment id → vip

	jobs           map[string]*zatterav1.Job
	alertRules     map[string]*zatterav1.AlertRule
	notifyChannels map[string]*zatterav1.NotificationChannel

	events          []*zatterav1.Event
	audit           []*zatterav1.AuditEntry
	appliedRequests []*internalv1.AppliedRequest
	appliedSet      map[string]struct{}

	hub *Hub
}

// New returns an empty Store.
func New() *Store {
	return &Store{
		users:             map[string]*zatterav1.User{},
		usersByEmail:      map[string]string{},
		projects:          map[string]*zatterav1.Project{},
		projectMembers:    map[string]map[string]*zatterav1.ProjectMember{},
		apps:              map[string]*zatterav1.App{},
		environments:      map[string]*zatterav1.Environment{},
		envVars:           map[string]map[string]*zatterav1.EncryptedValue{},
		releases:          map[string]*zatterav1.Release{},
		deployments:       map[string]*zatterav1.Deployment{},
		builds:            map[string]*zatterav1.Build{},
		nodes:             map[string]*zatterav1.Node{},
		joinTokens:        map[string]*zatterav1.JoinToken{},
		assignments:       map[string]*zatterav1.Assignment{},
		assignmentsByNode: map[string]map[string]struct{}{},
		tokens:            map[string]*zatterav1.Token{},
		tokensByHash:      map[string]string{},
		domains:           map[string]*zatterav1.Domain{},
		domainsByHostname: map[string]string{},
		kv:                map[string]kvEntry{},
		dnsProviders:      map[string]*zatterav1.DNSProviderConfig{},
		volumes:           map[string]*zatterav1.Volume{},
		volumeSnapshots:   map[string]*zatterav1.VolumeSnapshot{},
		backupRecords:     map[string]*zatterav1.BackupRecord{},
		networkAllocs:     map[string]*internalv1.NetworkAllocation{},
		serviceVIPs:       map[string]string{},
		jobs:              map[string]*zatterav1.Job{},
		alertRules:        map[string]*zatterav1.AlertRule{},
		notifyChannels:    map[string]*zatterav1.NotificationChannel{},
		appliedSet:        map[string]struct{}{},
		hub:               newHub(),
	}
}

// Version returns the monotonic mutation counter.
func (s *Store) Version() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.version
}

// Watch subscribes to change notifications for the given kinds (all kinds if
// empty). See Hub for delivery semantics.
func (s *Store) Watch(kinds ...Kind) *Subscription {
	return s.hub.subscribe(kinds)
}

// clone deep-copies a proto message preserving its concrete type.
func clone[T proto.Message](m T) T {
	return proto.Clone(m).(T)
}

// cloneAll deep-copies a slice of proto messages.
func cloneAll[T proto.Message](in []T) []T {
	out := make([]T, len(in))
	for i, m := range in {
		out[i] = clone(m)
	}
	return out
}

// touch must be called (with the write lock held) by every mutating method.
func (s *Store) touch(kind Kind, id string) {
	s.version++
	s.hub.publish(Change{Kind: kind, ID: id})
}
