package state

import (
	"fmt"
	"maps"
	"sort"
	"strings"

	internalv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

// --- nodes & join tokens ---

func (s *Store) PutNode(n *zatterav1.Node) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := n.GetMeta().GetId()
	s.nodes[id] = clone(n)
	s.touch(KindNode, id)
}

func (s *Store) DeleteNode(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.nodes[id]; ok {
		delete(s.nodes, id)
		s.touch(KindNode, id)
	}
}

func (s *Store) Node(id string) (*zatterav1.Node, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n, ok := s.nodes[id]
	if !ok {
		return nil, false
	}
	return clone(n), true
}

func (s *Store) ListNodes() []*zatterav1.Node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return sortByID(cloneMapValues(s.nodes))
}

func (s *Store) PutJoinToken(t *zatterav1.JoinToken) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := t.GetMeta().GetId()
	s.joinTokens[id] = clone(t)
	s.touch(KindJoinToken, id)
}

func (s *Store) ListJoinTokens() []*zatterav1.JoinToken {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return sortByID(cloneMapValues(s.joinTokens))
}

// --- assignments ---

func (s *Store) PutAssignment(a *zatterav1.Assignment) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.putAssignmentLocked(a)
}

func (s *Store) putAssignmentLocked(a *zatterav1.Assignment) {
	id := a.GetMeta().GetId()
	if prev, ok := s.assignments[id]; ok && prev.GetNodeId() != a.GetNodeId() {
		delete(s.assignmentsByNode[prev.GetNodeId()], id)
	}
	s.assignments[id] = clone(a)
	byNode, ok := s.assignmentsByNode[a.GetNodeId()]
	if !ok {
		byNode = map[string]struct{}{}
		s.assignmentsByNode[a.GetNodeId()] = byNode
	}
	byNode[id] = struct{}{}
	s.touch(KindAssignment, id)
}

// PutAssignments applies a batch in one lock acquisition.
func (s *Store) PutAssignments(as []*zatterav1.Assignment) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, a := range as {
		s.putAssignmentLocked(a)
	}
}

func (s *Store) DeleteAssignments(ids []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		if prev, ok := s.assignments[id]; ok {
			delete(s.assignmentsByNode[prev.GetNodeId()], id)
			delete(s.assignments, id)
			s.touch(KindAssignment, id)
		}
	}
}

func (s *Store) Assignment(id string) (*zatterav1.Assignment, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.assignments[id]
	if !ok {
		return nil, false
	}
	return clone(a), true
}

// SetAssignmentObserved merges an observed status batch for one node.
// Unknown assignment ids are skipped (they may have been deleted since the
// agent reported).
func (s *Store) SetAssignmentObserved(nodeID string, observed map[string]*zatterav1.AssignmentObserved) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, obs := range observed {
		a, ok := s.assignments[id]
		if !ok || a.GetNodeId() != nodeID {
			continue
		}
		next := clone(obs)
		// A coarse liveness RUNNING report must not downgrade the finer
		// HEALTHY/UNHEALTHY state the health monitor owns: the monitor only
		// re-emits on transition, so the downgrade would stick and the router
		// (which serves only HEALTHY endpoints) would drop a live instance.
		// Crashes still surface as FAILED/STOPPED, which are not RUNNING.
		if next.GetState() == zatterav1.InstanceState_INSTANCE_STATE_RUNNING {
			if cur := a.GetObserved().GetState(); cur == zatterav1.InstanceState_INSTANCE_STATE_HEALTHY ||
				cur == zatterav1.InstanceState_INSTANCE_STATE_UNHEALTHY {
				next.State = cur
			}
		}
		a.Observed = next
		// The agent reports the host ports it actually bound; promote them into
		// the assignment so routing/proxy read them from the desired object
		// (T-15). Empty maps (e.g. a stop transition) don't clobber prior ports.
		if len(obs.GetMeshPortBindings()) > 0 {
			a.MeshPortBindings = maps.Clone(obs.GetMeshPortBindings())
		}
		s.touch(KindAssignment, id)
	}
}

func (s *Store) ListAssignmentsByNode(nodeID string) []*zatterav1.Assignment {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := s.assignmentsByNode[nodeID]
	out := make([]*zatterav1.Assignment, 0, len(ids))
	for id := range ids {
		out = append(out, clone(s.assignments[id]))
	}
	return sortByID(out)
}

// ListAssignments filters by environment (or everything when empty).
func (s *Store) ListAssignments(envID string) []*zatterav1.Assignment {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*zatterav1.Assignment
	for _, a := range s.assignments {
		if envID == "" || a.GetEnvironmentId() == envID {
			out = append(out, clone(a))
		}
	}
	return sortByID(out)
}

// --- tokens ---

func (s *Store) PutToken(t *zatterav1.Token) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := t.GetMeta().GetId()
	if prev, ok := s.tokens[id]; ok {
		delete(s.tokensByHash, prev.GetSecretHash())
	}
	s.tokens[id] = clone(t)
	s.tokensByHash[t.GetSecretHash()] = id
	s.touch(KindToken, id)
}

func (s *Store) DeleteToken(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if prev, ok := s.tokens[id]; ok {
		delete(s.tokensByHash, prev.GetSecretHash())
		delete(s.tokens, id)
		s.touch(KindToken, id)
	}
}

// TokenByHash is the auth hot path.
func (s *Store) TokenByHash(hash string) (*zatterav1.Token, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.tokensByHash[hash]
	if !ok {
		return nil, false
	}
	return clone(s.tokens[id]), true
}

func (s *Store) Token(id string) (*zatterav1.Token, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tokens[id]
	if !ok {
		return nil, false
	}
	return clone(t), true
}

func (s *Store) ListTokens(userID string) []*zatterav1.Token {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*zatterav1.Token
	for _, t := range s.tokens {
		if userID == "" || t.GetUserId() == userID {
			out = append(out, clone(t))
		}
	}
	return sortByID(out)
}

// TouchTokens updates last_used_at in batch (from a periodic flush).
func (s *Store) TouchTokens(lastUsed map[string]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, unixMs := range lastUsed {
		if t, ok := s.tokens[id]; ok {
			if t.LastUsedAt == nil || t.LastUsedAt.AsTime().UnixMilli() < unixMs {
				t.LastUsedAt = timestampFromUnixMs(unixMs)
				s.touch(KindToken, id)
			}
		}
	}
}

// --- domains ---

func (s *Store) PutDomain(d *zatterav1.Domain) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := d.GetMeta().GetId()
	if prev, ok := s.domains[id]; ok {
		delete(s.domainsByHostname, prev.GetHostname())
	}
	s.domains[id] = clone(d)
	s.domainsByHostname[d.GetHostname()] = id
	s.touch(KindDomain, id)
}

func (s *Store) DeleteDomain(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if prev, ok := s.domains[id]; ok {
		delete(s.domainsByHostname, prev.GetHostname())
		delete(s.domains, id)
		s.touch(KindDomain, id)
	}
}

func (s *Store) Domain(id string) (*zatterav1.Domain, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.domains[id]
	if !ok {
		return nil, false
	}
	return clone(d), true
}

// DomainByHostname is used by the route builder and the ACME on-demand policy.
func (s *Store) DomainByHostname(hostname string) (*zatterav1.Domain, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.domainsByHostname[hostname]
	if !ok {
		return nil, false
	}
	return clone(s.domains[id]), true
}

func (s *Store) ListDomains(projectID string) []*zatterav1.Domain {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*zatterav1.Domain
	for _, d := range s.domains {
		if projectID == "" || d.GetProjectId() == projectID {
			out = append(out, clone(d))
		}
	}
	return sortByID(out)
}

// --- replicated KV (certmagic storage + internal locks) ---

// ErrKVConflict is returned when a CAS expectation fails.
var ErrKVConflict = fmt.Errorf("state: kv version conflict")

// PutKV stores a key. expectedVersion: -1 unconditional, 0 requires absent,
// >0 requires the current version to match. Returns the new version.
func (s *Store) PutKV(key string, value []byte, expectedVersion int64, expiresAtUnixMs int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, exists := s.kv[key]
	if expectedVersion >= 0 {
		curVersion := int64(0)
		if exists {
			curVersion = cur.version
		}
		if curVersion != expectedVersion {
			return 0, ErrKVConflict
		}
	}
	next := cur.version + 1
	s.kv[key] = kvEntry{value: append([]byte(nil), value...), version: next, expiresAt: expiresAtUnixMs}
	s.touch(KindKV, key)
	return next, nil
}

func (s *Store) DeleteKV(key string, expectedVersion int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, exists := s.kv[key]
	if !exists {
		return nil
	}
	if expectedVersion >= 0 && cur.version != expectedVersion {
		return ErrKVConflict
	}
	delete(s.kv, key)
	s.touch(KindKV, key)
	return nil
}

// KV returns value, version, expiry (unix ms) for a key.
func (s *Store) KV(key string) (value []byte, version int64, expiresAtUnixMs int64, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, exists := s.kv[key]
	if !exists {
		return nil, 0, 0, false
	}
	return append([]byte(nil), e.value...), e.version, e.expiresAt, true
}

func (s *Store) ListKVPrefix(prefix string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []string
	for k := range s.kv {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// --- dns providers ---

func (s *Store) PutDNSProvider(p *zatterav1.DNSProviderConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := p.GetMeta().GetId()
	s.dnsProviders[id] = clone(p)
	s.touch(KindDNSProvider, id)
}

func (s *Store) DeleteDNSProvider(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.dnsProviders[id]; ok {
		delete(s.dnsProviders, id)
		s.touch(KindDNSProvider, id)
	}
}

func (s *Store) ListDNSProviders() []*zatterav1.DNSProviderConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return sortByID(cloneMapValues(s.dnsProviders))
}

// --- volumes & snapshots ---

func (s *Store) PutVolume(v *zatterav1.Volume) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := v.GetMeta().GetId()
	s.volumes[id] = clone(v)
	s.touch(KindVolume, id)
}

func (s *Store) DeleteVolume(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.volumes[id]; ok {
		delete(s.volumes, id)
		s.touch(KindVolume, id)
	}
}

func (s *Store) Volume(id string) (*zatterav1.Volume, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.volumes[id]
	if !ok {
		return nil, false
	}
	return clone(v), true
}

func (s *Store) VolumeByName(projectID, envID, name string) (*zatterav1.Volume, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, v := range s.volumes {
		if v.GetProjectId() == projectID && v.GetEnvironmentId() == envID && v.GetName() == name {
			return clone(v), true
		}
	}
	return nil, false
}

func (s *Store) ListVolumes(projectID string) []*zatterav1.Volume {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*zatterav1.Volume
	for _, v := range s.volumes {
		if projectID == "" || v.GetProjectId() == projectID {
			out = append(out, clone(v))
		}
	}
	return sortByID(out)
}

func (s *Store) SetVolumeLease(volumeID string, lease *zatterav1.VolumeLease) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.volumes[volumeID]; ok {
		if lease == nil {
			v.Lease = nil
		} else {
			v.Lease = clone(lease)
		}
		s.touch(KindVolume, volumeID)
	}
}

func (s *Store) PutVolumeSnapshot(snap *zatterav1.VolumeSnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := snap.GetMeta().GetId()
	s.volumeSnapshots[id] = clone(snap)
	s.touch(KindVolumeSnapshot, id)
}

func (s *Store) DeleteVolumeSnapshot(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.volumeSnapshots[id]; ok {
		delete(s.volumeSnapshots, id)
		s.touch(KindVolumeSnapshot, id)
	}
}

func (s *Store) ListVolumeSnapshots(volumeID string) []*zatterav1.VolumeSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*zatterav1.VolumeSnapshot
	for _, snap := range s.volumeSnapshots {
		if volumeID == "" || snap.GetVolumeId() == volumeID {
			out = append(out, clone(snap))
		}
	}
	return sortByID(out)
}

// --- backup ---

func (s *Store) SetBackupConfig(c *zatterav1.BackupConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.backupConfig = clone(c)
	s.touch(KindBackup, "config")
}

func (s *Store) BackupConfig() (*zatterav1.BackupConfig, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.backupConfig == nil {
		return nil, false
	}
	return clone(s.backupConfig), true
}

func (s *Store) PutBackupRecord(r *zatterav1.BackupRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := r.GetMeta().GetId()
	s.backupRecords[id] = clone(r)
	s.touch(KindBackup, id)
}

func (s *Store) ListBackupRecords() []*zatterav1.BackupRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return sortByID(cloneMapValues(s.backupRecords))
}

// --- network allocations & service VIPs ---

func networkAllocKey(projectID, envID, nodeID string) string {
	return projectID + "/" + envID + "/" + nodeID
}

func (s *Store) SetNetworkAllocation(projectID, envID, nodeID, subnetCIDR string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := networkAllocKey(projectID, envID, nodeID)
	if subnetCIDR == "" {
		delete(s.networkAllocs, key)
	} else {
		s.networkAllocs[key] = &internalv1.NetworkAllocation{
			ProjectId:     projectID,
			EnvironmentId: envID,
			NodeId:        nodeID,
			SubnetCidr:    subnetCIDR,
		}
	}
	s.touch(KindNetworkAlloc, key)
}

func (s *Store) NetworkAllocation(projectID, envID, nodeID string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.networkAllocs[networkAllocKey(projectID, envID, nodeID)]
	if !ok {
		return "", false
	}
	return a.GetSubnetCidr(), true
}

func (s *Store) ListNetworkAllocations() []*internalv1.NetworkAllocation {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*internalv1.NetworkAllocation, 0, len(s.networkAllocs))
	for _, a := range s.networkAllocs {
		out = append(out, clone(a))
	}
	sort.Slice(out, func(i, j int) bool {
		return networkAllocKey(out[i].GetProjectId(), out[i].GetEnvironmentId(), out[i].GetNodeId()) <
			networkAllocKey(out[j].GetProjectId(), out[j].GetEnvironmentId(), out[j].GetNodeId())
	})
	return out
}

func (s *Store) SetServiceVIP(envID, vip string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if vip == "" {
		delete(s.serviceVIPs, envID)
	} else {
		s.serviceVIPs[envID] = vip
	}
	s.touch(KindServiceVIP, envID)
}

func (s *Store) ServiceVIP(envID string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	vip, ok := s.serviceVIPs[envID]
	return vip, ok
}

func (s *Store) ListServiceVIPs() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]string, len(s.serviceVIPs))
	for k, v := range s.serviceVIPs {
		out[k] = v
	}
	return out
}

// --- jobs ---

func (s *Store) PutJob(j *zatterav1.Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := j.GetMeta().GetId()
	s.jobs[id] = clone(j)
	s.touch(KindJob, id)
}

func (s *Store) Job(id string) (*zatterav1.Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[id]
	if !ok {
		return nil, false
	}
	return clone(j), true
}

func (s *Store) ListJobs(projectID, envID string) []*zatterav1.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*zatterav1.Job
	for _, j := range s.jobs {
		if projectID != "" && j.GetProjectId() != projectID {
			continue
		}
		if envID != "" && j.GetEnvironmentId() != envID {
			continue
		}
		out = append(out, clone(j))
	}
	return sortByID(out)
}

// --- alerts ---

func (s *Store) PutAlertRule(r *zatterav1.AlertRule) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := r.GetMeta().GetId()
	s.alertRules[id] = clone(r)
	s.touch(KindAlertRule, id)
}

func (s *Store) DeleteAlertRule(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.alertRules[id]; ok {
		delete(s.alertRules, id)
		s.touch(KindAlertRule, id)
	}
}

func (s *Store) ListAlertRules() []*zatterav1.AlertRule {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return sortByID(cloneMapValues(s.alertRules))
}

func (s *Store) PutNotificationChannel(c *zatterav1.NotificationChannel) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := c.GetMeta().GetId()
	s.notifyChannels[id] = clone(c)
	s.touch(KindNotifyChannel, id)
}

func (s *Store) DeleteNotificationChannel(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.notifyChannels[id]; ok {
		delete(s.notifyChannels, id)
		s.touch(KindNotifyChannel, id)
	}
}

func (s *Store) ListNotificationChannels() []*zatterav1.NotificationChannel {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return sortByID(cloneMapValues(s.notifyChannels))
}

// --- events & audit rings ---

func (s *Store) AppendEvents(events []*zatterav1.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range events {
		s.events = append(s.events, clone(e))
		s.touch(KindEvent, e.GetMeta().GetId())
	}
	if over := len(s.events) - eventRingCap; over > 0 {
		s.events = append([]*zatterav1.Event(nil), s.events[over:]...)
	}
}

func (s *Store) ListEvents(limit int) []*zatterav1.Event {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := len(s.events)
	if limit <= 0 || limit > n {
		limit = n
	}
	return cloneAll(s.events[n-limit:])
}

// AuditSince returns every audit entry created at or after sinceMs, in append
// order (oldest first). The archiver walks the ring forward from its cursor
// with this; unlike QueryAudit it is uncapped, because skipping entries would
// silently lose them from the archive.
func (s *Store) AuditSince(sinceMs int64) []*zatterav1.AuditEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*zatterav1.AuditEntry
	for _, e := range s.audit {
		if e.GetMeta().GetCreatedAt().AsTime().UnixMilli() >= sinceMs {
			out = append(out, clone(e))
		}
	}
	return out
}

// EventsSince is AuditSince for the event ring.
func (s *Store) EventsSince(sinceMs int64) []*zatterav1.Event {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*zatterav1.Event
	for _, e := range s.events {
		if e.GetMeta().GetCreatedAt().AsTime().UnixMilli() >= sinceMs {
			out = append(out, clone(e))
		}
	}
	return out
}

// QueryEvents returns the newest events matching the filter, newest first —
// the same ordering contract as QueryAudit, so the two read consistently.
// (ListEvents keeps its append-order tail for the alert engine, which replays
// events chronologically.)
func (s *Store) QueryEvents(filter func(*zatterav1.Event) bool, limit int) []*zatterav1.Event {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit <= 0 {
		limit = 100
	}
	var out []*zatterav1.Event
	for i := len(s.events) - 1; i >= 0 && len(out) < limit; i-- {
		if filter == nil || filter(s.events[i]) {
			out = append(out, clone(s.events[i]))
		}
	}
	return out
}

func (s *Store) AppendAudit(entries []*zatterav1.AuditEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range entries {
		s.audit = append(s.audit, clone(e))
	}
	if over := len(s.audit) - auditRingCap; over > 0 {
		s.audit = append([]*zatterav1.AuditEntry(nil), s.audit[over:]...)
	}
	s.touch(KindEvent, "audit")
}

// QueryAudit returns the newest entries matching the filter, newest first.
func (s *Store) QueryAudit(filter func(*zatterav1.AuditEntry) bool, limit int) []*zatterav1.AuditEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit <= 0 {
		limit = 100
	}
	var out []*zatterav1.AuditEntry
	for i := len(s.audit) - 1; i >= 0 && len(out) < limit; i-- {
		if filter == nil || filter(s.audit[i]) {
			out = append(out, clone(s.audit[i]))
		}
	}
	return out
}

// --- applied-request idempotency ring ---

// MarkApplied records a request id; returns false if it was already applied.
func (s *Store) MarkApplied(requestID string, raftIndex uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.appliedSet[requestID]; dup {
		return false
	}
	s.appliedSet[requestID] = struct{}{}
	s.appliedRequests = append(s.appliedRequests, &internalv1.AppliedRequest{
		RequestId: requestID,
		RaftIndex: raftIndex,
	})
	if over := len(s.appliedRequests) - appliedRingCap; over > 0 {
		for _, old := range s.appliedRequests[:over] {
			delete(s.appliedSet, old.GetRequestId())
		}
		s.appliedRequests = append([]*internalv1.AppliedRequest(nil), s.appliedRequests[over:]...)
	}
	return true
}
