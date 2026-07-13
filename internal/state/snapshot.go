package state

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	internalv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

func timestampFromUnixMs(ms int64) *timestamppb.Timestamp {
	return timestamppb.New(time.UnixMilli(ms))
}

// resetLocked empties every collection (write lock held). Keeps hub, version.
func (s *Store) resetLocked() {
	s.org = nil
	s.clusterKey = nil
	s.users = map[string]*zatterav1.User{}
	s.usersByEmail = map[string]string{}
	s.projects = map[string]*zatterav1.Project{}
	s.projectMembers = map[string]map[string]*zatterav1.ProjectMember{}
	s.apps = map[string]*zatterav1.App{}
	s.environments = map[string]*zatterav1.Environment{}
	s.envVars = map[string]map[string]*zatterav1.EncryptedValue{}
	s.releases = map[string]*zatterav1.Release{}
	s.deployments = map[string]*zatterav1.Deployment{}
	s.builds = map[string]*zatterav1.Build{}
	s.nodes = map[string]*zatterav1.Node{}
	s.joinTokens = map[string]*zatterav1.JoinToken{}
	s.assignments = map[string]*zatterav1.Assignment{}
	s.assignmentsByNode = map[string]map[string]struct{}{}
	s.tokens = map[string]*zatterav1.Token{}
	s.tokensByHash = map[string]string{}
	s.domains = map[string]*zatterav1.Domain{}
	s.domainsByHostname = map[string]string{}
	s.kv = map[string]kvEntry{}
	s.dnsProviders = map[string]*zatterav1.DNSProviderConfig{}
	s.volumes = map[string]*zatterav1.Volume{}
	s.volumeSnapshots = map[string]*zatterav1.VolumeSnapshot{}
	s.backupConfig = nil
	s.backupRecords = map[string]*zatterav1.BackupRecord{}
	s.networkAllocs = map[string]*internalv1.NetworkAllocation{}
	s.serviceVIPs = map[string]string{}
	s.jobs = map[string]*zatterav1.Job{}
	s.alertRules = map[string]*zatterav1.AlertRule{}
	s.notifyChannels = map[string]*zatterav1.NotificationChannel{}
	s.events = nil
	s.audit = nil
	s.appliedRequests = nil
	s.appliedSet = map[string]struct{}{}
}

// SnapshotProto serializes the entire store into a Snapshot message
// (ADR-0004). Called by the Raft FSM under snapshotting.
func (s *Store) SnapshotProto(fsmIndex uint64) *internalv1.Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snap := &internalv1.Snapshot{FsmIndex: fsmIndex}
	if s.org != nil {
		snap.Org = clone(s.org)
	}
	if s.clusterKey != nil {
		snap.ClusterKeyMaterial = clone(s.clusterKey)
	}
	snap.Users = cloneMapValues(s.users)
	snap.Projects = cloneMapValues(s.projects)
	for _, byUser := range s.projectMembers {
		for _, m := range byUser {
			snap.ProjectMembers = append(snap.ProjectMembers, clone(m))
		}
	}
	snap.Apps = cloneMapValues(s.apps)
	snap.Environments = cloneMapValues(s.environments)
	for envID, vars := range s.envVars {
		for k, v := range vars {
			snap.EnvVars = append(snap.EnvVars, &internalv1.EnvVarEntry{
				EnvironmentId: envID,
				Key:           k,
				Value:         clone(v),
			})
		}
	}
	snap.Releases = cloneMapValues(s.releases)
	snap.Deployments = cloneMapValues(s.deployments)
	snap.Builds = cloneMapValues(s.builds)
	snap.Nodes = cloneMapValues(s.nodes)
	snap.JoinTokens = cloneMapValues(s.joinTokens)
	snap.Assignments = cloneMapValues(s.assignments)
	snap.Tokens = cloneMapValues(s.tokens)
	snap.Domains = cloneMapValues(s.domains)
	for k, e := range s.kv {
		entry := &internalv1.KVEntry{Key: k, Value: append([]byte(nil), e.value...), Version: e.version}
		if e.expiresAt != 0 {
			entry.ExpiresAt = timestampFromUnixMs(e.expiresAt)
		}
		snap.Kv = append(snap.Kv, entry)
	}
	snap.DnsProviders = cloneMapValues(s.dnsProviders)
	snap.Volumes = cloneMapValues(s.volumes)
	snap.VolumeSnapshots = cloneMapValues(s.volumeSnapshots)
	if s.backupConfig != nil {
		snap.BackupConfig = clone(s.backupConfig)
	}
	snap.BackupRecords = cloneMapValues(s.backupRecords)
	for _, a := range s.networkAllocs {
		snap.NetworkAllocations = append(snap.NetworkAllocations, clone(a))
	}
	for envID, vip := range s.serviceVIPs {
		snap.ServiceVips = append(snap.ServiceVips, &internalv1.ServiceVIP{EnvironmentId: envID, Vip: vip})
	}
	snap.Jobs = cloneMapValues(s.jobs)
	snap.AlertRules = cloneMapValues(s.alertRules)
	snap.NotificationChannels = cloneMapValues(s.notifyChannels)
	snap.Events = cloneAll(s.events)
	snap.Audit = cloneAll(s.audit)
	snap.AppliedRequests = cloneAll(s.appliedRequests)
	return snap
}

// RestoreProto replaces the entire store content with the snapshot's.
// Watch subscribers receive a single wildcard notification per kind.
func (s *Store) RestoreProto(snap *internalv1.Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.resetLocked()

	if snap.GetOrg() != nil {
		s.org = clone(snap.GetOrg())
	}
	if snap.GetClusterKeyMaterial() != nil {
		s.clusterKey = clone(snap.GetClusterKeyMaterial())
	}
	for _, u := range snap.GetUsers() {
		s.users[u.GetMeta().GetId()] = clone(u)
		s.usersByEmail[u.GetEmail()] = u.GetMeta().GetId()
	}
	for _, p := range snap.GetProjects() {
		s.projects[p.GetMeta().GetId()] = clone(p)
	}
	for _, m := range snap.GetProjectMembers() {
		byUser, ok := s.projectMembers[m.GetProjectId()]
		if !ok {
			byUser = map[string]*zatterav1.ProjectMember{}
			s.projectMembers[m.GetProjectId()] = byUser
		}
		byUser[m.GetUserId()] = clone(m)
	}
	for _, a := range snap.GetApps() {
		s.apps[a.GetMeta().GetId()] = clone(a)
	}
	for _, e := range snap.GetEnvironments() {
		s.environments[e.GetMeta().GetId()] = clone(e)
	}
	for _, ev := range snap.GetEnvVars() {
		vars, ok := s.envVars[ev.GetEnvironmentId()]
		if !ok {
			vars = map[string]*zatterav1.EncryptedValue{}
			s.envVars[ev.GetEnvironmentId()] = vars
		}
		vars[ev.GetKey()] = clone(ev.GetValue())
	}
	for _, r := range snap.GetReleases() {
		s.releases[r.GetMeta().GetId()] = clone(r)
	}
	for _, d := range snap.GetDeployments() {
		s.deployments[d.GetMeta().GetId()] = clone(d)
	}
	for _, b := range snap.GetBuilds() {
		s.builds[b.GetMeta().GetId()] = clone(b)
	}
	for _, n := range snap.GetNodes() {
		s.nodes[n.GetMeta().GetId()] = clone(n)
	}
	for _, t := range snap.GetJoinTokens() {
		s.joinTokens[t.GetMeta().GetId()] = clone(t)
	}
	for _, a := range snap.GetAssignments() {
		id := a.GetMeta().GetId()
		s.assignments[id] = clone(a)
		byNode, ok := s.assignmentsByNode[a.GetNodeId()]
		if !ok {
			byNode = map[string]struct{}{}
			s.assignmentsByNode[a.GetNodeId()] = byNode
		}
		byNode[id] = struct{}{}
	}
	for _, t := range snap.GetTokens() {
		s.tokens[t.GetMeta().GetId()] = clone(t)
		s.tokensByHash[t.GetSecretHash()] = t.GetMeta().GetId()
	}
	for _, d := range snap.GetDomains() {
		s.domains[d.GetMeta().GetId()] = clone(d)
		s.domainsByHostname[d.GetHostname()] = d.GetMeta().GetId()
	}
	for _, e := range snap.GetKv() {
		var exp int64
		if e.GetExpiresAt() != nil {
			exp = e.GetExpiresAt().AsTime().UnixMilli()
		}
		s.kv[e.GetKey()] = kvEntry{
			value:     append([]byte(nil), e.GetValue()...),
			version:   e.GetVersion(),
			expiresAt: exp,
		}
	}
	for _, p := range snap.GetDnsProviders() {
		s.dnsProviders[p.GetMeta().GetId()] = clone(p)
	}
	for _, v := range snap.GetVolumes() {
		s.volumes[v.GetMeta().GetId()] = clone(v)
	}
	for _, vs := range snap.GetVolumeSnapshots() {
		s.volumeSnapshots[vs.GetMeta().GetId()] = clone(vs)
	}
	if snap.GetBackupConfig() != nil {
		s.backupConfig = clone(snap.GetBackupConfig())
	}
	for _, r := range snap.GetBackupRecords() {
		s.backupRecords[r.GetMeta().GetId()] = clone(r)
	}
	for _, a := range snap.GetNetworkAllocations() {
		s.networkAllocs[networkAllocKey(a.GetProjectId(), a.GetEnvironmentId(), a.GetNodeId())] = clone(a)
	}
	for _, v := range snap.GetServiceVips() {
		s.serviceVIPs[v.GetEnvironmentId()] = v.GetVip()
	}
	for _, j := range snap.GetJobs() {
		s.jobs[j.GetMeta().GetId()] = clone(j)
	}
	for _, r := range snap.GetAlertRules() {
		s.alertRules[r.GetMeta().GetId()] = clone(r)
	}
	for _, c := range snap.GetNotificationChannels() {
		s.notifyChannels[c.GetMeta().GetId()] = clone(c)
	}
	s.events = cloneAll(snap.GetEvents())
	s.audit = cloneAll(snap.GetAudit())
	s.appliedRequests = cloneAll(snap.GetAppliedRequests())
	for _, ar := range s.appliedRequests {
		s.appliedSet[ar.GetRequestId()] = struct{}{}
	}

	// One coarse notification per kind: subscribers re-list everything.
	for _, k := range []Kind{
		KindOrg, KindUser, KindProject, KindProjectMember, KindApp, KindEnvironment,
		KindEnvVar, KindRelease, KindDeployment, KindBuild, KindNode, KindJoinToken,
		KindAssignment, KindToken, KindDomain, KindKV, KindDNSProvider, KindVolume,
		KindVolumeSnapshot, KindBackup, KindNetworkAlloc, KindServiceVIP, KindJob,
		KindAlertRule, KindNotifyChannel, KindEvent, KindClusterKey,
	} {
		s.touch(k, "*")
	}
}
