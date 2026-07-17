package raftstore

import (
	"fmt"

	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
)

// apply dispatches one decoded command to the state store. Handlers are
// dumb, deterministic writes: semantic validation (uniqueness, references,
// permissions) happens in the API layer before proposing, and anything that
// MUST hold under concurrency is re-checked here (e.g. KV CAS).
//
// When adding a mutation: add the oneof case in fsm.proto, regenerate, add
// exactly one case here. Never reorder or renumber.
func (f *FSM) apply(cmd *clusterv1.Command) error {
	s := f.store
	switch m := cmd.Mutation.(type) {

	// --- org / users / projects ---
	case *clusterv1.Command_PutOrg:
		s.SetOrg(m.PutOrg.GetOrg())
	case *clusterv1.Command_PutUser:
		s.PutUser(m.PutUser.GetUser())
	case *clusterv1.Command_DeleteUser:
		s.DeleteUser(m.DeleteUser.GetId())
	case *clusterv1.Command_PutProject:
		s.PutProject(m.PutProject.GetProject())
	case *clusterv1.Command_DeleteProject:
		s.DeleteProject(m.DeleteProject.GetId())
	case *clusterv1.Command_PutProjectMember:
		s.PutProjectMember(m.PutProjectMember.GetMember())
	case *clusterv1.Command_DeleteProjectMember:
		s.DeleteProjectMember(m.DeleteProjectMember.GetProjectId(), m.DeleteProjectMember.GetUserId())

	// --- apps / environments / env vars ---
	case *clusterv1.Command_PutApp:
		s.PutApp(m.PutApp.GetApp())
	case *clusterv1.Command_DeleteApp:
		s.DeleteApp(m.DeleteApp.GetId())
	case *clusterv1.Command_PutEnvironment:
		s.PutEnvironment(m.PutEnvironment.GetEnvironment())
	case *clusterv1.Command_DeleteEnvironment:
		s.DeleteEnvironment(m.DeleteEnvironment.GetId())
	case *clusterv1.Command_SetEnvVars:
		s.SetEnvVars(m.SetEnvVars.GetEnvironmentId(), m.SetEnvVars.GetSet(), m.SetEnvVars.GetUnset())

	// --- releases / deployments / builds ---
	case *clusterv1.Command_PutRelease:
		s.PutRelease(m.PutRelease.GetRelease())
	case *clusterv1.Command_DeleteRelease:
		s.DeleteRelease(m.DeleteRelease.GetId())
	case *clusterv1.Command_PutDeployment:
		s.PutDeployment(m.PutDeployment.GetDeployment())
	case *clusterv1.Command_SetDeploymentPhase:
		return f.applySetDeploymentPhase(cmd.GetTime(), m.SetDeploymentPhase)
	case *clusterv1.Command_PromoteRelease:
		return f.applyPromoteRelease(m.PromoteRelease)
	case *clusterv1.Command_PutBuild:
		s.PutBuild(m.PutBuild.GetBuild())

	// --- nodes / join / assignments ---
	case *clusterv1.Command_PutNode:
		s.PutNode(m.PutNode.GetNode())
	case *clusterv1.Command_DeleteNode:
		s.DeleteNode(m.DeleteNode.GetId())
	case *clusterv1.Command_SetNodeStatus:
		return f.applySetNodeStatus(m.SetNodeStatus)
	case *clusterv1.Command_PutJoinToken:
		s.PutJoinToken(m.PutJoinToken.GetToken())
	case *clusterv1.Command_ConsumeJoinToken:
		return f.applyConsumeJoinToken(m.ConsumeJoinToken)
	case *clusterv1.Command_PutAssignments:
		s.PutAssignments(m.PutAssignments.GetAssignments())
	case *clusterv1.Command_DeleteAssignments:
		s.DeleteAssignments(m.DeleteAssignments.GetAssignmentIds())
	case *clusterv1.Command_SetAssignmentsObserved:
		s.SetAssignmentObserved(m.SetAssignmentsObserved.GetNodeId(), m.SetAssignmentsObserved.GetObserved())

	// --- auth ---
	case *clusterv1.Command_PutToken:
		s.PutToken(m.PutToken.GetToken())
	case *clusterv1.Command_DeleteToken:
		s.DeleteToken(m.DeleteToken.GetId())
	case *clusterv1.Command_TouchTokens:
		lastUsed := make(map[string]int64, len(m.TouchTokens.GetLastUsed()))
		for id, ts := range m.TouchTokens.GetLastUsed() {
			lastUsed[id] = ts.AsTime().UnixMilli()
		}
		s.TouchTokens(lastUsed)
	case *clusterv1.Command_PutClusterKeyMaterial:
		s.SetClusterKeyMaterial(m.PutClusterKeyMaterial.GetMaterial())

	// --- domains / kv / dns ---
	case *clusterv1.Command_PutDomain:
		s.PutDomain(m.PutDomain.GetDomain())
	case *clusterv1.Command_DeleteDomain:
		s.DeleteDomain(m.DeleteDomain.GetId())
	case *clusterv1.Command_PutKv:
		var exp int64
		if m.PutKv.GetExpiresAt() != nil {
			exp = m.PutKv.GetExpiresAt().AsTime().UnixMilli()
		}
		_, err := s.PutKV(m.PutKv.GetKey(), m.PutKv.GetValue(), m.PutKv.GetExpectedVersion(), exp)
		return err
	case *clusterv1.Command_DeleteKv:
		return s.DeleteKV(m.DeleteKv.GetKey(), m.DeleteKv.GetExpectedVersion())
	case *clusterv1.Command_PutDnsProvider:
		s.PutDNSProvider(m.PutDnsProvider.GetProvider())
	case *clusterv1.Command_DeleteDnsProvider:
		s.DeleteDNSProvider(m.DeleteDnsProvider.GetId())

	// --- volumes / backup / allocations ---
	case *clusterv1.Command_PutVolume:
		s.PutVolume(m.PutVolume.GetVolume())
	case *clusterv1.Command_DeleteVolume:
		s.DeleteVolume(m.DeleteVolume.GetId())
	case *clusterv1.Command_PutVolumeLease:
		s.SetVolumeLease(m.PutVolumeLease.GetVolumeId(), m.PutVolumeLease.GetLease())
	case *clusterv1.Command_PutVolumeSnapshot:
		s.PutVolumeSnapshot(m.PutVolumeSnapshot.GetSnapshot())
	case *clusterv1.Command_DeleteVolumeSnapshot:
		s.DeleteVolumeSnapshot(m.DeleteVolumeSnapshot.GetId())
	case *clusterv1.Command_PutBackupConfig:
		s.SetBackupConfig(m.PutBackupConfig.GetConfig())
	case *clusterv1.Command_PutBackupRecord:
		s.PutBackupRecord(m.PutBackupRecord.GetRecord())
	case *clusterv1.Command_PutNetworkAllocation:
		a := m.PutNetworkAllocation
		s.SetNetworkAllocation(a.GetProjectId(), a.GetEnvironmentId(), a.GetNodeId(), a.GetSubnetCidr())
	case *clusterv1.Command_PutServiceVip:
		s.SetServiceVIP(m.PutServiceVip.GetEnvironmentId(), m.PutServiceVip.GetVip())

	// --- jobs ---
	case *clusterv1.Command_PutJob:
		s.PutJob(m.PutJob.GetJob())

	// --- alerts / events / audit ---
	case *clusterv1.Command_PutAlertRule:
		s.PutAlertRule(m.PutAlertRule.GetRule())
	case *clusterv1.Command_DeleteAlertRule:
		s.DeleteAlertRule(m.DeleteAlertRule.GetId())
	case *clusterv1.Command_PutNotificationChannel:
		s.PutNotificationChannel(m.PutNotificationChannel.GetChannel())
	case *clusterv1.Command_DeleteNotificationChannel:
		s.DeleteNotificationChannel(m.DeleteNotificationChannel.GetId())
	case *clusterv1.Command_AppendEvents:
		s.AppendEvents(m.AppendEvents.GetEvents())
	case *clusterv1.Command_AppendAudit:
		s.AppendAudit(m.AppendAudit.GetEntries())

	case nil:
		return fmt.Errorf("raftstore: command without mutation (request_id=%s)", cmd.GetRequestId())
	default:
		// A newer node proposed a mutation this binary doesn't know. With
		// additive-only evolution this happens only during rolling upgrades;
		// skipping is wrong (state divergence), so surface loudly.
		return fmt.Errorf("raftstore: unknown mutation %T — upgrade this node", m)
	}
	return nil
}

func (f *FSM) applySetDeploymentPhase(now *timestamppb.Timestamp, m *clusterv1.SetDeploymentPhase) error {
	d, ok := f.store.Deployment(m.GetDeploymentId())
	if !ok {
		return fmt.Errorf("raftstore: deployment %s not found", m.GetDeploymentId())
	}
	// meta.updated_at marks phase entry — the orchestrator times phase deadlines
	// (healthcheck grace, drain) from it, so bump it deterministically here.
	if now != nil {
		d.GetMeta().UpdatedAt = now
	}
	d.Phase = m.GetPhase()
	if m.GetError() != "" {
		d.Error = m.GetError()
	}
	if m.GetPromotedAt() != nil {
		d.PromotedAt = m.GetPromotedAt()
	}
	if m.GetDrainDeadline() != nil {
		d.DrainDeadline = m.GetDrainDeadline()
	}
	f.store.PutDeployment(d)
	return nil
}

// applyPromoteRelease is the atomic red/green traffic switch.
func (f *FSM) applyPromoteRelease(m *clusterv1.PromoteRelease) error {
	env, ok := f.store.Environment(m.GetEnvironmentId())
	if !ok {
		return fmt.Errorf("raftstore: environment %s not found", m.GetEnvironmentId())
	}
	if _, ok := f.store.Release(m.GetReleaseId()); !ok {
		return fmt.Errorf("raftstore: release %s not found", m.GetReleaseId())
	}
	env.ActiveReleaseId = m.GetReleaseId()
	env.RouteGeneration++
	f.store.PutEnvironment(env)
	return nil
}

func (f *FSM) applySetNodeStatus(m *clusterv1.SetNodeStatus) error {
	n, ok := f.store.Node(m.GetNodeId())
	if !ok {
		return fmt.Errorf("raftstore: node %s not found", m.GetNodeId())
	}
	n.Status = m.GetStatus()
	if m.GetLastHeartbeatAt() != nil {
		n.LastHeartbeatAt = m.GetLastHeartbeatAt()
	}
	if m.GetSchedulableSet() {
		n.Schedulable = m.GetSchedulable()
	}
	f.store.PutNode(n)
	return nil
}

func (f *FSM) applyConsumeJoinToken(m *clusterv1.ConsumeJoinToken) error {
	for _, t := range f.store.ListJoinTokens() {
		if t.GetMeta().GetId() == m.GetTokenId() {
			if t.GetSingleUse() && t.GetUsed() {
				return fmt.Errorf("raftstore: join token %s already used", m.GetTokenId())
			}
			t.Used = true
			f.store.PutJoinToken(t)
			return nil
		}
	}
	return fmt.Errorf("raftstore: join token %s not found", m.GetTokenId())
}
