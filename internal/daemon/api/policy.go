package api

// methodAuth is the fail-closed authorization tier for every gRPC method. Any
// method NOT in this table is denied by the auth interceptor, and
// ValidateMethodTable rejects startup if a registered method is missing here.
//
// Project-scoped methods listed as reqUser are further gated per-project by the
// RBAC interceptor (T-05); the tier here only decides "authenticated user vs
// node vs admin". Node-tier methods are the node↔control contracts.
var methodAuth = map[string]Requirement{
	// --- AuthService ---
	"/zattera.v1.AuthService/Login":       reqPublic,
	"/zattera.v1.AuthService/WhoAmI":      reqUser,
	"/zattera.v1.AuthService/Unseal":      reqAdmin,
	"/zattera.v1.AuthService/CreateToken": reqUser,
	"/zattera.v1.AuthService/ListTokens":  reqUser,
	"/zattera.v1.AuthService/RevokeToken": reqUser,
	"/zattera.v1.AuthService/CreateUser":  reqAdmin,
	"/zattera.v1.AuthService/ListUsers":   reqAdmin,

	// --- ProjectService (project scope enforced by RBAC in T-05) ---
	"/zattera.v1.ProjectService/CreateProject": reqUser,
	"/zattera.v1.ProjectService/ListProjects":  reqUser,
	"/zattera.v1.ProjectService/GetProject":    reqUser,
	"/zattera.v1.ProjectService/DeleteProject": reqUser,
	"/zattera.v1.ProjectService/AddMember":     reqUser,
	"/zattera.v1.ProjectService/RemoveMember":  reqUser,
	"/zattera.v1.ProjectService/ListMembers":   reqUser,

	// --- AppService ---
	"/zattera.v1.AppService/CreateApp":      reqUser,
	"/zattera.v1.AppService/ListApps":       reqUser,
	"/zattera.v1.AppService/GetApp":         reqUser,
	"/zattera.v1.AppService/DeleteApp":      reqUser,
	"/zattera.v1.AppService/ApplyAppConfig": reqUser,
	"/zattera.v1.AppService/SetEnvVars":     reqUser,
	"/zattera.v1.AppService/GetEnvVars":     reqUser,
	"/zattera.v1.AppService/SetReplicas":    reqUser,

	// --- DeployService ---
	"/zattera.v1.DeployService/Deploy":          reqUser,
	"/zattera.v1.DeployService/Rollback":        reqUser,
	"/zattera.v1.DeployService/GetDeployment":   reqUser,
	"/zattera.v1.DeployService/ListDeployments": reqUser,
	"/zattera.v1.DeployService/ListReleases":    reqUser,
	"/zattera.v1.DeployService/ListInstances":   reqUser,
	"/zattera.v1.DeployService/WatchDeployment": reqUser,
	"/zattera.v1.DeployService/UploadSource":    reqUser,

	// --- NodeService (reads open to users; mutations admin) ---
	"/zattera.v1.NodeService/ListNodes":       reqUser,
	"/zattera.v1.NodeService/GetNode":         reqUser,
	"/zattera.v1.NodeService/CreateJoinToken": reqAdmin,
	"/zattera.v1.NodeService/SetNodeLabels":   reqAdmin,
	"/zattera.v1.NodeService/DrainNode":       reqAdmin,
	"/zattera.v1.NodeService/RemoveNode":      reqAdmin,
	"/zattera.v1.NodeService/CordonNode":      reqAdmin,
	"/zattera.v1.NodeService/UncordonNode":    reqAdmin,
	"/zattera.v1.NodeService/UpgradePlan":     reqAdmin,
	"/zattera.v1.NodeService/UpgradeNode":     reqAdmin,

	// --- StateService (cluster-wide desired state; admin) ---
	"/zattera.v1.StateService/Export": reqAdmin,
	"/zattera.v1.StateService/Apply":  reqAdmin,

	// --- AuditService ---
	"/zattera.v1.AuditService/QueryAudit": reqAdmin,
	// ListEvents is readable by any user; the handler scopes non-admins to
	// projects they belong to (T-76).
	"/zattera.v1.AuditService/ListEvents": reqUser,

	// --- DomainService ---
	"/zattera.v1.DomainService/AddDomain":     reqUser,
	"/zattera.v1.DomainService/RemoveDomain":  reqUser,
	"/zattera.v1.DomainService/ListDomains":   reqUser,
	"/zattera.v1.DomainService/SetMiddleware": reqUser,

	// --- VolumeService ---
	"/zattera.v1.VolumeService/CreateVolume":    reqUser,
	"/zattera.v1.VolumeService/DeleteVolume":    reqUser,
	"/zattera.v1.VolumeService/ListVolumes":     reqUser,
	"/zattera.v1.VolumeService/SnapshotVolume":  reqUser,
	"/zattera.v1.VolumeService/ListSnapshots":   reqUser,
	"/zattera.v1.VolumeService/RestoreSnapshot": reqUser,
	"/zattera.v1.VolumeService/ListFiles":       reqUser,
	"/zattera.v1.VolumeService/ReadFile":        reqUser,
	"/zattera.v1.VolumeService/WriteFile":       reqUser,

	// --- JobService ---
	"/zattera.v1.JobService/RunJob":    reqUser,
	"/zattera.v1.JobService/GetJob":    reqUser,
	"/zattera.v1.JobService/ListJobs":  reqUser,
	"/zattera.v1.JobService/CancelJob": reqUser,

	// --- ExecService ---
	"/zattera.v1.ExecService/Exec":        reqUser,
	"/zattera.v1.ExecService/PortForward": reqUser,
	"/zattera.v1.ExecService/Top":         reqUser,

	// --- LogService / MetricsService ---
	"/zattera.v1.LogService/Query":     reqUser,
	"/zattera.v1.MetricsService/Stats": reqUser,

	// --- AlertService ---
	"/zattera.v1.AlertService/PutRule":       reqUser,
	"/zattera.v1.AlertService/DeleteRule":    reqUser,
	"/zattera.v1.AlertService/ListRules":     reqUser,
	"/zattera.v1.AlertService/PutChannel":    reqAdmin,
	"/zattera.v1.AlertService/DeleteChannel": reqAdmin,
	"/zattera.v1.AlertService/ListChannels":  reqAdmin,

	// --- BackupService (admin) ---
	"/zattera.v1.BackupService/SetBackupConfig": reqAdmin,
	"/zattera.v1.BackupService/TriggerBackup":   reqAdmin,
	"/zattera.v1.BackupService/ListBackups":     reqAdmin,

	// --- Node↔control contracts (mTLS node identity) ---
	"/zattera.cluster.v1.AgentSyncService/Sync":              reqNode,
	"/zattera.cluster.v1.MeshService/WatchPeers":             reqNode,
	"/zattera.cluster.v1.MeshService/ReportObservedEndpoint": reqNode,
	"/zattera.cluster.v1.MeshService/PunchStream":            reqNode,
	"/zattera.cluster.v1.MeshService/RequestPunch":           reqNode,
	"/zattera.cluster.v1.RouteService/WatchRoutes":           reqNode,
	"/zattera.cluster.v1.ActivatorService/Activate":          reqNode,

	// JoinService.Join is intentionally reqPublic: the join token IS the auth
	// (T-17), verified inside the handler.
	"/zattera.cluster.v1.JoinService/Join": reqPublic,

	// KeyService hands the cluster data key to an already-enrolled node, so it
	// requires a cluster-signed node cert (reqNode). The handler further
	// restricts it to nodes holding the control role.
	"/zattera.cluster.v1.KeyService/FetchDataKey": reqNode,
}
