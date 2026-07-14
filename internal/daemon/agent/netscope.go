package agent

// NetScope is one (project, env) bridge network this node runs containers on,
// with the gateway IP where the per-network internal DNS resolver binds.
type NetScope struct {
	Gateway   string
	ProjectID string
	EnvID     string
}

// NetworkScopes returns the distinct (project, env) bridge scopes this node
// currently has assignments for — one per bridge gateway. It is the input to
// the internal DNS resolver's Reconcile (F26): the resolver serves
// <svc>.internal on each gateway within that gateway's project+env scope.
func (e *Executor) NetworkScopes() []NetScope {
	set := e.current()
	if set == nil {
		return nil
	}
	runtimes := set.GetRuntime()
	byGateway := map[string]NetScope{}
	for _, a := range set.GetAssignments() {
		subnet := runtimes[a.GetMeta().GetId()].GetSubnetCidr()
		if subnet == "" {
			continue // no per-env bridge (e.g. single-node/dev host networking)
		}
		gw, err := GatewayIP(subnet)
		if err != nil {
			continue
		}
		byGateway[gw] = NetScope{Gateway: gw, ProjectID: a.GetProjectId(), EnvID: a.GetEnvironmentId()}
	}
	out := make([]NetScope, 0, len(byGateway))
	for _, s := range byGateway {
		out = append(out, s)
	}
	return out
}
