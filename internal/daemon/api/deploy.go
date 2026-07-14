package api

import (
	"context"
	"hash/fnv"
	"sort"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/appconfig"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// DeployServer implements DeployService: it turns image refs (or completed
// builds) into Releases + red/green Deployments. The orchestrator (T-26) drives
// the deployment through its phases; this service only creates work.
type DeployServer struct {
	zatterav1.UnimplementedDeployServiceServer
	store *state.Store
	raft  Applier
	clock clock.Clock
}

// NewDeployServer builds the deploy service.
func NewDeployServer(store *state.Store, raft Applier, clk clock.Clock) *DeployServer {
	return &DeployServer{store: store, raft: raft, clock: clk}
}

// Deploy creates a release from an image ref (or completed build) and a PENDING
// deployment for it.
func (s *DeployServer) Deploy(ctx context.Context, req *zatterav1.DeployRequest) (*zatterav1.Deployment, error) {
	env, err := s.resolveEnv(req.GetEnvironmentId())
	if err != nil {
		return nil, err
	}
	if s.hasActiveDeployment(env.GetMeta().GetId()) {
		return nil, status.Error(codes.FailedPrecondition, "a deployment is already in progress for this environment")
	}

	imageRef, err := s.resolveImage(req)
	if err != nil {
		return nil, err
	}

	rel := s.buildRelease(env, imageRef)
	dep := s.buildDeployment(env, rel.GetMeta().GetId(), false)

	if err := s.commit(ctx, rel, dep); err != nil {
		return nil, err
	}
	return dep, nil
}

// Rollback creates a deployment that re-promotes a prior release (default: the
// release before the current active one). No new release is minted.
func (s *DeployServer) Rollback(ctx context.Context, req *zatterav1.RollbackRequest) (*zatterav1.Deployment, error) {
	env, err := s.resolveEnv(req.GetEnvironmentId())
	if err != nil {
		return nil, err
	}
	if s.hasActiveDeployment(env.GetMeta().GetId()) {
		return nil, status.Error(codes.FailedPrecondition, "a deployment is already in progress for this environment")
	}

	target := req.GetToReleaseId()
	if target == "" {
		prev, ok := s.previousRelease(env)
		if !ok {
			return nil, status.Error(codes.FailedPrecondition, "no previous release to roll back to")
		}
		target = prev
	} else if r, ok := s.store.Release(target); !ok || r.GetEnvironmentId() != env.GetMeta().GetId() {
		return nil, status.Error(codes.NotFound, "target release not found in this environment")
	}

	dep := s.buildDeployment(env, target, true)
	if err := s.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_PutDeployment{PutDeployment: &clusterv1.PutDeployment{Deployment: dep}}}); err != nil {
		return nil, err
	}
	return dep, nil
}

// GetDeployment returns one deployment by id.
func (s *DeployServer) GetDeployment(_ context.Context, req *zatterav1.GetDeploymentRequest) (*zatterav1.Deployment, error) {
	d, ok := s.store.Deployment(req.GetDeploymentId())
	if !ok {
		return nil, status.Error(codes.NotFound, "deployment not found")
	}
	return d, nil
}

// ListDeployments lists an environment's deployments.
func (s *DeployServer) ListDeployments(_ context.Context, req *zatterav1.ListDeploymentsRequest) (*zatterav1.ListDeploymentsResponse, error) {
	return &zatterav1.ListDeploymentsResponse{Deployments: s.store.ListDeployments(req.GetEnvironmentId())}, nil
}

// ListReleases lists an environment's releases (newest version first).
func (s *DeployServer) ListReleases(_ context.Context, req *zatterav1.ListReleasesRequest) (*zatterav1.ListReleasesResponse, error) {
	return &zatterav1.ListReleasesResponse{Releases: s.store.ListReleases(req.GetEnvironmentId())}, nil
}

// ListInstances lists assignments, optionally filtered by env and app.
func (s *DeployServer) ListInstances(_ context.Context, req *zatterav1.ListInstancesRequest) (*zatterav1.ListInstancesResponse, error) {
	var out []*zatterav1.Assignment
	for _, a := range s.store.ListAssignments(req.GetEnvironmentId()) {
		if req.GetAppId() != "" && a.GetAppId() != req.GetAppId() {
			continue
		}
		out = append(out, a)
	}
	return &zatterav1.ListInstancesResponse{Instances: out}, nil
}

// WatchDeployment streams a deployment, resending on every phase change.
func (s *DeployServer) WatchDeployment(req *zatterav1.GetDeploymentRequest, stream zatterav1.DeployService_WatchDeploymentServer) error {
	d, ok := s.store.Deployment(req.GetDeploymentId())
	if !ok {
		return status.Error(codes.NotFound, "deployment not found")
	}
	if err := stream.Send(d); err != nil {
		return err
	}
	lastPhase := d.GetPhase()

	sub := s.store.Watch(state.KindDeployment)
	defer sub.Close()
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-sub.Notify():
			sub.Drain()
			cur, ok := s.store.Deployment(req.GetDeploymentId())
			if !ok {
				return nil
			}
			if cur.GetPhase() != lastPhase {
				lastPhase = cur.GetPhase()
				if err := stream.Send(cur); err != nil {
					return err
				}
			}
		}
	}
}

// --- helpers --------------------------------------------------------------

func (s *DeployServer) resolveEnv(envID string) (*zatterav1.Environment, error) {
	env, ok := s.store.Environment(envID)
	if !ok {
		return nil, status.Error(codes.NotFound, "environment not found")
	}
	return env, nil
}

// hasActiveDeployment reports whether a non-terminal deployment exists for env.
func (s *DeployServer) hasActiveDeployment(envID string) bool {
	for _, d := range s.store.ListDeployments(envID) {
		if !deploymentTerminal(d.GetPhase()) {
			return true
		}
	}
	return false
}

// resolveImage returns the image ref to deploy from the request.
func (s *DeployServer) resolveImage(req *zatterav1.DeployRequest) (string, error) {
	if ref := req.GetImageRef(); ref != "" {
		return ref, nil
	}
	if bid := req.GetBuildId(); bid != "" {
		b, ok := s.store.Build(bid)
		if !ok {
			return "", status.Error(codes.NotFound, "build not found")
		}
		if b.GetStatus() != zatterav1.BuildStatus_BUILD_STATUS_SUCCEEDED || b.GetImageRef() == "" {
			return "", status.Error(codes.FailedPrecondition, "build has not produced an image yet")
		}
		return b.GetImageRef(), nil
	}
	return "", status.Error(codes.InvalidArgument, "either image_ref or build_id is required")
}

// buildRelease freezes the environment's current spec into a new release.
func (s *DeployServer) buildRelease(env *zatterav1.Environment, imageRef string) *zatterav1.Release {
	envID := env.GetMeta().GetId()
	spec := proto.Clone(env.GetService()).(*zatterav1.ServiceSpec)
	hash := appconfig.ConfigHash(spec, s.envVarVersion(envID))
	return &zatterav1.Release{
		Meta:          newMeta(ids.New(), s.clock.Now()),
		EnvironmentId: envID,
		AppId:         env.GetAppId(),
		ProjectId:     env.GetProjectId(),
		Version:       s.store.NextReleaseVersion(envID),
		ImageRef:      imageRef,
		ConfigHash:    hash,
		Service:       spec,
	}
}

func (s *DeployServer) buildDeployment(env *zatterav1.Environment, releaseID string, rollback bool) *zatterav1.Deployment {
	return &zatterav1.Deployment{
		Meta:              newMeta(ids.New(), s.clock.Now()),
		EnvironmentId:     env.GetMeta().GetId(),
		AppId:             env.GetAppId(),
		ProjectId:         env.GetProjectId(),
		ReleaseId:         releaseID,
		PreviousReleaseId: env.GetActiveReleaseId(),
		Phase:             zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PENDING,
		IsRollback:        rollback,
	}
}

// previousRelease returns the id of the highest-version release below the
// active one (the natural rollback target).
func (s *DeployServer) previousRelease(env *zatterav1.Environment) (string, bool) {
	active, ok := s.store.Release(env.GetActiveReleaseId())
	if !ok {
		return "", false
	}
	var best *zatterav1.Release
	for _, r := range s.store.ListReleases(env.GetMeta().GetId()) {
		if r.GetVersion() >= active.GetVersion() {
			continue
		}
		if best == nil || r.GetVersion() > best.GetVersion() {
			best = r
		}
	}
	if best == nil {
		return "", false
	}
	return best.GetMeta().GetId(), true
}

// commit persists the release then the deployment (both through raft).
func (s *DeployServer) commit(ctx context.Context, rel *zatterav1.Release, dep *zatterav1.Deployment) error {
	if err := s.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_PutRelease{PutRelease: &clusterv1.PutRelease{Release: rel}}}); err != nil {
		return err
	}
	return s.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_PutDeployment{PutDeployment: &clusterv1.PutDeployment{Deployment: dep}}})
}

func (s *DeployServer) apply(ctx context.Context, cmd *clusterv1.Command) error {
	id, _ := IdentityFrom(ctx)
	cmd.RequestId = ids.New()
	cmd.Actor = id.Actor()
	cmd.Time = timestamppb.Now()
	return toStatus(s.raft.Apply(ctx, cmd))
}

// envVarVersion fingerprints an environment's sealed env vars so a value change
// flows into the release config hash (there is no separate counter yet).
func (s *DeployServer) envVarVersion(envID string) uint64 {
	vars := s.store.EnvVars(envID)
	if len(vars) == 0 {
		return 0
	}
	keys := make([]string, 0, len(vars))
	for k := range vars {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	marshal := proto.MarshalOptions{Deterministic: true}
	h := fnv.New64a()
	for _, k := range keys {
		_, _ = h.Write([]byte(k))
		if b, err := marshal.Marshal(vars[k]); err == nil {
			_, _ = h.Write(b)
		}
	}
	return h.Sum64()
}

// deploymentTerminal reports whether a phase is finished.
func deploymentTerminal(p zatterav1.DeploymentPhase) bool {
	switch p {
	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_SUCCEEDED,
		zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_FAILED,
		zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_ROLLED_BACK,
		zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_SUPERSEDED,
		zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_UNSPECIFIED:
		return true
	default:
		return false
	}
}
