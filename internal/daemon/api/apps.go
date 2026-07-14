package api

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// AppServer implements zatterav1.AppServiceServer.
type AppServer struct {
	zatterav1.UnimplementedAppServiceServer
	store  *state.Store
	raft   Applier
	clock  clock.Clock
	sealer secrets.Sealer // may be nil until the cluster key is unsealed
}

// NewAppServer builds the app service. sealer may be nil; env-var mutations then
// return FailedPrecondition.
func NewAppServer(store *state.Store, raft Applier, clk clock.Clock, sealer secrets.Sealer) *AppServer {
	return &AppServer{store: store, raft: raft, clock: clk, sealer: sealer}
}

// CreateApp creates an app and its default production + staging environments.
func (s *AppServer) CreateApp(ctx context.Context, req *zatterav1.CreateAppRequest) (*zatterav1.App, error) {
	pid := req.GetProjectId()
	if !validDNSName(req.GetName()) {
		return nil, status.Error(codes.InvalidArgument, "name must be DNS-safe: [a-z0-9-], 1-40 chars")
	}
	if _, exists := s.store.AppByName(pid, req.GetName()); exists {
		return nil, status.Errorf(codes.AlreadyExists, "app %q already exists", req.GetName())
	}
	id, _ := IdentityFrom(ctx)
	now := s.clock.Now()
	app := &zatterav1.App{
		Meta:      newMeta(ids.New(), now),
		ProjectId: pid,
		Name:      req.GetName(),
		Build:     req.GetBuild(),
	}
	if err := s.apply(ctx, id.UserID, &clusterv1.Command{Mutation: &clusterv1.Command_PutApp{PutApp: &clusterv1.PutApp{App: app}}}); err != nil {
		return nil, toStatus(err)
	}
	for _, e := range []struct {
		name string
		typ  zatterav1.EnvironmentType
	}{
		{"production", zatterav1.EnvironmentType_ENVIRONMENT_TYPE_PRODUCTION},
		{"staging", zatterav1.EnvironmentType_ENVIRONMENT_TYPE_STAGING},
	} {
		env := &zatterav1.Environment{
			Meta:      newMeta(ids.New(), now),
			AppId:     app.GetMeta().GetId(),
			ProjectId: pid,
			Name:      e.name,
			Type:      e.typ,
			Service:   defaultServiceSpec(),
		}
		if err := s.apply(ctx, id.UserID, &clusterv1.Command{Mutation: &clusterv1.Command_PutEnvironment{PutEnvironment: &clusterv1.PutEnvironment{Environment: env}}}); err != nil {
			return nil, toStatus(err)
		}
	}
	return app, nil
}

// ListApps lists a project's apps.
func (s *AppServer) ListApps(_ context.Context, req *zatterav1.ListAppsRequest) (*zatterav1.ListAppsResponse, error) {
	return &zatterav1.ListAppsResponse{Apps: s.store.ListApps(req.GetProjectId())}, nil
}

// GetApp returns an app (by id or name) with its environments.
func (s *AppServer) GetApp(_ context.Context, req *zatterav1.GetAppRequest) (*zatterav1.GetAppResponse, error) {
	app, err := s.resolveApp(req.GetProjectId(), req.GetAppId())
	if err != nil {
		return nil, err
	}
	return &zatterav1.GetAppResponse{
		App:          app,
		Environments: s.store.ListEnvironments(req.GetProjectId(), app.GetMeta().GetId()),
	}, nil
}

// DeleteApp deletes an app and its environments (+ env vars).
func (s *AppServer) DeleteApp(ctx context.Context, req *zatterav1.DeleteAppRequest) (*emptypb.Empty, error) {
	app, err := s.resolveApp(req.GetProjectId(), req.GetAppId())
	if err != nil {
		return nil, err
	}
	id, _ := IdentityFrom(ctx)
	for _, env := range s.store.ListEnvironments(req.GetProjectId(), app.GetMeta().GetId()) {
		if err := s.apply(ctx, id.UserID, &clusterv1.Command{Mutation: &clusterv1.Command_DeleteEnvironment{DeleteEnvironment: &clusterv1.DeleteByID{Id: env.GetMeta().GetId()}}}); err != nil {
			return nil, toStatus(err)
		}
	}
	if err := s.apply(ctx, id.UserID, &clusterv1.Command{Mutation: &clusterv1.Command_DeleteApp{DeleteApp: &clusterv1.DeleteByID{Id: app.GetMeta().GetId()}}}); err != nil {
		return nil, toStatus(err)
	}
	return &emptypb.Empty{}, nil
}

// ApplyAppConfig upserts build config and per-env ServiceSpecs. Environments
// absent from the request are left untouched; new ones are created.
func (s *AppServer) ApplyAppConfig(ctx context.Context, req *zatterav1.ApplyAppConfigRequest) (*zatterav1.GetAppResponse, error) {
	app, err := s.resolveApp(req.GetProjectId(), req.GetAppId())
	if err != nil {
		return nil, err
	}
	id, _ := IdentityFrom(ctx)

	if req.GetBuild() != nil || req.GetGithub() != nil {
		app = clone(app)
		if req.GetBuild() != nil {
			app.Build = req.GetBuild()
		}
		if req.GetGithub() != nil {
			app.Github = req.GetGithub()
		}
		app.GetMeta().UpdatedAt = timestamppb.New(s.clock.Now())
		if err := s.apply(ctx, id.UserID, &clusterv1.Command{Mutation: &clusterv1.Command_PutApp{PutApp: &clusterv1.PutApp{App: app}}}); err != nil {
			return nil, toStatus(err)
		}
	}

	for name, spec := range req.GetEnvironments() {
		if !validDNSName(name) {
			return nil, status.Errorf(codes.InvalidArgument, "environment name %q is not DNS-safe", name)
		}
		env, ok := s.store.EnvironmentByName(app.GetMeta().GetId(), name)
		if ok {
			env = clone(env)
			env.Service = spec
			env.GetMeta().UpdatedAt = timestamppb.New(s.clock.Now())
		} else {
			env = &zatterav1.Environment{
				Meta:      newMeta(ids.New(), s.clock.Now()),
				AppId:     app.GetMeta().GetId(),
				ProjectId: req.GetProjectId(),
				Name:      name,
				Type:      envTypeForName(name),
				Service:   spec,
			}
		}
		if err := s.apply(ctx, id.UserID, &clusterv1.Command{Mutation: &clusterv1.Command_PutEnvironment{PutEnvironment: &clusterv1.PutEnvironment{Environment: env}}}); err != nil {
			return nil, toStatus(err)
		}
	}
	return &zatterav1.GetAppResponse{
		App:          mustApp(s.store, app.GetMeta().GetId()),
		Environments: s.store.ListEnvironments(req.GetProjectId(), app.GetMeta().GetId()),
	}, nil
}

// SetEnvVars seals each value and applies the set/unset batch. v1 semantics:
// changing env vars does NOT hot-restart running instances; the change folds
// into the next release's config hash (via DeployServer.envVarVersion) and
// takes effect on the next deploy or rollback.
func (s *AppServer) SetEnvVars(ctx context.Context, req *zatterav1.SetEnvVarsRequest) (*emptypb.Empty, error) {
	env, err := s.resolveEnv(req.GetProjectId(), req.GetEnvironmentId())
	if err != nil {
		return nil, err
	}
	if len(req.GetSet()) > 0 && s.sealer == nil {
		return nil, status.Error(codes.FailedPrecondition, "cluster key is not unsealed; cannot store secrets")
	}
	set := make(map[string]*zatterav1.EncryptedValue, len(req.GetSet()))
	for k, v := range req.GetSet() {
		sealed, err := s.sealer.Seal([]byte(v))
		if err != nil {
			return nil, status.Error(codes.Internal, "sealing failed")
		}
		set[k] = sealed
	}
	id, _ := IdentityFrom(ctx)
	cmd := &clusterv1.Command{Mutation: &clusterv1.Command_SetEnvVars{SetEnvVars: &clusterv1.SetEnvVars{
		EnvironmentId: env.GetMeta().GetId(),
		Set:           set,
		Unset:         req.GetUnset(),
	}}}
	if err := s.apply(ctx, id.UserID, cmd); err != nil {
		return nil, toStatus(err)
	}
	return &emptypb.Empty{}, nil
}

// GetEnvVars returns env var keys; values only when reveal is set (the RBAC
// tier already restricts this method to DEVELOPER+).
func (s *AppServer) GetEnvVars(_ context.Context, req *zatterav1.GetEnvVarsRequest) (*zatterav1.GetEnvVarsResponse, error) {
	env, err := s.resolveEnv(req.GetProjectId(), req.GetEnvironmentId())
	if err != nil {
		return nil, err
	}
	sealed := s.store.EnvVars(env.GetMeta().GetId())
	out := make(map[string]string, len(sealed))
	for k, v := range sealed {
		if !req.GetReveal() {
			out[k] = ""
			continue
		}
		if s.sealer == nil {
			return nil, status.Error(codes.FailedPrecondition, "cluster key is not unsealed; cannot reveal secrets")
		}
		pt, err := s.sealer.Open(v)
		if err != nil {
			return nil, status.Error(codes.Internal, "unsealing failed")
		}
		out[k] = string(pt)
	}
	return &zatterav1.GetEnvVarsResponse{Vars: out}, nil
}

// SetReplicas updates an environment's replica range.
func (s *AppServer) SetReplicas(ctx context.Context, req *zatterav1.SetReplicasRequest) (*zatterav1.Environment, error) {
	env, err := s.resolveEnv(req.GetProjectId(), req.GetEnvironmentId())
	if err != nil {
		return nil, err
	}
	if req.GetMax() > 0 && req.GetMin() > req.GetMax() {
		return nil, status.Error(codes.InvalidArgument, "replicas.min > max")
	}
	env = clone(env)
	if env.Service == nil {
		env.Service = defaultServiceSpec()
	}
	env.Service.Replicas = &zatterav1.ReplicaRange{Min: req.GetMin(), Max: req.GetMax()}
	env.GetMeta().UpdatedAt = timestamppb.New(s.clock.Now())
	id, _ := IdentityFrom(ctx)
	if err := s.apply(ctx, id.UserID, &clusterv1.Command{Mutation: &clusterv1.Command_PutEnvironment{PutEnvironment: &clusterv1.PutEnvironment{Environment: env}}}); err != nil {
		return nil, toStatus(err)
	}
	return env, nil
}

// --- helpers ---

func (s *AppServer) apply(ctx context.Context, actorUser string, cmd *clusterv1.Command) error {
	cmd.RequestId = ids.New()
	cmd.Actor = "user:" + actorUser
	cmd.Time = timestamppb.Now()
	return s.raft.Apply(ctx, cmd)
}

// resolveApp finds an app by id or name within a project.
func (s *AppServer) resolveApp(projectID, appIDorName string) (*zatterav1.App, error) {
	if a, ok := s.store.App(appIDorName); ok && a.GetProjectId() == projectID {
		return a, nil
	}
	if a, ok := s.store.AppByName(projectID, appIDorName); ok {
		return a, nil
	}
	return nil, status.Errorf(codes.NotFound, "app %q not found", appIDorName)
}

// resolveEnv finds an environment by id and verifies it belongs to the project
// (guards against cross-project access via a foreign env id).
func (s *AppServer) resolveEnv(projectID, envID string) (*zatterav1.Environment, error) {
	env, ok := s.store.Environment(envID)
	if !ok || env.GetProjectId() != projectID {
		return nil, status.Errorf(codes.NotFound, "environment %q not found", envID)
	}
	return env, nil
}

func mustApp(st *state.Store, id string) *zatterav1.App {
	a, _ := st.App(id)
	return a
}

func envTypeForName(name string) zatterav1.EnvironmentType {
	switch name {
	case "production":
		return zatterav1.EnvironmentType_ENVIRONMENT_TYPE_PRODUCTION
	case "staging":
		return zatterav1.EnvironmentType_ENVIRONMENT_TYPE_STAGING
	default:
		return zatterav1.EnvironmentType_ENVIRONMENT_TYPE_PREVIEW
	}
}

// defaultServiceSpec is the spec new environments start with: 1 replica, one
// http port on 8080, and default health-check timings.
func defaultServiceSpec() *zatterav1.ServiceSpec {
	return &zatterav1.ServiceSpec{
		Replicas: &zatterav1.ReplicaRange{Min: 1, Max: 1},
		Ports: []*zatterav1.PortSpec{
			{Name: "http", ContainerPort: 8080, Protocol: zatterav1.Protocol_PROTOCOL_HTTP},
		},
		Healthcheck: &zatterav1.HealthCheck{
			Type:               zatterav1.HealthCheckType_HEALTH_CHECK_TYPE_UNSPECIFIED,
			Interval:           durationpb.New(10 * time.Second),
			Timeout:            durationpb.New(5 * time.Second),
			GracePeriod:        durationpb.New(60 * time.Second),
			UnhealthyThreshold: 3,
		},
		StopGrace: durationpb.New(10 * time.Second),
	}
}
