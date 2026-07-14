package api

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

func TestDeploy(t *testing.T) {
	const depEnvID = "env-deploy"
	rs := raftstore.NewTestStore(t)
	st := rs.State()
	s := NewDeployServer(st, rs, clock.NewFake())
	st.PutEnvironment(&zatterav1.Environment{
		Meta:      &zatterav1.Meta{Id: depEnvID},
		ProjectId: "p1",
		AppId:     "a1",
		Name:      "production",
		Service:   &zatterav1.ServiceSpec{Replicas: &zatterav1.ReplicaRange{Min: 1}},
	})
	ctx := context.Background()

	// promote simulates the orchestrator finishing a deployment and switching
	// the active release.
	promote := func(t *testing.T, dep *zatterav1.Deployment) {
		t.Helper()
		d, _ := st.Deployment(dep.GetMeta().GetId())
		d.Phase = zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_SUCCEEDED
		st.PutDeployment(d)
		env, _ := st.Environment(depEnvID)
		env.ActiveReleaseId = dep.GetReleaseId()
		st.PutEnvironment(env)
	}

	var dep1, dep2 *zatterav1.Deployment

	t.Run("deploy creates release v1 and a PENDING deployment", func(t *testing.T) {
		var err error
		dep1, err = s.Deploy(ctx, &zatterav1.DeployRequest{EnvironmentId: depEnvID, ImageRef: "nginx:1"})
		if err != nil {
			t.Fatalf("deploy: %v", err)
		}
		if dep1.GetPhase() != zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PENDING {
			t.Fatalf("phase = %v, want PENDING", dep1.GetPhase())
		}
		if dep1.GetPreviousReleaseId() != "" || dep1.GetIsRollback() {
			t.Fatalf("first deploy should have no previous release and not be a rollback: %+v", dep1)
		}
		rels := st.ListReleases(depEnvID)
		if len(rels) != 1 || rels[0].GetVersion() != 1 || rels[0].GetImageRef() != "nginx:1" {
			t.Fatalf("expected release v1 nginx:1, got %+v", rels)
		}
		if rels[0].GetConfigHash() == "" || rels[0].GetService() == nil {
			t.Fatalf("release must carry a config hash + frozen spec: %+v", rels[0])
		}
	})

	t.Run("a second deploy while one is in progress is rejected", func(t *testing.T) {
		_, err := s.Deploy(ctx, &zatterav1.DeployRequest{EnvironmentId: depEnvID, ImageRef: "nginx:2"})
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("concurrent deploy should 409, got %v", err)
		}
	})

	t.Run("after the first completes, a new deploy is release v2 with previous set", func(t *testing.T) {
		promote(t, dep1)
		var err error
		dep2, err = s.Deploy(ctx, &zatterav1.DeployRequest{EnvironmentId: depEnvID, ImageRef: "nginx:2"})
		if err != nil {
			t.Fatalf("deploy v2: %v", err)
		}
		if dep2.GetPreviousReleaseId() != dep1.GetReleaseId() {
			t.Fatalf("v2 previous_release_id = %q, want %q", dep2.GetPreviousReleaseId(), dep1.GetReleaseId())
		}
		rel, _ := st.Release(dep2.GetReleaseId())
		if rel.GetVersion() != 2 {
			t.Fatalf("second release version = %d, want 2", rel.GetVersion())
		}
	})

	t.Run("rollback defaults to the previous release", func(t *testing.T) {
		promote(t, dep2) // active is now v2
		dep, err := s.Rollback(ctx, &zatterav1.RollbackRequest{EnvironmentId: depEnvID})
		if err != nil {
			t.Fatalf("rollback: %v", err)
		}
		if !dep.GetIsRollback() {
			t.Fatal("rollback deployment must set is_rollback")
		}
		if dep.GetReleaseId() != dep1.GetReleaseId() {
			t.Fatalf("rollback target = %q, want v1 release %q", dep.GetReleaseId(), dep1.GetReleaseId())
		}
		// No new release is minted on rollback.
		if got := len(st.ListReleases(depEnvID)); got != 2 {
			t.Fatalf("rollback should not mint a release, have %d", got)
		}
	})

	t.Run("get, list and instances", func(t *testing.T) {
		got, err := s.GetDeployment(ctx, &zatterav1.GetDeploymentRequest{DeploymentId: dep1.GetMeta().GetId()})
		if err != nil || got.GetMeta().GetId() != dep1.GetMeta().GetId() {
			t.Fatalf("get deployment: %v", err)
		}
		if _, err := s.GetDeployment(ctx, &zatterav1.GetDeploymentRequest{DeploymentId: "nope"}); status.Code(err) != codes.NotFound {
			t.Fatalf("missing deployment should be NotFound, got %v", err)
		}
		deps, _ := s.ListDeployments(ctx, &zatterav1.ListDeploymentsRequest{EnvironmentId: depEnvID})
		if len(deps.GetDeployments()) < 3 {
			t.Fatalf("expected at least 3 deployments, got %d", len(deps.GetDeployments()))
		}

		st.PutAssignment(&zatterav1.Assignment{
			Meta: &zatterav1.Meta{Id: "asg1"}, EnvironmentId: depEnvID, AppId: "a1",
			Desired: zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN,
		})
		inst, _ := s.ListInstances(ctx, &zatterav1.ListInstancesRequest{EnvironmentId: depEnvID, AppId: "a1"})
		if len(inst.GetInstances()) != 1 {
			t.Fatalf("expected 1 instance, got %d", len(inst.GetInstances()))
		}

		// WatchDeployment sends the current state immediately; the fake cancels
		// its context on that first send so the watch loop returns.
		wctx, wcancel := context.WithCancel(ctx)
		fw := &fakeDeployWatch{ctx: wctx, cancel: wcancel}
		if err := s.WatchDeployment(&zatterav1.GetDeploymentRequest{DeploymentId: dep1.GetMeta().GetId()}, fw); err != nil {
			t.Fatalf("watch: %v", err)
		}
		if fw.got == nil || fw.got.GetMeta().GetId() != dep1.GetMeta().GetId() {
			t.Fatal("watch should send the current deployment first")
		}
	})

	t.Run("deploy without image or build is rejected", func(t *testing.T) {
		// Use a fresh env with no in-flight deployment.
		st.PutEnvironment(&zatterav1.Environment{Meta: &zatterav1.Meta{Id: "env-2"}, Service: &zatterav1.ServiceSpec{}})
		if _, err := s.Deploy(ctx, &zatterav1.DeployRequest{EnvironmentId: "env-2"}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("deploy without image_ref/build_id should be InvalidArgument, got %v", err)
		}
		if _, err := s.Deploy(ctx, &zatterav1.DeployRequest{EnvironmentId: "ghost", ImageRef: "x"}); status.Code(err) != codes.NotFound {
			t.Fatalf("deploy to unknown env should be NotFound, got %v", err)
		}
	})
}

// fakeDeployWatch captures the first streamed deployment and cancels its context
// so WatchDeployment's loop returns.
type fakeDeployWatch struct {
	grpc.ServerStream
	ctx    context.Context
	cancel context.CancelFunc
	got    *zatterav1.Deployment
}

func (f *fakeDeployWatch) Send(d *zatterav1.Deployment) error {
	f.got = d
	f.cancel()
	return nil
}

func (f *fakeDeployWatch) Context() context.Context { return f.ctx }
