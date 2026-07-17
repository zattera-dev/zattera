package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/emptypb"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/appconfig"
	"github.com/zattera-dev/zattera/internal/cli/ui"
	"github.com/zattera-dev/zattera/pkg/apiclient"
)

// deployWatchTimeout bounds a deploy/rollback watch (covers healthcheck grace).
const deployWatchTimeout = 5 * time.Minute

func newDeployCmd() *cobra.Command {
	var image, app, env string
	var prod bool
	cmd := &cobra.Command{
		Use:   "deploy",
		Short: "Deploy an image, or build and deploy the current directory",
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, cctx, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			proj, err := projectName(cctx)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), deployWatchTimeout)
			defer cancel()

			p := printerFor(cmd)
			if image != "" {
				appName, err := resolveAppName(app)
				if err != nil {
					return err
				}
				envName := deployEnvName(env, prod)
				envID, err := resolveEnv(ctx, client, proj, appName, envName)
				if err != nil {
					return err
				}
				dep, err := client.Deploys.Deploy(ctx, &zatterav1.DeployRequest{
					ProjectId: proj, EnvironmentId: envID, ImageRef: image,
				})
				if err != nil {
					return apiError(err)
				}
				return watchDeployment(ctx, client, p, deployTarget{project: proj, app: appName, env: envName, envID: envID}, dep)
			}
			return deploySource(ctx, client, p, proj, deployEnvName(env, prod))
		},
	}
	cmd.Flags().StringVar(&image, "image", "", "container image to deploy (e.g. nginx:alpine)")
	cmd.Flags().StringVar(&app, "app", "", "app name (default: name in ./zattera.toml)")
	cmd.Flags().StringVar(&env, "env", "", "environment name (default: staging)")
	cmd.Flags().BoolVar(&prod, "prod", false, "shortcut for --env production")
	addProjectFlag(cmd)
	return cmd
}

// deployTarget carries the names needed for the success line + URL.
type deployTarget struct {
	project string
	app     string
	env     string
	envID   string
	// Source-build fields drive the "✓ Built …" line when set.
	sourceBuild bool
	buildType   string
	buildStart  time.Time
}

// watchDeployment streams a deployment to completion, rendering phase progress
// and a final success/URL line. The stream is redialed a few times so a leader
// failover mid-deploy doesn't fail the command. Returns a non-nil error (→
// non-zero exit) when the deployment ends FAILED/SUPERSEDED/ROLLED_BACK.
func watchDeployment(ctx context.Context, client *apiclient.Client, p *ui.Printer, tgt deployTarget, dep *zatterav1.Deployment) error {
	depID := dep.GetMeta().GetId()
	lastPhase := zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_UNSPECIFIED
	builtPrinted := false

	for redials := 0; ; redials++ {
		stream, err := client.Deploys.WatchDeployment(ctx, &zatterav1.GetDeploymentRequest{ProjectId: tgt.project, DeploymentId: depID})
		if err != nil {
			if redials < 3 {
				time.Sleep(200 * time.Millisecond)
				continue
			}
			return apiError(err)
		}
		for {
			d, err := stream.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return errors.New("deployment watch ended before completion")
				}
				break // stream dropped (failover); redial
			}
			if jsonFlag {
				_ = p.EmitJSON(d)
			} else if d.GetPhase() != lastPhase {
				lastPhase = d.GetPhase()
				p.Infof("  %s", phaseLabel(d.GetPhase()))
			}
			// A source build has completed once the deployment moves past
			// BUILDING into a placement/serving phase.
			if tgt.sourceBuild && !builtPrinted && buildDone(d.GetPhase()) {
				builtPrinted = true
				p.Successf("Built %s (%s, %s)", tgt.app, tgt.buildType, time.Since(tgt.buildStart).Round(time.Second))
			}
			if success, done := deployOutcome(d.GetPhase()); done {
				return finishDeploy(ctx, client, p, tgt, d, success)
			}
		}
		if redials >= 3 {
			return errors.New("deployment watch lost connection")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// finishDeploy prints the terminal outcome. Success is reached once traffic has
// switched (DRAINING_OLD) — blue is merely draining.
func finishDeploy(ctx context.Context, client *apiclient.Client, p *ui.Printer, tgt deployTarget, d *zatterav1.Deployment, success bool) error {
	if !success {
		msg := d.GetError()
		if msg == "" {
			msg = "deployment " + phaseLabel(d.GetPhase())
		}
		p.Errorf("deploy failed: %s", msg)
		return fmt.Errorf("deploy failed: %s", msg)
	}
	version, healthy := releaseSummary(ctx, client, tgt, d.GetReleaseId())
	p.Successf("Released v%d → %s (red/green, %d replica(s) healthy)", version, tgt.env, healthy)
	p.URL(deployURL(ctx, client, tgt))
	return nil
}

// releaseSummary resolves the deployed release's version and healthy replica
// count (best-effort; zero on lookup failure).
func releaseSummary(ctx context.Context, client *apiclient.Client, tgt deployTarget, releaseID string) (version uint64, healthy int) {
	if rels, err := client.Deploys.ListReleases(ctx, &zatterav1.ListReleasesRequest{ProjectId: tgt.project, EnvironmentId: tgt.envID}); err == nil {
		for _, r := range rels.GetReleases() {
			if r.GetMeta().GetId() == releaseID {
				version = r.GetVersion()
			}
		}
	}
	if inst, err := client.Deploys.ListInstances(ctx, &zatterav1.ListInstancesRequest{ProjectId: tgt.project, EnvironmentId: tgt.envID}); err == nil {
		for _, a := range inst.GetInstances() {
			if a.GetReleaseId() == releaseID && a.GetObserved().GetState() == zatterav1.InstanceState_INSTANCE_STATE_HEALTHY {
				healthy++
			}
		}
	}
	return version, healthy
}

// deployURL is the env's explicit custom domain if one is attached, else the
// implicit <app>-<env>.<cluster-domain> host (cluster domain from WhoAmI).
func deployURL(ctx context.Context, client *apiclient.Client, tgt deployTarget) string {
	if resp, err := client.Domains.ListDomains(ctx, &zatterav1.ListDomainsRequest{ProjectId: tgt.project}); err == nil {
		for _, d := range resp.GetDomains() {
			if d.GetEnvironmentId() == tgt.envID && !d.GetClusterSubdomain() && d.GetHostname() != "" {
				return "https://" + d.GetHostname()
			}
		}
	}
	if who, err := client.Auth.WhoAmI(ctx, &emptypb.Empty{}); err == nil && who.GetClusterDomain() != "" {
		return fmt.Sprintf("https://%s-%s.%s", tgt.app, tgt.env, who.GetClusterDomain())
	}
	return fmt.Sprintf("https://%s-%s.<cluster-domain>", tgt.app, tgt.env)
}

// deployOutcome classifies a phase: (success, done). Traffic-switched phases
// count as success; the abort phases as failure.
func deployOutcome(p zatterav1.DeploymentPhase) (success, done bool) {
	switch p {
	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_DRAINING_OLD,
		zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_SUCCEEDED:
		return true, true
	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_FAILED,
		zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_SUPERSEDED,
		zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_ROLLED_BACK:
		return false, true
	default:
		return false, false
	}
}

func phaseLabel(p zatterav1.DeploymentPhase) string {
	switch p {
	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PENDING:
		return "pending"
	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_BUILDING:
		return "building"
	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PLACING:
		return "placing replicas"
	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_STARTING:
		return "starting"
	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_HEALTHCHECKING:
		return "health checking"
	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PROMOTING:
		return "promoting"
	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_DRAINING_OLD:
		return "released (draining old)"
	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_SUCCEEDED:
		return "succeeded"
	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_FAILED:
		return "failed"
	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_SUPERSEDED:
		return "superseded"
	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_ROLLED_BACK:
		return "rolled back"
	default:
		return "unknown"
	}
}

// deploySource applies zattera.toml, tars the current directory (honouring
// ignore files), uploads it, and watches the resulting build+deployment.
func deploySource(ctx context.Context, client *apiclient.Client, p *ui.Printer, proj, envName string) error {
	data, err := os.ReadFile("zattera.toml")
	if err != nil {
		return errors.New("no --image given and no zattera.toml in the current directory")
	}
	ac, err := appconfig.Parse(data)
	if err != nil {
		return err
	}
	if ac.Name == "" {
		return errors.New("zattera.toml has no [app] name")
	}

	// Ensure the app exists and its config is applied before deploying.
	if _, gerr := client.Apps.GetApp(ctx, &zatterav1.GetAppRequest{ProjectId: proj, AppId: ac.Name}); gerr != nil {
		if _, cerr := client.Apps.CreateApp(ctx, &zatterav1.CreateAppRequest{ProjectId: proj, Name: ac.Name, Build: ac.Build}); cerr != nil {
			return apiError(cerr)
		}
	}
	if _, aerr := client.Apps.ApplyAppConfig(ctx, &zatterav1.ApplyAppConfigRequest{
		ProjectId: proj, AppId: ac.Name, Build: ac.Build, Github: ac.GitHub, Environments: ac.Services,
		IdleTimeouts: idleTimeoutsProto(ac.IdleTimeouts),
	}); aerr != nil {
		return apiError(aerr)
	}

	envID, err := resolveEnv(ctx, client, proj, ac.Name, envName)
	if err != nil {
		return err
	}

	dep, err := uploadSource(ctx, client, proj, ac.Name, envID, ac.Build.GetType())
	if err != nil {
		return err
	}
	// If the user cancels now, the build continues server-side.
	p.Infof("  uploaded source; deployment %s", dep.GetMeta().GetId())

	tgt := deployTarget{
		project: proj, app: ac.Name, env: envName, envID: envID,
		sourceBuild: true, buildType: buildTypeLabel(ac.Build.GetType()), buildStart: time.Now(),
	}
	return watchDeployment(ctx, client, p, tgt, dep)
}

// uploadSource streams a tar.gz of the current directory to UploadSource in 1MB
// chunks (header first) and returns the auto-created deployment.
func uploadSource(ctx context.Context, client *apiclient.Client, proj, app, envID string, bt zatterav1.BuildType) (*zatterav1.Deployment, error) {
	stream, err := client.Deploys.UploadSource(ctx)
	if err != nil {
		return nil, apiError(err)
	}
	if err := stream.Send(&zatterav1.UploadSourceChunk{Header: &zatterav1.UploadSourceHeader{
		ProjectId: proj, AppId: app, EnvironmentId: envID, BuildType: bt,
	}}); err != nil {
		return nil, apiError(err)
	}

	pr, pw := io.Pipe()
	go func() { pw.CloseWithError(writeSourceTar(".", pw)) }()
	defer func() { _ = pr.Close() }()

	buf := make([]byte, 1<<20)
	for {
		n, rerr := pr.Read(buf)
		if n > 0 {
			if serr := stream.Send(&zatterav1.UploadSourceChunk{Data: buf[:n]}); serr != nil {
				// Send returns io.EOF once the server has returned (usually an
				// error); the real status is on CloseAndRecv. Surface it.
				if _, cerr := stream.CloseAndRecv(); cerr != nil {
					return nil, apiError(cerr)
				}
				return nil, apiError(serr)
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return nil, fmt.Errorf("packaging source: %w", rerr)
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return nil, apiError(err)
	}
	return resp.GetDeployment(), nil
}

// buildDone reports whether a deployment phase is at or past the point where the
// source build has finished (green placement onward).
func buildDone(phase zatterav1.DeploymentPhase) bool {
	switch phase {
	case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PLACING,
		zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_STARTING,
		zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_HEALTHCHECKING,
		zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_PROMOTING,
		zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_DRAINING_OLD,
		zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_SUCCEEDED:
		return true
	default:
		return false
	}
}

func buildTypeLabel(t zatterav1.BuildType) string {
	switch t {
	case zatterav1.BuildType_BUILD_TYPE_NIXPACKS:
		return "nixpacks"
	case zatterav1.BuildType_BUILD_TYPE_DOCKERFILE:
		return "dockerfile"
	case zatterav1.BuildType_BUILD_TYPE_IMAGE:
		return "image"
	default:
		return "auto"
	}
}

// deployEnvName resolves the target env: --prod wins, else --env, else staging.
func deployEnvName(env string, prod bool) string {
	if prod {
		return "production"
	}
	if env != "" {
		return env
	}
	return "staging"
}

// resolveAppName returns the --app value or the app name from ./zattera.toml.
func resolveAppName(app string) (string, error) {
	if app != "" {
		return app, nil
	}
	data, err := os.ReadFile("zattera.toml")
	if err != nil {
		return "", errors.New("no --app given and no zattera.toml in the current directory")
	}
	ac, err := appconfig.Parse(data)
	if err != nil {
		return "", err
	}
	if ac.Name == "" {
		return "", errors.New("zattera.toml has no [app] name")
	}
	return ac.Name, nil
}
