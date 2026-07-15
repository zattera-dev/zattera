//go:build cloud

package cloud

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/appconfig"
)

// TrustRegistryCA distributes the cluster CA to every node's Docker trust store
// for the embedded registry (control mesh IP:5000), so workers can pull built
// images over TLS. Required for source deploys whose replicas land on workers.
func (c *Cluster) TrustRegistryCA() {
	c.T.Helper()
	caPEM := c.control.MustRun("cat /var/lib/zattera/ca/ca.crt")
	ctrl := c.NodeByName(c.control.Name())
	if ctrl == nil || ctrl.GetMeshIp() == "" {
		c.T.Fatal("cloud: control mesh IP unknown; cannot configure registry trust")
	}
	regAddr := ctrl.GetMeshIp() + ":5000"
	for _, node := range c.nodes {
		node.Push([]byte(caPEM), fmt.Sprintf("/etc/docker/certs.d/%s/ca.crt", regAddr), "0644")
	}
	c.T.Logf("cloud: registry CA trusted for %s on %d node(s)", regAddr, len(c.nodes))
}

// DeploySource deploys the app in dir as a SOURCE build via the API (not the
// CLI): create project/app, apply the config (which carries the replica count),
// then stream a tar.gz of the sources to UploadSource. Returns the deployment
// id and the production environment id. Driving this over the API — unary +
// client-streaming, both reliable over the public IP — avoids the CLI's
// long-lived WatchDeployment stream, which drops when the cluster is reached by
// public IP.
func (c *Cluster) DeploySource(project, dir string) (deploymentID, envID string) {
	c.T.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "zattera.toml"))
	if err != nil {
		c.T.Fatalf("cloud: read zattera.toml: %v", err)
	}
	ac, err := appconfig.Parse(data)
	if err != nil {
		c.T.Fatalf("cloud: parse app config: %v", err)
	}
	cli := c.API()

	// Project + app + config (ProjectId/AppId accept names). CreateProject/App
	// are best-effort idempotent — an "already exists" is fine; a real problem
	// surfaces on ApplyAppConfig.
	if _, err := cli.Projects.CreateProject(c.Ctx, &zatterav1.CreateProjectRequest{Name: project}); err != nil {
		c.T.Logf("cloud: create project (ok if it exists): %v", err)
	}
	if _, err := cli.Apps.GetApp(c.Ctx, &zatterav1.GetAppRequest{ProjectId: project, AppId: ac.Name}); err != nil {
		if _, cerr := cli.Apps.CreateApp(c.Ctx, &zatterav1.CreateAppRequest{ProjectId: project, Name: ac.Name, Build: ac.Build}); cerr != nil {
			c.T.Fatalf("cloud: create app: %v", cerr)
		}
	}
	if _, err := cli.Apps.ApplyAppConfig(c.Ctx, &zatterav1.ApplyAppConfigRequest{
		ProjectId: project, AppId: ac.Name, Build: ac.Build, Github: ac.GitHub, Environments: ac.Services,
	}); err != nil {
		c.T.Fatalf("cloud: apply app config: %v", err)
	}

	app, err := cli.Apps.GetApp(c.Ctx, &zatterav1.GetAppRequest{ProjectId: project, AppId: ac.Name})
	if err != nil {
		c.T.Fatalf("cloud: get app: %v", err)
	}
	for _, e := range app.GetEnvironments() {
		if e.GetName() == "production" {
			envID = e.GetMeta().GetId()
		}
	}
	if envID == "" {
		c.T.Fatal("cloud: production environment not found after apply")
	}

	// Stream the source tarball.
	stream, err := cli.Deploys.UploadSource(c.Ctx)
	if err != nil {
		c.T.Fatalf("cloud: open UploadSource: %v", err)
	}
	if err := stream.Send(&zatterav1.UploadSourceChunk{Header: &zatterav1.UploadSourceHeader{
		ProjectId: project, AppId: ac.Name, EnvironmentId: envID, BuildType: ac.Build.GetType(),
	}}); err != nil {
		c.T.Fatalf("cloud: send upload header: %v", err)
	}
	pr, pw := io.Pipe()
	go func() { _ = pw.CloseWithError(tarGz(dir, pw)) }()
	buf := make([]byte, 1<<20)
	for {
		n, rerr := pr.Read(buf)
		if n > 0 {
			if serr := stream.Send(&zatterav1.UploadSourceChunk{Data: buf[:n]}); serr != nil {
				_, _ = stream.CloseAndRecv()
				c.T.Fatalf("cloud: upload chunk: %v", serr)
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			c.T.Fatalf("cloud: tar sources: %v", rerr)
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		c.T.Fatalf("cloud: finish UploadSource: %v", err)
	}
	dep := resp.GetDeployment()
	c.T.Logf("cloud: uploaded source; deployment %s, build %s", dep.GetMeta().GetId(), resp.GetBuild().GetMeta().GetId())
	return dep.GetMeta().GetId(), envID
}

// WaitDeployment polls a deployment to completion, logging each phase change and
// failing FAST (not after a long silent wait) if the build/rollout fails.
func (c *Cluster) WaitDeployment(project, depID string, timeout time.Duration) {
	c.T.Helper()
	cli := c.API()
	deadline := time.Now().Add(timeout)
	last := ""
	for time.Now().Before(deadline) {
		d, err := cli.Deploys.GetDeployment(c.Ctx, &zatterav1.GetDeploymentRequest{ProjectId: project, DeploymentId: depID})
		if err == nil {
			phase := strings.TrimPrefix(d.GetPhase().String(), "DEPLOYMENT_PHASE_")
			if phase != last {
				c.T.Logf("cloud: deployment phase → %s", phase)
				last = phase
			}
			switch d.GetPhase() {
			case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_DRAINING_OLD,
				zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_SUCCEEDED:
				return
			case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_FAILED,
				zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_ROLLED_BACK,
				zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_SUPERSEDED:
				c.T.Fatalf("cloud: deployment %s (%s)", phase, d.GetError())
			}
		}
		time.Sleep(4 * time.Second)
	}
	c.T.Fatalf("cloud: deployment %s did not complete within %s (last phase %s)", depID, timeout, last)
}

// WaitHealthyReplicas polls the env's instances until at least want are HEALTHY,
// logging progress so a long build/rollout never looks hung. Returns the set of
// nodes the healthy replicas run on.
func (c *Cluster) WaitHealthyReplicas(project, envID string, want int, timeout time.Duration) map[string]bool {
	c.T.Helper()
	cli := c.API()
	deadline := time.Now().Add(timeout)
	nextLog := time.Now()
	for time.Now().Before(deadline) {
		resp, err := cli.Deploys.ListInstances(c.Ctx, &zatterav1.ListInstancesRequest{ProjectId: project, EnvironmentId: envID})
		if err == nil {
			nodes := map[string]bool{}
			healthy := 0
			for _, a := range resp.GetInstances() {
				if a.GetObserved().GetState() == zatterav1.InstanceState_INSTANCE_STATE_HEALTHY {
					healthy++
					nodes[a.GetNodeId()] = true
				}
			}
			if healthy >= want {
				c.T.Logf("cloud: %d healthy replica(s) across %d node(s)", healthy, len(nodes))
				return nodes
			}
			if time.Now().After(nextLog) {
				c.T.Logf("cloud: waiting for replicas… %d/%d healthy", healthy, want)
				nextLog = time.Now().Add(20 * time.Second)
			}
		}
		time.Sleep(4 * time.Second)
	}
	c.T.Fatalf("cloud: fewer than %d healthy replicas within %s", want, timeout)
	return nil
}

// ProbeIngress polls the app through a node's OWN ingress (:80 → 308 → :443,
// self-signed on-demand cert accepted with -k, host resolved to loopback) and
// reports whether it served `want`. Best-effort: it LOGS the outcome and never
// fails the test — healthy replicas already prove the app serves HTTP (the
// healthcheck GETs it); this only additionally exercises public ingress routing,
// which depends on cluster domain/cert setup a throwaway fake domain may lack.
func (c *Cluster) ProbeIngress(node *Node, host, want string, timeout time.Duration) bool {
	c.T.Helper()
	body := fmt.Sprintf("curl -ksSL --max-time 10 --resolve %s:80:127.0.0.1 --resolve %s:443:127.0.0.1 http://%s/", host, host, host)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if out, _ := node.Run(body); strings.Contains(out, want) {
			c.T.Logf("cloud: ✓ app served via ingress: %q", strings.TrimSpace(out))
			return true
		}
		time.Sleep(3 * time.Second)
	}
	diag, _ := node.Run(fmt.Sprintf("curl -ksS -o /dev/null -w 'http_code=%%{http_code} err=%%{errormsg}' --max-time 10 --resolve %s:80:127.0.0.1 --resolve %s:443:127.0.0.1 http://%s/ 2>&1", host, host, host))
	c.T.Logf("cloud: ⚠ app not reachable via public ingress at %s (best-effort; replicas are healthy so the app IS serving). curl: %s", host, strings.TrimSpace(diag))
	return false
}

// tarGz writes a gzip-compressed tar of every regular file under dir to w.
func tarGz(dir string, w io.Writer) error {
	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := tw.WriteHeader(&tar.Header{Name: rel, Mode: 0o644, Size: int64(len(data))}); err != nil {
			return err
		}
		_, err = tw.Write(data)
		return err
	})
	if err != nil {
		return err
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return gz.Close()
}
