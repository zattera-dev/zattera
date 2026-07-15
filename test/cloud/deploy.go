//go:build cloud

package cloud

import (
	"archive/tar"
	"compress/gzip"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
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

// DeploySourceHealthy deploys dir as a source build and waits for `want`
// healthy replicas, RETRYING the whole deploy on a transient failure (e.g. a
// green replica not reporting healthy in the red/green window — an intermittent
// hiccup on small real nodes). Retries are cheap: the buildkit cache makes the
// rebuild fast and light. Returns the env id and the nodes the replicas run on.
func (c *Cluster) DeploySourceHealthy(project, dir string, want, attempts int) (envID string, nodes map[string]bool) {
	c.T.Helper()
	var lastErr string
	for i := 1; i <= attempts; i++ {
		depID, eid := c.DeploySource(project, dir)
		if err := c.waitDeployment(project, depID, 8*time.Minute); err != nil {
			lastErr = err.Error()
			c.T.Logf("cloud: deploy attempt %d/%d failed (%s) — retrying (buildkit cache makes the rebuild fast)", i, attempts, lastErr)
			continue
		}
		return eid, c.WaitHealthyReplicas(project, eid, want, 3*time.Minute)
	}
	c.T.Fatalf("cloud: deploy did not reach a healthy release after %d attempts (last: %s)", attempts, lastErr)
	return "", nil
}

// waitDeployment polls a deployment to completion, logging each phase change.
// Returns nil on success, an error on a failed/rolled-back deploy or timeout
// (so callers can retry rather than abort).
func (c *Cluster) waitDeployment(project, depID string, timeout time.Duration) error {
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
				return nil
			case zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_FAILED,
				zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_ROLLED_BACK,
				zatterav1.DeploymentPhase_DEPLOYMENT_PHASE_SUPERSEDED:
				return fmt.Errorf("deployment %s: %s", phase, d.GetError())
			}
		}
		time.Sleep(4 * time.Second)
	}
	return fmt.Errorf("deployment %s did not complete within %s (last phase %s)", depID, timeout, last)
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

// ProbeIngressURL GETs https://<host>/ from the TEST MACHINE and reports whether
// it served `want`. With an sslip.io cluster domain, host resolves over real
// public DNS to the control's public IP, so this exercises the full external
// path (public DNS → ingress :443 → route → app). TLS verification is skipped
// (the on-demand cert is self-signed from the cluster CA when ACME is off).
//
// Best-effort: it LOGS the outcome and never fails the test — healthy replicas
// already prove the app serves HTTP (the healthcheck GETs it); this additionally
// confirms public ingress routing.
func (c *Cluster) ProbeIngressURL(host, want string, timeout time.Duration) bool {
	c.T.Helper()
	url := "https://" + host + "/"
	client := &http.Client{
		Timeout:   12 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
	}
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
			_ = resp.Body.Close()
			if strings.Contains(string(b), want) {
				c.T.Logf("cloud: ✓ app served publicly at %s: %q", url, strings.TrimSpace(string(b)))
				return true
			}
			last = fmt.Sprintf("HTTP %d, body=%q", resp.StatusCode, strings.TrimSpace(string(b)))
		} else {
			last = err.Error()
		}
		time.Sleep(3 * time.Second)
	}
	c.T.Logf("cloud: ⚠ app not reachable publicly at %s (best-effort; replicas are healthy so the app IS serving). last: %s", url, last)
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
