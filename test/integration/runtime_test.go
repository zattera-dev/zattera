//go:build integration

package integration

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/zattera-dev/zattera/internal/daemon/runtime"
)

const testImage = "alpine:3.20"

func newRuntime(t *testing.T) *runtime.Docker {
	t.Helper()
	RequireDocker(t)
	d, err := runtime.NewDocker()
	if err != nil {
		t.Fatalf("NewDocker: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := d.Ping(ctx); err != nil {
		t.Skipf("docker not reachable: %v", err)
	}
	return d
}

func TestDockerRuntime(t *testing.T) {
	d := newRuntime(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Pull.
	if err := d.EnsureImage(ctx, testImage, nil, nil); err != nil {
		t.Fatalf("EnsureImage: %v", err)
	}
	// Idempotent second pull.
	if err := d.EnsureImage(ctx, testImage, nil, nil); err != nil {
		t.Fatalf("EnsureImage (cached): %v", err)
	}

	name := fmt.Sprintf("zt-it-%d", time.Now().UnixNano())
	id, err := d.CreateContainer(ctx, runtime.ContainerSpec{
		Name:    name,
		Image:   testImage,
		Command: []string{"sh", "-c", "echo hello-stdout; sleep 30"},
		Labels:  map[string]string{"dev.zattera/test": "1"},
		Ports:   []runtime.PortBinding{{ContainerPort: 8080, Protocol: "tcp", HostIP: "127.0.0.1", HostPort: 0}},
		Restart: runtime.RestartNever,
	})
	if err != nil {
		t.Fatalf("CreateContainer: %v", err)
	}
	t.Cleanup(func() {
		_ = d.RemoveContainer(context.Background(), id, true)
	})

	if err := d.StartContainer(ctx, id); err != nil {
		t.Fatalf("StartContainer: %v", err)
	}

	// Inspect: running + effective host port bound on 127.0.0.1. Docker Desktop
	// (macOS) can populate NetworkSettings.Ports a beat after start, so retry.
	st := inspectWithPort(t, d, id)
	if !st.Running {
		t.Fatal("container not running")
	}
	if len(st.Ports) == 0 || st.Ports[0].HostPort == 0 {
		t.Errorf("no effective host port bound: %+v", st.Ports)
	}
	if st.Labels[runtime.ManagedLabel] != "true" {
		t.Errorf("ManagedLabel missing: %+v", st.Labels)
	}

	// Logs contain the echoed line.
	if got := collectLogs(t, d, id); !strings.Contains(got, "hello-stdout") {
		t.Errorf("logs missing echo, got: %q", got)
	}

	// Exec: true → 0, false → 1.
	if code := execCode(t, d, id, "true"); code != 0 {
		t.Errorf("exec true exit = %d, want 0", code)
	}
	if code := execCode(t, d, id, "false"); code != 1 {
		t.Errorf("exec false exit = %d, want 1", code)
	}

	// Exec stdout capture.
	var out bytes.Buffer
	if _, err := d.Exec(ctx, id, runtime.ExecSpec{Command: []string{"echo", "exec-out"}}, nil, &out, nil, nil); err != nil {
		t.Fatalf("exec echo: %v", err)
	}
	if !strings.Contains(out.String(), "exec-out") {
		t.Errorf("exec stdout = %q", out.String())
	}

	// Stats one-shot.
	if _, err := d.Stats(ctx, id); err != nil {
		t.Errorf("Stats: %v", err)
	}

	// List filters on the managed label + our label.
	list, err := d.ListContainers(ctx, map[string]string{"dev.zattera/test": "1"})
	if err != nil {
		t.Fatalf("ListContainers: %v", err)
	}
	if !containsID(list, id) {
		t.Errorf("list did not contain our container")
	}

	// Stop + remove.
	if err := d.StopContainer(ctx, id, 5*time.Second); err != nil {
		t.Fatalf("StopContainer: %v", err)
	}
	if err := d.RemoveContainer(ctx, id, false); err != nil {
		t.Fatalf("RemoveContainer: %v", err)
	}
	// Inspect after removal → ErrNotFound.
	if _, err := d.InspectContainer(ctx, id); err != runtime.ErrNotFound {
		t.Errorf("inspect after remove err = %v, want ErrNotFound", err)
	}
}

func TestDockerNetworkAndVolume(t *testing.T) {
	d := newRuntime(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	netName := fmt.Sprintf("zt-it-net-%d", time.Now().UnixNano())
	info, err := d.EnsureNetwork(ctx, runtime.NetworkSpec{Name: netName, Subnet: "10.201.55.0/24"})
	if err != nil {
		t.Fatalf("EnsureNetwork: %v", err)
	}
	t.Cleanup(func() { _ = d.RemoveNetwork(context.Background(), netName) })
	if info.Subnet != "10.201.55.0/24" {
		t.Errorf("subnet = %q", info.Subnet)
	}
	// Idempotent.
	if _, err := d.EnsureNetwork(ctx, runtime.NetworkSpec{Name: netName, Subnet: "10.201.55.0/24"}); err != nil {
		t.Fatalf("EnsureNetwork (idempotent): %v", err)
	}

	volName := fmt.Sprintf("zt-it-vol-%d", time.Now().UnixNano())
	if err := d.EnsureVolume(ctx, volName, map[string]string{"dev.zattera/test": "1"}); err != nil {
		t.Fatalf("EnsureVolume: %v", err)
	}
	t.Cleanup(func() { _ = d.RemoveVolume(context.Background(), volName) })
	// Idempotent.
	if err := d.EnsureVolume(ctx, volName, nil); err != nil {
		t.Fatalf("EnsureVolume (idempotent): %v", err)
	}
	// VolumeHostPath returns a path (content lives in the VM on macOS).
	path, err := d.VolumeHostPath(ctx, volName)
	if err != nil {
		t.Fatalf("VolumeHostPath: %v", err)
	}
	if path == "" {
		t.Error("empty volume host path")
	}
}

// inspectWithPort polls until the effective host port is bound (or times out),
// tolerating Docker Desktop's slightly-delayed port publishing.
func inspectWithPort(t *testing.T, d *runtime.Docker, id string) runtime.ContainerState {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	deadline := time.Now().Add(8 * time.Second)
	var st runtime.ContainerState
	for {
		var err error
		st, err = d.InspectContainer(ctx, id)
		if err != nil {
			t.Fatalf("InspectContainer: %v", err)
		}
		if len(st.Ports) > 0 && st.Ports[0].HostPort != 0 {
			return st
		}
		if time.Now().After(deadline) {
			return st
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func collectLogs(t *testing.T, d *runtime.Docker, id string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Retry briefly: the echo may not have flushed yet.
	deadline := time.Now().Add(8 * time.Second)
	for {
		ch, err := d.Logs(ctx, id, runtime.LogsOptions{})
		if err != nil {
			t.Fatalf("Logs: %v", err)
		}
		var b strings.Builder
		for e := range ch {
			b.WriteString(e.Line)
			b.WriteString("\n")
		}
		if strings.Contains(b.String(), "hello-stdout") || time.Now().After(deadline) {
			return b.String()
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func execCode(t *testing.T, d *runtime.Docker, id, cmd string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	code, err := d.Exec(ctx, id, runtime.ExecSpec{Command: []string{cmd}}, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Exec %s: %v", cmd, err)
	}
	return code
}

func containsID(list []runtime.ContainerInfo, id string) bool {
	for _, c := range list {
		if c.ID == id {
			return true
		}
	}
	return false
}
