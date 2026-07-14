// Package fakeruntime is an in-memory ContainerRuntime for unit and
// simcluster tests. Containers "run" instantly; behavior can be scripted per
// image via Hooks (fail pulls, exit with codes, block starts).
package fakeruntime

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/zattera-dev/zattera/internal/daemon/runtime"
)

// Hooks lets tests inject failures and behaviors keyed by image ref.
type Hooks struct {
	// FailPull returns an error for EnsureImage of matching refs.
	FailPull func(ref string) error
	// OnStart may return an error to fail StartContainer.
	OnStart func(c *Container) error
	// ExitImmediately: containers of this image exit right after start with
	// the given code.
	ExitCode func(image string) (code int, exits bool)
	// Exec, when set, overrides Exec for a running container — e.g. to echo
	// stdin→stdout or return a specific exit code (T-49).
	Exec func(id string, spec runtime.ExecSpec, stdin io.Reader, stdout, stderr io.Writer, resize <-chan runtime.TermSize) (int, error)
}

// Container is the fake's record of one container.
type Container struct {
	ID      string
	Spec    runtime.ContainerSpec
	Running bool
	Created time.Time
	Started time.Time
	Stopped time.Time
	Exit    int
	IP      string
	// LogLines can be appended by tests; Logs() serves them.
	LogLines []runtime.LogEntry
}

// Fake implements runtime.ContainerRuntime.
type Fake struct {
	mu         sync.Mutex
	seq        int
	containers map[string]*Container
	images     map[string]bool
	networks   map[string]runtime.NetworkInfo
	volumes    map[string]bool
	Hooks      Hooks
}

var _ runtime.ContainerRuntime = (*Fake)(nil)

func New() *Fake {
	return &Fake{
		containers: map[string]*Container{},
		images:     map[string]bool{},
		networks:   map[string]runtime.NetworkInfo{},
		volumes:    map[string]bool{},
	}
}

// Snapshot returns a copy of all containers for assertions.
func (f *Fake) Snapshot() []Container {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Container, 0, len(f.containers))
	for _, c := range f.containers {
		out = append(out, *c)
	}
	return out
}

// Get returns one container by id.
func (f *Fake) Get(id string) (Container, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.containers[id]
	if !ok {
		return Container{}, false
	}
	return *c, true
}

func (f *Fake) Ping(context.Context) error { return nil }

func (f *Fake) EnsureImage(_ context.Context, ref string, _ *runtime.RegistryAuth, progress func(string)) error {
	if f.Hooks.FailPull != nil {
		if err := f.Hooks.FailPull(ref); err != nil {
			return err
		}
	}
	if progress != nil {
		progress("pulled " + ref)
	}
	f.mu.Lock()
	f.images[ref] = true
	f.mu.Unlock()
	return nil
}

func (f *Fake) CreateContainer(_ context.Context, spec runtime.ContainerSpec) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	id := fmt.Sprintf("fake-%04d", f.seq)
	f.containers[id] = &Container{
		ID:      id,
		Spec:    spec,
		Created: time.Now(),
		IP:      fmt.Sprintf("172.30.0.%d", f.seq%250+2),
	}
	return id, nil
}

func (f *Fake) StartContainer(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.containers[id]
	if !ok {
		return runtime.ErrNotFound
	}
	if f.Hooks.OnStart != nil {
		if err := f.Hooks.OnStart(c); err != nil {
			return err
		}
	}
	c.Running = true
	c.Started = time.Now()
	// Allocate host ports for bindings that asked for 0.
	for i := range c.Spec.Ports {
		if c.Spec.Ports[i].HostPort == 0 {
			c.Spec.Ports[i].HostPort = uint32(30000 + f.seq*10 + i)
		}
	}
	if f.Hooks.ExitCode != nil {
		if code, exits := f.Hooks.ExitCode(c.Spec.Image); exits {
			c.Running = false
			c.Exit = code
			c.Stopped = time.Now()
		}
	}
	return nil
}

func (f *Fake) StopContainer(_ context.Context, id string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.containers[id]
	if !ok {
		return runtime.ErrNotFound
	}
	if c.Running {
		c.Running = false
		c.Stopped = time.Now()
	}
	return nil
}

func (f *Fake) RemoveContainer(_ context.Context, id string, force bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.containers[id]
	if !ok {
		return runtime.ErrNotFound
	}
	if c.Running && !force {
		return fmt.Errorf("fakeruntime: container %s is running", id)
	}
	delete(f.containers, id)
	return nil
}

func (f *Fake) InspectContainer(_ context.Context, id string) (runtime.ContainerState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.containers[id]
	if !ok {
		return runtime.ContainerState{}, runtime.ErrNotFound
	}
	return runtime.ContainerState{
		ID:         c.ID,
		Name:       c.Spec.Name,
		Running:    c.Running,
		ExitCode:   c.Exit,
		StartedAt:  c.Started,
		FinishedAt: c.Stopped,
		Ports:      append([]runtime.PortBinding(nil), c.Spec.Ports...),
		Labels:     c.Spec.Labels,
		IPAddress:  c.IP,
		Image:      c.Spec.Image,
	}, nil
}

func (f *Fake) ListContainers(_ context.Context, labels map[string]string) ([]runtime.ContainerInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []runtime.ContainerInfo
	for _, c := range f.containers {
		match := true
		for k, v := range labels {
			if c.Spec.Labels[k] != v {
				match = false
				break
			}
		}
		if match {
			out = append(out, runtime.ContainerInfo{
				ID: c.ID, Name: c.Spec.Name, Image: c.Spec.Image,
				Labels: c.Spec.Labels, Running: c.Running,
			})
		}
	}
	return out, nil
}

func (f *Fake) Logs(ctx context.Context, id string, opts runtime.LogsOptions) (<-chan runtime.LogEntry, error) {
	f.mu.Lock()
	c, ok := f.containers[id]
	if !ok {
		f.mu.Unlock()
		return nil, runtime.ErrNotFound
	}
	lines := append([]runtime.LogEntry(nil), c.LogLines...)
	f.mu.Unlock()

	ch := make(chan runtime.LogEntry, len(lines)+1)
	for _, l := range lines {
		if !opts.Since.IsZero() && l.Time.Before(opts.Since) {
			continue
		}
		ch <- l
	}
	if !opts.Follow {
		close(ch)
	} else {
		go func() {
			<-ctx.Done()
			close(ch)
		}()
	}
	return ch, nil
}

func (f *Fake) Exec(_ context.Context, id string, spec runtime.ExecSpec, stdin io.Reader, stdout, stderr io.Writer, resize <-chan runtime.TermSize) (int, error) {
	f.mu.Lock()
	c, ok := f.containers[id]
	hook := f.Hooks.Exec
	f.mu.Unlock()
	if !ok {
		return -1, runtime.ErrNotFound
	}
	if !c.Running {
		return -1, fmt.Errorf("fakeruntime: container %s not running", id)
	}
	if hook != nil {
		return hook(id, spec, stdin, stdout, stderr, resize)
	}
	if stdout != nil {
		fmt.Fprintf(stdout, "exec: %s\n", strings.Join(spec.Command, " "))
	}
	return 0, nil
}

func (f *Fake) Stats(_ context.Context, id string) (runtime.StatsSample, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.containers[id]; !ok {
		return runtime.StatsSample{}, runtime.ErrNotFound
	}
	return runtime.StatsSample{CPUPercent: 1.0, MemoryBytes: 10 << 20}, nil
}

func (f *Fake) Top(_ context.Context, id string) ([]string, [][]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.containers[id]; !ok {
		return nil, nil, runtime.ErrNotFound
	}
	return []string{"PID", "CMD"}, [][]string{{"1", "/app"}}, nil
}

func (f *Fake) CopyFrom(context.Context, string, string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func (f *Fake) CopyTo(_ context.Context, _, _ string, tarStream io.Reader) error {
	_, err := io.Copy(io.Discard, tarStream)
	return err
}

func (f *Fake) EnsureNetwork(_ context.Context, spec runtime.NetworkSpec) (runtime.NetworkInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if info, ok := f.networks[spec.Name]; ok {
		return info, nil
	}
	info := runtime.NetworkInfo{
		ID: "net-" + spec.Name, Name: spec.Name, Subnet: spec.Subnet,
		Gateway: strings.TrimSuffix(spec.Subnet, "0/24") + "1",
	}
	f.networks[spec.Name] = info
	return info, nil
}

func (f *Fake) RemoveNetwork(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.networks, name)
	return nil
}

func (f *Fake) EnsureVolume(_ context.Context, name string, _ map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.volumes[name] = true
	return nil
}

func (f *Fake) RemoveVolume(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.volumes, name)
	return nil
}

func (f *Fake) VolumeHostPath(_ context.Context, name string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.volumes[name] {
		return "", runtime.ErrNotFound
	}
	return "/fake/volumes/" + name, nil
}
