// docker.go is the ONLY file permitted to import the Docker SDK (spec rule 3).
// It maps the frozen ContainerRuntime interface onto Docker Engine v28.
package runtime

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	imagetypes "github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	networktypes "github.com/docker/docker/api/types/network"
	volumetypes "github.com/docker/docker/api/types/volume"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
)

// Docker implements ContainerRuntime against a local Docker Engine.
type Docker struct {
	cli *client.Client
}

// NewDocker connects to the engine using standard env configuration and
// negotiates the API version.
func NewDocker() (*Docker, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("runtime: docker client: %w", err)
	}
	return &Docker{cli: cli}, nil
}

// Ping verifies the engine is reachable.
func (d *Docker) Ping(ctx context.Context) error {
	if _, err := d.cli.Ping(ctx); err != nil {
		return fmt.Errorf("runtime: ping: %w", err)
	}
	return nil
}

// ImageLoad imports a docker-save-format tar stream into the local image store.
func (d *Docker) ImageLoad(ctx context.Context, tar io.Reader) error {
	resp, err := d.cli.ImageLoad(ctx, tar)
	if err != nil {
		return fmt.Errorf("runtime: image load: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body) // drain so the load completes
	return nil
}

// EnsureImage pulls ref unless it is already present locally. Progress lines are
// forwarded to progress (may be nil). Context cancellation aborts the pull.
func (d *Docker) EnsureImage(ctx context.Context, ref string, auth *RegistryAuth, progress func(status string)) error {
	if _, err := d.cli.ImageInspect(ctx, ref); err == nil {
		return nil // already present
	}
	opts := imagetypes.PullOptions{}
	if auth != nil {
		enc, err := encodeAuth(auth)
		if err != nil {
			return err
		}
		opts.RegistryAuth = enc
	}
	rc, err := d.cli.ImagePull(ctx, ref, opts)
	if err != nil {
		return fmt.Errorf("runtime: pull %s: %w", ref, err)
	}
	// Drain in a goroutine so ctx cancellation can close the reader promptly.
	done := make(chan error, 1)
	go func() { done <- drainPull(rc, progress) }()
	select {
	case <-ctx.Done():
		_ = rc.Close()
		<-done
		return ctx.Err()
	case err := <-done:
		_ = rc.Close()
		return err
	}
}

// CreateContainer creates (but never starts) a container. Tty is always false
// so log streams stay multiplexed.
func (d *Docker) CreateContainer(ctx context.Context, spec ContainerSpec) (string, error) {
	labels := map[string]string{}
	for k, v := range spec.Labels {
		labels[k] = v
	}
	labels[ManagedLabel] = "true"

	exposed, bindings := portMaps(spec.Ports)
	cfg := &container.Config{
		Image:        spec.Image,
		Entrypoint:   spec.Entrypoint,
		Cmd:          spec.Command,
		Env:          spec.Env,
		Labels:       labels,
		Tty:          false,
		WorkingDir:   spec.WorkingDir,
		User:         spec.User,
		ExposedPorts: exposed,
	}
	hostCfg := &container.HostConfig{
		PortBindings:  bindings,
		RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyMode(spec.Restart)},
		Resources:     resources(spec.Resources),
		Mounts:        mounts(spec.Mounts),
		DNS:           spec.DNS,
		Privileged:    spec.Privileged,
	}
	var netCfg *networktypes.NetworkingConfig
	if spec.Network != "" {
		netCfg = &networktypes.NetworkingConfig{
			EndpointsConfig: map[string]*networktypes.EndpointSettings{spec.Network: {}},
		}
	}
	resp, err := d.cli.ContainerCreate(ctx, cfg, hostCfg, netCfg, nil, spec.Name)
	if err != nil {
		return "", fmt.Errorf("runtime: create %s: %w", spec.Name, err)
	}
	return resp.ID, nil
}

// StartContainer starts a created container.
func (d *Docker) StartContainer(ctx context.Context, id string) error {
	if err := d.cli.ContainerStart(ctx, id, container.StartOptions{}); err != nil {
		return normalizeErr("start", err)
	}
	return nil
}

// StopContainer sends SIGTERM then SIGKILL after the timeout (or the engine
// default when timeout <= 0). Stopping a stopped/absent container is a no-op.
func (d *Docker) StopContainer(ctx context.Context, id string, timeout time.Duration) error {
	opts := container.StopOptions{}
	if timeout > 0 {
		secs := int(timeout.Seconds())
		opts.Timeout = &secs
	}
	if err := d.cli.ContainerStop(ctx, id, opts); err != nil {
		if errdefs.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("runtime: stop: %w", err)
	}
	return nil
}

// RemoveContainer removes a container; force removes a running one. Removing an
// absent container is a no-op.
func (d *Docker) RemoveContainer(ctx context.Context, id string, force bool) error {
	if err := d.cli.ContainerRemove(ctx, id, container.RemoveOptions{Force: force}); err != nil {
		if errdefs.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("runtime: remove: %w", err)
	}
	return nil
}

// InspectContainer returns a normalized state; ErrNotFound if it is gone.
func (d *Docker) InspectContainer(ctx context.Context, id string) (ContainerState, error) {
	c, err := d.cli.ContainerInspect(ctx, id)
	if err != nil {
		return ContainerState{}, normalizeErr("inspect", err)
	}
	st := ContainerState{
		ID:        c.ID,
		Name:      strings.TrimPrefix(c.Name, "/"),
		Image:     c.Config.Image,
		Labels:    c.Config.Labels,
		Ports:     inspectPorts(c.NetworkSettings.Ports),
		IPAddress: primaryIP(c.NetworkSettings.Networks),
	}
	if c.State != nil {
		st.Running = c.State.Running
		st.ExitCode = c.State.ExitCode
		st.OOMKilled = c.State.OOMKilled
		st.StartedAt = parseTime(c.State.StartedAt)
		st.FinishedAt = parseTime(c.State.FinishedAt)
	}
	return st, nil
}

// ListContainers filters by ManagedLabel plus every requested label (AND).
func (d *Docker) ListContainers(ctx context.Context, labels map[string]string) ([]ContainerInfo, error) {
	args := filters.NewArgs()
	args.Add("label", ManagedLabel)
	for k, v := range labels {
		args.Add("label", k+"="+v)
	}
	list, err := d.cli.ContainerList(ctx, container.ListOptions{All: true, Filters: args})
	if err != nil {
		return nil, fmt.Errorf("runtime: list: %w", err)
	}
	out := make([]ContainerInfo, 0, len(list))
	for _, c := range list {
		name := ""
		if len(c.Names) > 0 {
			name = strings.TrimPrefix(c.Names[0], "/")
		}
		out = append(out, ContainerInfo{
			ID:      c.ID,
			Name:    name,
			Image:   c.Image,
			Labels:  c.Labels,
			Running: c.State == "running",
		})
	}
	return out, nil
}

// Logs streams demuxed, timestamped log lines. The channel closes at stream end
// or on ctx cancellation.
func (d *Docker) Logs(ctx context.Context, id string, opts LogsOptions) (<-chan LogEntry, error) {
	logOpts := container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Timestamps: true,
		Follow:     opts.Follow,
	}
	if !opts.Since.IsZero() {
		logOpts.Since = opts.Since.Format(time.RFC3339Nano)
	}
	if opts.Tail > 0 {
		logOpts.Tail = strconv.Itoa(opts.Tail)
	}
	rc, err := d.cli.ContainerLogs(ctx, id, logOpts)
	if err != nil {
		return nil, normalizeErr("logs", err)
	}
	ch := make(chan LogEntry, 64)
	go streamLogs(ctx, rc, ch)
	return ch, nil
}

// Exec runs a command in a container and returns its exit code.
func (d *Docker) Exec(ctx context.Context, id string, spec ExecSpec, stdin io.Reader, stdout, stderr io.Writer, resize <-chan TermSize) (int, error) {
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	created, err := d.cli.ContainerExecCreate(ctx, id, container.ExecOptions{
		Cmd:          spec.Command,
		Tty:          spec.TTY,
		Env:          spec.Env,
		WorkingDir:   spec.WorkDir,
		AttachStdin:  stdin != nil,
		AttachStdout: true,
		AttachStderr: true,
	})
	if err != nil {
		return -1, normalizeErr("exec create", err)
	}
	att, err := d.cli.ContainerExecAttach(ctx, created.ID, container.ExecAttachOptions{Tty: spec.TTY})
	if err != nil {
		return -1, fmt.Errorf("runtime: exec attach: %w", err)
	}
	defer att.Close()

	if resize != nil {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case sz, ok := <-resize:
					if !ok {
						return
					}
					_ = d.cli.ContainerExecResize(ctx, created.ID, container.ResizeOptions{Height: uint(sz.Rows), Width: uint(sz.Cols)})
				}
			}
		}()
	}

	if stdin != nil {
		go func() {
			_, _ = io.Copy(att.Conn, stdin)
			_ = att.CloseWrite()
		}()
	}

	copyDone := make(chan error, 1)
	go func() {
		var cerr error
		if spec.TTY {
			_, cerr = io.Copy(stdout, att.Reader)
		} else {
			_, cerr = stdcopy.StdCopy(stdout, stderr, att.Reader)
		}
		copyDone <- cerr
	}()
	select {
	case <-ctx.Done():
		return -1, ctx.Err()
	case <-copyDone:
	}

	// Poll until the exec reports completion.
	for {
		insp, err := d.cli.ContainerExecInspect(ctx, created.ID)
		if err != nil {
			return -1, fmt.Errorf("runtime: exec inspect: %w", err)
		}
		if !insp.Running {
			return insp.ExitCode, nil
		}
		select {
		case <-ctx.Done():
			return -1, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// Stats returns a one-shot resource sample.
func (d *Docker) Stats(ctx context.Context, id string) (StatsSample, error) {
	resp, err := d.cli.ContainerStatsOneShot(ctx, id)
	if err != nil {
		return StatsSample{}, normalizeErr("stats", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var s container.StatsResponse
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return StatsSample{}, fmt.Errorf("runtime: decode stats: %w", err)
	}
	return statsSample(&s), nil
}

// Top lists processes running in a container.
func (d *Docker) Top(ctx context.Context, id string) ([]string, [][]string, error) {
	body, err := d.cli.ContainerTop(ctx, id, nil)
	if err != nil {
		return nil, nil, normalizeErr("top", err)
	}
	return body.Titles, body.Processes, nil
}

// CopyFrom streams a tar of a container path.
func (d *Docker) CopyFrom(ctx context.Context, id, path string) (io.ReadCloser, error) {
	rc, _, err := d.cli.CopyFromContainer(ctx, id, path)
	if err != nil {
		return nil, normalizeErr("copy from", err)
	}
	return rc, nil
}

// CopyTo extracts a tar stream into a container path.
func (d *Docker) CopyTo(ctx context.Context, id, path string, tarStream io.Reader) error {
	if err := d.cli.CopyToContainer(ctx, id, path, tarStream, container.CopyToContainerOptions{}); err != nil {
		return normalizeErr("copy to", err)
	}
	return nil
}

// EnsureNetwork creates a bridge network with a fixed subnet, or returns the
// existing one.
func (d *Docker) EnsureNetwork(ctx context.Context, spec NetworkSpec) (NetworkInfo, error) {
	if info, err := d.inspectNetwork(ctx, spec.Name); err == nil {
		return info, nil
	} else if !errdefs.IsNotFound(err) {
		return NetworkInfo{}, fmt.Errorf("runtime: inspect network: %w", err)
	}
	labels := map[string]string{ManagedLabel: "true"}
	for k, v := range spec.Labels {
		labels[k] = v
	}
	opts := networktypes.CreateOptions{Driver: "bridge", Labels: labels}
	if spec.Subnet != "" {
		opts.IPAM = &networktypes.IPAM{Config: []networktypes.IPAMConfig{{Subnet: spec.Subnet}}}
	}
	if _, err := d.cli.NetworkCreate(ctx, spec.Name, opts); err != nil {
		return NetworkInfo{}, fmt.Errorf("runtime: create network: %w", err)
	}
	return d.inspectNetwork(ctx, spec.Name)
}

// RemoveNetwork removes a network; absent is a no-op.
func (d *Docker) RemoveNetwork(ctx context.Context, name string) error {
	if err := d.cli.NetworkRemove(ctx, name); err != nil {
		if errdefs.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("runtime: remove network: %w", err)
	}
	return nil
}

// EnsureVolume creates a named volume unless it exists.
func (d *Docker) EnsureVolume(ctx context.Context, name string, labels map[string]string) error {
	if _, err := d.cli.VolumeInspect(ctx, name); err == nil {
		return nil
	} else if !errdefs.IsNotFound(err) {
		return fmt.Errorf("runtime: inspect volume: %w", err)
	}
	merged := map[string]string{ManagedLabel: "true"}
	for k, v := range labels {
		merged[k] = v
	}
	if _, err := d.cli.VolumeCreate(ctx, volumetypes.CreateOptions{Name: name, Labels: merged}); err != nil {
		return fmt.Errorf("runtime: create volume: %w", err)
	}
	return nil
}

// RemoveVolume removes a named volume; absent is a no-op.
func (d *Docker) RemoveVolume(ctx context.Context, name string) error {
	if err := d.cli.VolumeRemove(ctx, name, false); err != nil {
		if errdefs.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("runtime: remove volume: %w", err)
	}
	return nil
}

// VolumeHostPath returns the host path of a named volume's data.
func (d *Docker) VolumeHostPath(ctx context.Context, name string) (string, error) {
	v, err := d.cli.VolumeInspect(ctx, name)
	if err != nil {
		return "", normalizeErr("volume host path", err)
	}
	return v.Mountpoint, nil
}

// Platform returns the engine's execution platform as a raw "os/arch" string
// (e.g. "linux/aarch64" — Docker reports uname-style arch, so callers must
// normalize). This is what containers actually run on, which is NOT the
// daemon binary's platform on macOS/Windows: there the daemon is darwin/
// windows while the engine runs a linux VM (T-97).
func (d *Docker) Platform(ctx context.Context) (string, error) {
	info, err := d.cli.Info(ctx)
	if err != nil {
		return "", normalizeErr("engine info", err)
	}
	if info.OSType == "" || info.Architecture == "" {
		return "", fmt.Errorf("runtime: engine info reported empty platform (os=%q arch=%q)", info.OSType, info.Architecture)
	}
	return info.OSType + "/" + info.Architecture, nil
}

// EnginePlatform queries the local engine's platform with a short-lived
// client. It exists because the node registers itself (and joins) before the
// long-lived runtime is constructed, and the node's advertised os-arch must
// describe the engine, not the daemon binary (T-97).
func EnginePlatform(ctx context.Context) (string, error) {
	d, err := NewDocker()
	if err != nil {
		return "", err
	}
	defer func() { _ = d.cli.Close() }()
	return d.Platform(ctx)
}

func (d *Docker) inspectNetwork(ctx context.Context, name string) (NetworkInfo, error) {
	n, err := d.cli.NetworkInspect(ctx, name, networktypes.InspectOptions{})
	if err != nil {
		return NetworkInfo{}, err
	}
	info := NetworkInfo{ID: n.ID, Name: n.Name}
	if len(n.IPAM.Config) > 0 {
		info.Subnet = n.IPAM.Config[0].Subnet
		info.Gateway = n.IPAM.Config[0].Gateway
	}
	return info, nil
}

// --- pure helpers (unit-tested) ---

func encodeAuth(auth *RegistryAuth) (string, error) {
	b, err := json.Marshal(map[string]string{
		"username":      auth.Username,
		"password":      auth.Password,
		"serveraddress": auth.ServerAddress,
	})
	if err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(b), nil
}

func resources(r Resources) container.Resources {
	res := container.Resources{
		NanoCPUs: int64(r.CPUMillis) * 1_000_000, // 1000 millis = 1 core = 1e9 nanoCPUs
		Memory:   int64(r.MemoryMB) << 20,
	}
	if r.PidsLimit > 0 {
		res.PidsLimit = &r.PidsLimit
	}
	return res
}

func mounts(ms []Mount) []mount.Mount {
	if len(ms) == 0 {
		return nil
	}
	out := make([]mount.Mount, 0, len(ms))
	for _, m := range ms {
		mt := mount.Mount{Target: m.Target, ReadOnly: m.ReadOnly}
		if m.VolumeName != "" {
			mt.Type = mount.TypeVolume
			mt.Source = m.VolumeName
		} else {
			mt.Type = mount.TypeBind
			mt.Source = m.HostPath
		}
		out = append(out, mt)
	}
	return out
}

// portMaps builds the exposed-port set and host bindings. HostPort 0 means the
// engine picks one.
func portMaps(ports []PortBinding) (nat.PortSet, nat.PortMap) {
	if len(ports) == 0 {
		return nil, nil
	}
	exposed := nat.PortSet{}
	bindings := nat.PortMap{}
	for _, p := range ports {
		proto := p.Protocol
		if proto == "" {
			proto = "tcp"
		}
		np := nat.Port(fmt.Sprintf("%d/%s", p.ContainerPort, proto))
		exposed[np] = struct{}{}
		hostPort := ""
		if p.HostPort != 0 {
			hostPort = strconv.Itoa(int(p.HostPort))
		}
		bindings[np] = append(bindings[np], nat.PortBinding{HostIP: p.HostIP, HostPort: hostPort})
	}
	return exposed, bindings
}

// inspectPorts reads effective host bindings from an inspect result.
func inspectPorts(pm nat.PortMap) []PortBinding {
	var out []PortBinding
	for np, binds := range pm {
		for _, b := range binds {
			hp, _ := strconv.Atoi(b.HostPort)
			out = append(out, PortBinding{
				ContainerPort: uint32(np.Int()),
				Protocol:      np.Proto(),
				HostIP:        b.HostIP,
				HostPort:      uint32(hp),
			})
		}
	}
	return out
}

func primaryIP(networks map[string]*networktypes.EndpointSettings) string {
	for _, n := range networks {
		if n.IPAddress != "" {
			return n.IPAddress
		}
	}
	return ""
}

func parseTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}
	}
	// Docker uses a zero sentinel "0001-01-01T00:00:00Z".
	if t.Year() <= 1 {
		return time.Time{}
	}
	return t
}

func statsSample(s *container.StatsResponse) StatsSample {
	sample := StatsSample{MemoryBytes: s.MemoryStats.Usage}
	cpuDelta := float64(s.CPUStats.CPUUsage.TotalUsage) - float64(s.PreCPUStats.CPUUsage.TotalUsage)
	sysDelta := float64(s.CPUStats.SystemUsage) - float64(s.PreCPUStats.SystemUsage)
	onlineCPUs := float64(s.CPUStats.OnlineCPUs)
	if onlineCPUs == 0 {
		onlineCPUs = float64(len(s.CPUStats.CPUUsage.PercpuUsage))
	}
	if cpuDelta > 0 && sysDelta > 0 {
		sample.CPUPercent = (cpuDelta / sysDelta) * onlineCPUs * 100.0
	}
	for _, nw := range s.Networks {
		sample.NetRxBytes += nw.RxBytes
		sample.NetTxBytes += nw.TxBytes
	}
	return sample
}

// drainPull reads the pull JSON stream, forwarding status lines.
func drainPull(rc io.Reader, progress func(status string)) error {
	dec := json.NewDecoder(rc)
	for {
		var msg struct {
			Status string `json:"status"`
			Error  string `json:"error"`
		}
		if err := dec.Decode(&msg); err == io.EOF {
			return nil
		} else if err != nil {
			return fmt.Errorf("runtime: pull stream: %w", err)
		}
		if msg.Error != "" {
			return fmt.Errorf("runtime: pull: %s", msg.Error)
		}
		if progress != nil && msg.Status != "" {
			progress(msg.Status)
		}
	}
}

// streamLogs demuxes the multiplexed (Tty:false) log stream into timestamped
// LogEntry lines and closes ch at EOF or ctx cancellation.
func streamLogs(ctx context.Context, rc io.ReadCloser, ch chan<- LogEntry) {
	defer close(ch)
	defer func() { _ = rc.Close() }()

	// Close the reader when ctx is canceled so the blocking read returns.
	go func() {
		<-ctx.Done()
		_ = rc.Close()
	}()

	br := bufio.NewReader(rc)
	header := make([]byte, 8)
	for {
		if _, err := io.ReadFull(br, header); err != nil {
			return
		}
		stderr := header[0] == 2
		size := binary.BigEndian.Uint32(header[4:8])
		payload := make([]byte, size)
		if _, err := io.ReadFull(br, payload); err != nil {
			return
		}
		for _, line := range splitLines(payload) {
			ts, text := splitTimestamp(line)
			select {
			case <-ctx.Done():
				return
			case ch <- LogEntry{Time: ts, Stderr: stderr, Line: text}:
			}
		}
	}
}

func splitLines(b []byte) []string {
	s := strings.TrimRight(string(b), "\n")
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

// splitTimestamp separates the RFC3339Nano prefix that Timestamps:true prepends.
func splitTimestamp(line string) (time.Time, string) {
	sp := strings.IndexByte(line, ' ')
	if sp < 0 {
		return time.Time{}, line
	}
	if t, err := time.Parse(time.RFC3339Nano, line[:sp]); err == nil {
		return t, line[sp+1:]
	}
	return time.Time{}, line
}

// normalizeErr maps Docker's not-found to the runtime contract's ErrNotFound.
func normalizeErr(op string, err error) error {
	if errdefs.IsNotFound(err) {
		return ErrNotFound
	}
	return fmt.Errorf("runtime: %s: %w", op, err)
}

// compile-time assertion that Docker satisfies the interface.
var _ ContainerRuntime = (*Docker)(nil)
