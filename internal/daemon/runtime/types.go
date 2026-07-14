// Package runtime defines the ContainerRuntime interface — the ONLY place in
// the codebase allowed to touch the Docker SDK is its docker implementation
// (docker.go, task T-15). Everything else (agent, builder, volumes, jobs)
// consumes this interface; tests use testutil/fakeruntime.
//
// FROZEN CONTRACT: extend by adding methods deliberately, never by leaking
// Docker SDK types through it.
package runtime

import (
	"context"
	"errors"
	"io"
	"time"
)

// ErrNotFound is returned for unknown containers/networks/volumes.
var ErrNotFound = errors.New("runtime: not found")

// ManagedLabel marks every object Zattera creates. List operations MUST
// filter on it so we never touch user-owned containers.
const ManagedLabel = "dev.zattera/managed"

// Common label keys set on created objects.
const (
	LabelAssignmentID  = "dev.zattera/assignment-id"
	LabelEnvironmentID = "dev.zattera/environment-id"
	LabelAppID         = "dev.zattera/app-id"
	LabelProjectID     = "dev.zattera/project-id"
	LabelBuildID       = "dev.zattera/build-id"
	LabelJobID         = "dev.zattera/job-id"
	LabelRole          = "dev.zattera/role" // "service" | "build" | "job" | "system"
)

// RegistryAuth is basic-auth for image pulls/pushes.
type RegistryAuth struct {
	Username string
	Password string
	// ServerAddress like "10.90.0.1:5000"; empty for Docker Hub.
	ServerAddress string
}

// PortBinding publishes a container port on a host IP. HostPort 0 lets the
// runtime allocate one; the effective port is reported via Inspect.
type PortBinding struct {
	Name          string // PortSpec.name, for correlation
	ContainerPort uint32
	Protocol      string // "tcp" | "udp"
	HostIP        string // mesh IP for cluster traffic; never 0.0.0.0 unless explicit
	HostPort      uint32
}

// Mount attaches a named volume or a host path.
type Mount struct {
	VolumeName string // named Docker volume (preferred)
	HostPath   string // bind mount (system use only)
	Target     string
	ReadOnly   bool
}

// Resources are cgroup limits.
type Resources struct {
	CPUMillis uint32 // 1000 = one core (converted to NanoCPUs)
	MemoryMB  uint32
	PidsLimit int64
}

// RestartPolicy mirrors Docker's.
type RestartPolicy string

const (
	RestartNever         RestartPolicy = "no"
	RestartOnFailure     RestartPolicy = "on-failure"
	RestartUnlessStopped RestartPolicy = "unless-stopped"
)

// ContainerSpec is everything needed to create a container.
type ContainerSpec struct {
	Name       string
	Image      string
	Command    []string // empty = image default (Docker CMD)
	Entrypoint []string // empty = image default (Docker ENTRYPOINT)
	Env        []string // "KEY=value"
	Labels     map[string]string
	Ports      []PortBinding
	Mounts     []Mount
	Resources  Resources
	Restart    RestartPolicy
	// Network to attach (per project+env bridge). Empty = default bridge.
	Network string
	// DNS servers for the container (the per-network internal resolver).
	DNS []string
	// WorkingDir override; empty = image default.
	WorkingDir string
	// User override ("uid:gid"); empty = image default.
	User string
	// Privileged is required only for system containers (never services).
	Privileged bool
	// StopGrace between SIGTERM and SIGKILL at StopContainer time.
	StopGrace time.Duration
}

// ContainerState is a normalized inspect result.
type ContainerState struct {
	ID         string
	Name       string
	Running    bool
	ExitCode   int
	OOMKilled  bool
	StartedAt  time.Time
	FinishedAt time.Time
	// Ports actually bound (HostPort filled in).
	Ports  []PortBinding
	Labels map[string]string
	// IPAddress on its primary attached network.
	IPAddress string
	Image     string
}

// ContainerInfo is a normalized list entry.
type ContainerInfo struct {
	ID      string
	Name    string
	Image   string
	Labels  map[string]string
	Running bool
}

// LogEntry is one demuxed log line.
type LogEntry struct {
	Time   time.Time
	Stderr bool
	Line   string
}

// LogsOptions selects a log window.
type LogsOptions struct {
	Since  time.Time
	Follow bool
	Tail   int // 0 = all
}

// ExecSpec runs a command inside a running container.
type ExecSpec struct {
	Command []string
	TTY     bool
	Env     []string
	WorkDir string
}

// TermSize is a TTY resize event.
type TermSize struct {
	Cols uint32
	Rows uint32
}

// StatsSample is a one-shot resource reading.
type StatsSample struct {
	CPUPercent  float64
	MemoryBytes uint64
	NetRxBytes  uint64
	NetTxBytes  uint64
}

// NetworkSpec creates a bridge network with a fixed subnet.
type NetworkSpec struct {
	Name   string
	Subnet string // CIDR, e.g. "10.201.4.0/24"
	Labels map[string]string
}

// NetworkInfo describes an existing network.
type NetworkInfo struct {
	ID      string
	Name    string
	Subnet  string
	Gateway string
}

// ContainerRuntime abstracts the container engine on one node.
//
// Semantics every implementation (and the fake) must honor:
//   - EnsureImage is idempotent; progress may be nil.
//   - CreateContainer never starts the container.
//   - StopContainer sends SIGTERM, waits spec.StopGrace (or the passed
//     timeout if > 0), then SIGKILL. Stopping a stopped container is a no-op.
//   - RemoveContainer(force=true) removes running containers.
//   - ListContainers filters by ALL given labels (AND); implementations must
//     additionally always filter on ManagedLabel.
//   - Logs returns a channel that is closed when the log stream ends or ctx
//     is canceled. With Follow, it stays open.
//   - Exec blocks until the process exits and returns its exit code.
type ContainerRuntime interface {
	EnsureImage(ctx context.Context, ref string, auth *RegistryAuth, progress func(status string)) error
	// ImageLoad imports an image tar stream (docker-save format) into the local
	// image store. Used by the dev builder to load a freshly built image without
	// a registry round-trip.
	ImageLoad(ctx context.Context, tar io.Reader) error
	CreateContainer(ctx context.Context, spec ContainerSpec) (id string, err error)
	StartContainer(ctx context.Context, id string) error
	StopContainer(ctx context.Context, id string, timeout time.Duration) error
	RemoveContainer(ctx context.Context, id string, force bool) error
	InspectContainer(ctx context.Context, id string) (ContainerState, error)
	ListContainers(ctx context.Context, labels map[string]string) ([]ContainerInfo, error)
	Logs(ctx context.Context, id string, opts LogsOptions) (<-chan LogEntry, error)
	Exec(ctx context.Context, id string, spec ExecSpec, stdin io.Reader, stdout, stderr io.Writer, resize <-chan TermSize) (exitCode int, err error)
	Stats(ctx context.Context, id string) (StatsSample, error)
	Top(ctx context.Context, id string) (titles []string, processes [][]string, err error)
	// CopyFrom streams a tar archive of a container path; CopyTo extracts one.
	CopyFrom(ctx context.Context, id, path string) (io.ReadCloser, error)
	CopyTo(ctx context.Context, id, path string, tarStream io.Reader) error

	EnsureNetwork(ctx context.Context, spec NetworkSpec) (NetworkInfo, error)
	RemoveNetwork(ctx context.Context, name string) error
	EnsureVolume(ctx context.Context, name string, labels map[string]string) error
	RemoveVolume(ctx context.Context, name string) error
	// VolumeHostPath returns the host filesystem path of a named volume's data
	// (used by the snapshot engine).
	VolumeHostPath(ctx context.Context, name string) (string, error)

	// Ping verifies the engine is reachable (startup check).
	Ping(ctx context.Context) error
}
