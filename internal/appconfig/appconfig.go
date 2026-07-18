// Package appconfig parses the per-app zattera.toml (spec §4) into the proto
// shapes ApplyAppConfig consumes, and computes the deterministic config hash
// that identifies a release's effective configuration.
//
// The parser is strict: unknown keys are hard errors, durations are TOML
// strings ("15m") parsed with time.ParseDuration, and every validation failure
// carries an actionable, dotted-path message (e.g.
// "env.production.replicas.min > max").
package appconfig

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/platform"
)

// AppConfig is the parsed, defaulted result. Build/Services/Domains are exactly
// what ApplyAppConfigRequest wants; IdleTimeouts and Image carry fields that do
// not fit into ServiceSpec.
type AppConfig struct {
	Name     string
	Build    *zatterav1.BuildConfig
	GitHub   *zatterav1.GitHubConfig
	Image    string // pre-built image ref when [build] type = "image"
	Services map[string]*zatterav1.ServiceSpec
	Domains  map[string][]string
	// IdleTimeouts is the scale-to-zero idle window per env (lives on
	// Environment, not ServiceSpec).
	IdleTimeouts map[string]time.Duration
}

// --- TOML schema (strict) ---

type file struct {
	App    appSection             `toml:"app"`
	Build  *buildSection          `toml:"build"`
	GitHub *githubSection         `toml:"github"`
	Deploy *deploySection         `toml:"deploy"`
	Env    map[string]*envSection `toml:"env"`
	Cron   []cronSection          `toml:"cron"`
}

type appSection struct {
	Name string `toml:"name"`
}

type buildSection struct {
	Type       string            `toml:"type"`
	Dockerfile string            `toml:"dockerfile"`
	Context    string            `toml:"context"`
	Image      string            `toml:"image"`
	Args       map[string]string `toml:"args"`
	Platforms  []string          `toml:"platforms"`
}

type githubSection struct {
	Repo            string `toml:"repo"`
	PreviewsEnabled bool   `toml:"previews"`
}

type deploySection struct {
	Healthcheck *healthcheckSection `toml:"healthcheck"`
}

type healthcheckSection struct {
	Type               string `toml:"type"`
	Path               string `toml:"path"`
	Port               uint32 `toml:"port"`
	Command            string `toml:"command"`
	Interval           string `toml:"interval"`
	Timeout            string `toml:"timeout"`
	GracePeriod        string `toml:"grace_period"`
	UnhealthyThreshold uint32 `toml:"unhealthy_threshold"`
}

type envSection struct {
	Replicas       *int              `toml:"replicas"`
	MinReplicas    *uint32           `toml:"min_replicas"`
	MaxReplicas    *uint32           `toml:"max_replicas"`
	Autoscale      *autoscaleSection `toml:"autoscale"`
	Domains        []string          `toml:"domains"`
	IdleTimeout    string            `toml:"idle_timeout"`
	Stateful       bool              `toml:"stateful"`
	ScaleToZero    bool              `toml:"scale_to_zero"`
	MaxConcurrency uint32            `toml:"max_concurrency"`
	Command        string            `toml:"command"`
	StopGrace      string            `toml:"stop_grace"`
	Resources      *resourceSection  `toml:"resources"`
	Ports          []portSection     `toml:"ports"`
	Volumes        []volumeSection   `toml:"volumes"`
	Cron           []cronSection     `toml:"cron"`
	Placement      map[string]string `toml:"placement"`
	RateLimit      *rateLimitSection `toml:"rate_limit"`
}

type rateLimitSection struct {
	RequestsPerSecond uint32 `toml:"requests_per_second"`
	Burst             uint32 `toml:"burst"`
}

type autoscaleSection struct {
	TargetCPUPercent    uint32 `toml:"target_cpu_percent"`
	TargetMemoryPercent uint32 `toml:"target_memory_percent"`
	TargetRPSPerReplica uint32 `toml:"target_rps_per_replica"`
}

type resourceSection struct {
	CPUMillis uint32 `toml:"cpu_millis"`
	MemoryMB  uint32 `toml:"memory_mb"`
}

type portSection struct {
	Name          string `toml:"name"`
	ContainerPort uint32 `toml:"container_port"`
	Protocol      string `toml:"protocol"`
}

type volumeSection struct {
	Name      string `toml:"name"`
	MountPath string `toml:"mount_path"`
}

type cronSection struct {
	Name        string `toml:"name"`
	Schedule    string `toml:"schedule"`
	Command     string `toml:"command"`
	Concurrency string `toml:"concurrency"`
	MaxRetries  uint32 `toml:"max_retries"`
}

// Parse reads a zattera.toml document.
func Parse(data []byte) (*AppConfig, error) {
	var f file
	meta, err := toml.Decode(string(data), &f)
	if err != nil {
		return nil, fmt.Errorf("appconfig: %w", err)
	}
	if undecoded := meta.Undecoded(); len(undecoded) > 0 {
		keys := make([]string, len(undecoded))
		for i, k := range undecoded {
			keys[i] = k.String()
		}
		sort.Strings(keys)
		return nil, fmt.Errorf("appconfig: unknown keys: %s", strings.Join(keys, ", "))
	}
	return build(&f)
}

func build(f *file) (*AppConfig, error) {
	if f.App.Name == "" {
		return nil, fmt.Errorf("appconfig: [app] name is required")
	}
	bc, err := buildConfig(f.Build)
	if err != nil {
		return nil, err
	}
	cfg := &AppConfig{
		Name:         f.App.Name,
		Build:        bc,
		Image:        buildImage(f.Build),
		Services:     map[string]*zatterav1.ServiceSpec{},
		Domains:      map[string][]string{},
		IdleTimeouts: map[string]time.Duration{},
	}
	if f.GitHub != nil {
		cfg.GitHub = &zatterav1.GitHubConfig{Repo: f.GitHub.Repo, PreviewEnvironments: f.GitHub.PreviewsEnabled}
	}

	hc, err := healthCheck(f.Deploy)
	if err != nil {
		return nil, err
	}
	globalCron, err := cronSpecs("cron", f.Cron)
	if err != nil {
		return nil, err
	}

	for name, env := range f.Env {
		spec, domains, idle, err := serviceSpec(name, env, hc, globalCron)
		if err != nil {
			return nil, err
		}
		cfg.Services[name] = spec
		if len(domains) > 0 {
			cfg.Domains[name] = domains
		}
		if idle > 0 {
			cfg.IdleTimeouts[name] = idle
		}
	}
	return cfg, nil
}

func buildConfig(b *buildSection) (*zatterav1.BuildConfig, error) {
	bc := &zatterav1.BuildConfig{DockerfilePath: "Dockerfile", ContextDir: "."}
	if b == nil {
		return bc, nil
	}
	bc.Type = buildType(b.Type)
	if b.Dockerfile != "" {
		bc.DockerfilePath = b.Dockerfile
	}
	if b.Context != "" {
		bc.ContextDir = b.Context
	}
	if len(b.Args) > 0 {
		bc.BuildArgs = b.Args
	}
	// Platforms are normalized here so everything downstream (builds,
	// releases, placement) sees canonical "os/arch" strings. Absent = empty
	// (cluster-arch default resolved at build time).
	for _, p := range b.Platforms {
		n, err := platform.Normalize(p)
		if err != nil {
			return nil, fmt.Errorf("appconfig: build.platforms: %w", err)
		}
		bc.Platforms = append(bc.Platforms, n)
	}
	return bc, nil
}

func buildImage(b *buildSection) string {
	if b == nil {
		return ""
	}
	return b.Image
}

func buildType(s string) zatterav1.BuildType {
	switch strings.ToLower(s) {
	case "nixpacks":
		return zatterav1.BuildType_BUILD_TYPE_NIXPACKS
	case "dockerfile":
		return zatterav1.BuildType_BUILD_TYPE_DOCKERFILE
	case "image":
		return zatterav1.BuildType_BUILD_TYPE_IMAGE
	default:
		return zatterav1.BuildType_BUILD_TYPE_UNSPECIFIED
	}
}

func serviceSpec(name string, env *envSection, hc *zatterav1.HealthCheck, globalCron []*zatterav1.CronSpec) (*zatterav1.ServiceSpec, []string, time.Duration, error) {
	spec := &zatterav1.ServiceSpec{
		Stateful:       env.Stateful,
		ScaleToZero:    env.ScaleToZero,
		MaxConcurrency: env.MaxConcurrency,
		Command:        env.Command,
	}

	// Replicas.
	repl, err := replicaRange(name, env)
	if err != nil {
		return nil, nil, 0, err
	}
	spec.Replicas = repl

	// Autoscale.
	if a := env.Autoscale; a != nil {
		spec.Autoscale = &zatterav1.Autoscale{
			TargetCpuPercent:    a.TargetCPUPercent,
			TargetMemoryPercent: a.TargetMemoryPercent,
			TargetRpsPerReplica: a.TargetRPSPerReplica,
		}
	}

	// Resources.
	if r := env.Resources; r != nil {
		spec.Resources = &zatterav1.ResourceLimits{CpuMillis: r.CPUMillis, MemoryMb: r.MemoryMB}
	}

	// Ports (default http/8080).
	spec.Ports = ports(env.Ports)

	// Healthcheck (default HTTP /healthz when an http port exists).
	spec.Healthcheck = defaultedHealthCheck(hc, spec.Ports)

	// Volumes.
	for _, v := range env.Volumes {
		if v.Name == "" || v.MountPath == "" {
			return nil, nil, 0, fmt.Errorf("appconfig: env.%s.volumes: name and mount_path are required", name)
		}
		spec.Volumes = append(spec.Volumes, &zatterav1.VolumeMount{VolumeName: v.Name, MountPath: v.MountPath})
	}

	// Cron: per-env overrides global.
	if len(env.Cron) > 0 {
		c, err := cronSpecs("env."+name+".cron", env.Cron)
		if err != nil {
			return nil, nil, 0, err
		}
		spec.Cron = c
	} else {
		spec.Cron = globalCron
	}

	// Stop grace.
	if env.StopGrace != "" {
		d, err := parseDuration("env."+name+".stop_grace", env.StopGrace)
		if err != nil {
			return nil, nil, 0, err
		}
		spec.StopGrace = durationpb.New(d)
	} else {
		spec.StopGrace = durationpb.New(10 * time.Second)
	}

	if len(env.Placement) > 0 {
		spec.PlacementConstraints = env.Placement
	}

	// Rate limit: absent section = off. burst defaults to one second's worth of
	// requests, and may not be below rps — a burst under the sustained rate can
	// never be refilled into, so it would silently cap throughput below the
	// stated limit.
	if rl := env.RateLimit; rl != nil {
		if rl.RequestsPerSecond == 0 {
			return nil, nil, 0, fmt.Errorf("appconfig: env.%s.rate_limit.requests_per_second must be > 0 (omit the section to disable)", name)
		}
		burst := rl.Burst
		if burst == 0 {
			burst = rl.RequestsPerSecond
		} else if burst < rl.RequestsPerSecond {
			return nil, nil, 0, fmt.Errorf("appconfig: env.%s.rate_limit.burst (%d) must be >= requests_per_second (%d)", name, burst, rl.RequestsPerSecond)
		}
		spec.RateLimit = &zatterav1.RateLimit{RequestsPerSecond: rl.RequestsPerSecond, Burst: burst}
	}

	// Idle timeout (Environment-level, returned separately).
	var idle time.Duration
	if env.IdleTimeout != "" {
		idle, err = parseDuration("env."+name+".idle_timeout", env.IdleTimeout)
		if err != nil {
			return nil, nil, 0, err
		}
	}
	return spec, env.Domains, idle, nil
}

func replicaRange(name string, env *envSection) (*zatterav1.ReplicaRange, error) {
	var min, max uint32 = 1, 1
	switch {
	case env.MinReplicas != nil || env.MaxReplicas != nil:
		if env.MinReplicas != nil {
			min = *env.MinReplicas
		}
		if env.MaxReplicas != nil {
			max = *env.MaxReplicas
		} else {
			max = min
		}
	case env.Replicas != nil:
		if *env.Replicas < 0 {
			return nil, fmt.Errorf("appconfig: env.%s.replicas must be >= 0", name)
		}
		min = uint32(*env.Replicas)
		max = min
	}
	if max < min {
		return nil, fmt.Errorf("appconfig: env.%s.replicas.min > max (%d > %d)", name, min, max)
	}
	return &zatterav1.ReplicaRange{Min: min, Max: max}, nil
}

func ports(in []portSection) []*zatterav1.PortSpec {
	if len(in) == 0 {
		return []*zatterav1.PortSpec{{Name: "http", ContainerPort: 8080, Protocol: zatterav1.Protocol_PROTOCOL_HTTP}}
	}
	out := make([]*zatterav1.PortSpec, 0, len(in))
	for _, p := range in {
		name := p.Name
		if name == "" {
			name = "http"
		}
		out = append(out, &zatterav1.PortSpec{Name: name, ContainerPort: p.ContainerPort, Protocol: protocol(p.Protocol)})
	}
	return out
}

func protocol(s string) zatterav1.Protocol {
	switch strings.ToLower(s) {
	case "tcp":
		return zatterav1.Protocol_PROTOCOL_TCP
	case "udp":
		return zatterav1.Protocol_PROTOCOL_UDP
	case "http", "":
		return zatterav1.Protocol_PROTOCOL_HTTP
	default:
		return zatterav1.Protocol_PROTOCOL_HTTP
	}
}

func healthCheck(d *deploySection) (*zatterav1.HealthCheck, error) {
	if d == nil || d.Healthcheck == nil {
		return nil, nil
	}
	h := d.Healthcheck
	hc := &zatterav1.HealthCheck{
		Type:               healthType(h.Type),
		Path:               h.Path,
		Port:               h.Port,
		Command:            h.Command,
		UnhealthyThreshold: h.UnhealthyThreshold,
	}
	for _, dp := range []struct {
		field string
		raw   string
		set   func(*durationpb.Duration)
	}{
		{"interval", h.Interval, func(d *durationpb.Duration) { hc.Interval = d }},
		{"timeout", h.Timeout, func(d *durationpb.Duration) { hc.Timeout = d }},
		{"grace_period", h.GracePeriod, func(d *durationpb.Duration) { hc.GracePeriod = d }},
	} {
		if dp.raw == "" {
			continue
		}
		v, err := parseDuration("deploy.healthcheck."+dp.field, dp.raw)
		if err != nil {
			return nil, err
		}
		dp.set(durationpb.New(v))
	}
	return hc, nil
}

func healthType(s string) zatterav1.HealthCheckType {
	switch strings.ToLower(s) {
	case "http":
		return zatterav1.HealthCheckType_HEALTH_CHECK_TYPE_HTTP
	case "tcp":
		return zatterav1.HealthCheckType_HEALTH_CHECK_TYPE_TCP
	case "exec":
		return zatterav1.HealthCheckType_HEALTH_CHECK_TYPE_EXEC
	default:
		return zatterav1.HealthCheckType_HEALTH_CHECK_TYPE_UNSPECIFIED
	}
}

// defaultedHealthCheck fills defaults: interval 10s, timeout 5s, grace 60s,
// threshold 3; and an HTTP /healthz check when none is declared but an http
// port exists.
func defaultedHealthCheck(hc *zatterav1.HealthCheck, portSpecs []*zatterav1.PortSpec) *zatterav1.HealthCheck {
	hasHTTP := false
	for _, p := range portSpecs {
		if p.GetProtocol() == zatterav1.Protocol_PROTOCOL_HTTP || p.GetProtocol() == zatterav1.Protocol_PROTOCOL_UNSPECIFIED {
			hasHTTP = true
			break
		}
	}
	if hc == nil {
		if !hasHTTP {
			return nil
		}
		hc = &zatterav1.HealthCheck{Type: zatterav1.HealthCheckType_HEALTH_CHECK_TYPE_HTTP, Path: "/healthz"}
	}
	if hc.GetType() == zatterav1.HealthCheckType_HEALTH_CHECK_TYPE_HTTP && hc.GetPath() == "" {
		hc.Path = "/healthz"
	}
	if hc.GetInterval() == nil {
		hc.Interval = durationpb.New(10 * time.Second)
	}
	if hc.GetTimeout() == nil {
		hc.Timeout = durationpb.New(5 * time.Second)
	}
	if hc.GetGracePeriod() == nil {
		hc.GracePeriod = durationpb.New(60 * time.Second)
	}
	if hc.GetUnhealthyThreshold() == 0 {
		hc.UnhealthyThreshold = 3
	}
	return hc
}

func cronSpecs(path string, in []cronSection) ([]*zatterav1.CronSpec, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]*zatterav1.CronSpec, 0, len(in))
	for i, c := range in {
		if c.Schedule == "" {
			return nil, fmt.Errorf("appconfig: %s[%d].schedule is required", path, i)
		}
		if fields := strings.Fields(c.Schedule); len(fields) != 5 {
			return nil, fmt.Errorf("appconfig: %s[%d].schedule %q must be a 5-field cron expression", path, i, c.Schedule)
		}
		out = append(out, &zatterav1.CronSpec{
			Name:        c.Name,
			Schedule:    c.Schedule,
			Command:     c.Command,
			Concurrency: concurrency(c.Concurrency),
			MaxRetries:  c.MaxRetries,
		})
	}
	return out, nil
}

func concurrency(s string) zatterav1.ConcurrencyPolicy {
	switch strings.ToLower(s) {
	case "forbid", "":
		return zatterav1.ConcurrencyPolicy_CONCURRENCY_POLICY_FORBID
	case "replace":
		return zatterav1.ConcurrencyPolicy_CONCURRENCY_POLICY_REPLACE
	case "allow":
		return zatterav1.ConcurrencyPolicy_CONCURRENCY_POLICY_ALLOW
	default:
		return zatterav1.ConcurrencyPolicy_CONCURRENCY_POLICY_FORBID
	}
}

func parseDuration(field, raw string) (time.Duration, error) {
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("appconfig: %s: invalid duration %q: %w", field, raw, err)
	}
	if d < 0 {
		return 0, fmt.Errorf("appconfig: %s: duration must be >= 0", field)
	}
	return d, nil
}

// ConfigHash is the deterministic identity of an effective service config: a
// sha256 over the deterministically-marshaled ServiceSpec plus the env-var
// version counter. Releases (T-28) and the agent compare it to decide whether a
// redeploy is needed. Plain proto.Marshal is NOT stable — determinism is
// required.
func ConfigHash(spec *zatterav1.ServiceSpec, envVarVersion uint64) string {
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(spec)
	if err != nil {
		// A ServiceSpec always marshals; fall back to a stable sentinel.
		b = []byte("unmarshalable")
	}
	h := sha256.New()
	h.Write(b)
	var v [8]byte
	binary.BigEndian.PutUint64(v[:], envVarVersion)
	h.Write(v[:])
	return hex.EncodeToString(h.Sum(nil))
}
