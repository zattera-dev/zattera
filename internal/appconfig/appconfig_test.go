package appconfig

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

func parseFile(t *testing.T, name string) *AppConfig {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := Parse(data)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	return cfg
}

func TestParseFull(t *testing.T) {
	cfg := parseFile(t, "full.toml")

	if cfg.Name != "web" {
		t.Errorf("name = %q", cfg.Name)
	}
	// Build.
	if cfg.Build.GetType() != zatterav1.BuildType_BUILD_TYPE_DOCKERFILE {
		t.Errorf("build type = %v", cfg.Build.GetType())
	}
	if cfg.Build.GetDockerfilePath() != "docker/Dockerfile" || cfg.Build.GetContextDir() != "src" {
		t.Errorf("build paths = %+v", cfg.Build)
	}
	if cfg.Build.GetBuildArgs()["NODE_ENV"] != "production" {
		t.Errorf("build args = %+v", cfg.Build.GetBuildArgs())
	}
	// Platforms are normalized on parse (aarch64 → arm64).
	if p := cfg.Build.GetPlatforms(); len(p) != 2 || p[0] != "linux/amd64" || p[1] != "linux/arm64" {
		t.Errorf("build platforms = %v", p)
	}
	if cfg.GitHub.GetRepo() != "acme/web" || !cfg.GitHub.GetPreviewEnvironments() {
		t.Errorf("github = %+v", cfg.GitHub)
	}

	// Production env.
	prod := cfg.Services["production"]
	if prod == nil {
		t.Fatal("no production spec")
	}
	if prod.GetReplicas().GetMin() != 2 || prod.GetReplicas().GetMax() != 6 {
		t.Errorf("replicas = %+v, want 2/6", prod.GetReplicas())
	}
	if prod.GetAutoscale().GetTargetCpuPercent() != 70 || prod.GetAutoscale().GetTargetRpsPerReplica() != 100 {
		t.Errorf("autoscale = %+v", prod.GetAutoscale())
	}
	if prod.GetResources().GetCpuMillis() != 500 || prod.GetResources().GetMemoryMb() != 512 {
		t.Errorf("resources = %+v", prod.GetResources())
	}
	if p := prod.GetPorts(); len(p) != 1 || p[0].GetContainerPort() != 3000 {
		t.Errorf("ports = %+v", p)
	}
	if v := prod.GetVolumes(); len(v) != 1 || v[0].GetVolumeName() != "uploads" || v[0].GetMountPath() != "/data/uploads" {
		t.Errorf("volumes = %+v", v)
	}
	if !prod.GetScaleToZero() || prod.GetMaxConcurrency() != 50 || prod.GetCommand() != "server --prod" {
		t.Errorf("prod flags = %+v", prod)
	}
	// Healthcheck (from [deploy.healthcheck]).
	hc := prod.GetHealthcheck()
	if hc.GetType() != zatterav1.HealthCheckType_HEALTH_CHECK_TYPE_HTTP || hc.GetPath() != "/status" {
		t.Errorf("healthcheck = %+v", hc)
	}
	if hc.GetInterval().AsDuration() != 15*time.Second || hc.GetUnhealthyThreshold() != 5 {
		t.Errorf("healthcheck timings = %+v", hc)
	}
	// Domains + idle timeout.
	if d := cfg.Domains["production"]; len(d) != 2 || d[0] != "web.example.com" {
		t.Errorf("domains = %+v", d)
	}
	if cfg.IdleTimeouts["production"] != 15*time.Minute {
		t.Errorf("idle timeout = %v", cfg.IdleTimeouts["production"])
	}

	// Global cron applies to production (no per-env override there).
	if c := prod.GetCron(); len(c) != 1 || c[0].GetName() != "nightly" {
		t.Errorf("prod cron = %+v", c)
	}
	// Staging overrides with its own cron.
	stg := cfg.Services["staging"]
	if c := stg.GetCron(); len(c) != 1 || c[0].GetName() != "staging-sync" {
		t.Errorf("staging cron = %+v", c)
	}
	if stg.GetReplicas().GetMin() != 1 || stg.GetReplicas().GetMax() != 1 {
		t.Errorf("staging replicas = %+v", stg.GetReplicas())
	}
}

func TestParseDefaults(t *testing.T) {
	cfg := parseFile(t, "minimal.toml")
	spec := cfg.Services["production"]
	if spec == nil {
		t.Fatal("no production spec")
	}
	// Default port http/8080.
	if p := spec.GetPorts(); len(p) != 1 || p[0].GetName() != "http" || p[0].GetContainerPort() != 8080 {
		t.Errorf("default port = %+v", p)
	}
	// Default healthcheck HTTP /healthz.
	hc := spec.GetHealthcheck()
	if hc.GetType() != zatterav1.HealthCheckType_HEALTH_CHECK_TYPE_HTTP || hc.GetPath() != "/healthz" {
		t.Errorf("default healthcheck = %+v", hc)
	}
	if hc.GetInterval().AsDuration() != 10*time.Second || hc.GetTimeout().AsDuration() != 5*time.Second ||
		hc.GetGracePeriod().AsDuration() != 60*time.Second || hc.GetUnhealthyThreshold() != 3 {
		t.Errorf("default healthcheck timings = %+v", hc)
	}
	// Default replicas 1/1.
	if spec.GetReplicas().GetMin() != 1 || spec.GetReplicas().GetMax() != 1 {
		t.Errorf("default replicas = %+v", spec.GetReplicas())
	}
	// Default build config.
	if cfg.Build.GetDockerfilePath() != "Dockerfile" || cfg.Build.GetContextDir() != "." {
		t.Errorf("default build = %+v", cfg.Build)
	}
}

func TestValidationErrors(t *testing.T) {
	cases := []struct {
		name string
		toml string
		want string
	}{
		{"unknown key", "[app]\nname='x'\n[env.prod]\nbogus=1\n", "unknown keys"},
		{"missing name", "[build]\ntype='image'\n", "[app] name is required"},
		{"min>max", "[app]\nname='x'\n[env.prod]\nmin_replicas=5\nmax_replicas=2\n", "replicas.min > max"},
		{"bad duration", "[app]\nname='x'\n[env.prod]\nidle_timeout='soon'\n", "invalid duration"},
		{"bad cron fields", "[app]\nname='x'\n[[cron]]\nschedule='0 2 * *'\n", "5-field cron"},
		{"volume missing path", "[app]\nname='x'\n[env.prod]\n[[env.prod.volumes]]\nname='v'\n", "mount_path are required"},
		{"bad platform", "[app]\nname='x'\n[build]\nplatforms=['linux/sparc']\n", "build.platforms"},
		{"malformed platform", "[app]\nname='x'\n[build]\nplatforms=['amd64']\n", "os/arch"},
		{"rate limit zero rps", "[app]\nname='x'\n[env.prod.rate_limit]\nburst=10\n", "requests_per_second must be > 0"},
		{"rate limit burst below rps", "[app]\nname='x'\n[env.prod.rate_limit]\nrequests_per_second=100\nburst=10\n", "must be >= requests_per_second"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.toml))
			if err == nil {
				t.Fatalf("expected error containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want substring %q", err.Error(), tc.want)
			}
		})
	}
}

func TestParseRateLimit(t *testing.T) {
	// Off unless declared — the default must stay "no limiting".
	cfg, err := Parse([]byte("[app]\nname='x'\n[env.prod]\n"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if rl := cfg.Services["prod"].GetRateLimit(); rl != nil {
		t.Fatalf("rate limit = %v with no [rate_limit] section, want nil", rl)
	}

	// burst defaults to one second's worth of requests.
	cfg, err = Parse([]byte("[app]\nname='x'\n[env.prod.rate_limit]\nrequests_per_second=50\n"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	rl := cfg.Services["prod"].GetRateLimit()
	if rl.GetRequestsPerSecond() != 50 || rl.GetBurst() != 50 {
		t.Fatalf("rate limit = %d rps / %d burst, want 50/50", rl.GetRequestsPerSecond(), rl.GetBurst())
	}

	// Explicit burst is preserved.
	cfg, err = Parse([]byte("[app]\nname='x'\n[env.prod.rate_limit]\nrequests_per_second=10\nburst=40\n"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	rl = cfg.Services["prod"].GetRateLimit()
	if rl.GetRequestsPerSecond() != 10 || rl.GetBurst() != 40 {
		t.Fatalf("rate limit = %d rps / %d burst, want 10/40", rl.GetRequestsPerSecond(), rl.GetBurst())
	}
}

func TestConfigHashStableAndSensitive(t *testing.T) {
	cfg := parseFile(t, "full.toml")
	spec := cfg.Services["production"]

	h1 := ConfigHash(spec, 7)
	h2 := ConfigHash(spec, 7)
	if h1 != h2 {
		t.Errorf("hash not stable: %s vs %s", h1, h2)
	}
	// Env-var version change → new hash.
	if ConfigHash(spec, 8) == h1 {
		t.Error("hash insensitive to env var version")
	}
	// Any spec field change → new hash.
	changed := parseFile(t, "full.toml").Services["production"]
	changed.Replicas.Max = 99
	if ConfigHash(changed, 7) == h1 {
		t.Error("hash insensitive to replicas change")
	}
	// A different but equal spec produces the same hash (determinism across
	// independent parses).
	fresh := parseFile(t, "full.toml").Services["production"]
	if ConfigHash(fresh, 7) != h1 {
		t.Error("hash differs for equal specs from independent parses")
	}
}
