package runtime

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
)

// These unit tests cover the pure mapping helpers only; the full lifecycle runs
// against real Docker in test/integration (mock-based unit tests are low-value).

func TestResources(t *testing.T) {
	r := resources(Resources{CPUMillis: 1500, MemoryMB: 512, PidsLimit: 100})
	if r.NanoCPUs != 1_500_000_000 {
		t.Errorf("NanoCPUs = %d, want 1.5e9", r.NanoCPUs)
	}
	if r.Memory != 512<<20 {
		t.Errorf("Memory = %d, want %d", r.Memory, 512<<20)
	}
	if r.PidsLimit == nil || *r.PidsLimit != 100 {
		t.Errorf("PidsLimit = %v, want 100", r.PidsLimit)
	}
	// Zero pids limit is unset.
	if resources(Resources{}).PidsLimit != nil {
		t.Error("zero PidsLimit should be nil")
	}
}

func TestPortMaps(t *testing.T) {
	exposed, bindings := portMaps([]PortBinding{
		{ContainerPort: 8080, Protocol: "tcp", HostIP: "10.90.0.1", HostPort: 0},
		{ContainerPort: 53, Protocol: "udp", HostIP: "127.0.0.1", HostPort: 5353},
	})
	if _, ok := exposed["8080/tcp"]; !ok {
		t.Errorf("8080/tcp not exposed: %v", exposed)
	}
	if b := bindings["8080/tcp"]; len(b) != 1 || b[0].HostIP != "10.90.0.1" || b[0].HostPort != "" {
		t.Errorf("8080 binding = %+v (HostPort should be empty for auto-allocation)", b)
	}
	if b := bindings["53/udp"]; len(b) != 1 || b[0].HostPort != "5353" {
		t.Errorf("53/udp binding = %+v", b)
	}
	// Empty ports → nil maps.
	if e, b := portMaps(nil); e != nil || b != nil {
		t.Error("empty ports should yield nil maps")
	}
}

func TestEncodeAuth(t *testing.T) {
	enc, err := encodeAuth(&RegistryAuth{Username: "node-1", Password: "pw", ServerAddress: "10.90.0.1:5000"})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := base64.URLEncoding.DecodeString(enc)
	if err != nil {
		t.Fatalf("not base64url: %v", err)
	}
	var m map[string]string
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m["username"] != "node-1" || m["password"] != "pw" || m["serveraddress"] != "10.90.0.1:5000" {
		t.Errorf("auth fields wrong: %+v", m)
	}
}

func TestSplitTimestamp(t *testing.T) {
	ts := "2026-07-13T10:00:00.123456789Z hello world"
	got, text := splitTimestamp(ts)
	if text != "hello world" {
		t.Errorf("text = %q", text)
	}
	want, _ := time.Parse(time.RFC3339Nano, "2026-07-13T10:00:00.123456789Z")
	if !got.Equal(want) {
		t.Errorf("time = %v, want %v", got, want)
	}
	// A line without a valid timestamp keeps the whole text.
	if _, text := splitTimestamp("no-timestamp-here"); text != "no-timestamp-here" {
		t.Errorf("fallback text = %q", text)
	}
}

func TestSplitLines(t *testing.T) {
	got := splitLines([]byte("a\nb\nc\n"))
	if len(got) != 3 || got[0] != "a" || got[2] != "c" {
		t.Errorf("splitLines = %v", got)
	}
	if len(splitLines([]byte(""))) != 0 {
		t.Error("empty payload should yield no lines")
	}
}

func TestParseTime(t *testing.T) {
	if !parseTime("").IsZero() {
		t.Error("empty time should be zero")
	}
	// Docker's zero sentinel normalizes to zero.
	if !parseTime("0001-01-01T00:00:00Z").IsZero() {
		t.Error("sentinel should be zero")
	}
	got := parseTime("2026-07-13T10:00:00Z")
	if got.Year() != 2026 {
		t.Errorf("year = %d", got.Year())
	}
}

func TestStatsSample(t *testing.T) {
	var s container.StatsResponse
	s.CPUStats.CPUUsage.TotalUsage = 200
	s.PreCPUStats.CPUUsage.TotalUsage = 100
	s.CPUStats.SystemUsage = 2000
	s.PreCPUStats.SystemUsage = 1000
	s.CPUStats.OnlineCPUs = 4
	s.MemoryStats.Usage = 1 << 20
	sample := statsSample(&s)
	// (100/1000) * 4 * 100 = 40%
	if sample.CPUPercent != 40 {
		t.Errorf("CPU%% = %v, want 40", sample.CPUPercent)
	}
	if sample.MemoryBytes != 1<<20 {
		t.Errorf("mem = %d", sample.MemoryBytes)
	}
}
