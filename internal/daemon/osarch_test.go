package daemon

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/scheduler"
	"github.com/zattera-dev/zattera/internal/pkgutil/platform"
	"github.com/zattera-dev/zattera/internal/state"
)

// swapEnginePlatform stubs the engine query for one test.
func swapEnginePlatform(t *testing.T, fn func(context.Context) (string, error)) {
	t.Helper()
	prev := enginePlatform
	enginePlatform = fn
	t.Cleanup(func() { enginePlatform = prev })
}

func TestNodeOsArch(t *testing.T) {
	logBuf := &bytes.Buffer{}
	log := slog.New(slog.NewTextHandler(logBuf, nil))

	t.Run("engine platform is normalized (linux/aarch64 -> linux/arm64)", func(t *testing.T) {
		swapEnginePlatform(t, func(context.Context) (string, error) { return "linux/aarch64", nil })
		if got := nodeOsArch(context.Background(), log); got != "linux/arm64" {
			t.Fatalf("got %q, want linux/arm64", got)
		}
	})

	t.Run("engine unreachable falls back to the binary's platform", func(t *testing.T) {
		logBuf.Reset()
		swapEnginePlatform(t, func(context.Context) (string, error) { return "", errors.New("no docker") })
		if got := nodeOsArch(context.Background(), log); got != platform.Local() {
			t.Fatalf("got %q, want %q", got, platform.Local())
		}
		if !strings.Contains(logBuf.String(), "engine unreachable") {
			t.Error("fallback must be logged, not silent")
		}
	})

	t.Run("unparseable engine platform falls back and warns", func(t *testing.T) {
		logBuf.Reset()
		swapEnginePlatform(t, func(context.Context) (string, error) { return "plan9/mips", nil })
		if got := nodeOsArch(context.Background(), log); got != platform.Local() {
			t.Fatalf("got %q, want %q", got, platform.Local())
		}
		if !strings.Contains(logBuf.String(), "unrecognized platform") {
			t.Error("unparseable engine platform must warn")
		}
	})
}

// TestPlacementUsesEngineArch is the T-97 regression at the scheduler boundary:
// a node whose ENGINE is linux/arm64 must accept a linux/arm64 release even
// when the daemon binary is darwin/arm64 — i.e. what matters is the advertised
// OsArch, which nodeOsArch now sources from the engine.
func TestPlacementUsesEngineArch(t *testing.T) {
	swapEnginePlatform(t, func(context.Context) (string, error) { return "linux/aarch64", nil })
	osArch := nodeOsArch(context.Background(), slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))

	st := state.New()
	st.PutNode(&zatterav1.Node{
		Meta:        &zatterav1.Meta{Id: "n1"},
		Status:      zatterav1.NodeStatus_NODE_STATUS_ALIVE,
		Schedulable: true,
		OsArch:      osArch, // what registerLocalNode/join now advertise
		Labels:      map[string]string{"zattera.dev/os-arch": osArch},
		Capacity:    &zatterav1.ResourceLimits{CpuMillis: 4000, MemoryMb: 4096},
	})
	rel := &zatterav1.Release{
		Service:   &zatterav1.ServiceSpec{},
		Platforms: []string{"linux/arm64"},
	}
	picks, err := scheduler.Place(st, rel, "env1", 1, nil)
	if err != nil || len(picks) != 1 {
		t.Fatalf("linux/arm64 release must place on an engine-arch node: picks=%v err=%v", picks, err)
	}
}
