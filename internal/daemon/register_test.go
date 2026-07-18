package daemon

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/zattera-dev/zattera/internal/config"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
)

// Boot registration is the one place Node.os_arch must always be right (T-87):
// arch-aware placement reads the field, so a node that registers without it
// silently becomes "runs anything". Since T-97 the value is the container
// ENGINE's platform, not the daemon binary's — on macOS those differ and the
// binary's value made every linux image unplaceable.
func TestRegisterLocalNodeSetsOsArch(t *testing.T) {
	swapEnginePlatform(t, func(context.Context) (string, error) { return "linux/aarch64", nil })
	rs := raftstore.NewTestStore(t)
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.Config{NodeName: "boot-1", DataDir: t.TempDir(), Roles: []string{config.RoleControl, config.RoleWorker}}

	if err := registerLocalNode(context.Background(), rs, cfg, "node-boot-1", log); err != nil {
		t.Fatalf("registerLocalNode: %v", err)
	}

	node, ok := rs.State().Node("node-boot-1")
	if !ok {
		t.Fatal("node not registered")
	}
	if got := node.GetOsArch(); got != "linux/arm64" {
		t.Fatalf("os_arch = %q, want the normalized engine platform linux/arm64", got)
	}
	if got := node.GetLabels()["zattera.dev/os-arch"]; got != "linux/arm64" {
		t.Fatalf("os-arch label = %q, want linux/arm64", got)
	}
}

// Re-registration happens on every daemon restart and PutNode replaces the
// record wholesale — so it must refresh node-asserted facts without destroying
// operator state (custom labels from `zt nodes label`, the cordon flag).
func TestRegisterLocalNodePreservesOperatorState(t *testing.T) {
	swapEnginePlatform(t, func(context.Context) (string, error) { return "linux/amd64", nil })
	rs := raftstore.NewTestStore(t)
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.Config{NodeName: "boot-1", DataDir: t.TempDir(), Roles: []string{config.RoleControl, config.RoleWorker}}
	ctx := context.Background()

	if err := registerLocalNode(ctx, rs, cfg, "n1", log); err != nil {
		t.Fatalf("first register: %v", err)
	}

	// Operator actions between restarts: a custom label and a cordon.
	node, _ := rs.State().Node("n1")
	node.Labels["region"] = "eu"
	node.Labels["zattera.dev/os-arch"] = "stale/value" // must be refreshed, not preserved
	node.Schedulable = false
	created := node.GetMeta().GetCreatedAt()
	rs.State().PutNode(node)

	// Daemon restart.
	if err := registerLocalNode(ctx, rs, cfg, "n1", log); err != nil {
		t.Fatalf("re-register: %v", err)
	}

	got, _ := rs.State().Node("n1")
	if got.GetLabels()["region"] != "eu" {
		t.Error("custom label was wiped by re-registration")
	}
	if got.GetSchedulable() {
		t.Error("re-registration silently uncordoned the node")
	}
	if got.GetLabels()["zattera.dev/os-arch"] != "linux/amd64" || got.GetOsArch() != "linux/amd64" {
		t.Errorf("node-asserted os-arch must be refreshed, got label=%q field=%q",
			got.GetLabels()["zattera.dev/os-arch"], got.GetOsArch())
	}
	if got.GetMeta().GetCreatedAt().AsTime() != created.AsTime() {
		t.Error("creation time must be preserved")
	}
}
