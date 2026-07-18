package daemon

import (
	"context"
	"log/slog"
	"time"

	crt "github.com/zattera-dev/zattera/internal/daemon/runtime"
	"github.com/zattera-dev/zattera/internal/pkgutil/platform"
)

// enginePlatformTimeout bounds the engine query at registration/join time.
// Docker being slow must not delay node boot; the fallback is safe.
const enginePlatformTimeout = 5 * time.Second

// enginePlatform is swapped in tests; production asks the local Docker engine.
var enginePlatform = crt.EnginePlatform

// nodeOsArch returns the platform this node's CONTAINERS run on, which is what
// arch-aware placement (T-88) must match against. That is the engine's
// platform, not the daemon binary's: on macOS the daemon is darwin/arm64 while
// Docker Desktop executes linux/arm64, and advertising the binary's platform
// made every linux image unplaceable on a --dev node (T-97).
//
// Falls back to platform.Local() when the engine is unreachable or reports
// something unparseable, logging at WARN when that fallback disagrees with
// what the engine said — a silent mismatch is what made T-97 hard to see.
func nodeOsArch(ctx context.Context, log *slog.Logger) string {
	local := platform.Local()
	ctx, cancel := context.WithTimeout(ctx, enginePlatformTimeout)
	defer cancel()

	raw, err := enginePlatform(ctx)
	if err != nil {
		log.Warn("node os-arch: container engine unreachable; advertising the daemon binary's platform",
			"fallback", local, "err", err)
		return local
	}
	norm, err := platform.Normalize(raw)
	if err != nil {
		log.Warn("node os-arch: engine reported an unrecognized platform; advertising the daemon binary's platform",
			"engine", raw, "fallback", local, "err", err)
		return local
	}
	if norm != local {
		log.Info("node os-arch: engine platform differs from the daemon binary (normal on macOS/Windows)",
			"engine", norm, "binary", local)
	}
	return norm
}
