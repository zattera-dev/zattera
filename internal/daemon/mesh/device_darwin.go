//go:build darwin

package mesh

import (
	"fmt"
	"log/slog"
	"net/netip"
	"os/exec"
	"strings"
)

// defaultInterfaceName asks the kernel for the next utunN (macOS requires the
// utun prefix; the real name is read back from the created device).
func defaultInterfaceName() string { return "utun" }

// newKernelBackend is never available on macOS — the mesh always uses the
// userspace device here.
func newKernelBackend(string, uint16, *slog.Logger) (deviceBackend, string, error) {
	return nil, "", errKernelUnsupported
}

// configureInterface assigns the mesh address to the point-to-point utun device
// and routes the mesh CIDR through it. macOS dev only; production runs on Linux.
func configureInterface(name string, meshIP netip.Addr) error {
	if meshIP.IsValid() {
		if err := run("ifconfig", name, "inet", meshIP.String(), meshIP.String(), "netmask", "255.255.0.0"); err != nil {
			return err
		}
	}
	// Best-effort route; ignore "already exists".
	_ = run("route", "-q", "-n", "add", "-inet", "-net", meshCIDR, "-interface", name)
	return nil
}

func run(name string, args ...string) error {
	out, err := exec.Command(name, args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}
