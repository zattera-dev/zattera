//go:build linux

package mesh

import (
	"fmt"
	"net/netip"
	"os/exec"
	"strings"
)

// defaultInterfaceName is the fixed WG interface name on Linux.
func defaultInterfaceName() string { return "zt0" }

// configureInterface assigns the mesh address, sets the MTU, brings the link up
// and installs the mesh route. All operations are idempotent (replace).
func configureInterface(name string, meshIP netip.Addr) error {
	if meshIP.IsValid() {
		if err := ipCmd("addr", "replace", meshIP.String()+"/16", "dev", name); err != nil {
			return err
		}
	}
	if err := ipCmd("link", "set", "dev", name, "mtu", fmt.Sprint(meshMTU), "up"); err != nil {
		return err
	}
	return ipCmd("route", "replace", meshCIDR, "dev", name)
}

func ipCmd(args ...string) error {
	out, err := exec.Command("ip", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("ip %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}
