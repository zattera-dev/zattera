package agent

import (
	"fmt"
	"net"
	"strings"
)

// subnetPool is the /16 pool carved into a /24 per (project, env, node) bridge
// network (spec §2.8). Subnets are cluster-unique so container IPs never
// collide across multi-role nodes.
const (
	subnetOctet0 = 10
	subnetOctet1 = 201
	subnetSlots  = 256 // 10.201.0.0/16 → 256 /24s
)

// NetworkName is the node-local Docker network name for an environment. It is
// deterministic (idempotent EnsureNetwork) and valid per Docker's naming rules.
func NetworkName(projectID, envID string) string {
	return strings.ToLower(fmt.Sprintf("zt-%s-%s", short(projectID), shortN(envID, 12)))
}

// NextFreeSubnet returns the lowest 10.201.X.0/24 not present in used, or an
// error when the pool is exhausted.
func NextFreeSubnet(used []string) (string, error) {
	taken := make([]bool, subnetSlots)
	for _, c := range used {
		if x, ok := subnetIndex(c); ok {
			taken[x] = true
		}
	}
	for x := 0; x < subnetSlots; x++ {
		if !taken[x] {
			return fmt.Sprintf("%d.%d.%d.0/24", subnetOctet0, subnetOctet1, x), nil
		}
	}
	return "", fmt.Errorf("agent: subnet pool %d.%d.0.0/16 exhausted", subnetOctet0, subnetOctet1)
}

// GatewayIP returns the gateway (.1) address of a /24 subnet — where the
// per-network internal DNS resolver binds (T-47).
func GatewayIP(cidr string) (string, error) {
	_, ipnet, err := net.ParseCIDR(cidr)
	if err != nil {
		return "", fmt.Errorf("agent: parse subnet %q: %w", cidr, err)
	}
	v4 := ipnet.IP.To4()
	if v4 == nil {
		return "", fmt.Errorf("agent: subnet %q is not IPv4", cidr)
	}
	return fmt.Sprintf("%d.%d.%d.1", v4[0], v4[1], v4[2]), nil
}

// subnetIndex extracts X from a "10.201.X.0/24" CIDR.
func subnetIndex(cidr string) (int, bool) {
	_, ipnet, err := net.ParseCIDR(cidr)
	if err != nil {
		return 0, false
	}
	v4 := ipnet.IP.To4()
	if v4 == nil || v4[0] != subnetOctet0 || v4[1] != subnetOctet1 {
		return 0, false
	}
	return int(v4[2]), true
}

func shortN(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}
