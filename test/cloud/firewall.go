//go:build cloud

package cloud

import (
	"strconv"
	"time"

	"github.com/zattera-dev/zattera/test/cloud/provider"
)

// Zattera's inbound ports (see the real-cluster docs): API, ingress, registry,
// and the WireGuard UDP port.
var zatteraInboundRules = []provider.FirewallRule{
	{Direction: "in", Protocol: "tcp", Port: "22"},    // ssh (harness access)
	{Direction: "in", Protocol: "tcp", Port: "80"},    // ingress http / ACME
	{Direction: "in", Protocol: "tcp", Port: "443"},   // ingress https
	{Direction: "in", Protocol: "tcp", Port: "8443"},  // control API
	{Direction: "in", Protocol: "tcp", Port: "5000"},  // registry
	{Direction: "in", Protocol: "udp", Port: "51820"}, // wireguard mesh
}

// OpenZatteraPorts applies a firewall opening Zattera's ports to the given
// nodes. Idempotent per run: the firewall is labelled with the run id and torn
// down with the cluster. Note: with no firewall attached, Hetzner servers are
// fully open by default — use this only when a test needs an explicit baseline
// or alongside IsolateInbound on other nodes.
func (c *Cluster) OpenZatteraPorts(nodes ...*Node) {
	c.T.Helper()
	ids := serverIDs(c.T, nodes)
	labels := c.baseLabels()
	labels["zattera-fw"] = "open"
	if _, err := c.driver.CreateFirewall(c.Ctx, "zt-open-"+c.runID, zatteraInboundRules, labels, ids); err != nil {
		c.T.Fatalf("cloud: open ports firewall: %v", err)
	}
}

// IsolateInbound makes a node behave like a NATted/unreachable peer WITHOUT
// removing its public IP: it applies a firewall that DROPS all inbound except
// SSH (so the harness keeps access and observability), while egress stays open.
// The node can still reach out — dial the control plane, punch outbound — but no
// peer can open a direct session to it, which is exactly the condition that
// forces disco/hole-punching and, failing that, the DERP relay (mesh phases
// B/C/D). This is the practical "simulate a NATted node" that keeps install and
// debugging working; for a true no-public-IP node see SimulateNATNoPublicIP.
func (c *Cluster) IsolateInbound(node *Node, allowSSHFrom ...string) {
	c.T.Helper()
	ssh := provider.FirewallRule{Direction: "in", Protocol: "tcp", Port: "22"}
	if len(allowSSHFrom) > 0 {
		ssh.SourceIPs = allowSSHFrom
	}
	labels := c.baseLabels()
	labels["zattera-fw"] = "isolate"
	// A firewall with only an inbound-SSH allow rule drops every other inbound
	// packet (Hetzner default-deny once any firewall is attached); egress is
	// unaffected.
	if _, err := c.driver.CreateFirewall(c.Ctx, "zt-nat-"+node.Name(), []provider.FirewallRule{ssh}, labels, []int64{serverID(c.T, node)}); err != nil {
		c.T.Fatalf("cloud: isolate %s: %v", node.Name(), err)
	}
	c.T.Logf("cloud: %s isolated (inbound dropped except ssh) — simulating an unreachable/NAT peer", node.Name())
}

// SimulateNATNoPublicIP is the advanced, fully-realistic path: it puts the node
// on a private network with NO public IPv4 and routes its egress through a
// gateway node (which must already sit on the same private network with a
// public IP). The harness reaches the node's SSH via the gateway as a jump
// host. Use for testing true NAT traversal; heavier than IsolateInbound.
//
// gateway must be a public node already brought up (typically the control
// node). Returns the private network id (torn down with the cluster).
func (c *Cluster) SimulateNATNoPublicIP(gateway *Node, arch string) *Node {
	c.T.Helper()
	netID := c.privateNetwork()

	// Gateway joins the network and NATs egress for it.
	c.attachToNetwork(gateway, netID)
	c.setupNATGateway(gateway)

	worker := c.CreateNode(NodeSpec{
		Role:         "worker",
		Arch:         arch,
		NoPublicIPv4: true,
		NetworkIDs:   []int64{netID},
	})
	// Default route for the private subnet via the gateway's private IP.
	if gw := gateway.privateIP(); gw != "" {
		if err := c.driver.AddRouteToNetwork(c.Ctx, netID, "0.0.0.0/0", gw); err != nil {
			c.T.Logf("cloud: add NAT route (continuing): %v", err)
		}
	}
	worker.connectSSHViaJump(gateway)
	return worker
}

// privateNetwork creates (once per run) a private network for NAT simulation.
func (c *Cluster) privateNetwork() int64 {
	c.T.Helper()
	if c.networkID != 0 {
		return c.networkID
	}
	labels := c.baseLabels()
	id, err := c.driver.CreateNetwork(c.Ctx, "zt-net-"+c.runID, "10.44.0.0/16", networkZoneFor(c.region), labels)
	if err != nil {
		c.T.Fatalf("cloud: create private network: %v", err)
	}
	c.networkID = id
	return id
}

// setupNATGateway enables IP forwarding + masquerade on a gateway node so
// private-network peers can egress through it.
func (c *Cluster) setupNATGateway(gw *Node) {
	c.T.Helper()
	gw.MustRun(`set -e
sysctl -w net.ipv4.ip_forward=1
# Masquerade traffic from the private subnet out the default (public) iface.
iface=$(ip route show default | awk '{print $5; exit}')
iptables -t nat -C POSTROUTING -s 10.44.0.0/16 -o "$iface" -j MASQUERADE 2>/dev/null \
  || iptables -t nat -A POSTROUTING -s 10.44.0.0/16 -o "$iface" -j MASQUERADE`)
}

// cleanupNetworkResources deletes firewalls + networks created for this run.
// Called from teardown before/after servers (firewalls detach automatically
// when their servers are destroyed).
func (c *Cluster) cleanupNetworkResources() {
	fws, err := c.driver.ListFirewalls(c.Ctx, c.runSelector())
	if err != nil {
		c.T.Logf("cloud: list firewalls: %v", err)
	}
	for _, id := range fws {
		if err := c.driver.DeleteFirewall(c.Ctx, id); err != nil {
			c.T.Logf("cloud: delete firewall %d: %v", id, err)
		}
	}
	nets, err := c.driver.ListNetworks(c.Ctx, c.runSelector())
	if err != nil {
		c.T.Logf("cloud: list networks: %v", err)
	}
	for _, id := range nets {
		if err := c.driver.DeleteNetwork(c.Ctx, id); err != nil {
			c.T.Logf("cloud: delete network %d: %v", id, err)
		}
	}
}

// --- small helpers --------------------------------------------------------

// attachToNetwork joins an already-running node (e.g. the control node, created
// before the private network existed) to the network and refreshes its private
// IP from the provider.
func (c *Cluster) attachToNetwork(n *Node, netID int64) {
	c.T.Helper()
	if err := c.driver.AttachServerToNetwork(c.Ctx, n.machine.ProviderID, netID); err != nil {
		c.T.Fatalf("cloud: attach %s to network: %v", n.Name(), err)
	}
	// The private IP appears asynchronously; poll a few times.
	for i := 0; i < 10; i++ {
		if m, err := c.driver.Get(c.Ctx, n.machine.ProviderID); err == nil && m.PrivateIPv4 != "" {
			n.machine = m
			return
		}
		time.Sleep(2 * time.Second)
	}
	c.T.Logf("cloud: %s private IP not visible yet after attach", n.Name())
}

func (n *Node) privateIP() string { return n.machine.PrivateIPv4 }

func serverID(t testingT, n *Node) int64 {
	id, err := strconv.ParseInt(n.machine.ProviderID, 10, 64)
	if err != nil {
		t.Fatalf("cloud: bad provider id %q: %v", n.machine.ProviderID, err)
	}
	return id
}

func serverIDs(t testingT, nodes []*Node) []int64 {
	ids := make([]int64, 0, len(nodes))
	for _, n := range nodes {
		ids = append(ids, serverID(t, n))
	}
	return ids
}

// networkZoneFor maps a Hetzner location to its network zone.
func networkZoneFor(region string) string {
	switch region {
	case "ash":
		return "us-east"
	case "hil":
		return "us-west"
	case "sin":
		return "ap-southeast"
	default: // nbg1, fsn1, hel1
		return "eu-central"
	}
}

// testingT is the tiny slice of *testing.T these helpers need (keeps them
// callable from non-test code paths without importing testing widely).
type testingT interface {
	Fatalf(format string, args ...any)
}
