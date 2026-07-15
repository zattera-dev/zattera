//go:build cloud

package cloud

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// bundleCommands are the per-node diagnostics captured into a failure bundle.
// Each entry becomes <node>/<file> in the bundle directory. Kept broad on
// purpose: an agent reading the bundle should be able to reconstruct why a
// node misbehaved without a live session.
var bundleCommands = []struct {
	file string
	cmd  string
}{
	{"zattera.journal", "journalctl -u zattera --no-pager -n 2000"},
	{"docker.journal", "journalctl -u docker --no-pager -n 300"},
	{"systemctl-status", "systemctl status zattera --no-pager -l || true"},
	{"docker-ps", "docker ps -a 2>/dev/null || true"},
	{"docker-container-logs", `for c in $(docker ps -aq --filter label=dev.zattera/managed=true 2>/dev/null); do echo "==== $c ===="; docker logs --tail 200 "$c" 2>&1; done`},
	{"wireguard", "wg show 2>/dev/null || echo '(wireguard-tools not installed)'; echo '--- links ---'; ip -d link show 2>/dev/null || true"},
	{"ip-addr", "ip -o addr show"},
	{"ip-route", "ip route show; echo '--- rules ---'; ip rule show 2>/dev/null || true"},
	{"iptables", "iptables -S 2>/dev/null; echo '--- nat ---'; iptables -t nat -S 2>/dev/null || true"},
	{"config", "cat /etc/zattera/config.toml 2>/dev/null || true"},
	{"disk", "df -h; echo '--- data dir ---'; du -sh /var/lib/zattera 2>/dev/null || true"},
}

// collectBundles writes a per-node diagnostics directory under the OS temp dir
// (which survives the test, unlike t.TempDir) and returns its path. Best-effort:
// a node with no SSH session (or a dead one) gets a note instead of a failure.
func (c *Cluster) collectBundles() string {
	root := filepath.Join(os.TempDir(), "zattera-cloud-"+c.runID)
	if err := os.MkdirAll(root, 0o755); err != nil {
		c.T.Logf("cloud: cannot create bundle dir: %v", err)
		return ""
	}

	// Cluster-level view from the control node's API (non-fatal in teardown).
	if nodes := c.listNodesQuiet(); len(nodes) > 0 {
		var b strings.Builder
		for _, n := range nodes {
			fmt.Fprintf(&b, "%s\tstatus=%v\tos_arch=%s\tschedulable=%v\tmesh_ip=%s\n",
				n.GetName(), n.GetStatus(), n.GetOsArch(), n.GetSchedulable(), n.GetMeshIp())
		}
		_ = os.WriteFile(filepath.Join(root, "cluster-nodes.txt"), []byte(b.String()), 0o644)
	}

	for _, n := range c.nodes {
		dir := filepath.Join(root, n.Name())
		if err := os.MkdirAll(dir, 0o755); err != nil {
			continue
		}
		// Metadata even if SSH is dead.
		meta := fmt.Sprintf("name=%s\nrole=%s\narch=%s\nprovider_id=%s\npublic_ipv4=%s\npublic_ipv6=%s\nprivate_ipv4=%s\n",
			n.Name(), n.spec.Role, n.Arch(), n.ProviderID(), n.PublicIPv4(), n.PublicIPv6(), n.machine.PrivateIPv4)
		_ = os.WriteFile(filepath.Join(dir, "machine.txt"), []byte(meta), 0o644)

		n.mu.Lock()
		hasSSH := n.ssh != nil
		n.mu.Unlock()
		if !hasSSH {
			_ = os.WriteFile(filepath.Join(dir, "NO_SSH.txt"), []byte("no ssh session to this node\n"), 0o644)
			continue
		}
		for _, bc := range bundleCommands {
			out, _ := n.Run(bc.cmd)
			_ = os.WriteFile(filepath.Join(dir, bc.file), []byte(out), 0o644)
		}
	}
	return root
}

// printAttachKit prints everything an operator or agent needs to log into the
// still-running cluster and inspect it live (only used with ZT_CLOUD_KEEP=1 on
// failure). The private key was written to keyPath() at run start.
func (c *Cluster) printAttachKit() {
	var b strings.Builder
	b.WriteString("\n════════════════════ CLOUD ATTACH KIT (cluster kept alive) ════════════════════\n")
	fmt.Fprintf(&b, "run id:   %s\n", c.runID)
	fmt.Fprintf(&b, "ssh key:  %s\n", c.keyPath())
	if c.control != nil {
		fmt.Fprintf(&b, "control:  https://%s:8443  (admin token in the control node's journal)\n", c.control.PublicIPv4())
	}
	b.WriteString("\nPer node:\n")
	for _, n := range c.nodes {
		ip := n.PublicIPv4()
		fmt.Fprintf(&b, "\n  • %s  [%s, %s]\n", n.Name(), n.spec.Role, n.Arch())
		if ip == "" {
			fmt.Fprintf(&b, "      (no public IP — reach via the gateway: ssh -J root@<gateway> root@%s)\n", n.machine.PrivateIPv4)
			continue
		}
		fmt.Fprintf(&b, "      ssh -i %s -o StrictHostKeyChecking=no root@%s\n", c.keyPath(), ip)
		fmt.Fprintf(&b, "      ssh -i %s -o StrictHostKeyChecking=no root@%s 'journalctl -u zattera -f'\n", c.keyPath(), ip)
		fmt.Fprintf(&b, "      ssh -i %s -o StrictHostKeyChecking=no root@%s 'docker ps -a; wg show'\n", c.keyPath(), ip)
	}
	fmt.Fprintf(&b, "\nDestroy when done:  HCLOUD_TOKEN=... go test -tags cloud ./test/cloud/ -run TestCloudReap -v\n")
	fmt.Fprintf(&b, "(or just wait — the reaper destroys anything older than ZT_CLOUD_MAX_AGE, default %s)\n", defaultMaxAge)
	b.WriteString("═══════════════════════════════════════════════════════════════════════════════\n")
	c.T.Log(b.String())
}
