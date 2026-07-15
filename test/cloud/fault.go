//go:build cloud

package cloud

import (
	"fmt"
	"time"
)

// Fault injection primitives for chaos-style tests. All are driven over SSH and
// are best-effort: they fail the test only when the underlying command errors
// in a way that means the fault was NOT applied.

// StopDaemon stops the zattera service (graceful) — simulates a clean node
// shutdown / rolling restart.
func (n *Node) StopDaemon() { n.MustRun("systemctl stop zattera") }

// StartDaemon starts the zattera service again.
func (n *Node) StartDaemon() { n.MustRun("systemctl start zattera") }

// KillDaemon SIGKILLs the daemon (ungraceful crash). systemd (Restart=always)
// brings it back — use StopDaemon to keep it down.
func (n *Node) KillDaemon() {
	n.MustRun("systemctl kill -s SIGKILL zattera || pkill -9 -x zattera || true")
}

// Reboot hard-reboots the node and waits for SSH to return.
func (n *Node) Reboot() {
	n.c.T.Helper()
	// The reboot drops our connection; ignore the resulting error.
	_, _ = n.Run("nohup sh -c 'sleep 1; reboot' >/dev/null 2>&1 &")
	time.Sleep(10 * time.Second)
	n.connectSSH()
	n.c.T.Logf("cloud: %s rebooted", n.spec.Name)
}

// KillContainer SIGKILLs a running app container to test replica recovery. If
// nameSubstring is empty, the newest zattera-managed container is killed.
func (n *Node) KillContainer(nameSubstring string) {
	n.c.T.Helper()
	filter := "--filter label=dev.zattera/managed=true"
	pick := "head -1"
	if nameSubstring != "" {
		filter = fmt.Sprintf("--filter name=%s", shQuote(nameSubstring))
	}
	cmd := fmt.Sprintf("docker ps -q %s | %s | xargs -r docker kill", filter, pick)
	n.MustRun(cmd)
}

// CPULoad pins all cores for d using a dependency-free busy loop per CPU
// (no stress-ng needed). Returns once the load has been launched; it stops
// itself after d.
func (n *Node) CPULoad(d time.Duration) {
	n.c.T.Helper()
	secs := int(d.Seconds())
	if secs < 1 {
		secs = 1
	}
	// One `yes`-style spinner per core, self-terminating via timeout.
	script := fmt.Sprintf(`ncpu=$(nproc); for i in $(seq 1 "$ncpu"); do timeout %d sh -c 'while :; do :; done' & done; echo "loading $ncpu cores for %ds"`, secs, secs)
	n.MustRun(script)
}

// FillDisk writes megabytes of zeros to the data dir to simulate disk pressure.
// Removes the file after the test via t.Cleanup.
func (n *Node) FillDisk(megabytes int) {
	n.c.T.Helper()
	path := "/var/lib/zattera/.harness-ballast"
	n.MustRun(fmt.Sprintf("dd if=/dev/zero of=%s bs=1M count=%d status=none", path, megabytes))
	n.c.T.Cleanup(func() { _, _ = n.Run("rm -f " + path) })
}

// AddNetLatency adds one-way delay (and optional loss %) to the node's default
// interface via tc netem — surfaces MTU/latency-sensitive mesh bugs a
// single-host container rig cannot. Clear with ClearNetImpairment.
func (n *Node) AddNetLatency(delay time.Duration, lossPercent int) {
	n.c.T.Helper()
	ms := int(delay.Milliseconds())
	loss := ""
	if lossPercent > 0 {
		loss = fmt.Sprintf(" loss %d%%", lossPercent)
	}
	n.MustRun(fmt.Sprintf(`iface=$(ip route show default | awk '{print $5; exit}')
tc qdisc replace dev "$iface" root netem delay %dms%s`, ms, loss))
	n.c.T.Cleanup(func() { n.ClearNetImpairment() })
}

// ClearNetImpairment removes any tc netem qdisc.
func (n *Node) ClearNetImpairment() {
	_, _ = n.Run(`iface=$(ip route show default | awk '{print $5; exit}'); tc qdisc del dev "$iface" root 2>/dev/null || true`)
}

// BlockMeshUDP drops the WireGuard UDP port inbound+outbound via iptables,
// simulating a mid-run network partition of this node's mesh datapath (the
// control-plane API stays reachable). Clear with UnblockMeshUDP.
func (n *Node) BlockMeshUDP() {
	n.c.T.Helper()
	n.MustRun(`iptables -C INPUT -p udp --dport 51820 -j DROP 2>/dev/null || iptables -A INPUT -p udp --dport 51820 -j DROP
iptables -C OUTPUT -p udp --dport 51820 -j DROP 2>/dev/null || iptables -A OUTPUT -p udp --dport 51820 -j DROP`)
	n.c.T.Cleanup(func() { n.UnblockMeshUDP() })
}

// UnblockMeshUDP removes the mesh-UDP drop rules.
func (n *Node) UnblockMeshUDP() {
	_, _ = n.Run(`iptables -D INPUT -p udp --dport 51820 -j DROP 2>/dev/null || true
iptables -D OUTPUT -p udp --dport 51820 -j DROP 2>/dev/null || true`)
}
