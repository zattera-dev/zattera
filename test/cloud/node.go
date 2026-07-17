//go:build cloud

package cloud

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/zattera-dev/zattera/internal/cloud/provider"
)

// NodeSpec describes a node to create.
type NodeSpec struct {
	Name  string // optional; defaults to zt-<run>-<role>-<n>
	Role  string // "control" | "worker"
	Arch  string // "amd64" | "arm64"
	Image string // optional; defaults to the cluster image

	// NAT simulation: no public IPv4, reachable/egressing only via a private
	// network. Requires NetworkIDs and (for egress) a gateway node.
	NoPublicIPv4 bool
	NetworkIDs   []int64
}

// Node is a live cloud VM plus its SSH session and Zattera role.
type Node struct {
	c       *Cluster
	machine provider.Machine
	spec    NodeSpec

	mu  sync.Mutex
	ssh *ssh.Client

	// populated for the control node after bring-up.
	bootstrapToken string
	caFingerprint  string
}

var (
	tokenRE = regexp.MustCompile(`zpat_[A-Za-z0-9_-]+`)
	caFPRE  = regexp.MustCompile(`sha256=([0-9a-f]+)`)
)

// CreateNode provisions a VM, waits for it to run, and opens an SSH session.
// It does NOT install Zattera — call InstallDocker/InstallBinary or the
// higher-level StartControl/JoinWorker helpers.
func (c *Cluster) CreateNode(spec NodeSpec) *Node {
	c.T.Helper()
	if spec.Arch == "" {
		spec.Arch = "amd64"
	}
	serverType := c.resolveServerType(spec.Arch)
	if spec.Name == "" {
		spec.Name = fmt.Sprintf("zt-%s-%s-%d", c.runID, spec.Role, len(c.nodes)+1)
	}
	image := spec.Image
	if image == "" {
		image = defaultImage
	}

	labels := c.baseLabels()
	labels[labelRole] = spec.Role
	labels[labelName] = spec.Name

	ms := provider.MachineSpec{
		Name:       spec.Name,
		Region:     c.region,
		ServerType: serverType,
		Image:      image,
		Labels:     labels,
		SSHKeyIDs:  []int64{c.sshKeyID},
		NetworkIDs: spec.NetworkIDs,
	}
	if spec.NoPublicIPv4 {
		no := false
		ms.EnableIPv4 = &no
	}

	c.T.Logf("cloud: creating %s (%s/%s, %s)", spec.Name, spec.Arch, serverType, c.region)
	m, err := c.driver.Create(c.Ctx, ms)
	if err != nil {
		c.T.Fatalf("cloud: create %s: %v", spec.Name, err)
	}
	n := &Node{c: c, machine: m, spec: spec}
	c.nodes = append(c.nodes, n)

	n.waitRunning()
	if !spec.NoPublicIPv4 {
		n.connectSSH()
	}
	return n
}

// PublicIPv4 / PublicIPv6 return the node's public addresses (empty for a
// NAT-simulated node with no public IPv4).
func (n *Node) PublicIPv4() string { return n.machine.PublicIPv4 }
func (n *Node) PublicIPv6() string { return n.machine.PublicIPv6 }
func (n *Node) Name() string       { return n.spec.Name }
func (n *Node) Arch() string       { return n.spec.Arch }
func (n *Node) ProviderID() string { return n.machine.ProviderID }

// waitRunning polls the provider until the machine reports "running" and a
// public IPv4 is assigned (unless NAT-simulated).
func (n *Node) waitRunning() {
	n.c.T.Helper()
	deadline := time.Now().Add(provisionTimeout)
	for time.Now().Before(deadline) {
		m, err := n.c.driver.Get(n.c.Ctx, n.machine.ProviderID)
		if err == nil {
			n.machine = m
			ipReady := n.spec.NoPublicIPv4 || m.PublicIPv4 != ""
			if m.Status == provider.StatusRunning && ipReady {
				return
			}
		}
		time.Sleep(3 * time.Second)
	}
	n.c.T.Fatalf("cloud: %s never reached running within %s", n.spec.Name, provisionTimeout)
}

// connectSSH dials the node over SSH with the run's ephemeral key, retrying
// while sshd comes up. Host keys are ignored — these are ephemeral hosts.
func (n *Node) connectSSH() {
	n.c.T.Helper()
	cfg := &ssh.ClientConfig{
		User:            "root",
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(n.c.signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         10 * time.Second,
	}
	addr := net.JoinHostPort(n.machine.PublicIPv4, "22")
	deadline := time.Now().Add(provisionTimeout)
	for time.Now().Before(deadline) {
		client, err := ssh.Dial("tcp", addr, cfg)
		if err == nil {
			n.mu.Lock()
			n.ssh = client
			n.mu.Unlock()
			return
		}
		time.Sleep(3 * time.Second)
	}
	n.c.T.Fatalf("cloud: %s ssh never came up at %s", n.spec.Name, addr)
}

// connectSSHViaJump reaches a node that has no public IP by tunnelling through
// a public gateway node (which must already have an SSH session and sit on the
// same private network). Used for true no-public-IP NAT simulation.
func (n *Node) connectSSHViaJump(gateway *Node) {
	n.c.T.Helper()
	target := net.JoinHostPort(n.machine.PrivateIPv4, "22")
	cfg := &ssh.ClientConfig{
		User:            "root",
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(n.c.signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         10 * time.Second,
	}
	deadline := time.Now().Add(provisionTimeout)
	for time.Now().Before(deadline) {
		gateway.mu.Lock()
		gw := gateway.ssh
		gateway.mu.Unlock()
		if gw != nil {
			conn, err := gw.Dial("tcp", target)
			if err == nil {
				sshConn, chans, reqs, cerr := ssh.NewClientConn(conn, target, cfg)
				if cerr == nil {
					n.mu.Lock()
					n.ssh = ssh.NewClient(sshConn, chans, reqs)
					n.mu.Unlock()
					return
				}
			}
		}
		time.Sleep(3 * time.Second)
	}
	n.c.T.Fatalf("cloud: %s ssh-via-jump never came up (private %s)", n.spec.Name, n.machine.PrivateIPv4)
}

// Run executes a command over SSH and returns combined stdout+stderr.
func (n *Node) Run(cmd string) (string, error) {
	n.mu.Lock()
	client := n.ssh
	n.mu.Unlock()
	if client == nil {
		return "", fmt.Errorf("cloud: %s has no ssh session", n.spec.Name)
	}
	sess, err := client.NewSession()
	if err != nil {
		return "", err
	}
	defer func() { _ = sess.Close() }()
	var out bytes.Buffer
	sess.Stdout = &out
	sess.Stderr = &out
	err = sess.Run(cmd)
	return out.String(), err
}

// MustRun runs cmd and fails the test on error, echoing the output.
func (n *Node) MustRun(cmd string) string {
	n.c.T.Helper()
	out, err := n.Run(cmd)
	if err != nil {
		n.c.T.Fatalf("cloud: %s: `%s` failed: %v\n%s", n.spec.Name, cmd, err, out)
	}
	return out
}

// Push writes content to remotePath on the node by STREAMING it over the SSH
// session's stdin into `cat` — no scp dependency, and no size limit (embedding
// bytes in the command string blows past the remote ARG_MAX for large files
// like the ~44 MiB binary).
func (n *Node) Push(content []byte, remotePath, mode string) {
	n.c.T.Helper()
	n.mu.Lock()
	client := n.ssh
	n.mu.Unlock()
	if client == nil {
		n.c.T.Fatalf("cloud: %s has no ssh session", n.spec.Name)
		return
	}
	sess, err := client.NewSession()
	if err != nil {
		n.c.T.Fatalf("cloud: push new session: %v", err)
		return
	}
	defer func() { _ = sess.Close() }()

	dir := filepath.Dir(remotePath)
	cmd := fmt.Sprintf("mkdir -p %s && cat > %s && chmod %s %s",
		shQuote(dir), shQuote(remotePath), mode, shQuote(remotePath))
	sess.Stdin = bytes.NewReader(content)
	var out bytes.Buffer
	sess.Stdout = &out
	sess.Stderr = &out
	if err := sess.Run(cmd); err != nil {
		n.c.T.Fatalf("cloud: push to %s: %v\n%s", remotePath, err, out.String())
	}
}

// InstallDocker installs Docker (idempotent) and enables the service.
func (n *Node) InstallDocker() {
	n.c.T.Helper()
	n.c.T.Logf("cloud: [%s] installing docker", n.spec.Name)
	n.MustRun(`set -e
if systemctl is-active --quiet firewalld 2>/dev/null; then systemctl disable --now firewalld; fi
if ! command -v ping >/dev/null 2>&1; then DEBIAN_FRONTEND=noninteractive apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq iputils-ping; fi
if ! command -v docker >/dev/null 2>&1; then curl -fsSL https://get.docker.com | sh; fi
systemctl enable --now docker
docker info --format '{{.ServerVersion}}' >/dev/null`)
}

// InstallBinary cross-compiles zattera for the node's arch, uploads it, and
// installs the systemd unit.
func (n *Node) InstallBinary() {
	n.c.T.Helper()
	bin := n.c.buildBinary(n.spec.Arch)
	data, err := os.ReadFile(bin)
	if err != nil {
		n.c.T.Fatalf("cloud: read binary: %v", err)
	}
	n.c.T.Logf("cloud: [%s] uploading zattera (linux/%s, %d MiB)", n.spec.Name, n.spec.Arch, len(data)/(1<<20))
	n.MustRun("systemctl stop zattera 2>/dev/null || true; mkdir -p /etc/zattera")
	n.Push(data, "/usr/local/bin/zattera", "0755")
	n.Push([]byte(systemdUnit), "/etc/systemd/system/zattera.service", "0644")
	n.MustRun("systemctl daemon-reload")
}

// WriteControlConfig writes a control+worker config (mesh advertising the
// node's public IPv4).
func (n *Node) WriteControlConfig(domain string) {
	cfg := fmt.Sprintf(`node_name = %q
data_dir  = "/var/lib/zattera"
roles     = ["control", "worker"]
domain    = %q

[api]
listen         = ":8443"
advertise_addr = "%s:8443"

[registry]
listen = ":5000"

[mesh]
listen_port      = 51820
public_endpoints = ["%s:51820"]

[acme]
disabled = true
`, n.spec.Name, domain, n.machine.PublicIPv4, n.machine.PublicIPv4)
	n.Push([]byte(cfg), "/etc/zattera/config.toml", "0644")
}

// WriteWorkerConfig writes a worker config that joins controlIP with token.
// WriteJoiningControlConfig writes the config for a control node that JOINS an
// existing cluster (T-55): it carries the control role AND a [join] block, and
// advertises its own public endpoint so the other control nodes peer with it
// directly.
func (n *Node) WriteJoiningControlConfig(controlIP, token, domain string) {
	cfg := fmt.Sprintf(`node_name = %q
data_dir  = "/var/lib/zattera"
roles     = ["control", "worker"]
domain    = %q

[join]
addr  = "%s:8443"
token = %q

[api]
listen         = ":8443"
advertise_addr = "%s:8443"

[registry]
listen = ":5000"

[mesh]
listen_port      = 51820
public_endpoints = ["%s:51820"]

[acme]
disabled = true
`, n.spec.Name, domain, controlIP, token, n.machine.PublicIPv4, n.machine.PublicIPv4)
	n.Push([]byte(cfg), "/etc/zattera/config.toml", "0644")
}

func (n *Node) WriteWorkerConfig(controlIP, token string) {
	n.writeWorkerConfig(controlIP, token, "", true)
}

// WriteMeshsockWorkerConfig writes a worker config on the meshsock datapath
// (UDP hole punching + relay) — used for NAT'd worker↔worker tests.
func (n *Node) WriteMeshsockWorkerConfig(controlIP, token string) {
	n.writeWorkerConfig(controlIP, token, "meshsock", true)
}

// WriteHubWorkerConfig writes a worker config that advertises NO public endpoint,
// so it has no direct worker↔worker path and its cross-worker traffic must route
// through the active control hub. Used to exercise multi-hub failover (T-55c).
func (n *Node) WriteHubWorkerConfig(controlIP, token string) {
	n.writeWorkerConfig(controlIP, token, "", false)
}

func (n *Node) writeWorkerConfig(controlIP, token, mode string, advertiseEndpoint bool) {
	// Mesh endpoint: a NAT node (or a deliberately hub-routed one) advertises
	// nothing and lets the hub route its traffic.
	meshEndpoint := ""
	if advertiseEndpoint && n.machine.PublicIPv4 != "" {
		meshEndpoint = fmt.Sprintf("\npublic_endpoints = [\"%s:51820\"]", n.machine.PublicIPv4)
	}
	modeLine := ""
	if mode != "" {
		modeLine = fmt.Sprintf("\nmode             = %q", mode)
	}
	cfg := fmt.Sprintf(`node_name = %q
data_dir  = "/var/lib/zattera"
roles     = ["worker"]

[join]
addr  = "%s:8443"
token = %q

[mesh]
listen_port      = 51820%s%s
`, n.spec.Name, controlIP, token, meshEndpoint, modeLine)
	n.Push([]byte(cfg), "/etc/zattera/config.toml", "0644")
}

// StartService enables and (re)starts the zattera unit.
func (n *Node) StartService() {
	n.c.T.Helper()
	n.MustRun("systemctl reset-failed zattera 2>/dev/null || true; systemctl enable zattera; systemctl restart zattera")
}

// WaitAPIListening blocks until :8443 is accepting connections on the node.
func (n *Node) WaitAPIListening() {
	n.c.T.Helper()
	n.waitFor(joinTimeout, "API :8443", func() bool {
		out, _ := n.Run("ss -ltn | grep -q ':8443' && echo up")
		return strings.Contains(out, "up")
	})
}

// CaptureBootstrap polls the control node's journal for the one-time admin
// token + CA fingerprint (printed at first boot). It POLLS rather than reads
// once: on a slow node the token can land in the journal a little after :8443
// starts listening.
func (n *Node) CaptureBootstrap() (token, caFP string) {
	n.c.T.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		out, _ := n.Run("journalctl -u zattera --no-pager")
		if m := tokenRE.FindString(out); m != "" {
			n.bootstrapToken = m
			if fp := caFPRE.FindStringSubmatch(out); len(fp) == 2 {
				n.caFingerprint = fp[1]
			}
			return n.bootstrapToken, n.caFingerprint
		}
		time.Sleep(2 * time.Second)
	}
	n.c.T.Fatal("cloud: bootstrap token not found in control journal after 90s (stale data dir? destroy and retry)")
	return "", ""
}

// Journal returns the last n lines of a systemd unit's log (for bundles).
func (n *Node) Journal(unit string, lines int) string {
	out, _ := n.Run(fmt.Sprintf("journalctl -u %s --no-pager -n %d", shQuote(unit), lines))
	return out
}

func (n *Node) waitFor(timeout time.Duration, what string, cond func() bool) {
	n.c.T.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(3 * time.Second)
	}
	n.c.T.Fatalf("cloud: %s: %s not ready within %s", n.spec.Name, what, timeout)
}

// --- binary build (cached per arch) ---------------------------------------

func (c *Cluster) buildBinary(arch string) string {
	c.T.Helper()
	if c.binDir == "" {
		c.binDir = c.T.TempDir()
	}
	out := filepath.Join(c.binDir, "zattera-linux-"+arch)
	if _, err := os.Stat(out); err == nil {
		return out // cached
	}
	root := repoRoot(c.T)
	c.T.Logf("cloud: building zattera for linux/%s", arch)
	cmd := exec.CommandContext(c.Ctx, "go", "build", "-trimpath", "-o", out, "./cmd/zattera")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH="+arch)
	if b, err := cmd.CombinedOutput(); err != nil {
		c.T.Fatalf("cloud: build linux/%s: %v\n%s", arch, err, b)
	}
	return out
}

func repoRoot(t *testing.T) string {
	t.Helper()
	root := repoRootDir()
	if root == "" {
		t.Fatal("cloud: cannot locate repo root")
	}
	return root
}

const systemdUnit = `[Unit]
Description=Zattera node
After=network-online.target docker.service
Wants=network-online.target
Requires=docker.service

[Service]
ExecStart=/usr/local/bin/zattera server --config /etc/zattera/config.toml
Restart=always
RestartSec=3
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
`

// generateSSHKey returns an ssh signer, the authorized-keys public line, and
// the PEM-encoded private key.
func generateSSHKey(t *testing.T) (ssh.Signer, string, []byte) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("cloud: gen ssh key: %v", err)
	}
	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatalf("cloud: ssh signer: %v", err)
	}
	sshPub, err := ssh.NewPublicKey(pub)
	if err != nil {
		t.Fatalf("cloud: ssh public key: %v", err)
	}
	block, err := ssh.MarshalPrivateKey(priv, "zattera-harness")
	if err != nil {
		t.Fatalf("cloud: marshal private key: %v", err)
	}
	return signer, strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sshPub))), pem.EncodeToMemory(block)
}
