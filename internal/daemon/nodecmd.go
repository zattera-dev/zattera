package daemon

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/spf13/cobra"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/pkg/apiclient"
)

// Node lifecycle commands: turn a bare host (with the binary installed) into a
// control or worker node with one command, and remove it again. They write
// /etc/zattera/config.toml, install a systemd unit, and start the service — the
// steps an operator would otherwise do by hand.
const (
	nodeConfigPath = "/etc/zattera/config.toml"
	nodeUnitPath   = "/etc/systemd/system/zattera.service"
	defaultDataDir = "/var/lib/zattera"
	installOneLine = "curl -fsSL https://get.zattera.dev | sh"
)

// nodeCommands returns the `cluster` command group that manages a node's
// lifecycle on a host (init a control node, join a worker, tear it down).
func nodeCommands() []*cobra.Command {
	cluster := &cobra.Command{
		Use:   "cluster",
		Short: "Set up or tear down this host as a Zattera node",
	}
	cluster.AddCommand(newInitCmd(), newJoinCmd(), newTeardownCmd())
	return []*cobra.Command{cluster}
}

// newInitCmd bootstraps this host as a control node.
func newInitCmd() *cobra.Command {
	var nodeName, domain, email, advertise, dataDir, clusterName string
	var staging, yes bool
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Bootstrap this host as a Zattera control node (writes config + starts the service)",
		Long: "Interactively configures this host as the first (control) node of a cluster: " +
			"writes /etc/zattera/config.toml, installs a systemd service, starts it, and prints " +
			"the command to log in from your workstation plus the one-liner to add more nodes.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := requireRoot(); err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			in := bufio.NewReader(cmd.InOrStdin())
			host, _ := os.Hostname()
			pub := detectPublicIP()

			if !yes {
				nodeName = ask(cmd, in, "Node name", orDefault(nodeName, host))
				domain = ask(cmd, in, "Cluster app domain (e.g. apps.example.com)", domain)
				email = ask(cmd, in, "Email for Let's Encrypt certs (blank = self-signed only)", email)
				advertise = ask(cmd, in, "Public host/IP that CLI and workers use to reach this node", orDefault(advertise, pub))
			} else {
				nodeName = orDefault(nodeName, host)
				advertise = orDefault(advertise, pub)
			}
			if domain == "" {
				return fmt.Errorf("a cluster app domain is required")
			}
			if advertise == "" {
				return fmt.Errorf("could not detect this host's public IP; pass --advertise")
			}
			if dataDir == "" {
				dataDir = defaultDataDir
			}
			if clusterName == "" {
				clusterName = firstLabel(domain)
			}
			meshIP := orDefault(pub, advertise)

			if err := writeNodeConfig(controlConfigTOML(nodeName, dataDir, domain, advertise, meshIP, email, staging)); err != nil {
				return err
			}
			if err := installUnit(); err != nil {
				return err
			}
			fmt.Fprintln(out, "Starting zattera…")
			if err := startService(); err != nil {
				return err
			}
			if err := waitAPI("127.0.0.1:8443", 90*time.Second); err != nil {
				return fmt.Errorf("the API did not come up: %w (inspect: journalctl -u zattera -n 50)", err)
			}

			caPEM, err := os.ReadFile(filepath.Join(dataDir, "ca", "ca.crt"))
			if err != nil {
				return fmt.Errorf("read cluster CA: %w", err)
			}
			fp, err := caFingerprintPEM(caPEM)
			if err != nil {
				return err
			}
			token, err := captureAdminToken(30 * time.Second)
			if err != nil {
				return err
			}
			join, err := createLocalJoinToken("127.0.0.1:8443", token)
			if err != nil {
				return fmt.Errorf("mint join token: %w", err)
			}

			printInitSummary(out, nodeName, clusterName, advertise, fp, token, join)
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVar(&nodeName, "node-name", "", "node name (default: hostname)")
	f.StringVar(&domain, "domain", "", "cluster app domain, e.g. apps.example.com")
	f.StringVar(&email, "email", "", "email for Let's Encrypt certificates")
	f.StringVar(&advertise, "advertise", "", "public host/IP CLI + workers use (default: detected public IP)")
	f.StringVar(&dataDir, "data-dir", "", "data directory (default: /var/lib/zattera)")
	f.StringVar(&clusterName, "cluster-name", "", "CLI context name to suggest (default: from domain)")
	f.BoolVar(&staging, "acme-staging", false, "use the Let's Encrypt staging endpoint")
	f.BoolVar(&yes, "yes", false, "non-interactive: use flags/defaults, no prompts")
	return cmd
}

// newJoinCmd joins this host to an existing cluster as a worker.
func newJoinCmd() *cobra.Command {
	var nodeName, dataDir, token string
	cmd := &cobra.Command{
		Use:   "join <control-addr>",
		Short: "Join this host to a cluster as a worker (writes config + starts the service)",
		Long: "Configures this host as a worker of an existing cluster: writes " +
			"/etc/zattera/config.toml with the join address + token, installs a systemd service, " +
			"and starts it. <control-addr> is the control node's host:8443.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := requireRoot(); err != nil {
				return err
			}
			addr := args[0]
			if token == "" {
				return fmt.Errorf("--token is required (get one on the control node's CLI: zt nodes join-token create)")
			}
			if nodeName == "" {
				nodeName, _ = os.Hostname()
			}
			if dataDir == "" {
				dataDir = defaultDataDir
			}
			meshIP := detectPublicIP()

			if err := writeNodeConfig(workerConfigTOML(nodeName, dataDir, addr, token, meshIP)); err != nil {
				return err
			}
			if err := installUnit(); err != nil {
				return err
			}
			if err := startService(); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(),
				"✓ %s is joining %s.\n  Confirm from your workstation:  zt nodes ls\n", nodeName, addr)
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVar(&nodeName, "node-name", "", "node name (default: hostname)")
	f.StringVar(&token, "token", "", "join token (required)")
	f.StringVar(&dataDir, "data-dir", "", "data directory (default: /var/lib/zattera)")
	return cmd
}

// newTeardownCmd removes the local node: stops + deletes the service and config.
func newTeardownCmd() *cobra.Command {
	var yes, keepData bool
	var dataDir string
	cmd := &cobra.Command{
		Use:   "teardown",
		Short: "Remove Zattera from this host: stop + delete the service, config, and (by default) data",
		Long: "Stops and removes the systemd service, deletes /etc/zattera, force-removes managed " +
			"containers/networks, and (unless --keep-data) deletes the data directory. Docker itself " +
			"is left installed. Does not touch the rest of the cluster.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := requireRoot(); err != nil {
				return err
			}
			if dataDir == "" {
				dataDir = defaultDataDir
			}
			out := cmd.OutOrStdout()
			if !yes {
				in := bufio.NewReader(cmd.InOrStdin())
				resp := ask(cmd, in, "Remove Zattera from this host? This stops the node and deletes its config"+
					dataNote(keepData)+" (type 'yes')", "")
				if strings.ToLower(resp) != "yes" {
					fmt.Fprintln(out, "aborted")
					return nil
				}
			}
			// Best-effort throughout: teardown must make progress even if a step
			// was already done or the service is unhealthy.
			_ = run("systemctl", "disable", "--now", "zattera")
			_ = os.Remove(nodeUnitPath)
			_ = run("systemctl", "daemon-reload")
			reapManagedDocker()
			_ = os.RemoveAll("/etc/zattera")
			if !keepData {
				_ = os.RemoveAll(dataDir)
			}
			fmt.Fprintln(out, "✓ Zattera removed from this host (Docker left installed).")
			return nil
		},
	}
	f := cmd.Flags()
	f.BoolVar(&yes, "yes", false, "skip the confirmation prompt")
	f.BoolVar(&keepData, "keep-data", false, "keep the data directory (state, volumes, images)")
	f.StringVar(&dataDir, "data-dir", "", "data directory (default: /var/lib/zattera)")
	return cmd
}

// --- config templates -------------------------------------------------------

func controlConfigTOML(name, dataDir, domain, advertiseHost, meshIP, email string, staging bool) string {
	var b strings.Builder
	fmt.Fprintf(&b, "node_name = %q\n", name)
	fmt.Fprintf(&b, "data_dir  = %q\n", dataDir)
	fmt.Fprintf(&b, "roles     = [\"control\", \"worker\"]\n")
	fmt.Fprintf(&b, "domain    = %q\n\n", domain)
	fmt.Fprintf(&b, "[api]\nlisten         = \":8443\"\nadvertise_addr = %q\n\n", advertiseHost+":8443")
	fmt.Fprintf(&b, "[registry]\nlisten = \":5000\"\n\n")
	fmt.Fprintf(&b, "[mesh]\nlisten_port      = 51820\npublic_endpoints = [%q]\n", meshIP+":51820")
	if email != "" {
		fmt.Fprintf(&b, "\n[acme]\nemail   = %q\nstaging = %v\n", email, staging)
	}
	return b.String()
}

func workerConfigTOML(name, dataDir, joinAddr, token, meshIP string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "node_name = %q\n", name)
	fmt.Fprintf(&b, "data_dir  = %q\n", dataDir)
	fmt.Fprintf(&b, "roles     = [\"worker\"]\n\n")
	fmt.Fprintf(&b, "[join]\naddr  = %q\ntoken = %q\n\n", joinAddr, token)
	fmt.Fprintf(&b, "[mesh]\nlisten_port      = 51820\n")
	if meshIP != "" {
		fmt.Fprintf(&b, "public_endpoints = [%q]\n", meshIP+":51820")
	}
	return b.String()
}

// --- systemd + host helpers -------------------------------------------------

func writeNodeConfig(content string) error {
	if err := os.MkdirAll(filepath.Dir(nodeConfigPath), 0o755); err != nil {
		return fmt.Errorf("create /etc/zattera: %w", err)
	}
	if err := os.WriteFile(nodeConfigPath, []byte(content), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", nodeConfigPath, err)
	}
	return nil
}

func installUnit() error {
	exe, err := os.Executable()
	if err != nil || exe == "" {
		exe = "/usr/local/bin/zattera"
	}
	unit := fmt.Sprintf(`[Unit]
Description=Zattera node
After=network-online.target docker.service
Wants=network-online.target
Requires=docker.service

[Service]
ExecStart=%s server --config %s
Restart=always
RestartSec=3
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
`, exe, nodeConfigPath)
	if err := os.WriteFile(nodeUnitPath, []byte(unit), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", nodeUnitPath, err)
	}
	return nil
}

func startService() error {
	if err := run("systemctl", "daemon-reload"); err != nil {
		return err
	}
	return run("systemctl", "enable", "--now", "zattera")
}

func reapManagedDocker() {
	// Force-remove managed containers, then their per-env bridge networks.
	sh(`docker ps -aq --filter label=dev.zattera/managed=true | xargs -r docker rm -f`)
	sh(`docker rm -f zt-system-buildkitd 2>/dev/null`)
	sh(`docker network ls --filter name=zt- -q | xargs -r docker network rm`)
	sh(`docker volume rm zt-buildkit-cache 2>/dev/null`)
}

func run(name string, args ...string) error {
	c := exec.Command(name, args...)
	c.Stdout, c.Stderr = os.Stdout, os.Stderr
	return c.Run()
}

func sh(script string) { _ = exec.Command("sh", "-c", script).Run() }

func requireRoot() error {
	if os.Geteuid() != 0 {
		return fmt.Errorf("this command configures the host and must run as root (try: sudo zattera %s)",
			strings.Join(os.Args[1:], " "))
	}
	return nil
}

// waitAPI blocks until host:port accepts a TCP connection, or the timeout.
func waitAPI(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		c, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err == nil {
			_ = c.Close()
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout after %s", timeout)
		}
		time.Sleep(time.Second)
	}
}

var tokenRE = regexp.MustCompile(`zpat_[A-Za-z0-9_-]+`)

// captureAdminToken reads the first-boot admin token from the systemd journal.
func captureAdminToken(timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for {
		out, _ := exec.Command("journalctl", "-u", "zattera", "--no-pager", "-o", "cat").Output()
		if m := tokenRE.FindString(string(out)); m != "" {
			return m, nil
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("admin token not found in the journal " +
				"(it is printed once on first boot); retrieve it with: journalctl -u zattera | grep zpat_")
		}
		time.Sleep(2 * time.Second)
	}
}

// caFingerprintPEM is the hex sha256 of the CA certificate DER — the value
// `zt login --ca-pin` expects (matches daemon.caFingerprint).
func caFingerprintPEM(pemBytes []byte) (string, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return "", fmt.Errorf("cluster CA is not valid PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", fmt.Errorf("parse cluster CA: %w", err)
	}
	sum := sha256.Sum256(cert.Raw)
	return hex.EncodeToString(sum[:]), nil
}

// mintJoinToken creates a reusable worker join token via the local API.
func createLocalJoinToken(apiAddr, adminToken string) (string, error) {
	cl, err := apiclient.New(apiclient.Config{Address: apiAddr, Token: adminToken, InsecureSkipVerify: true})
	if err != nil {
		return "", err
	}
	defer func() { _ = cl.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := cl.Nodes.CreateJoinToken(ctx, &zatterav1.CreateJoinTokenRequest{
		SingleUse: false,
		Roles:     []zatterav1.NodeRole{zatterav1.NodeRole_NODE_ROLE_WORKER},
	})
	if err != nil {
		return "", err
	}
	return resp.GetToken(), nil
}

// detectPublicIP returns this host's public IP (best effort): an external echo
// first, then the local outbound source address.
func detectPublicIP() string {
	client := &http.Client{Timeout: 3 * time.Second}
	for _, u := range []string{"https://api.ipify.org", "https://ifconfig.me/ip", "https://icanhazip.com"} {
		resp, err := client.Get(u)
		if err != nil {
			continue
		}
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 64))
		_ = resp.Body.Close()
		if ip := strings.TrimSpace(string(b)); net.ParseIP(ip) != nil {
			return ip
		}
	}
	if c, err := net.Dial("udp", "1.1.1.1:80"); err == nil {
		defer func() { _ = c.Close() }()
		if a, ok := c.LocalAddr().(*net.UDPAddr); ok {
			return a.IP.String()
		}
	}
	return ""
}

// --- small helpers ----------------------------------------------------------

func ask(cmd *cobra.Command, r *bufio.Reader, label, def string) string {
	w := cmd.ErrOrStderr()
	if def != "" {
		fmt.Fprintf(w, "%s [%s]: ", label, def)
	} else {
		fmt.Fprintf(w, "%s: ", label)
	}
	line, _ := r.ReadString('\n')
	if line = strings.TrimSpace(line); line != "" {
		return line
	}
	return def
}

func orDefault(v, def string) string {
	if v != "" {
		return v
	}
	return def
}

func firstLabel(domain string) string {
	// apps.example.com → example ; a bare label → itself.
	parts := strings.Split(domain, ".")
	if len(parts) >= 2 {
		return parts[len(parts)-2]
	}
	return domain
}

func dataNote(keepData bool) string {
	if keepData {
		return ""
	}
	return " and data (state, volumes, images)"
}

func printInitSummary(w io.Writer, nodeName, cluster, advertise, fp, adminToken, joinToken string) {
	api := "https://" + advertise + ":8443"
	fmt.Fprintf(w, `
✓ Control node %q is up.

  Log in from your workstation (installs the CLI too):
    %s
    zt login --server %s --ca-pin %s --token %s --context %s

  Add more nodes — run this on each new server:
    %s && sudo zattera cluster join %s:8443 --token %s

  The admin token above is shown once — store it safely.
`, nodeName, installOneLine, api, fp, adminToken, cluster, installOneLine, advertise, joinToken)
}
