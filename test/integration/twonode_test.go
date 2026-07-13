//go:build integration

package integration

import (
	"context"
	"regexp"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/emptypb"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/pkg/apiclient"
)

const (
	twoNodeSubnet = "172.28.0.0/16"
	nodeAIP       = "172.28.0.10"
	nodeBIP       = "172.28.0.11"
)

var bootstrapTokenRE = regexp.MustCompile(`Bootstrap admin token:\s*(zpat_\S+)`)

// TestTwoNodeJoin brings up a control node (A) and a worker (B) that joins it,
// then asserts both are ALIVE and that B can reach A over the WireGuard hub.
func TestTwoNodeJoin(t *testing.T) {
	RequireDocker(t)
	arch := dockerServerArch(t)
	bin := buildLinuxBinary(t, arch)
	net := createNetwork(t, twoNodeSubnet)

	// Node A: control+worker, mesh enabled, advertising its (static) endpoint so
	// the worker can reach the hub. A config file carries the mesh endpoint.
	aConfig := "" +
		"data_dir = \"/data\"\n" +
		"node_name = \"node-a\"\n" +
		"domain = \"test.local\"\n" +
		"[mesh]\n" +
		"public_endpoints = [\"" + nodeAIP + ":51820\"]\n"
	runNode(t, "zt-a", net, nodeAIP, bin,
		map[string]string{"/etc/zattera/config.toml": aConfig},
		"server", "--config", "/etc/zattera/config.toml")

	token := waitForLog(t, "zt-a", bootstrapTokenRE, 60*time.Second)

	// Client against A (self-signed dev cert → skip verify; token authenticates).
	cli, err := apiclient.New(apiclient.Config{Address: nodeAIP + ":8443", Token: token, InsecureSkipVerify: true})
	if err != nil {
		t.Fatalf("api client: %v", err)
	}

	// Mint a join token via the API.
	var joinToken string
	waitUntil(t, 60*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, err := cli.Nodes.CreateJoinToken(ctx, &zatterav1.CreateJoinTokenRequest{
			Roles: []zatterav1.NodeRole{zatterav1.NodeRole_NODE_ROLE_WORKER},
		})
		if err != nil {
			return false
		}
		joinToken = resp.GetToken()
		return joinToken != ""
	}, "control API to mint a join token")

	// Node B joins A.
	runNode(t, "zt-b", net, nodeBIP, bin, nil,
		"server", "--data-dir", "/data", "--join", nodeAIP+":8443", "--token", joinToken)

	// Both nodes should become ALIVE within 60s.
	waitUntil(t, 60*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, err := cli.Nodes.ListNodes(ctx, &emptypb.Empty{})
		if err != nil {
			return false
		}
		alive := 0
		for _, n := range resp.GetNodes() {
			if n.GetStatus() == zatterav1.NodeStatus_NODE_STATUS_ALIVE {
				alive++
			}
		}
		return alive == 2
	}, "two ALIVE nodes")

	// The worker can reach the control node over the WireGuard hub.
	waitUntil(t, 60*time.Second, func() bool {
		out, err := dockerExec(t, "zt-b", "ping", "-c", "1", "-W", "2", "10.90.0.1")
		if err != nil {
			t.Logf("ping not yet reachable: %v\n%s", err, out)
			return false
		}
		return true
	}, "worker to ping the control mesh IP")
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("timeout waiting for %s", what)
}
