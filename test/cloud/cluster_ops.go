//go:build cloud

package cloud

import (
	"context"
	"net"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/emptypb"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/pkg/apiclient"
)

// StartControl brings up a control+worker node end to end: create → docker →
// binary → config → start → wait API → capture bootstrap token. It memoizes the
// control node and an authenticated API client on the cluster for JoinWorker,
// Nodes(), and assertions.
func (c *Cluster) StartControl(arch, domain string) *Node {
	c.T.Helper()
	n := c.CreateNode(NodeSpec{Role: "control", Arch: arch})
	n.InstallDocker()
	n.InstallBinary()
	n.WriteControlConfig(domain)
	n.StartService()
	n.WaitAPIListening()
	n.CaptureBootstrap()
	c.control = n
	c.T.Logf("cloud: control %s up at https://%s:8443", n.Name(), n.PublicIPv4())
	return n
}

// JoinWorker brings up a worker that joins the control node and waits until it
// registers in the cluster. Requires StartControl first.
func (c *Cluster) JoinWorker(arch string) *Node {
	c.T.Helper()
	if c.control == nil {
		c.T.Fatal("cloud: JoinWorker requires StartControl first")
	}
	token := c.workerJoinToken()
	n := c.CreateNode(NodeSpec{Role: "worker", Arch: arch})
	n.InstallDocker()
	n.InstallBinary()
	n.WriteWorkerConfig(c.control.PublicIPv4(), token)
	n.StartService()
	c.WaitNodeRegistered(n.Name())
	c.T.Logf("cloud: worker %s (%s) joined", n.Name(), arch)
	return n
}

// API returns an authenticated API client for the control node (memoized),
// using the bootstrap admin token. Verification is skipped (test cluster,
// self-signed until ACME) — the token authenticates.
func (c *Cluster) API() *apiclient.Client {
	c.T.Helper()
	if c.api != nil {
		return c.api
	}
	if c.control == nil || c.control.bootstrapToken == "" {
		c.T.Fatal("cloud: API() requires a started control node")
	}
	cli, err := apiclient.New(apiclient.Config{
		Address:            net.JoinHostPort(c.control.PublicIPv4(), "8443"),
		Token:              c.control.bootstrapToken,
		InsecureSkipVerify: true,
	})
	if err != nil {
		c.T.Fatalf("cloud: api client: %v", err)
	}
	c.api = cli
	return cli
}

// workerJoinToken mints a reusable worker join token via the API.
func (c *Cluster) workerJoinToken() string {
	c.T.Helper()
	ctx, cancel := context.WithTimeout(c.Ctx, 15*time.Second)
	defer cancel()
	resp, err := c.API().Nodes.CreateJoinToken(ctx, &zatterav1.CreateJoinTokenRequest{
		SingleUse: false,
		Roles:     []zatterav1.NodeRole{zatterav1.NodeRole_NODE_ROLE_WORKER},
	})
	if err != nil {
		c.T.Fatalf("cloud: create join token: %v", err)
	}
	return resp.GetToken()
}

// Nodes lists the cluster's registered nodes via the API.
func (c *Cluster) Nodes() []*zatterav1.Node {
	c.T.Helper()
	ctx, cancel := context.WithTimeout(c.Ctx, 15*time.Second)
	defer cancel()
	resp, err := c.API().Nodes.ListNodes(ctx, &emptypb.Empty{})
	if err != nil {
		c.T.Fatalf("cloud: list nodes: %v", err)
	}
	return resp.GetNodes()
}

// listNodesQuiet lists nodes without failing the test — for use in failure
// teardown (bundle collection), where a Fatalf would abort cleanup.
func (c *Cluster) listNodesQuiet() []*zatterav1.Node {
	if c.control == nil || c.control.bootstrapToken == "" {
		return nil
	}
	cli, err := apiclient.New(apiclient.Config{
		Address:            net.JoinHostPort(c.control.PublicIPv4(), "8443"),
		Token:              c.control.bootstrapToken,
		InsecureSkipVerify: true,
	})
	if err != nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(c.Ctx, 15*time.Second)
	defer cancel()
	resp, err := cli.Nodes.ListNodes(ctx, &emptypb.Empty{})
	if err != nil {
		return nil
	}
	return resp.GetNodes()
}

// WaitNodeRegistered blocks until a node with the given name appears in the
// cluster's node list.
func (c *Cluster) WaitNodeRegistered(name string) {
	c.T.Helper()
	deadline := time.Now().Add(joinTimeout)
	for time.Now().Before(deadline) {
		for _, n := range c.Nodes() {
			if n.GetName() == name {
				return
			}
		}
		time.Sleep(3 * time.Second)
	}
	c.T.Fatalf("cloud: node %q never registered within %s", name, joinTimeout)
}

// WaitNodesReady blocks until at least count nodes are registered AND report
// status ALIVE — the "the cluster is fully up" barrier scenarios wait on.
func (c *Cluster) WaitNodesReady(count int) {
	c.T.Helper()
	deadline := time.Now().Add(joinTimeout)
	var alive int
	for time.Now().Before(deadline) {
		alive = 0
		for _, n := range c.Nodes() {
			if n.GetStatus() == zatterav1.NodeStatus_NODE_STATUS_ALIVE {
				alive++
			}
		}
		if alive >= count {
			return
		}
		time.Sleep(3 * time.Second)
	}
	c.T.Fatalf("cloud: only %d/%d nodes ALIVE within %s", alive, count, joinTimeout)
}

// NodeByName returns the cluster's view of a node (nil if absent).
func (c *Cluster) NodeByName(name string) *zatterav1.Node {
	for _, n := range c.Nodes() {
		if n.GetName() == name {
			return n
		}
	}
	return nil
}

// nodeArchStrings is a small helper for assertions: name→os_arch across the
// cluster.
func (c *Cluster) nodeArchStrings() map[string]string {
	out := map[string]string{}
	for _, n := range c.Nodes() {
		out[n.GetName()] = strings.ToLower(n.GetOsArch())
	}
	return out
}
