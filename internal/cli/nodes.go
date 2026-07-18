package cli

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/emptypb"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/pkgutil/version"
	"github.com/zattera-dev/zattera/pkg/apiclient"
)

func newNodesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "nodes",
		Aliases: []string{"node"},
		Short:   "Inspect cluster nodes and manage join tokens",
	}
	cmd.AddCommand(newNodesLsCmd(), newJoinTokenCmd(), newNodesDrainCmd(), newNodesRmCmd(),
		newNodeCordonCmd(false), newNodeCordonCmd(true), newNodesLabelCmd())
	return cmd
}

// reservedLabelPrefix marks labels a node asserts about itself (os-arch). They
// describe hardware, not intent: letting an operator overwrite them would make
// the scheduler place images on machines that cannot run them.
const reservedLabelPrefix = "zattera.dev/"

func newNodesLabelCmd() *cobra.Command {
	var overwrite bool
	cmd := &cobra.Command{
		Use:   "label <name> KEY=VALUE|KEY- [KEY=VALUE|KEY- ...]",
		Short: "Set or remove node labels (matched by placement constraints)",
		Long: "Labels are matched by [env.<name>.placement] in zattera.toml, and the\n" +
			"\"region\" label additionally spreads replicas across regions.\n\n" +
			"Labels merge by default: existing keys are kept, and changing one requires\n" +
			"--overwrite so a typo cannot silently repoint placement. KEY- removes a key.",
		Args: cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			sets, removes, err := parseLabelArgs(args[1:])
			if err != nil {
				return err
			}
			client, _, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			ctx, cancel := cmdContext(cmd)
			defer cancel()

			id, err := resolveNodeID(ctx, client, args[0])
			if err != nil {
				return err
			}
			node, err := client.Nodes.GetNode(ctx, &zatterav1.GetNodeRequest{NodeId: id})
			if err != nil {
				return apiError(err)
			}
			labels, err := mergeLabels(node.GetLabels(), sets, removes, overwrite)
			if err != nil {
				return err
			}

			// SetNodeLabels writes Schedulable unconditionally, so echoing the
			// node's current value is what keeps a labeling call from silently
			// uncordoning a cordoned node.
			updated, err := client.Nodes.SetNodeLabels(ctx, &zatterav1.SetNodeLabelsRequest{
				NodeId:      id,
				Labels:      labels,
				Schedulable: node.GetSchedulable(),
			})
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			if p.JSON {
				return p.EmitJSON(updated.GetLabels())
			}
			p.Successf("node %s labels updated", args[0])
			p.Infof("%s", nodeLabels(updated.GetLabels()))
			return nil
		},
	}
	cmd.Flags().BoolVar(&overwrite, "overwrite", false, "allow changing labels that already exist")
	return cmd
}

// parseLabelArgs splits "key=value" (set) from "key-" (remove) arguments.
func parseLabelArgs(args []string) (sets map[string]string, removes []string, err error) {
	sets = map[string]string{}
	seen := map[string]bool{}
	for _, a := range args {
		var key string
		switch {
		case strings.HasSuffix(a, "-") && !strings.Contains(a, "="):
			key = strings.TrimSuffix(a, "-")
			removes = append(removes, key)
		case strings.Contains(a, "="):
			k, v, _ := strings.Cut(a, "=")
			key, sets[k] = k, v
		default:
			return nil, nil, fmt.Errorf("invalid label %q: want KEY=VALUE or KEY-", a)
		}
		if key == "" {
			return nil, nil, fmt.Errorf("invalid label %q: empty key", a)
		}
		if seen[key] {
			return nil, nil, fmt.Errorf("label %q given twice", key)
		}
		seen[key] = true
	}
	return sets, removes, nil
}

// mergeLabels applies sets/removes onto current. Without overwrite it refuses to
// change a key that already exists — placement constraints are matched exactly,
// so a silent overwrite would move workloads with no diff to review.
func mergeLabels(current, sets map[string]string, removes []string, overwrite bool) (map[string]string, error) {
	merged := map[string]string{}
	for k, v := range current {
		merged[k] = v
	}
	for k, v := range sets {
		if strings.HasPrefix(k, reservedLabelPrefix) {
			return nil, fmt.Errorf("label %q is reserved: %s* labels are set by the node itself", k, reservedLabelPrefix)
		}
		if old, exists := merged[k]; exists && old != v && !overwrite {
			return nil, fmt.Errorf("label %q already set to %q; pass --overwrite to change it", k, old)
		}
		merged[k] = v
	}
	for _, k := range removes {
		if strings.HasPrefix(k, reservedLabelPrefix) {
			return nil, fmt.Errorf("label %q is reserved: %s* labels are set by the node itself", k, reservedLabelPrefix)
		}
		delete(merged, k)
	}
	return merged, nil
}

func newNodesDrainCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "drain <name>",
		Short: "Drain a node (migrate instances away, then mark it DRAINED)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, _, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Minute)
			defer cancel()

			id, err := resolveNodeID(ctx, client, args[0])
			if err != nil {
				return err
			}
			if _, err := client.Nodes.DrainNode(ctx, &zatterav1.DrainNodeRequest{NodeId: id}); err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			p.Infof("draining %s...", args[0])
			for {
				n, err := client.Nodes.GetNode(ctx, &zatterav1.GetNodeRequest{NodeId: id})
				if err != nil {
					return apiError(err)
				}
				if n.GetStatus() == zatterav1.NodeStatus_NODE_STATUS_DRAINED {
					p.Successf("node %s drained", args[0])
					return nil
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(2 * time.Second):
				}
			}
		},
	}
}

func newNodesRmCmd() *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:     "rm <name>",
		Aliases: []string{"remove"},
		Short:   "Remove a drained node from the cluster",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, _, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			ctx, cancel := cmdContext(cmd)
			defer cancel()

			id, err := resolveNodeID(ctx, client, args[0])
			if err != nil {
				return err
			}
			if _, err := client.Nodes.RemoveNode(ctx, &zatterav1.RemoveNodeRequest{NodeId: id, Force: force}); err != nil {
				return apiError(err)
			}
			printerFor(cmd).Successf("node %s removed", args[0])
			return nil
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "remove even if not drained")
	return cmd
}

// resolveNodeID maps a node name to its id.
func resolveNodeID(ctx context.Context, client *apiclient.Client, name string) (string, error) {
	resp, err := client.Nodes.ListNodes(ctx, &emptypb.Empty{})
	if err != nil {
		return "", apiError(err)
	}
	for _, n := range resp.GetNodes() {
		if n.GetName() == name || n.GetMeta().GetId() == name {
			return n.GetMeta().GetId(), nil
		}
	}
	return "", fmt.Errorf("node %q not found", name)
}

func newNodesLsCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List cluster nodes",
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, _, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			ctx, cancel := cmdContext(cmd)
			defer cancel()
			resp, err := client.Nodes.ListNodes(ctx, &emptypb.Empty{})
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(resp.GetNodes())
			}
			rows := make([][]string, 0, len(resp.GetNodes()))
			newest := newestVersion(resp.GetNodes())
			skew := false
			for _, n := range resp.GetNodes() {
				ver := n.GetBinaryVersion()
				if ver == "" {
					ver = "-"
				} else if ver != newest {
					// Mark anything not on the newest version so skew is
					// obvious without diffing the column by eye (T-93).
					ver += " !"
					skew = true
				}
				rows = append(rows, []string{
					n.GetName(),
					nodeRoles(n.GetRoles()),
					nodeStatusLabel(n),
					ver,
					n.GetMeshIp(),
					nodeLabels(n.GetLabels()),
				})
			}
			p.Table([]string{"NAME", "ROLES", "STATUS", "VERSION", "MESH IP", "LABELS"}, rows)
			if skew {
				p.Infof("! version skew — run 'zt cluster upgrade' to bring every node to %s", newest)
			}
			return nil
		},
	}
}

// nodeStatusLabel renders the status, distinguishing a cordoned node (ALIVE but
// unschedulable) — otherwise it would read as a perfectly normal node while
// silently receiving no new work.
func nodeStatusLabel(n *zatterav1.Node) string {
	label := strings.TrimPrefix(n.GetStatus().String(), "NODE_STATUS_")
	if n.GetStatus() == zatterav1.NodeStatus_NODE_STATUS_ALIVE && !n.GetSchedulable() {
		return label + ",CORDONED"
	}
	return label
}

// newestVersion returns the highest comparable version across nodes.
func newestVersion(nodes []*zatterav1.Node) string {
	best := ""
	for _, n := range nodes {
		v := n.GetBinaryVersion()
		if v == "" || version.Parse(v).Unknown {
			continue
		}
		if best == "" || version.Older(best, v) {
			best = v
		}
	}
	return best
}

func newNodeCordonCmd(uncordon bool) *cobra.Command {
	use, short := "cordon <name>", "Stop scheduling new work on a node (running containers stay up)"
	if uncordon {
		use, short = "uncordon <name>", "Return a cordoned or drained node to service"
	}
	cmd := &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, _, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			ctx, cancel := cmdContext(cmd)
			defer cancel()
			id, err := resolveNodeID(ctx, client, args[0])
			if err != nil {
				return err
			}
			p := printerFor(cmd)
			if uncordon {
				if _, err := client.Nodes.UncordonNode(ctx, &zatterav1.UncordonNodeRequest{NodeId: id}); err != nil {
					return apiError(err)
				}
				p.Successf("node %s is schedulable again", args[0])
				return nil
			}
			if _, err := client.Nodes.CordonNode(ctx, &zatterav1.CordonNodeRequest{NodeId: id}); err != nil {
				return apiError(err)
			}
			p.Successf("node %s cordoned — running containers keep serving, no new work lands here", args[0])
			return nil
		},
	}
	return cmd
}

func newJoinTokenCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "join-token",
		Short: "Manage node join tokens",
	}
	var (
		singleUse bool
		worker    bool
		control   bool
	)
	create := &cobra.Command{
		Use:   "create",
		Short: "Create a join token",
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, _, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			var roles []zatterav1.NodeRole
			if control {
				roles = append(roles, zatterav1.NodeRole_NODE_ROLE_CONTROL)
			}
			if worker || !control {
				roles = append(roles, zatterav1.NodeRole_NODE_ROLE_WORKER)
			}
			ctx, cancel := cmdContext(cmd)
			defer cancel()
			resp, err := client.Nodes.CreateJoinToken(ctx, &zatterav1.CreateJoinTokenRequest{SingleUse: singleUse, Roles: roles})
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(map[string]string{"token": resp.GetToken()})
			}
			// The token is a credential; print it plainly on stdout so it can be
			// piped to the joining node.
			p.Successf("Join token (store securely, shown once):")
			cmd.Println(resp.GetToken())
			return nil
		},
	}
	create.Flags().BoolVar(&singleUse, "single-use", true, "token can be used only once")
	create.Flags().BoolVar(&worker, "worker", false, "allow joining as a worker (default)")
	create.Flags().BoolVar(&control, "control", false, "allow joining as a control node")
	cmd.AddCommand(create)
	return cmd
}

func nodeRoles(roles []zatterav1.NodeRole) string {
	parts := make([]string, 0, len(roles))
	for _, r := range roles {
		parts = append(parts, strings.ToLower(strings.TrimPrefix(r.String(), "NODE_ROLE_")))
	}
	return strings.Join(parts, ",")
}

func nodeLabels(labels map[string]string) string {
	parts := make([]string, 0, len(labels))
	for k, v := range labels {
		parts = append(parts, k+"="+v)
	}
	return strings.Join(parts, ",")
}
