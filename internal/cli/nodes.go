package cli

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/emptypb"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/pkg/apiclient"
)

func newNodesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "nodes",
		Aliases: []string{"node"},
		Short:   "Inspect cluster nodes and manage join tokens",
	}
	cmd.AddCommand(newNodesLsCmd(), newJoinTokenCmd(), newNodesDrainCmd(), newNodesRmCmd())
	return cmd
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
			for _, n := range resp.GetNodes() {
				rows = append(rows, []string{
					n.GetName(),
					nodeRoles(n.GetRoles()),
					strings.TrimPrefix(n.GetStatus().String(), "NODE_STATUS_"),
					n.GetMeshIp(),
					nodeLabels(n.GetLabels()),
				})
			}
			p.Table([]string{"NAME", "ROLES", "STATUS", "MESH IP", "LABELS"}, rows)
			return nil
		},
	}
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
