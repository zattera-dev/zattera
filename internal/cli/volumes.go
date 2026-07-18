package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

func newVolumesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "volume",
		Aliases: []string{"volumes", "vol"},
		Short:   "Manage node-pinned persistent volumes",
	}
	cmd.AddCommand(newVolumeLsCmd(), newVolumeCreateCmd(), newVolumeRmCmd(),
		newVolumeSnapshotCmd(), newVolumeSnapshotsCmd(), newVolumeRestoreCmd(),
		newVolumeBrowseCmd())
	return cmd
}

func newVolumeLsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ls",
		Short: "List volumes in the project",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, cctx, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			proj, err := projectName(cctx)
			if err != nil {
				return err
			}
			ctx, cancel := cmdContext(cmd)
			defer cancel()

			resp, err := client.Volumes.ListVolumes(ctx, &zatterav1.ListVolumesRequest{ProjectId: proj})
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(resp.GetVolumes())
			}
			rows := make([][]string, 0, len(resp.GetVolumes()))
			for _, v := range resp.GetVolumes() {
				rows = append(rows, []string{
					shortID(v.GetMeta().GetId()),
					v.GetName(),
					shortID(v.GetEnvironmentId()),
					v.GetNodeId(),
					volumeStatus(v.GetStatus()),
				})
			}
			p.Table([]string{"ID", "NAME", "ENV", "NODE", "STATUS"}, rows)
			return nil
		},
	}
	addProjectFlag(cmd)
	return cmd
}

func newVolumeCreateCmd() *cobra.Command {
	var app, env, node string
	cmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a volume for a stateful service's environment",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, cctx, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			proj, err := projectName(cctx)
			if err != nil {
				return err
			}
			appName, err := resolveAppName(app)
			if err != nil {
				return err
			}
			ctx, cancel := cmdContext(cmd)
			defer cancel()

			envID, err := resolveEnv(ctx, client, proj, appName, env)
			if err != nil {
				return err
			}
			v, err := client.Volumes.CreateVolume(ctx, &zatterav1.CreateVolumeRequest{
				ProjectId:     proj,
				EnvironmentId: envID,
				Name:          args[0],
				NodeId:        node,
			})
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(v)
			}
			p.Successf("Volume %q created on node %s", v.GetName(), v.GetNodeId())
			return nil
		},
	}
	cmd.Flags().StringVar(&app, "app", "", "app name (default: name in ./zattera.toml)")
	cmd.Flags().StringVar(&env, "env", "production", "environment")
	cmd.Flags().StringVar(&node, "node", "", "pin to this node id (default: least-used healthy node)")
	addProjectFlag(cmd)
	return cmd
}

func newVolumeRmCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "rm <volume-id>",
		Aliases: []string{"delete"},
		Short:   "Delete a volume (refused while its service is running)",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, cctx, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			proj, err := projectName(cctx)
			if err != nil {
				return err
			}
			ctx, cancel := cmdContext(cmd)
			defer cancel()

			if _, err := client.Volumes.DeleteVolume(ctx, &zatterav1.DeleteVolumeRequest{ProjectId: proj, VolumeId: args[0]}); err != nil {
				return apiError(err)
			}
			printerFor(cmd).Successf("Volume %s deleted", shortID(args[0]))
			return nil
		},
	}
	addProjectFlag(cmd)
	return cmd
}

func newVolumeSnapshotCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "snapshot <volume-id>",
		Short: "Take an on-demand snapshot of a volume",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, cctx, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			proj, err := projectName(cctx)
			if err != nil {
				return err
			}
			ctx, cancel := cmdContext(cmd)
			defer cancel()

			snap, err := client.Volumes.SnapshotVolume(ctx, &zatterav1.SnapshotVolumeRequest{ProjectId: proj, VolumeId: args[0]})
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(snap)
			}
			p.Successf("Snapshot %s complete (%s)", shortID(snap.GetMeta().GetId()), fmtBytes(float64(snap.GetLogicalSizeBytes())))
			return nil
		},
	}
	addProjectFlag(cmd)
	return cmd
}

func newVolumeSnapshotsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "snapshots <volume-id>",
		Short: "List a volume's snapshots",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, cctx, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			proj, err := projectName(cctx)
			if err != nil {
				return err
			}
			ctx, cancel := cmdContext(cmd)
			defer cancel()

			resp, err := client.Volumes.ListSnapshots(ctx, &zatterav1.ListSnapshotsRequest{ProjectId: proj, VolumeId: args[0]})
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(resp.GetSnapshots())
			}
			rows := make([][]string, 0, len(resp.GetSnapshots()))
			for _, s := range resp.GetSnapshots() {
				rows = append(rows, []string{
					shortID(s.GetMeta().GetId()),
					snapshotStatus(s.GetStatus()),
					fmtBytes(float64(s.GetLogicalSizeBytes())),
					s.GetMeta().GetCreatedAt().AsTime().Format("2006-01-02 15:04"),
				})
			}
			p.Table([]string{"ID", "STATUS", "SIZE", "CREATED"}, rows)
			return nil
		},
	}
	addProjectFlag(cmd)
	return cmd
}

func newVolumeRestoreCmd() *cobra.Command {
	var snapshotID string
	cmd := &cobra.Command{
		Use:   "restore <volume-id> --snapshot <snapshot-id>",
		Short: "Restore a snapshot into its volume (service must be stopped)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if snapshotID == "" {
				return fmt.Errorf("--snapshot is required")
			}
			client, cctx, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			proj, err := projectName(cctx)
			if err != nil {
				return err
			}
			ctx, cancel := cmdContext(cmd)
			defer cancel()

			vol, err := client.Volumes.RestoreSnapshot(ctx, &zatterav1.RestoreSnapshotRequest{
				ProjectId: proj, VolumeId: args[0], SnapshotId: snapshotID,
			})
			if err != nil {
				return apiError(err)
			}
			printerFor(cmd).Successf("Restored snapshot %s into volume %s", shortID(snapshotID), shortID(vol.GetMeta().GetId()))
			return nil
		},
	}
	cmd.Flags().StringVar(&snapshotID, "snapshot", "", "snapshot id to restore")
	addProjectFlag(cmd)
	return cmd
}

func snapshotStatus(s zatterav1.SnapshotStatus) string {
	switch s {
	case zatterav1.SnapshotStatus_SNAPSHOT_STATUS_RUNNING:
		return "running"
	case zatterav1.SnapshotStatus_SNAPSHOT_STATUS_COMPLETE:
		return "complete"
	case zatterav1.SnapshotStatus_SNAPSHOT_STATUS_FAILED:
		return "failed"
	default:
		return "unknown"
	}
}

func volumeStatus(s zatterav1.VolumeStatus) string {
	switch s {
	case zatterav1.VolumeStatus_VOLUME_STATUS_ACTIVE:
		return "active"
	case zatterav1.VolumeStatus_VOLUME_STATUS_NODE_LOST:
		return "node-lost"
	case zatterav1.VolumeStatus_VOLUME_STATUS_RESTORING:
		return "restoring"
	default:
		return "unknown"
	}
}
