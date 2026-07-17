package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/emptypb"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

// envFallback returns flag when set, else the named environment variable.
func envFallback(flag, env string) string {
	if flag != "" {
		return flag
	}
	return os.Getenv(env)
}

func newBackupCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backup",
		Short: "Configure and run full-platform backups (admin)",
	}
	cmd.AddCommand(newBackupConfigCmd(), newBackupRunCmd(), newBackupLsCmd())
	return cmd
}

func newBackupConfigCmd() *cobra.Command {
	var endpoint, bucket, region, prefix, accessKey, secretKey string
	cmd := &cobra.Command{
		Use:   "config --bucket NAME [--endpoint URL] [--region R] [--prefix P]",
		Short: "Set the S3 backup destination (credentials are sealed server-side)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if bucket == "" {
				return fmt.Errorf("--bucket is required")
			}
			client, _, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			ctx, cancel := cmdContext(cmd)
			defer cancel()

			ak := envFallback(accessKey, "AWS_ACCESS_KEY_ID")
			sk := envFallback(secretKey, "AWS_SECRET_ACCESS_KEY")
			_, err = client.Backup.SetBackupConfig(ctx, &zatterav1.SetBackupConfigRequest{
				Config: &zatterav1.BackupConfig{
					S3Endpoint: endpoint, S3Bucket: bucket, S3Region: region, S3Prefix: prefix,
				},
				S3AccessKeyPlain: ak,
				S3SecretKeyPlain: sk,
			})
			if err != nil {
				return apiError(err)
			}
			printerFor(cmd).Successf("Backup destination set to s3://%s/%s", bucket, prefix)
			return nil
		},
	}
	cmd.Flags().StringVar(&bucket, "bucket", "", "S3 bucket (required)")
	cmd.Flags().StringVar(&endpoint, "endpoint", "", "S3 endpoint (default AWS; http://host:port for MinIO)")
	cmd.Flags().StringVar(&region, "region", "us-east-1", "S3 region")
	cmd.Flags().StringVar(&prefix, "prefix", "", "key prefix within the bucket")
	cmd.Flags().StringVar(&accessKey, "access-key", "", "S3 access key (or AWS_ACCESS_KEY_ID)")
	cmd.Flags().StringVar(&secretKey, "secret-key", "", "S3 secret key (or AWS_SECRET_ACCESS_KEY)")
	return cmd
}

func newBackupRunCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run a full backup now (state + CA + volume snapshots)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, _, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			ctx, cancel := cmdContext(cmd)
			defer cancel()

			rec, err := client.Backup.TriggerBackup(ctx, &zatterav1.TriggerBackupRequest{Kind: "full"})
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(rec)
			}
			p.Successf("Backup %s complete (%s)", shortID(rec.GetMeta().GetId()), rec.GetManifestKey())
			return nil
		},
	}
	return cmd
}

func newBackupLsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ls",
		Short: "List past backups and the current destination",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, _, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			ctx, cancel := cmdContext(cmd)
			defer cancel()

			resp, err := client.Backup.ListBackups(ctx, &emptypb.Empty{})
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(resp)
			}
			if cfg := resp.GetConfig(); cfg.GetS3Bucket() != "" {
				p.Infof("Destination: s3://%s/%s", cfg.GetS3Bucket(), cfg.GetS3Prefix())
			} else {
				p.Infof("No backup destination configured.")
			}
			rows := make([][]string, 0, len(resp.GetBackups()))
			for _, b := range resp.GetBackups() {
				rows = append(rows, []string{
					shortID(b.GetMeta().GetId()),
					b.GetKind(),
					b.GetStatus(),
					b.GetMeta().GetCreatedAt().AsTime().Format("2006-01-02 15:04"),
				})
			}
			p.Table([]string{"ID", "KIND", "STATUS", "CREATED"}, rows)
			return nil
		},
	}
	return cmd
}
