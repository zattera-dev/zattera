package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

func newUnsealCmd() *cobra.Command {
	var passFile string
	cmd := &cobra.Command{
		Use:   "unseal --passphrase-file FILE",
		Short: "Install the cluster data key on a sealed node (admin)",
		Long: "Unseals the node this context points at, using the recovery passphrase\n" +
			"printed when the cluster was bootstrapped.\n\n" +
			"A node that did not bootstrap the cluster starts sealed: it serves\n" +
			"normally but holds no data key, so alerting, env-var writes, backups\n" +
			"and volume snapshots are unavailable until it is unsealed.\n\n" +
			"Unsealing is per-node and per-process — the key is held in memory. On\n" +
			"a multi-node cluster, unseal each sealed node. Nodes normally recover\n" +
			"the key by themselves at startup; this is the fallback when they\n" +
			"cannot (sealed_at_rest = true, or no reachable control peer).",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if passFile == "" {
				return fmt.Errorf("--passphrase-file is required")
			}
			raw, err := os.ReadFile(passFile)
			if err != nil {
				return fmt.Errorf("read passphrase: %w", err)
			}
			pass := strings.TrimSpace(string(raw))
			if pass == "" {
				return fmt.Errorf("passphrase file is empty")
			}
			client, _, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			ctx, cancel := cmdContext(cmd)
			defer cancel()
			resp, err := client.Auth.Unseal(ctx, &zatterav1.UnsealRequest{Passphrase: pass})
			if err != nil {
				return apiError(err)
			}
			if resp.GetAlreadyUnsealed() {
				printerFor(cmd).Successf("Node was already unsealed — nothing to do")
				return nil
			}
			printerFor(cmd).Successf("Node unsealed — alerting, env-var writes and backups are available again")
			return nil
		},
	}
	cmd.Flags().StringVar(&passFile, "passphrase-file", "", "file holding the cluster recovery passphrase")
	return cmd
}
