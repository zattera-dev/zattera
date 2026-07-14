package cli

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/pkg/apiclient"
)

func newReleasesCmd() *cobra.Command {
	var app, env string
	var prod bool
	cmd := &cobra.Command{
		Use:   "releases",
		Short: "List an environment's releases",
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
			appName, err := resolveAppName(app)
			if err != nil {
				return err
			}
			ctx, cancel := cmdContext(cmd)
			defer cancel()

			envID, err := resolveEnv(ctx, client, proj, appName, deployEnvName(env, prod))
			if err != nil {
				return err
			}
			resp, err := client.Deploys.ListReleases(ctx, &zatterav1.ListReleasesRequest{ProjectId: proj, EnvironmentId: envID})
			if err != nil {
				return apiError(err)
			}

			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(resp.GetReleases())
			}
			rows := make([][]string, 0, len(resp.GetReleases()))
			for _, r := range resp.GetReleases() {
				rows = append(rows, []string{
					"v" + strconv.FormatUint(r.GetVersion(), 10),
					r.GetImageRef(),
					shortID(r.GetConfigHash()),
					r.GetMeta().GetCreatedAt().AsTime().Format("2006-01-02 15:04"),
				})
			}
			p.Table([]string{"VERSION", "IMAGE", "CONFIG", "CREATED"}, rows)
			return nil
		},
	}
	cmd.Flags().StringVar(&app, "app", "", "app name (default: name in ./zattera.toml)")
	cmd.Flags().StringVar(&env, "env", "", "environment name (default: staging)")
	cmd.Flags().BoolVar(&prod, "prod", false, "shortcut for --env production")
	addProjectFlag(cmd)
	return cmd
}

// resolveReleaseByVersion maps a "vN"/"N" version selector to a release id.
func resolveReleaseByVersion(ctx context.Context, client *apiclient.Client, proj, envID, sel string) (string, error) {
	n, err := strconv.ParseUint(strings.TrimPrefix(sel, "v"), 10, 64)
	if err != nil {
		return "", fmt.Errorf("invalid release version %q", sel)
	}
	resp, err := client.Deploys.ListReleases(ctx, &zatterav1.ListReleasesRequest{ProjectId: proj, EnvironmentId: envID})
	if err != nil {
		return "", apiError(err)
	}
	for _, r := range resp.GetReleases() {
		if r.GetVersion() == n {
			return r.GetMeta().GetId(), nil
		}
	}
	return "", fmt.Errorf("release v%d not found", n)
}
