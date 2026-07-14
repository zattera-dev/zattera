package cli

import (
	"context"

	"github.com/spf13/cobra"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

func newRollbackCmd() *cobra.Command {
	var app, env, to string
	var prod bool
	cmd := &cobra.Command{
		Use:   "rollback",
		Short: "Roll an environment back to a previous release",
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
			ctx, cancel := context.WithTimeout(cmd.Context(), deployWatchTimeout)
			defer cancel()

			envName := deployEnvName(env, prod)
			envID, err := resolveEnv(ctx, client, proj, appName, envName)
			if err != nil {
				return err
			}
			var toReleaseID string
			if to != "" {
				if toReleaseID, err = resolveReleaseByVersion(ctx, client, proj, envID, to); err != nil {
					return err
				}
			}
			dep, err := client.Deploys.Rollback(ctx, &zatterav1.RollbackRequest{
				ProjectId: proj, EnvironmentId: envID, ToReleaseId: toReleaseID,
			})
			if err != nil {
				return apiError(err)
			}
			return watchDeployment(ctx, client, printerFor(cmd), deployTarget{project: proj, app: appName, env: envName, envID: envID}, dep)
		},
	}
	cmd.Flags().StringVar(&app, "app", "", "app name (default: name in ./zattera.toml)")
	cmd.Flags().StringVar(&env, "env", "", "environment name (default: staging)")
	cmd.Flags().StringVar(&to, "to", "", "release version to roll back to (e.g. v41; default: previous)")
	cmd.Flags().BoolVar(&prod, "prod", false, "shortcut for --env production")
	addProjectFlag(cmd)
	return cmd
}
