package cli

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/spf13/cobra"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

func newGitHubCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "github",
		Short: "Connect a GitHub repository for push-to-deploy",
	}
	cmd.AddCommand(newGitHubConnectCmd())
	return cmd
}

func newGitHubConnectCmd() *cobra.Command {
	var app, repo, branch, prodBranch string
	cmd := &cobra.Command{
		Use:   "connect",
		Short: "Wire a GitHub repo to an app and print setup instructions",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if app == "" || repo == "" {
				return fmt.Errorf("--app and --repo are required")
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

			secret, err := randomSecret()
			if err != nil {
				return err
			}
			branches := map[string]string{}
			if prodBranch != "" {
				branches[prodBranch] = "production"
			}
			if branch != "" {
				branches[branch] = "staging"
			}

			// Persist the repo → branch mapping on the app's GitHub config.
			if _, err := client.Apps.ApplyAppConfig(ctx, &zatterav1.ApplyAppConfigRequest{
				ProjectId: proj,
				AppId:     app,
				Github: &zatterav1.GitHubConfig{
					Repo:               repo,
					BranchEnvironments: branches,
				},
			}); err != nil {
				return apiError(err)
			}

			p := printerFor(cmd)
			webhookURL := fmt.Sprintf("%s/v1/github/webhook", cctx.Server)
			p.Successf("Connected %s → app %s", repo, app)
			p.Infof("Next steps on GitHub (Settings → Webhooks → Add webhook):")
			p.Infof("  Payload URL:  %s", webhookURL)
			p.Infof("  Content type: application/json")
			p.Infof("  Secret:       %s", secret)
			p.Infof("  Events:       Pushes, plus Pull requests for preview environments")
			p.Infof("")
			p.Infof("Then install the Zattera GitHub App on %s and store its", repo)
			p.Infof("private key and the webhook secret on the control plane.")
			return nil
		},
	}
	cmd.Flags().StringVar(&app, "app", "", "app name")
	cmd.Flags().StringVar(&repo, "repo", "", "GitHub repository (owner/name)")
	cmd.Flags().StringVar(&prodBranch, "prod-branch", "main", "branch that deploys to production")
	cmd.Flags().StringVar(&branch, "staging-branch", "", "branch that deploys to staging")
	addProjectFlag(cmd)
	return cmd
}

func randomSecret() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
