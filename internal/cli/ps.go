package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

func newPsCmd() *cobra.Command {
	var app string
	cmd := &cobra.Command{
		Use:   "ps",
		Short: "List running instances",
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

			var appID string
			if app != "" {
				a, err := client.Apps.GetApp(ctx, &zatterav1.GetAppRequest{ProjectId: proj, AppId: app})
				if err != nil {
					return apiError(err)
				}
				appID = a.GetApp().GetMeta().GetId()
			}
			resp, err := client.Deploys.ListInstances(ctx, &zatterav1.ListInstancesRequest{ProjectId: proj, AppId: appID})
			if err != nil {
				return apiError(err)
			}

			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(resp.GetInstances())
			}
			rows := make([][]string, 0, len(resp.GetInstances()))
			for _, a := range resp.GetInstances() {
				rows = append(rows, []string{
					shortID(a.GetAppId()),
					shortID(a.GetEnvironmentId()),
					shortID(a.GetReleaseId()),
					shortID(a.GetNodeId()),
					instanceState(a.GetObserved().GetState()),
					fmt.Sprintf("%d", a.GetObserved().GetRestarts()),
				})
			}
			p.Table([]string{"APP", "ENV", "RELEASE", "NODE", "STATE", "RESTARTS"}, rows)
			return nil
		},
	}
	cmd.Flags().StringVar(&app, "app", "", "filter by app name")
	addProjectFlag(cmd)
	return cmd
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// instanceState renders an InstanceState enum as a lowercase word.
func instanceState(s zatterav1.InstanceState) string {
	name := strings.TrimPrefix(s.String(), "INSTANCE_STATE_")
	return strings.ToLower(name)
}
