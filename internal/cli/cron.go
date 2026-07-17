package cli

import (
	"context"
	"fmt"
	"time"

	cron "github.com/robfig/cron/v3"
	"github.com/spf13/cobra"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/pkg/apiclient"
)

func newCronCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cron",
		Short: "Inspect scheduled (cron) jobs",
	}
	cmd.AddCommand(newCronLsCmd())
	return cmd
}

// cronRow is one schedule with its computed next run and last outcome.
type cronRow struct {
	Env         string `json:"env"`
	Name        string `json:"name"`
	Schedule    string `json:"schedule"`
	Concurrency string `json:"concurrency"`
	NextRun     string `json:"next_run"`
	LastStatus  string `json:"last_status"`
}

func newCronLsCmd() *cobra.Command {
	var app, env string
	cmd := &cobra.Command{
		Use:   "ls [app]",
		Short: "List cron schedules with their next run and last status",
		Args:  cobra.MaximumNArgs(1),
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
			appName := app
			if len(args) == 1 {
				appName = args[0]
			}
			appName, err = resolveAppName(appName)
			if err != nil {
				return err
			}
			ctx, cancel := cmdContext(cmd)
			defer cancel()

			resp, err := client.Apps.GetApp(ctx, &zatterav1.GetAppRequest{ProjectId: proj, AppId: appName})
			if err != nil {
				return apiError(err)
			}

			now := time.Now()
			var rows []cronRow
			for _, e := range resp.GetEnvironments() {
				if env != "" && e.GetName() != env {
					continue
				}
				for _, spec := range e.GetService().GetCron() {
					rows = append(rows, cronRow{
						Env:         e.GetName(),
						Name:        spec.GetName(),
						Schedule:    spec.GetSchedule(),
						Concurrency: concurrencyLabel(spec.GetConcurrency()),
						NextRun:     nextRun(spec.GetSchedule(), now),
						LastStatus:  lastCronStatus(ctx, client, proj, e.GetMeta().GetId(), spec.GetName()),
					})
				}
			}

			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(rows)
			}
			if len(rows) == 0 {
				p.Infof("No cron schedules defined for %q.", appName)
				return nil
			}
			table := make([][]string, 0, len(rows))
			for _, r := range rows {
				table = append(table, []string{r.Env, r.Name, r.Schedule, r.Concurrency, r.NextRun, r.LastStatus})
			}
			p.Table([]string{"ENV", "NAME", "SCHEDULE", "CONCURRENCY", "NEXT RUN", "LAST STATUS"}, table)
			return nil
		},
	}
	cmd.Flags().StringVar(&app, "app", "", "app name (default: name in ./zattera.toml)")
	cmd.Flags().StringVar(&env, "env", "", "limit to one environment")
	addProjectFlag(cmd)
	return cmd
}

// nextRun computes the next fire time of a schedule relative to now, or a reason
// it can't (invalid expression).
func nextRun(schedule string, now time.Time) string {
	sched, err := cron.ParseStandard(schedule)
	if err != nil {
		return "invalid schedule"
	}
	next := sched.Next(now)
	return fmt.Sprintf("%s (in %s)", next.Format("2006-01-02 15:04 MST"), roundDur(next.Sub(now)))
}

// lastCronStatus returns the most recent run's status for a cron, or "—" when it
// has never run.
func lastCronStatus(ctx context.Context, client *apiclient.Client, proj, envID, cronName string) string {
	resp, err := client.Jobs.ListJobs(ctx, &zatterav1.ListJobsRequest{ProjectId: proj, EnvironmentId: envID, CronName: cronName})
	if err != nil {
		return "—"
	}
	var latest *zatterav1.Job
	for _, j := range resp.GetJobs() {
		if latest == nil || j.GetMeta().GetCreatedAt().AsTime().After(latest.GetMeta().GetCreatedAt().AsTime()) {
			latest = j
		}
	}
	if latest == nil {
		return "—"
	}
	return jobStatus(latest.GetStatus())
}

func concurrencyLabel(p zatterav1.ConcurrencyPolicy) string {
	switch p {
	case zatterav1.ConcurrencyPolicy_CONCURRENCY_POLICY_REPLACE:
		return "replace"
	case zatterav1.ConcurrencyPolicy_CONCURRENCY_POLICY_ALLOW:
		return "allow"
	default:
		return "forbid"
	}
}

// roundDur trims a duration to a readable scale for the "in ..." hint.
func roundDur(d time.Duration) time.Duration {
	switch {
	case d >= time.Hour:
		return d.Round(time.Minute)
	case d >= time.Minute:
		return d.Round(time.Second)
	default:
		return d.Round(time.Second)
	}
}
