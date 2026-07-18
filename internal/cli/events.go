package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/pkg/apiclient"
)

// eventsPollInterval is the follow-mode poll cadence. There is no server-side
// event stream; the ring is cheap to re-query.
const eventsPollInterval = 2 * time.Second

// newEventsCmd builds `zattera events` — platform event surfacing (T-76).
func newEventsCmd() *cobra.Command {
	var (
		follow   bool
		since    time.Duration
		kind     string
		severity string
		limit    uint32
		archive  bool
	)
	cmd := &cobra.Command{
		Use:   "events",
		Short: "Show platform events (deploys, node health, certificates)",
		Long: "Show platform events, newest first.\n\n" +
			"Without --project only an org owner/admin may query cluster-wide; other\n" +
			"users must scope to a project they belong to.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if severity != "" && severity != "info" && severity != "warning" && severity != "error" {
				return fmt.Errorf("invalid --severity %q: want info, warning or error", severity)
			}
			if follow && archive {
				// Following re-queries every 2s; each archive read lists and
				// fetches objects, so this would hammer the bucket for data
				// that by definition is not new.
				return fmt.Errorf("--archive cannot be combined with --follow (the archive only holds settled history)")
			}
			client, _, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()

			// Follow runs until interrupted, so it must not inherit the 30s
			// request budget the one-shot path uses.
			ctx, cancel := cmdContext(cmd)
			if follow {
				ctx, cancel = context.WithCancel(cmd.Context())
			}
			defer cancel()

			projectID, err := resolveProjectID(ctx, client, projectFlag)
			if err != nil {
				return err
			}
			req := &zatterav1.ListEventsRequest{
				ProjectId:      projectID,
				KindPrefix:     kind,
				Severity:       severity,
				Limit:          limit,
				IncludeArchive: archive,
			}
			if since > 0 {
				req.SinceUnixMs = time.Now().Add(-since).UnixMilli()
			}

			if follow {
				return followEvents(ctx, cmd, client, req)
			}
			resp, err := client.Audit.ListEvents(ctx, req)
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(resp.GetEvents())
			}
			events := resp.GetEvents()
			if len(events) == 0 {
				p.Infof("no events match")
				return nil
			}
			rows := make([][]string, 0, len(events))
			for _, e := range events {
				rows = append(rows, []string{
					e.GetMeta().GetCreatedAt().AsTime().Local().Format("2006-01-02 15:04:05"),
					e.GetSeverity(),
					e.GetKind(),
					e.GetMessage(),
				})
			}
			p.Table([]string{"TIME", "SEVERITY", "KIND", "MESSAGE"}, rows)
			if n := resp.GetFromArchive(); n > 0 {
				p.Infof("merged: %d from archive, %d live", n, uint32(len(events))-n)
			}
			return nil
		},
	}
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "poll for new events until interrupted")
	cmd.Flags().DurationVar(&since, "since", 0, "only events newer than this (e.g. 1h, 30m)")
	cmd.Flags().StringVar(&kind, "kind", "", "only events whose kind has this prefix (e.g. deploy.)")
	cmd.Flags().StringVar(&severity, "severity", "", "only this severity (info, warning, error)")
	cmd.Flags().Uint32Var(&limit, "limit", 100, "maximum events per poll")
	cmd.Flags().BoolVar(&archive, "archive", false, "also read events aged out of cluster state into object storage")
	addProjectFlag(cmd)
	return cmd
}

// followEvents polls ListEvents and prints each event once, oldest first, so
// the output reads like a log tail. The server returns newest-first, so each
// batch is walked in reverse.
func followEvents(ctx context.Context, cmd *cobra.Command, client *apiclient.Client, req *zatterav1.ListEventsRequest) error {
	p := printerFor(cmd)
	out := cmd.OutOrStdout()
	// Events created within the same millisecond as the last one printed can
	// arrive in a later poll, so the cursor is inclusive and `seen` dedupes.
	// Only ids at the cursor millisecond need remembering.
	seen := map[string]bool{}
	var cursorMs int64

	for {
		resp, err := client.Audit.ListEvents(ctx, req)
		if err != nil {
			if ctx.Err() != nil {
				return nil // interrupted; not an error
			}
			return apiError(err)
		}
		events := resp.GetEvents()
		for i := len(events) - 1; i >= 0; i-- { // oldest first
			e := events[i]
			id := e.GetMeta().GetId()
			if seen[id] {
				continue
			}
			ms := e.GetMeta().GetCreatedAt().AsTime().UnixMilli()
			if ms > cursorMs {
				cursorMs = ms
				seen = map[string]bool{}
			}
			seen[id] = true
			if jsonFlag {
				_ = p.EmitJSON(e)
				continue
			}
			fmt.Fprintf(out, "%s  %-7s  %-24s %s\n",
				e.GetMeta().GetCreatedAt().AsTime().Local().Format("15:04:05"),
				e.GetSeverity(), e.GetKind(), e.GetMessage())
		}
		if cursorMs > 0 {
			req.SinceUnixMs = cursorMs
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(eventsPollInterval):
		}
	}
}
