package cli

import (
	"strings"
	"time"

	"github.com/spf13/cobra"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

// newAuditCmd builds `zattera audit` — the audit log query (T-76).
func newAuditCmd() *cobra.Command {
	var (
		since   time.Duration
		method  string
		actor   string
		limit   uint32
		archive bool
	)
	cmd := &cobra.Command{
		Use:   "audit",
		Short: "Query the audit log of mutating API calls",
		Long: "Query the audit log. Every mutating API call is recorded with its actor,\n" +
			"method, outcome and source address.\n\n" +
			"Without --project the whole cluster is queried, which requires an org\n" +
			"owner/admin token.\n\n" +
			"The log is a capped ring in cluster state, so old entries age out. Pass\n" +
			"--archive to also read entries that were swept to object storage (needs\n" +
			"archiving enabled on the backup destination).",
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, _, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			ctx, cancel := cmdContext(cmd)
			defer cancel()

			// --project is optional here: empty means cluster-wide.
			projectID, err := resolveProjectID(ctx, client, projectFlag)
			if err != nil {
				return err
			}
			req := &zatterav1.QueryAuditRequest{
				ProjectId:      projectID,
				MethodPrefix:   method,
				ActorUserId:    actor,
				Limit:          limit,
				IncludeArchive: archive,
			}
			if since > 0 {
				req.SinceUnixMs = time.Now().Add(-since).UnixMilli()
			}
			resp, err := client.Audit.QueryAudit(ctx, req)
			if err != nil {
				return apiError(err)
			}

			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(resp.GetEntries())
			}
			entries := resp.GetEntries()
			if len(entries) == 0 {
				p.Infof("no audit entries match")
				return nil
			}
			rows := make([][]string, 0, len(entries))
			for _, e := range entries {
				rows = append(rows, []string{
					e.GetMeta().GetCreatedAt().AsTime().Local().Format("2006-01-02 15:04:05"),
					auditActor(e),
					shortMethod(e.GetMethod()),
					e.GetOutcome(),
					e.GetRemoteAddr(),
				})
			}
			p.Table([]string{"TIME", "ACTOR", "METHOD", "OUTCOME", "FROM"}, rows)
			if n := resp.GetFromArchive(); n > 0 {
				p.Infof("merged: %d from archive, %d live", n, uint32(len(entries))-n)
			}
			return nil
		},
	}
	cmd.Flags().DurationVar(&since, "since", 0, "only entries newer than this (e.g. 1h, 30m)")
	cmd.Flags().StringVar(&method, "method", "", "only methods with this prefix (e.g. Deploy, /zattera.v1.AppService/)")
	cmd.Flags().StringVar(&actor, "actor", "", "only calls by this user id")
	cmd.Flags().Uint32Var(&limit, "limit", 100, "maximum entries to return")
	cmd.Flags().BoolVar(&archive, "archive", false, "also read entries aged out of cluster state into object storage")
	addProjectFlag(cmd)
	return cmd
}

// auditActor prefers the user id, falling back to the token id for calls made
// with a machine token that is not tied to a user.
func auditActor(e *zatterav1.AuditEntry) string {
	if u := e.GetActorUserId(); u != "" {
		return shortID(u)
	}
	if t := e.GetActorTokenId(); t != "" {
		return "token:" + shortID(t)
	}
	return "-"
}

// shortMethod trims the gRPC package qualifier: "/zattera.v1.AppService/
// CreateApp" renders as "AppService/CreateApp". A --method prefix filter is
// matched server-side against the full name, so this is display-only.
func shortMethod(m string) string {
	m = strings.TrimPrefix(m, "/")
	if i := strings.LastIndex(m, "."); i >= 0 {
		return m[i+1:]
	}
	return m
}
