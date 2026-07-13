package cli

import (
	"context"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/emptypb"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

func newProjectsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "projects",
		Aliases: []string{"project"},
		Short:   "Manage projects",
	}
	cmd.AddCommand(newProjectsCreateCmd(), newProjectsLsCmd(), newProjectsRmCmd(), newMembersCmd())
	return cmd
}

func newProjectsCreateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "create <name>",
		Short: "Create a project",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, _, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			ctx, cancel := cmdContext(cmd)
			defer cancel()
			proj, err := client.Projects.CreateProject(ctx, &zatterav1.CreateProjectRequest{Name: args[0]})
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(proj)
			}
			p.Successf("Created project %s", proj.GetName())
			return nil
		},
	}
}

func newProjectsLsCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List projects",
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, _, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			ctx, cancel := cmdContext(cmd)
			defer cancel()
			resp, err := client.Projects.ListProjects(ctx, &emptypb.Empty{})
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(resp.GetProjects())
			}
			rows := make([][]string, 0, len(resp.GetProjects()))
			for _, pr := range resp.GetProjects() {
				rows = append(rows, []string{pr.GetName(), pr.GetMeta().GetId()})
			}
			p.Table([]string{"NAME", "ID"}, rows)
			return nil
		},
	}
}

func newProjectsRmCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "rm <name>",
		Aliases: []string{"delete"},
		Short:   "Delete a project (cascades apps, envs, domains, volumes)",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, _, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			ctx, cancel := cmdContext(cmd)
			defer cancel()
			if _, err := client.Projects.DeleteProject(ctx, &zatterav1.DeleteProjectRequest{ProjectId: args[0]}); err != nil {
				return apiError(err)
			}
			printerFor(cmd).Successf("Deleted project %s", args[0])
			return nil
		},
	}
}

func newMembersCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "members",
		Short: "Manage project members",
	}
	add := &cobra.Command{
		Use:   "add <email>",
		Short: "Add a member to the project",
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
			role, _ := cmd.Flags().GetString("role")
			ctx, cancel := cmdContext(cmd)
			defer cancel()
			m, err := client.Projects.AddMember(ctx, &zatterav1.AddMemberRequest{
				ProjectId: proj, UserEmail: args[0], Role: parseRole(role),
			})
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(m)
			}
			p.Successf("Added %s as %s", args[0], m.GetRole())
			return nil
		},
	}
	add.Flags().String("role", "developer", "role: owner|admin|developer|viewer")
	addProjectFlag(add)

	rm := &cobra.Command{
		Use:   "rm <user-id>",
		Short: "Remove a member",
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
			if _, err := client.Projects.RemoveMember(ctx, &zatterav1.RemoveMemberRequest{ProjectId: proj, UserId: args[0]}); err != nil {
				return apiError(err)
			}
			printerFor(cmd).Successf("Removed member %s", args[0])
			return nil
		},
	}
	addProjectFlag(rm)

	ls := &cobra.Command{
		Use:   "ls",
		Short: "List members",
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
			resp, err := client.Projects.ListMembers(ctx, &zatterav1.ListMembersRequest{ProjectId: proj})
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(resp.GetMembers())
			}
			rows := make([][]string, 0, len(resp.GetMembers()))
			for _, m := range resp.GetMembers() {
				rows = append(rows, []string{m.GetUserId(), m.GetRole().String()})
			}
			p.Table([]string{"USER ID", "ROLE"}, rows)
			return nil
		},
	}
	addProjectFlag(ls)

	cmd.AddCommand(add, rm, ls)
	return cmd
}

func parseRole(s string) zatterav1.Role {
	switch s {
	case "owner":
		return zatterav1.Role_ROLE_OWNER
	case "admin":
		return zatterav1.Role_ROLE_ADMIN
	case "viewer":
		return zatterav1.Role_ROLE_VIEWER
	default:
		return zatterav1.Role_ROLE_DEVELOPER
	}
}

// cmdContext derives a request context with a sane timeout from the command.
func cmdContext(cmd *cobra.Command) (context.Context, context.CancelFunc) {
	return context.WithTimeout(cmd.Context(), 30*time.Second)
}
