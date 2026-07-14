package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/pkg/apiclient"
)

// newFsCmd groups exec-based container filesystem inspection. It is deliberately
// exec-based (portable, no docker archive plumbing): `ls` runs `ls -1ap` and
// `cat` runs `cat` inside the instance.
func newFsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fs",
		Short: "Inspect a running instance's filesystem (exec-based)",
	}
	cmd.AddCommand(newFsLsCmd(), newFsCatCmd())
	return cmd
}

func newFsLsCmd() *cobra.Command {
	var env, instance string
	cmd := &cobra.Command{
		Use:   "ls <app>:<path>",
		Short: "List a directory inside a running instance",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runFs(cmd, args[0], env, instance, func(path string) []string {
				return []string{"ls", "-1ap", "--", path}
			})
		},
	}
	cmd.Flags().StringVar(&env, "env", "", "environment (default: first with a healthy instance)")
	cmd.Flags().StringVar(&instance, "instance", "", "target a specific instance id")
	addProjectFlag(cmd)
	return cmd
}

func newFsCatCmd() *cobra.Command {
	var env, instance string
	cmd := &cobra.Command{
		Use:   "cat <app>:<path>",
		Short: "Print a file inside a running instance",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runFs(cmd, args[0], env, instance, func(path string) []string {
				return []string{"cat", "--", path}
			})
		},
	}
	cmd.Flags().StringVar(&env, "env", "", "environment (default: first with a healthy instance)")
	cmd.Flags().StringVar(&instance, "instance", "", "target a specific instance id")
	addProjectFlag(cmd)
	return cmd
}

// runFs resolves <app>:<path>, picks an instance, and runs the built command.
func runFs(cmd *cobra.Command, spec, env, instance string, build func(path string) []string) error {
	appName, path, ok := strings.Cut(spec, ":")
	if !ok || appName == "" || path == "" {
		return fmt.Errorf("expected <app>:<path>, got %q", spec)
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
	appName, err = resolveAppName(appName)
	if err != nil {
		return err
	}
	ctx, cancel := cmdContext(cmd)
	defer cancel()

	inst, err := pickInstance(ctx, client, proj, appName, env, instance)
	if err != nil {
		return err
	}
	return execCapture(ctx, client, proj, inst, build(path), cmd)
}

// execCapture runs a non-interactive command in an instance and streams its
// output to the command's stdout/stderr, propagating the remote exit code.
func execCapture(ctx context.Context, client *apiclient.Client, proj, instanceID string, command []string, cmd *cobra.Command) error {
	stream, err := client.Exec.Exec(ctx)
	if err != nil {
		return apiError(err)
	}
	if err := stream.Send(&zatterav1.ExecInput{Start: &zatterav1.ExecStart{
		ProjectId:  proj,
		InstanceId: instanceID,
		Command:    command,
	}}); err != nil {
		return apiError(err)
	}
	_ = stream.CloseSend() // no stdin

	var exitCode int32
	for {
		out, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return apiError(err)
		}
		if len(out.GetStdout()) > 0 {
			_, _ = cmd.OutOrStdout().Write(out.GetStdout())
		}
		if len(out.GetStderr()) > 0 {
			_, _ = cmd.ErrOrStderr().Write(out.GetStderr())
		}
		if out.GetExited() {
			exitCode = out.GetExitCode()
			break
		}
	}
	if exitCode != 0 {
		return exitError{code: int(exitCode)}
	}
	return nil
}
