package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/pkg/apiclient"
)

func newAttachCmd() *cobra.Command {
	var app, env, instance string
	var noTTY bool
	cmd := &cobra.Command{
		Use:   "attach [app] [-- command...]",
		Short: "Open an interactive shell (or run a command) in a running instance",
		Long: "Attach to a running instance. With no command, starts /bin/sh.\n" +
			"Everything after -- is run as the command, e.g.\n" +
			"  zattera attach api -- printenv",
		Args: cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Split positional app from the post-"--" command.
			cmdArgs := argsAfterDashDash(cmd, args)
			posArgs := args[:len(args)-len(cmdArgs)]
			appName := app
			if len(posArgs) >= 1 {
				appName = posArgs[0]
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

			command := cmdArgs
			tty := !noTTY && term.IsTerminal(int(os.Stdin.Fd())) && len(command) == 0
			if len(command) == 0 {
				command = []string{"/bin/sh"}
			}
			return runExec(ctx, client, proj, inst, command, tty, cmd)
		},
	}
	cmd.Flags().StringVar(&app, "app", "", "app name (default: name in ./zattera.toml)")
	cmd.Flags().StringVar(&env, "env", "", "environment (default: first with a healthy instance)")
	cmd.Flags().StringVar(&instance, "instance", "", "target a specific instance id")
	cmd.Flags().BoolVar(&noTTY, "no-tty", false, "do not allocate a TTY")
	addProjectFlag(cmd)
	return cmd
}

// runExec drives a bidi Exec stream: raw-mode stdin, SIGWINCH resize, output to
// stdout/stderr, and ALWAYS restores the terminal on exit (defer).
func runExec(ctx context.Context, client *apiclient.Client, proj, instanceID string, command []string, tty bool, cmd *cobra.Command) error {
	stream, err := client.Exec.Exec(ctx)
	if err != nil {
		return apiError(err)
	}

	start := &zatterav1.ExecStart{
		ProjectId:  proj,
		InstanceId: instanceID,
		Command:    command,
		Tty:        tty,
	}
	if tty {
		if w, h, err := term.GetSize(int(os.Stdin.Fd())); err == nil {
			start.InitialSize = &zatterav1.TerminalSize{Cols: uint32(w), Rows: uint32(h)}
		}
	}
	if err := stream.Send(&zatterav1.ExecInput{Start: start}); err != nil {
		return apiError(err)
	}

	// Raw mode for interactive TTYs; restore unconditionally on the way out.
	var restore func()
	if tty {
		oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
		if err == nil {
			restore = func() { _ = term.Restore(int(os.Stdin.Fd()), oldState) }
			defer restore()
			stopWinch := watchWinch(stream)
			defer stopWinch()
		}
	}

	// stdin → stream
	go func() {
		buf := make([]byte, 32*1024)
		for {
			n, err := os.Stdin.Read(buf)
			if n > 0 {
				if serr := stream.Send(&zatterav1.ExecInput{Stdin: append([]byte(nil), buf[:n]...)}); serr != nil {
					return
				}
			}
			if err != nil {
				_ = stream.CloseSend()
				return
			}
		}
	}()

	// stream → stdout/stderr
	var exitCode int32
	for {
		out, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if restore != nil {
				restore()
			}
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
	if restore != nil {
		restore()
	}
	if exitCode != 0 {
		return exitError{code: int(exitCode)}
	}
	return nil
}

func newTopCmd() *cobra.Command {
	var app, env, instance string
	cmd := &cobra.Command{
		Use:   "top [app]",
		Short: "Show the process table of a running instance",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			appName := app
			if len(args) == 1 {
				appName = args[0]
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
			resp, err := client.Exec.Top(ctx, &zatterav1.TopRequest{ProjectId: proj, InstanceId: inst})
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(resp)
			}
			rows := make([][]string, 0, len(resp.GetProcesses()))
			for _, proc := range resp.GetProcesses() {
				rows = append(rows, proc.GetFields())
			}
			p.Table(resp.GetTitles(), rows)
			return nil
		},
	}
	cmd.Flags().StringVar(&app, "app", "", "app name (default: name in ./zattera.toml)")
	cmd.Flags().StringVar(&env, "env", "", "environment (default: first with a healthy instance)")
	cmd.Flags().StringVar(&instance, "instance", "", "target a specific instance id")
	addProjectFlag(cmd)
	return cmd
}

// pickInstance resolves --instance, or selects a healthy instance of the app
// (optionally scoped to --env).
func pickInstance(ctx context.Context, client *apiclient.Client, proj, app, env, instance string) (string, error) {
	if instance != "" {
		return instance, nil
	}
	got, err := client.Apps.GetApp(ctx, &zatterav1.GetAppRequest{ProjectId: proj, AppId: app})
	if err != nil {
		return "", apiError(err)
	}
	appID := got.GetApp().GetMeta().GetId()

	var envID string
	if env != "" {
		envID, err = resolveEnv(ctx, client, proj, app, env)
		if err != nil {
			return "", err
		}
	}

	resp, err := client.Deploys.ListInstances(ctx, &zatterav1.ListInstancesRequest{
		ProjectId: proj, AppId: appID, EnvironmentId: envID,
	})
	if err != nil {
		return "", apiError(err)
	}
	var firstRunning string
	for _, a := range resp.GetInstances() {
		if a.GetObserved().GetState() == zatterav1.InstanceState_INSTANCE_STATE_HEALTHY {
			return a.GetMeta().GetId(), nil
		}
		if firstRunning == "" && a.GetObserved().GetContainerId() != "" {
			firstRunning = a.GetMeta().GetId()
		}
	}
	if firstRunning != "" {
		return firstRunning, nil
	}
	return "", errors.New("no running instance found (is the app deployed?)")
}

// argsAfterDashDash returns the args cobra recorded after a literal "--".
func argsAfterDashDash(cmd *cobra.Command, args []string) []string {
	if n := cmd.ArgsLenAtDash(); n >= 0 && n <= len(args) {
		return args[n:]
	}
	return nil
}

// exitError carries a non-zero remote exit status so the CLI exits with it.
type exitError struct{ code int }

func (e exitError) Error() string { return fmt.Sprintf("command exited with code %d", e.code) }

// ExitCode reports the process exit code the root command should use.
func (e exitError) ExitCode() int { return e.code }

// watchWinch forwards terminal resize events to the exec stream until stopped.
func watchWinch(stream zatterav1.ExecService_ExecClient) func() {
	ch := make(chan os.Signal, 1)
	notifyWinch(ch)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case <-ch:
				if w, h, err := term.GetSize(int(os.Stdin.Fd())); err == nil {
					_ = stream.Send(&zatterav1.ExecInput{Resize: &zatterav1.TerminalSize{Cols: uint32(w), Rows: uint32(h)}})
				}
			}
		}
	}()
	return func() {
		signal.Stop(ch)
		close(done)
	}
}
