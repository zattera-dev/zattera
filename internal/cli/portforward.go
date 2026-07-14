package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/spf13/cobra"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/pkg/apiclient"
)

func newPortForwardCmd() *cobra.Command {
	var app, env string
	cmd := &cobra.Command{
		Use:   "port-forward [app] <localPort>[:<portName>]",
		Short: "Forward a local port to a healthy instance of the app",
		Long: "Listen on localhost:<localPort> and tunnel each connection to the\n" +
			"named service port of a healthy instance. Example:\n" +
			"  zattera port-forward api 5432:db",
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			appName := app
			spec := args[len(args)-1]
			if len(args) == 2 {
				appName = args[0]
			}
			localPort, portName, err := parsePortSpec(spec)
			if err != nil {
				return err
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

			got, err := client.Apps.GetApp(ctx, &zatterav1.GetAppRequest{ProjectId: proj, AppId: appName})
			if err != nil {
				return apiError(err)
			}
			appID := got.GetApp().GetMeta().GetId()
			var envID string
			if env != "" {
				envID, err = resolveEnv(ctx, client, proj, appName, env)
				if err != nil {
					return err
				}
			}

			lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", localPort))
			if err != nil {
				return fmt.Errorf("listen on :%d: %w", localPort, err)
			}
			defer func() { _ = lis.Close() }()
			go func() { <-ctx.Done(); _ = lis.Close() }()

			p := printerFor(cmd)
			p.Successf("Forwarding 127.0.0.1:%d → %s (%s)", localPort, appName, portDesc(portName))
			for {
				conn, err := lis.Accept()
				if err != nil {
					if ctx.Err() != nil {
						return nil
					}
					return err
				}
				go func() {
					if err := forwardConn(ctx, client, proj, appID, envID, portName, conn); err != nil {
						p.Errorf("connection closed: %v", err)
					}
				}()
			}
		},
	}
	cmd.Flags().StringVar(&app, "app", "", "app name (default: name in ./zattera.toml)")
	cmd.Flags().StringVar(&env, "env", "", "environment (default: first with a healthy instance)")
	addProjectFlag(cmd)
	return cmd
}

// forwardConn tunnels one accepted local connection through ExecService.PortForward.
func forwardConn(ctx context.Context, client *apiclient.Client, proj, appID, envID, portName string, local net.Conn) error {
	defer func() { _ = local.Close() }()
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := client.Exec.PortForward(cctx)
	if err != nil {
		return apiError(err)
	}
	if err := stream.Send(&zatterav1.PortForwardInput{Start: &zatterav1.PortForwardStart{
		ProjectId:     proj,
		AppId:         appID,
		EnvironmentId: envID,
		PortName:      portName,
	}}); err != nil {
		return apiError(err)
	}

	done := make(chan struct{}, 2)
	// local → stream
	go func() {
		buf := make([]byte, 32*1024)
		for {
			n, err := local.Read(buf)
			if n > 0 {
				if serr := stream.Send(&zatterav1.PortForwardInput{Data: append([]byte(nil), buf[:n]...)}); serr != nil {
					break
				}
			}
			if err != nil {
				_ = stream.CloseSend()
				break
			}
		}
		done <- struct{}{}
	}()
	// stream → local
	go func() {
		for {
			out, err := stream.Recv()
			if errors.Is(err, io.EOF) || err != nil {
				break
			}
			if len(out.GetData()) > 0 {
				if _, werr := local.Write(out.GetData()); werr != nil {
					break
				}
			}
		}
		done <- struct{}{}
	}()
	<-done
	cancel()
	<-done
	return nil
}

// parsePortSpec parses "<localPort>" or "<localPort>:<portName>".
func parsePortSpec(spec string) (int, string, error) {
	local, name, hasName := strings.Cut(spec, ":")
	var port int
	if _, err := fmt.Sscanf(local, "%d", &port); err != nil || port <= 0 || port > 65535 {
		return 0, "", fmt.Errorf("invalid local port %q", local)
	}
	if hasName && name == "" {
		return 0, "", fmt.Errorf("empty port name in %q", spec)
	}
	return port, name, nil
}

func portDesc(name string) string {
	if name == "" {
		return "default port"
	}
	return "port " + name
}
