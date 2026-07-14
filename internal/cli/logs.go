package cli

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/timestamppb"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

func newLogsCmd() *cobra.Command {
	var app, env, since string
	var follow bool
	cmd := &cobra.Command{
		Use:   "logs [app]",
		Short: "Stream logs for an app across its instances",
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

			got, err := client.Apps.GetApp(ctx, &zatterav1.GetAppRequest{ProjectId: proj, AppId: appName})
			if err != nil {
				return apiError(err)
			}
			sel := &zatterav1.LogSelector{ProjectId: proj, AppId: got.GetApp().GetMeta().GetId()}
			if env != "" {
				envID, err := resolveEnv(ctx, client, proj, appName, env)
				if err != nil {
					return err
				}
				sel.EnvironmentId = envID
			}

			q := &zatterav1.LogQuery{Selector: sel, Follow: follow}
			if since != "" {
				d, perr := time.ParseDuration(since)
				if perr != nil {
					return fmt.Errorf("invalid --since %q: %w", since, perr)
				}
				q.Since = timestamppb.New(time.Now().Add(-d))
			}

			stream, err := client.Logs.Query(ctx, q)
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			for {
				line, err := stream.Recv()
				if errors.Is(err, io.EOF) {
					return nil
				}
				if err != nil {
					return apiError(err)
				}
				if jsonFlag {
					_ = p.EmitJSON(line)
					continue
				}
				fmt.Fprintf(os.Stdout, "%s │ %s\n", logPrefix(appName, line), line.GetLine())
			}
		},
	}
	cmd.Flags().StringVar(&app, "app", "", "app name (default: name in ./zattera.toml)")
	cmd.Flags().StringVar(&env, "env", "", "environment (default: all)")
	cmd.Flags().StringVar(&since, "since", "", "only logs newer than this (e.g. 10m, 1h)")
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "stream live logs")
	addProjectFlag(cmd)
	return cmd
}

// logPrefix renders a colored per-instance prefix "app-env-inst".
func logPrefix(app string, l *zatterav1.LogLine) string {
	name := l.GetAppName()
	if name == "" {
		name = app
	}
	inst := l.GetInstanceId()
	if len(inst) > 8 {
		inst = inst[len(inst)-8:]
	}
	label := name
	if e := l.GetEnvironmentName(); e != "" {
		label += "-" + e
	}
	if inst != "" {
		label += "-" + inst
	}
	return colorize(label, l.GetInstanceId())
}

// ansiPalette cycles distinct 256-color foregrounds per instance.
var ansiPalette = []int{2, 3, 4, 5, 6, 10, 11, 12, 13, 14}

func colorize(text, key string) string {
	if os.Getenv("NO_COLOR") != "" {
		return text
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	c := ansiPalette[int(h.Sum32())%len(ansiPalette)]
	return fmt.Sprintf("\x1b[38;5;%dm%s\x1b[0m", c, text)
}
