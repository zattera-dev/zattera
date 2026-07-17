package cli

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/timestamppb"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

func newStatsCmd() *cobra.Command {
	var nodesFlag bool
	var app, node string
	var since, step time.Duration
	cmd := &cobra.Command{
		Use:   "stats",
		Short: "Show resource stats (current values, or history with --since)",
		Long: "Stats from node heartbeats (current) or the embedded TSDB (history).\n" +
			"By default (or with --nodes) shows per-node CPU/memory/disk; with --app\n" +
			"shows per-environment traffic. Pass --since (e.g. 1h) to render history as\n" +
			"sparklines instead of a single current value.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, cctx, err := clientFromContext()
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()
			ctx, cancel := cmdContext(cmd)
			defer cancel()

			q := &zatterav1.StatsQuery{NodeId: node}
			appMode := app != ""
			if appMode {
				proj, err := projectName(cctx)
				if err != nil {
					return err
				}
				got, err := client.Apps.GetApp(ctx, &zatterav1.GetAppRequest{ProjectId: proj, AppId: app})
				if err != nil {
					return apiError(err)
				}
				q.AppId = got.GetApp().GetMeta().GetId()
			}

			historical := since > 0
			if historical {
				now := time.Now()
				q.Since = timestamppb.New(now.Add(-since))
				q.Until = timestamppb.New(now)
				if step > 0 {
					q.StepSeconds = uint32(step / time.Second)
				}
			}

			resp, err := client.Metrics.Stats(ctx, q)
			if err != nil {
				return apiError(err)
			}
			p := printerFor(cmd)
			if jsonFlag {
				return p.EmitJSON(resp.GetSeries())
			}
			_ = nodesFlag // --nodes is the default view
			scopeLabel, cols := "node", nodeMetricCols
			if appMode {
				scopeLabel, cols = "env", envMetricCols
			}
			if historical {
				renderStatsHistory(p.Table, resp.GetSeries(), scopeLabel, cols)
			} else {
				renderStatsTable(p.Table, resp.GetSeries(), scopeLabel, cols)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&nodesFlag, "nodes", false, "show per-node stats (default)")
	cmd.Flags().StringVar(&app, "app", "", "show per-environment traffic stats for this app")
	cmd.Flags().StringVar(&node, "node", "", "show stats for a single node id")
	cmd.Flags().DurationVar(&since, "since", 0, "show history over this window (e.g. 1h, 30m) as sparklines")
	cmd.Flags().DurationVar(&step, "step", 0, "resolution for --since (e.g. 15s, 5m); default is the server's raw step")
	addProjectFlag(cmd)
	return cmd
}

// statsColumn pairs a metric key with its column header and a value formatter.
type statsColumn struct {
	metric string
	header string
	format func(float64) string
}

var nodeMetricCols = []statsColumn{
	{"cpu_percent", "CPU%", fmtPercent},
	{"memory_bytes", "MEM", fmtBytes},
	{"memory_percent", "MEM%", fmtPercent},
	{"disk_bytes", "DISK", fmtBytes},
	{"disk_percent", "DISK%", fmtPercent},
}

var envMetricCols = []statsColumn{
	{"rps", "RPS", fmtFloat},
	{"inflight", "INFLIGHT", fmtFloat},
	{"error_rate", "ERR%", fmtPercent},
	{"latency_p50_ms", "P50ms", fmtFloat},
	{"latency_p99_ms", "P99ms", fmtFloat},
}

// renderStatsTable pivots the flat series list into one row per scope entity
// (node or env), one column per metric.
func renderStatsTable(table func([]string, [][]string), series []*zatterav1.Series, scopeLabel string, cols []statsColumn) {
	// entity id → metric → value
	rowsByID := map[string]map[string]float64{}
	for _, s := range series {
		id := s.GetLabels()[scopeLabel]
		if id == "" {
			continue
		}
		vals := rowsByID[id]
		if vals == nil {
			vals = map[string]float64{}
			rowsByID[id] = vals
		}
		if pts := s.GetPoints(); len(pts) > 0 {
			vals[s.GetMetric()] = pts[len(pts)-1].GetValue()
		}
	}

	ids := make([]string, 0, len(rowsByID))
	for id := range rowsByID {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	headers := make([]string, 0, len(cols)+1)
	headers = append(headers, strings.ToUpper(scopeLabel)) // "NODE" / "ENV"
	for _, c := range cols {
		headers = append(headers, c.header)
	}

	rows := make([][]string, 0, len(ids))
	for _, id := range ids {
		vals := rowsByID[id]
		row := make([]string, 0, len(cols)+1)
		row = append(row, shortID(id))
		for _, c := range cols {
			if v, ok := vals[c.metric]; ok {
				row = append(row, c.format(v))
			} else {
				row = append(row, "-")
			}
		}
		rows = append(rows, row)
	}
	table(headers, rows)
}

func fmtPercent(v float64) string { return fmt.Sprintf("%.1f%%", v) }
func fmtFloat(v float64) string   { return fmt.Sprintf("%.1f", v) }

func fmtBytes(v float64) string {
	const unit = 1024.0
	if v < unit {
		return fmt.Sprintf("%.0fB", v)
	}
	div, exp := unit, 0
	for n := v / unit; n >= unit && exp < 4; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", v/div, "KMGT"[exp])
}

// sparkBlocks ramps from lowest to highest for the eight-level sparkline.
var sparkBlocks = []rune("▁▂▃▄▅▆▇█")

// sparkline renders a series of values as a unicode block trend, normalized to
// the series' own min/max (a flat series renders as the lowest block).
func sparkline(vals []float64) string {
	if len(vals) == 0 {
		return ""
	}
	min, max := vals[0], vals[0]
	for _, v := range vals {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	span := max - min
	var b strings.Builder
	for _, v := range vals {
		idx := 0
		if span > 0 {
			idx = int((v-min)/span*float64(len(sparkBlocks)-1) + 0.5)
		}
		if idx < 0 {
			idx = 0
		} else if idx >= len(sparkBlocks) {
			idx = len(sparkBlocks) - 1
		}
		b.WriteRune(sparkBlocks[idx])
	}
	return b.String()
}

// renderStatsHistory renders one row per series: the scoped entity, the metric,
// a sparkline of its points over the window, and the latest value.
func renderStatsHistory(table func([]string, [][]string), series []*zatterav1.Series, scopeLabel string, cols []statsColumn) {
	byMetric := map[string]statsColumn{}
	for _, c := range cols {
		byMetric[c.metric] = c
	}

	sorted := append([]*zatterav1.Series(nil), series...)
	sort.Slice(sorted, func(i, j int) bool {
		li, lj := sorted[i].GetLabels()[scopeLabel], sorted[j].GetLabels()[scopeLabel]
		if li != lj {
			return li < lj
		}
		return sorted[i].GetMetric() < sorted[j].GetMetric()
	})

	headers := []string{strings.ToUpper(scopeLabel), "METRIC", "TREND", "LAST"}
	rows := make([][]string, 0, len(sorted))
	for _, s := range sorted {
		id := s.GetLabels()[scopeLabel]
		if id == "" {
			continue
		}
		pts := s.GetPoints()
		vals := make([]float64, 0, len(pts))
		for _, p := range pts {
			vals = append(vals, p.GetValue())
		}
		col, ok := byMetric[s.GetMetric()]
		header, format := s.GetMetric(), fmtFloat
		if ok {
			header, format = col.header, col.format
		}
		last := "-"
		if len(vals) > 0 {
			last = format(vals[len(vals)-1])
		}
		rows = append(rows, []string{shortID(id), header, sparkline(vals), last})
	}
	table(headers, rows)
}
