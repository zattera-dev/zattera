// Package ui centralizes CLI output: lipgloss styles, success/error lines in
// the Vercel-style format, tables, and a --json mode that suppresses all
// decoration. Every command routes its output through this package so
// `--json` is uniform (spec §5 polish requirements).
package ui

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

var (
	styleOK    = lipgloss.NewStyle().Foreground(lipgloss.Color("42"))  // green
	styleErr   = lipgloss.NewStyle().Foreground(lipgloss.Color("196")) // red
	styleURL   = lipgloss.NewStyle().Foreground(lipgloss.Color("39")).Underline(true)
	styleDim   = lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	styleTitle = lipgloss.NewStyle().Bold(true)
)

// Printer renders command output. JSON=true switches every helper to
// machine-readable output and silences decorations.
type Printer struct {
	Out  io.Writer
	Err  io.Writer
	JSON bool
}

// Successf prints a "✓" line (no-op in JSON mode — emit a JSON object instead).
func (p *Printer) Successf(format string, args ...any) {
	if p.JSON {
		return
	}
	fmt.Fprintln(p.Out, styleOK.Render("✓")+" "+fmt.Sprintf(format, args...))
}

// Infof prints a dim informational line (stderr; never pollutes stdout).
func (p *Printer) Infof(format string, args ...any) {
	if p.JSON {
		return
	}
	fmt.Fprintln(p.Err, styleDim.Render(fmt.Sprintf(format, args...)))
}

// Errorf prints a "✗" line to stderr (also in JSON mode, still on stderr).
func (p *Printer) Errorf(format string, args ...any) {
	fmt.Fprintln(p.Err, styleErr.Render("✗")+" "+fmt.Sprintf(format, args...))
}

// URL prints the final deploy URL line ("● https://...").
func (p *Printer) URL(u string) {
	if p.JSON {
		return
	}
	fmt.Fprintln(p.Out, styleOK.Render("●")+" "+styleURL.Render(u))
}

// EmitJSON marshals v to stdout (the single stdout artifact in JSON mode).
func (p *Printer) EmitJSON(v any) error {
	enc := json.NewEncoder(p.Out)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// Table renders a simple aligned table with a bold header.
func (p *Printer) Table(headers []string, rows [][]string) {
	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = len(h)
	}
	for _, r := range rows {
		for i, c := range r {
			if i < len(widths) && len(c) > widths[i] {
				widths[i] = len(c)
			}
		}
	}
	var b strings.Builder
	for i, h := range headers {
		b.WriteString(padRight(h, widths[i]+2))
	}
	fmt.Fprintln(p.Out, styleTitle.Render(strings.TrimRight(b.String(), " ")))
	for _, r := range rows {
		var rb strings.Builder
		for i, c := range r {
			if i < len(widths) {
				rb.WriteString(padRight(c, widths[i]+2))
			}
		}
		fmt.Fprintln(p.Out, strings.TrimRight(rb.String(), " "))
	}
}

func padRight(s string, w int) string {
	if len(s) >= w {
		return s + " "
	}
	return s + strings.Repeat(" ", w-len(s))
}
