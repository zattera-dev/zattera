// This file is the only place bubbletea is imported. Keep it that way: every
// other command is plain cobra + the ui.Printer, and pulling a TUI runtime into
// them would cost binary size for no benefit (T-77).
package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/pkg/apiclient"
)

// browseTimeout bounds one listing or download.
const browseTimeout = 5 * time.Minute

// volumeFS is the data source the browser model drives. The real one talks to
// VolumeService; tests supply a fake, so the model is exercised without a TTY
// or a server.
type volumeFS interface {
	// List returns one directory's entries (dirs first) and whether the
	// listing was truncated.
	List(ctx context.Context, dir string) ([]*zatterav1.FileInfo, bool, error)
	// Download streams a file to w, reporting bytes written so far.
	Download(ctx context.Context, file string, w io.Writer, onBytes func(int64)) error
}

func newVolumeBrowseCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "browse <volume>",
		Short: "Browse a volume's files (read-only)",
		Long: "Interactive read-only file browser for a volume.\n\n" +
			"Navigate with ↑/↓, enter to descend, backspace to go up, d to download the\n" +
			"selected file to the current directory, q to quit.\n\n" +
			"Read-only by design: there are no delete or upload keys. Use 'zt volume cp'\n" +
			"to write into a volume.",
		Args: cobra.ExactArgs(1),
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
			ctx, cancel := context.WithCancel(cmd.Context())
			defer cancel()

			volID, volName, err := resolveVolume(ctx, client, proj, args[0])
			if err != nil {
				return err
			}
			fs := &apiVolumeFS{client: client, project: proj, volumeID: volID}
			m := newBrowseModel(fs, volName)
			final, err := tea.NewProgram(m, tea.WithContext(ctx)).Run()
			if err != nil {
				return err
			}
			// Surface a fatal load error as a normal command failure so the
			// exit code is right for scripts.
			if bm, ok := final.(browseModel); ok && bm.fatal != nil {
				return bm.fatal
			}
			return nil
		},
	}
	addProjectFlag(cmd)
	return cmd
}

// resolveVolume maps a volume name or id to its id, returning the display name.
func resolveVolume(ctx context.Context, client *apiclient.Client, project, ref string) (id, name string, err error) {
	rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resp, err := client.Volumes.ListVolumes(rctx, &zatterav1.ListVolumesRequest{ProjectId: project})
	if err != nil {
		return "", "", apiError(err)
	}
	for _, v := range resp.GetVolumes() {
		if v.GetMeta().GetId() == ref || v.GetName() == ref {
			return v.GetMeta().GetId(), v.GetName(), nil
		}
	}
	return "", "", fmt.Errorf("volume %q not found in project %s", ref, project)
}

// --- messages ---

type filesMsg struct {
	dir       string
	files     []*zatterav1.FileInfo
	truncated bool
}
type loadErrMsg struct{ err error }
type dlProgressMsg struct{ done int64 }
type dlDoneMsg struct {
	name  string
	bytes int64
}
type dlErrMsg struct{ err error }

// --- model ---

type browseModel struct {
	fs     volumeFS
	volume string

	dir       string
	files     []*zatterav1.FileInfo
	truncated bool
	cursor    int
	offset    int // first visible row
	height    int // rows available for the list

	loading bool
	status  string
	errMsg  string
	// fatal is set when the first listing fails: there is nothing to browse,
	// so the program exits and the command reports the error.
	fatal error

	// download state
	dlName  string
	dlDone  int64
	dlTotal int64
	dlCh    chan tea.Msg

	quitting bool
}

func newBrowseModel(fs volumeFS, volume string) browseModel {
	return browseModel{fs: fs, volume: volume, dir: "/", loading: true, height: 15}
}

func (m browseModel) Init() tea.Cmd { return m.list(m.dir) }

// list loads a directory in the background.
func (m browseModel) list(dir string) tea.Cmd {
	fs := m.fs
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), browseTimeout)
		defer cancel()
		files, truncated, err := fs.List(ctx, dir)
		if err != nil {
			return loadErrMsg{err: err}
		}
		return filesMsg{dir: dir, files: files, truncated: truncated}
	}
}

func (m browseModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		// Header (2) + footer (3) leave the rest for entries.
		m.height = max(msg.Height-5, 3)
		m.clampCursor()
		return m, nil

	case filesMsg:
		m.loading = false
		m.dir, m.files, m.truncated = msg.dir, msg.files, msg.truncated
		m.cursor, m.offset = 0, 0
		m.errMsg = ""
		return m, nil

	case loadErrMsg:
		m.loading = false
		if m.files == nil && m.dir == "/" {
			// Nothing was ever loaded — quit instead of showing an empty pane.
			m.fatal = msg.err
			m.quitting = true
			return m, tea.Quit
		}
		m.errMsg = msg.err.Error()
		return m, nil

	case dlProgressMsg:
		m.dlDone = msg.done
		return m, waitForMsg(m.dlCh)

	case dlDoneMsg:
		m.dlCh = nil
		m.dlName = ""
		m.status = fmt.Sprintf("saved %s (%s)", msg.name, fmtBytes(float64(msg.bytes)))
		return m, nil

	case dlErrMsg:
		m.dlCh = nil
		m.dlName = ""
		m.errMsg = msg.err.Error()
		return m, nil

	case tea.KeyMsg:
		return m.onKey(msg)
	}
	return m, nil
}

func (m browseModel) onKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "q", "ctrl+c", "esc":
		m.quitting = true
		return m, tea.Quit

	case "up", "k":
		if m.cursor > 0 {
			m.cursor--
		}
		m.scrollToCursor()
		return m, nil

	case "down", "j":
		if m.cursor < len(m.files)-1 {
			m.cursor++
		}
		m.scrollToCursor()
		return m, nil

	case "home", "g":
		m.cursor, m.offset = 0, 0
		return m, nil

	case "end", "G":
		m.cursor = max(len(m.files)-1, 0)
		m.scrollToCursor()
		return m, nil

	case "enter", "right", "l":
		f := m.selected()
		if f == nil || !f.GetDir() {
			return m, nil
		}
		m.loading, m.status = true, ""
		return m, m.list(path.Join(m.dir, f.GetName()))

	case "backspace", "left", "h":
		if m.dir == "/" || m.dir == "" {
			return m, nil
		}
		m.loading, m.status = true, ""
		return m, m.list(path.Dir(m.dir))

	case "r":
		m.loading = true
		return m, m.list(m.dir)

	case "d":
		return m.startDownload()
	}
	return m, nil
}

// startDownload saves the selected file into the working directory. Directories
// are skipped: this is a file browser, not a sync tool.
func (m browseModel) startDownload() (tea.Model, tea.Cmd) {
	if m.dlCh != nil {
		m.status = "a download is already running"
		return m, nil
	}
	f := m.selected()
	if f == nil {
		return m, nil
	}
	if f.GetDir() {
		m.errMsg = "cannot download a directory"
		return m, nil
	}
	src := path.Join(m.dir, f.GetName())
	// Take the base name only: a volume path must never decide where a file
	// lands on the operator's machine.
	dest := path.Base(f.GetName())
	if dest == "." || dest == "/" || dest == "" {
		m.errMsg = "cannot download an unnamed file"
		return m, nil
	}

	ch := make(chan tea.Msg, 16)
	m.dlCh = ch
	m.dlName = f.GetName()
	m.dlDone, m.dlTotal = 0, int64(f.GetSizeBytes())
	m.errMsg, m.status = "", ""

	fs := m.fs
	go func() {
		defer close(ch)
		ctx, cancel := context.WithTimeout(context.Background(), browseTimeout)
		defer cancel()

		out, err := os.Create(dest)
		if err != nil {
			ch <- dlErrMsg{err: err}
			return
		}
		derr := fs.Download(ctx, src, out, func(n int64) {
			// Non-blocking: a slow UI must not stall the transfer.
			select {
			case ch <- dlProgressMsg{done: n}:
			default:
			}
		})
		cerr := out.Close()
		if derr != nil {
			_ = os.Remove(dest) // don't leave a truncated file behind
			ch <- dlErrMsg{err: derr}
			return
		}
		if cerr != nil {
			ch <- dlErrMsg{err: cerr}
			return
		}
		info, serr := os.Stat(dest)
		var size int64
		if serr == nil {
			size = info.Size()
		}
		ch <- dlDoneMsg{name: dest, bytes: size}
	}()
	return m, waitForMsg(ch)
}

// waitForMsg turns the next message on ch into a tea.Msg.
func waitForMsg(ch chan tea.Msg) tea.Cmd {
	if ch == nil {
		return nil
	}
	return func() tea.Msg {
		msg, ok := <-ch
		if !ok {
			return nil
		}
		return msg
	}
}

func (m browseModel) selected() *zatterav1.FileInfo {
	if m.cursor < 0 || m.cursor >= len(m.files) {
		return nil
	}
	return m.files[m.cursor]
}

func (m *browseModel) clampCursor() {
	if m.cursor >= len(m.files) {
		m.cursor = max(len(m.files)-1, 0)
	}
	m.scrollToCursor()
}

// scrollToCursor keeps the selected row inside the visible window.
func (m *browseModel) scrollToCursor() {
	if m.cursor < m.offset {
		m.offset = m.cursor
	}
	if m.height > 0 && m.cursor >= m.offset+m.height {
		m.offset = m.cursor - m.height + 1
	}
	if m.offset < 0 {
		m.offset = 0
	}
}

// --- view ---

var (
	browseTitle  = lipgloss.NewStyle().Bold(true)
	browseDim    = lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	browseSel    = lipgloss.NewStyle().Foreground(lipgloss.Color("39")).Bold(true)
	browseDir    = lipgloss.NewStyle().Foreground(lipgloss.Color("39"))
	browseWarn   = lipgloss.NewStyle().Foreground(lipgloss.Color("214"))
	browseErrSty = lipgloss.NewStyle().Foreground(lipgloss.Color("196"))
)

func (m browseModel) View() string {
	if m.quitting {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s %s\n", browseTitle.Render(m.volume), browseDim.Render(m.dir))

	switch {
	case m.loading:
		b.WriteString(browseDim.Render("loading…") + "\n")
	case len(m.files) == 0:
		b.WriteString(browseDim.Render("(empty)") + "\n")
	default:
		end := min(m.offset+m.height, len(m.files))
		for i := m.offset; i < end; i++ {
			b.WriteString(m.renderRow(i) + "\n")
		}
		if end < len(m.files) {
			fmt.Fprintf(&b, "%s\n", browseDim.Render(fmt.Sprintf("… %d more", len(m.files)-end)))
		}
	}

	if m.truncated {
		b.WriteString(browseWarn.Render(fmt.Sprintf(
			"! directory has more than %d entries; only the first %d are shown", browseCap, browseCap)) + "\n")
	}
	if m.dlCh != nil {
		b.WriteString(m.renderProgress() + "\n")
	}
	if m.errMsg != "" {
		b.WriteString(browseErrSty.Render("✗ "+m.errMsg) + "\n")
	} else if m.status != "" {
		b.WriteString(browseDim.Render("✓ "+m.status) + "\n")
	}
	b.WriteString(browseDim.Render("↑/↓ move · enter open · backspace up · d download · r refresh · q quit"))
	return b.String()
}

// browseCap mirrors the server's per-directory entry cap, for the warning text.
const browseCap = 5000

func (m browseModel) renderRow(i int) string {
	f := m.files[i]
	name := f.GetName()
	size := fmtBytes(float64(f.GetSizeBytes()))
	if f.GetDir() {
		name += "/"
		size = "-"
	}
	mod := ""
	if ms := f.GetModTimeUnixMs(); ms > 0 {
		mod = time.UnixMilli(ms).Local().Format("2006-01-02 15:04")
	}
	row := fmt.Sprintf("%-40s %10s  %s", truncateName(name, 40), size, mod)
	switch {
	case i == m.cursor:
		return browseSel.Render("> " + row)
	case f.GetDir():
		return "  " + browseDir.Render(row)
	default:
		return "  " + row
	}
}

// renderProgress draws a bar when the size is known and a byte count when it is
// not (a file that grew since the listing, say).
func (m browseModel) renderProgress() string {
	if m.dlTotal <= 0 {
		return browseDim.Render(fmt.Sprintf("downloading %s… %s", m.dlName, fmtBytes(float64(m.dlDone))))
	}
	const width = 24
	pct := float64(m.dlDone) / float64(m.dlTotal)
	if pct > 1 {
		pct = 1
	}
	filled := int(pct * width)
	bar := strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
	return browseDim.Render(fmt.Sprintf("downloading %s [%s] %3.0f%% (%s/%s)",
		m.dlName, bar, pct*100, fmtBytes(float64(m.dlDone)), fmtBytes(float64(m.dlTotal))))
}

// truncateName keeps long names from breaking the column layout.
func truncateName(s string, width int) string {
	if lipgloss.Width(s) <= width {
		return s
	}
	runes := []rune(s)
	if len(runes) <= 1 {
		return s
	}
	// Trim from the middle: both ends of a filename carry information.
	keep := width - 1
	head := keep / 2
	tail := keep - head
	return string(runes[:head]) + "…" + string(runes[len(runes)-tail:])
}

// --- API-backed volumeFS ---

type apiVolumeFS struct {
	client   *apiclient.Client
	project  string
	volumeID string
}

func (a *apiVolumeFS) List(ctx context.Context, dir string) ([]*zatterav1.FileInfo, bool, error) {
	resp, err := a.client.Volumes.ListFiles(ctx, &zatterav1.ListFilesRequest{
		ProjectId: a.project, VolumeId: a.volumeID, Path: dir,
	})
	if err != nil {
		return nil, false, apiError(err)
	}
	return resp.GetFiles(), resp.GetTruncated(), nil
}

func (a *apiVolumeFS) Download(ctx context.Context, file string, w io.Writer, onBytes func(int64)) error {
	stream, err := a.client.Volumes.ReadFile(ctx, &zatterav1.ReadFileRequest{
		ProjectId: a.project, VolumeId: a.volumeID, Path: file,
	})
	if err != nil {
		return apiError(err)
	}
	var done int64
	for {
		chunk, rerr := stream.Recv()
		if rerr == io.EOF {
			return nil
		}
		if rerr != nil {
			return apiError(rerr)
		}
		n, werr := w.Write(chunk.GetData())
		if werr != nil {
			return werr
		}
		done += int64(n)
		if onBytes != nil {
			onBytes(done)
		}
	}
}
