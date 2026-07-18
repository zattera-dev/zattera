package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
)

// fakeVolumeFS is an in-memory volume tree. Directory entries come back
// dirs-first, matching what the agent guarantees.
type fakeVolumeFS struct {
	dirs      map[string][]*zatterav1.FileInfo
	blobs     map[string][]byte
	truncated map[string]bool
	listErr   error
	dlErr     error
	// listed records the paths List was called with, so a test can assert
	// navigation actually moved.
	listed []string
}

func (f *fakeVolumeFS) List(_ context.Context, dir string) ([]*zatterav1.FileInfo, bool, error) {
	f.listed = append(f.listed, dir)
	if f.listErr != nil {
		return nil, false, f.listErr
	}
	files, ok := f.dirs[dir]
	if !ok {
		return nil, false, fmt.Errorf("%q not found", dir)
	}
	return files, f.truncated[dir], nil
}

func (f *fakeVolumeFS) Download(_ context.Context, file string, w io.Writer, onBytes func(int64)) error {
	if f.dlErr != nil {
		return f.dlErr
	}
	blob, ok := f.blobs[file]
	if !ok {
		return fmt.Errorf("%q not found", file)
	}
	// Two writes so progress is reported more than once.
	half := len(blob) / 2
	for _, part := range [][]byte{blob[:half], blob[half:]} {
		n, err := w.Write(part)
		if err != nil {
			return err
		}
		if onBytes != nil {
			onBytes(int64(n))
		}
	}
	return nil
}

func dirEntry(name string) *zatterav1.FileInfo {
	return &zatterav1.FileInfo{Name: name, Dir: true, Mode: "drwxr-xr-x"}
}

func fileEntry(name string, size uint64) *zatterav1.FileInfo {
	return &zatterav1.FileInfo{Name: name, SizeBytes: size, Mode: "-rw-r--r--", ModTimeUnixMs: 1_700_000_000_000}
}

func testFS() *fakeVolumeFS {
	return &fakeVolumeFS{
		dirs: map[string][]*zatterav1.FileInfo{
			"/":     {dirEntry("data"), fileEntry("README.md", 1024)},
			"/data": {dirEntry("nested"), fileEntry("dump.sql", 2048), fileEntry("notes.txt", 12)},
		},
		blobs:     map[string][]byte{"/data/dump.sql": []byte("0123456789abcdef")},
		truncated: map[string]bool{},
	}
}

// step applies a message and returns the concrete model plus any command.
func step(t *testing.T, m browseModel, msg tea.Msg) (browseModel, tea.Cmd) {
	t.Helper()
	next, cmd := m.Update(msg)
	bm, ok := next.(browseModel)
	if !ok {
		t.Fatalf("Update returned %T, want browseModel", next)
	}
	return bm, cmd
}

// runCmd executes a tea.Cmd and feeds its message back into the model.
func runCmd(t *testing.T, m browseModel, cmd tea.Cmd) browseModel {
	t.Helper()
	if cmd == nil {
		return m
	}
	msg := cmd()
	if msg == nil {
		return m
	}
	m, _ = step(t, m, msg)
	return m
}

func key(s string) tea.KeyMsg {
	switch s {
	case "enter":
		return tea.KeyMsg{Type: tea.KeyEnter}
	case "backspace":
		return tea.KeyMsg{Type: tea.KeyBackspace}
	case "up":
		return tea.KeyMsg{Type: tea.KeyUp}
	case "down":
		return tea.KeyMsg{Type: tea.KeyDown}
	default:
		return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(s)}
	}
}

// loaded returns a model with the root directory already listed.
func loaded(t *testing.T, fs *fakeVolumeFS) browseModel {
	t.Helper()
	m := newBrowseModel(fs, "pg-data")
	return runCmd(t, m, m.Init())
}

// TestVolumeBrowseNavigation drives the model through the key bindings the task
// specifies: move, descend, ascend.
func TestVolumeBrowseNavigation(t *testing.T) {
	fs := testFS()
	m := loaded(t, fs)

	if m.dir != "/" || len(m.files) != 2 {
		t.Fatalf("initial load: dir=%q files=%d", m.dir, len(m.files))
	}
	if m.cursor != 0 || m.files[0].GetName() != "data" {
		t.Fatalf("cursor should start on the first entry (a dir), got %d", m.cursor)
	}

	// Enter descends into the selected directory.
	m2, cmd := step(t, m, key("enter"))
	if !m2.loading {
		t.Error("descending should show a loading state")
	}
	m2 = runCmd(t, m2, cmd)
	if m2.dir != "/data" {
		t.Fatalf("after enter dir = %q, want /data", m2.dir)
	}
	if len(m2.files) != 3 || m2.cursor != 0 {
		t.Fatalf("descend left files=%d cursor=%d", len(m2.files), m2.cursor)
	}

	// Down moves; enter on a file is a no-op (files are not directories).
	m3, _ := step(t, m2, key("down"))
	if m3.cursor != 1 || m3.files[m3.cursor].GetName() != "dump.sql" {
		t.Fatalf("cursor after down = %d (%s)", m3.cursor, m3.files[m3.cursor].GetName())
	}
	before := len(fs.listed)
	m4, cmd := step(t, m3, key("enter"))
	if cmd != nil {
		t.Error("enter on a file must not trigger a listing")
	}
	if len(fs.listed) != before {
		t.Error("enter on a file issued a List call")
	}
	if m4.dir != "/data" {
		t.Errorf("enter on a file changed the directory to %q", m4.dir)
	}

	// Backspace ascends.
	m5, cmd := step(t, m4, key("backspace"))
	m5 = runCmd(t, m5, cmd)
	if m5.dir != "/" {
		t.Fatalf("after backspace dir = %q, want /", m5.dir)
	}

	// Backspace at the root is a no-op, not an error or a climb above it.
	before = len(fs.listed)
	m6, cmd := step(t, m5, key("backspace"))
	if cmd != nil || len(fs.listed) != before {
		t.Error("backspace at the root should do nothing")
	}
	if m6.dir != "/" {
		t.Errorf("backspace at the root moved to %q", m6.dir)
	}

	// Cursor cannot leave the list.
	m7, _ := step(t, m6, key("up"))
	if m7.cursor != 0 {
		t.Errorf("up at the top moved the cursor to %d", m7.cursor)
	}
	m8 := m7
	for i := 0; i < 10; i++ {
		m8, _ = step(t, m8, key("down"))
	}
	if m8.cursor != len(m8.files)-1 {
		t.Errorf("cursor ran past the last entry: %d of %d", m8.cursor, len(m8.files))
	}
}

// TestVolumeBrowseDownload covers `d`: the file lands in the working directory
// under its base name, progress is reported, and the result is announced.
func TestVolumeBrowseDownload(t *testing.T) {
	dir := t.TempDir()
	cwd, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(cwd) })
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}

	fs := testFS()
	m := loaded(t, fs)
	m, cmd := step(t, m, key("enter")) // into /data
	m = runCmd(t, m, cmd)
	m, _ = step(t, m, key("down")) // dump.sql

	m, cmd = step(t, m, key("d"))
	if m.dlCh == nil || m.dlName != "dump.sql" {
		t.Fatalf("download did not start: ch=%v name=%q", m.dlCh != nil, m.dlName)
	}
	if m.dlTotal != 2048 {
		t.Errorf("progress total = %d, want the size from the listing", m.dlTotal)
	}

	// Drain until the download reports done.
	var sawProgress bool
	for i := 0; i < 20 && m.dlCh != nil; i++ {
		if cmd == nil {
			t.Fatal("download stalled with no command to run")
		}
		msg := cmd()
		if _, ok := msg.(dlProgressMsg); ok {
			sawProgress = true
		}
		m, cmd = step(t, m, msg)
	}
	if !sawProgress {
		t.Error("no progress was reported during the download")
	}
	if m.errMsg != "" {
		t.Fatalf("download failed: %s", m.errMsg)
	}
	if !strings.Contains(m.status, "dump.sql") {
		t.Errorf("status does not mention the file: %q", m.status)
	}

	got, err := os.ReadFile(path.Join(dir, "dump.sql"))
	if err != nil {
		t.Fatalf("downloaded file missing: %v", err)
	}
	if string(got) != "0123456789abcdef" {
		t.Errorf("downloaded contents = %q", got)
	}
}

// TestVolumeBrowseDownloadRejectsDirectory keeps `d` from producing a nonsense
// zero-byte file named after a directory.
func TestVolumeBrowseDownloadRejectsDirectory(t *testing.T) {
	m := loaded(t, testFS()) // cursor on "data", a directory
	m, cmd := step(t, m, key("d"))
	if cmd != nil {
		t.Error("downloading a directory should not start a transfer")
	}
	if m.dlCh != nil {
		t.Error("download channel opened for a directory")
	}
	if !strings.Contains(m.errMsg, "directory") {
		t.Errorf("error message = %q, want it to mention a directory", m.errMsg)
	}
}

// TestVolumeBrowseDownloadFailureCleansUp checks that a failed transfer does
// not leave a truncated file on disk.
func TestVolumeBrowseDownloadFailureCleansUp(t *testing.T) {
	dir := t.TempDir()
	cwd, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(cwd) })
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}

	fs := testFS()
	fs.dlErr = errors.New("node went away")
	m := loaded(t, fs)
	m, cmd := step(t, m, key("enter"))
	m = runCmd(t, m, cmd)
	m, _ = step(t, m, key("down"))

	m, cmd = step(t, m, key("d"))
	for i := 0; i < 20 && m.dlCh != nil; i++ {
		if cmd == nil {
			break
		}
		msg := cmd()
		m, cmd = step(t, m, msg)
	}
	if !strings.Contains(m.errMsg, "node went away") {
		t.Fatalf("error not surfaced: %q", m.errMsg)
	}
	if _, err := os.Stat(path.Join(dir, "dump.sql")); !os.IsNotExist(err) {
		t.Error("a partial file was left behind after a failed download")
	}
}

// TestVolumeBrowseView pins what the operator actually sees.
func TestVolumeBrowseView(t *testing.T) {
	fs := testFS()
	fs.truncated["/"] = true
	m := loaded(t, fs)
	view := m.View()

	for _, want := range []string{"pg-data", "/", "data/", "README.md", "1.0KB", "download", "quit"} {
		if !strings.Contains(view, want) {
			t.Errorf("view missing %q:\n%s", want, view)
		}
	}
	// Sizes are human-readable, not raw bytes.
	if strings.Contains(view, "1024") {
		t.Errorf("view shows raw byte counts:\n%s", view)
	}
	// A truncated listing must say so — silently showing 5000 of 40000 entries
	// would read as "this is the whole directory".
	if !strings.Contains(view, "more than") {
		t.Errorf("truncated listing not flagged:\n%s", view)
	}
	// Directories show no size.
	if !strings.Contains(view, "data/") {
		t.Errorf("directory not marked with a trailing slash:\n%s", view)
	}
	// Read-only: no destructive keys are advertised.
	for _, forbidden := range []string{"delete", "upload", "rm "} {
		if strings.Contains(strings.ToLower(view), forbidden) {
			t.Errorf("read-only browser advertises %q:\n%s", forbidden, view)
		}
	}

	if q, _ := step(t, m, key("q")); q.View() != "" {
		t.Error("quitting should clear the view")
	}
}

// TestVolumeBrowseInitialLoadFailureQuits ensures a browse against a missing or
// unreachable volume exits with an error instead of showing an empty pane.
func TestVolumeBrowseInitialLoadFailureQuits(t *testing.T) {
	fs := testFS()
	fs.listErr = errors.New("volume lives on node-2, which is down")
	m := newBrowseModel(fs, "pg-data")
	m = runCmd(t, m, m.Init())

	if m.fatal == nil {
		t.Fatal("a failed initial load must be fatal")
	}
	if !strings.Contains(m.fatal.Error(), "node-2") {
		t.Errorf("fatal error lost the cause: %v", m.fatal)
	}

	// A failure *after* something loaded is recoverable, not fatal.
	fs2 := testFS()
	m2 := loaded(t, fs2)
	m2, _ = step(t, m2, loadErrMsg{err: errors.New("transient")})
	if m2.fatal != nil {
		t.Error("a later failure should not be fatal")
	}
	if !strings.Contains(m2.errMsg, "transient") {
		t.Errorf("later failure not shown: %q", m2.errMsg)
	}
}

// TestVolumeBrowseScrolling checks the window follows the cursor, so a long
// directory stays navigable.
func TestVolumeBrowseScrolling(t *testing.T) {
	files := make([]*zatterav1.FileInfo, 50)
	for i := range files {
		files[i] = fileEntry(fmt.Sprintf("file-%02d", i), uint64(i))
	}
	fs := &fakeVolumeFS{dirs: map[string][]*zatterav1.FileInfo{"/": files}, truncated: map[string]bool{}}
	m := loaded(t, fs)
	m, _ = step(t, m, tea.WindowSizeMsg{Width: 80, Height: 15})
	if m.height != 10 {
		t.Fatalf("list height = %d, want the window minus chrome", m.height)
	}

	for i := 0; i < 20; i++ {
		m, _ = step(t, m, key("down"))
	}
	if m.cursor != 20 {
		t.Fatalf("cursor = %d", m.cursor)
	}
	if m.cursor < m.offset || m.cursor >= m.offset+m.height {
		t.Fatalf("cursor %d outside the window [%d,%d)", m.cursor, m.offset, m.offset+m.height)
	}
	if !strings.Contains(m.View(), "file-20") {
		t.Errorf("selected row not visible:\n%s", m.View())
	}

	// End jumps to the last entry and the window follows.
	m, _ = step(t, m, key("G"))
	if m.cursor != 49 {
		t.Fatalf("G left the cursor at %d", m.cursor)
	}
	if !strings.Contains(m.View(), "file-49") {
		t.Errorf("last row not visible after G:\n%s", m.View())
	}
}

// TestVolumeBrowseTruncateName guards the column layout against long and
// multi-byte names.
func TestVolumeBrowseTruncateName(t *testing.T) {
	cases := []struct{ in string }{
		{"short.txt"},
		{strings.Repeat("a", 100)},
		{strings.Repeat("é", 60)},
		{"日本語のファイル名がとても長い場合のテストケースです.txt"},
	}
	for _, tc := range cases {
		got := truncateName(tc.in, 40)
		if w := len([]rune(got)); w > 40 {
			t.Errorf("truncateName(%q) is %d runes, want <= 40", tc.in, w)
		}
	}
	if got := truncateName("short.txt", 40); got != "short.txt" {
		t.Errorf("short names must be untouched, got %q", got)
	}
}
