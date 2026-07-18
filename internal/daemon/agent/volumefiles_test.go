package agent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/testutil/fakeruntime"
)

// fileRuntime is the fake container runtime with VolumeHostPath redirected at
// a real temp directory, so the file handlers run against a real filesystem.
type fileRuntime struct {
	*fakeruntime.Fake
	hostPath string
}

func (f fileRuntime) VolumeHostPath(context.Context, string) (string, error) {
	return f.hostPath, nil
}

func newFileRuntime(hostPath string) fileRuntime {
	return fileRuntime{Fake: fakeruntime.New(), hostPath: hostPath}
}

// chunkSink collects a ReadVolumeFile stream.
type chunkSink struct {
	grpc.ServerStream
	ctx  context.Context
	data []byte
}

func (c *chunkSink) Context() context.Context { return c.ctx }
func (c *chunkSink) Send(ch *clusterv1.TCPChunk) error {
	c.data = append(c.data, ch.GetData()...)
	return nil
}

// volumeFixture builds a volume tree:
//
//	<vol>/README.md
//	<vol>/data/dump.sql
//	<vol>/escape -> <outside>/secret.txt   (symlink out of the volume)
//
// and returns the server plus the directory holding the volume.
func volumeFixture(t *testing.T) (*LocalServer, string, string) {
	t.Helper()
	root := t.TempDir()
	vol := filepath.Join(root, "volume")
	outside := filepath.Join(root, "outside")
	for _, d := range []string{vol, filepath.Join(vol, "data"), outside} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write := func(p, content string) {
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(vol, "README.md"), "hello")
	write(filepath.Join(vol, "data", "dump.sql"), "SELECT 1;")
	write(filepath.Join(outside, "secret.txt"), "TOP SECRET")

	s := &LocalServer{rt: newFileRuntime(vol)}
	return s, vol, outside
}

func listReq(path string) *clusterv1.AgentListFilesRequest {
	return &clusterv1.AgentListFilesRequest{EnvironmentId: "env1", VolumeName: "pg-data", Path: path}
}

func readReq(path string) *clusterv1.AgentReadFileRequest {
	return &clusterv1.AgentReadFileRequest{EnvironmentId: "env1", VolumeName: "pg-data", Path: path}
}

// TestListVolumeFiles covers a normal listing: dirs first, then names, with
// sizes and modes carried through.
func TestListVolumeFiles(t *testing.T) {
	s, _, _ := volumeFixture(t)

	resp, err := s.ListVolumeFiles(context.Background(), listReq("/"))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(resp.GetFiles()) != 2 {
		t.Fatalf("got %d entries, want 2", len(resp.GetFiles()))
	}
	// Directories sort first regardless of name ("data" < "README.md" only
	// because of the dir rule; alphabetically uppercase R sorts first).
	if !resp.GetFiles()[0].GetDir() || resp.GetFiles()[0].GetName() != "data" {
		t.Errorf("directories must come first, got %+v", resp.GetFiles()[0])
	}
	readme := resp.GetFiles()[1]
	if readme.GetName() != "README.md" || readme.GetDir() {
		t.Fatalf("second entry = %+v", readme)
	}
	if readme.GetSizeBytes() != 5 {
		t.Errorf("size = %d, want 5", readme.GetSizeBytes())
	}
	if readme.GetModTimeUnixMs() == 0 || readme.GetMode() == "" {
		t.Errorf("metadata missing: %+v", readme)
	}
	if resp.GetTruncated() {
		t.Error("a 2-entry directory reported as truncated")
	}

	// Subdirectory.
	sub, err := s.ListVolumeFiles(context.Background(), listReq("/data"))
	if err != nil {
		t.Fatalf("list subdir: %v", err)
	}
	if len(sub.GetFiles()) != 1 || sub.GetFiles()[0].GetName() != "dump.sql" {
		t.Fatalf("subdir listing = %+v", sub.GetFiles())
	}

	// Missing directory is NotFound, not Internal.
	if _, err := s.ListVolumeFiles(context.Background(), listReq("/nope")); status.Code(err) != codes.NotFound {
		t.Errorf("missing dir error = %v, want NotFound", err)
	}
}

// TestListVolumeFilesTruncates checks the entry cap and that it is reported
// rather than silently applied.
func TestListVolumeFilesTruncates(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < maxListEntries+10; i++ {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("f%05d.txt", i)), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	s := &LocalServer{rt: newFileRuntime(root)}
	resp, err := s.ListVolumeFiles(context.Background(), listReq("/"))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(resp.GetFiles()) != maxListEntries {
		t.Fatalf("returned %d entries, want the %d cap", len(resp.GetFiles()), maxListEntries)
	}
	if !resp.GetTruncated() {
		t.Error("capped listing must set truncated — otherwise it reads as a complete directory")
	}
}

// TestVolumeFilesRejectsTraversal is the security case: a caller-supplied path
// must not reach outside the volume, whether lexically or through a symlink
// that workload code planted inside it.
func TestVolumeFilesRejectsTraversal(t *testing.T) {
	s, vol, outside := volumeFixture(t)
	ctx := context.Background()

	t.Run("lexical", func(t *testing.T) {
		for _, p := range []string{"../outside", "../../etc", "/../outside", "data/../../outside"} {
			resp, err := s.ListVolumeFiles(ctx, listReq(p))
			if err == nil {
				for _, f := range resp.GetFiles() {
					if f.GetName() == "secret.txt" {
						t.Fatalf("path %q escaped the volume: %+v", p, resp.GetFiles())
					}
				}
			}
		}
		// Reading straight out is refused too.
		if _, err := readAll(t, s, "../outside/secret.txt"); err == nil {
			t.Fatal("read escaped the volume via ..")
		}
	})

	t.Run("symlink", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("symlinks need privileges on windows")
		}
		link := filepath.Join(vol, "escape")
		if err := os.Symlink(filepath.Join(outside, "secret.txt"), link); err != nil {
			t.Skipf("symlink unsupported: %v", err)
		}
		data, err := readAll(t, s, "/escape")
		if err == nil {
			t.Fatalf("symlink out of the volume was readable: %q", data)
		}
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("symlink escape error = %v, want InvalidArgument", err)
		}

		// A symlink to a directory outside must not be listable either.
		dirLink := filepath.Join(vol, "escape-dir")
		if err := os.Symlink(outside, dirLink); err != nil {
			t.Skipf("symlink unsupported: %v", err)
		}
		if _, err := s.ListVolumeFiles(ctx, listReq("/escape-dir")); err == nil {
			t.Fatal("listed a directory symlinked out of the volume")
		}
	})
}

// TestReadVolumeFile covers the download path and its refusals.
func TestReadVolumeFile(t *testing.T) {
	s, _, _ := volumeFixture(t)

	data, err := readAll(t, s, "/data/dump.sql")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(data) != "SELECT 1;" {
		t.Fatalf("contents = %q", data)
	}

	// A directory is not a download.
	if _, err := readAll(t, s, "/data"); status.Code(err) != codes.InvalidArgument {
		t.Errorf("reading a directory = %v, want InvalidArgument", err)
	}
	// A missing file is NotFound.
	if _, err := readAll(t, s, "/nope.txt"); status.Code(err) != codes.NotFound {
		t.Errorf("missing file = %v, want NotFound", err)
	}
}

// TestReadVolumeFileChunks verifies a file larger than one chunk arrives whole.
func TestReadVolumeFileChunks(t *testing.T) {
	root := t.TempDir()
	big := make([]byte, readChunkSize*2+117)
	for i := range big {
		big[i] = byte(i % 251)
	}
	if err := os.WriteFile(filepath.Join(root, "big.bin"), big, 0o644); err != nil {
		t.Fatal(err)
	}
	s := &LocalServer{rt: newFileRuntime(root)}

	got, err := readAll(t, s, "/big.bin")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != len(big) {
		t.Fatalf("read %d bytes, want %d", len(got), len(big))
	}
	for i := range big {
		if got[i] != big[i] {
			t.Fatalf("byte %d differs", i)
		}
	}
}

// TestVolumeFilesNoRuntime keeps the RPCs Unimplemented on a node without a
// container runtime rather than panicking.
func TestVolumeFilesNoRuntime(t *testing.T) {
	s := &LocalServer{}
	if _, err := s.ListVolumeFiles(context.Background(), listReq("/")); status.Code(err) != codes.Unimplemented {
		t.Errorf("list without a runtime = %v, want Unimplemented", err)
	}
	sink := &chunkSink{ctx: context.Background()}
	if err := s.ReadVolumeFile(readReq("/x"), sink); status.Code(err) != codes.Unimplemented {
		t.Errorf("read without a runtime = %v, want Unimplemented", err)
	}
}

func readAll(t *testing.T, s *LocalServer, path string) ([]byte, error) {
	t.Helper()
	sink := &chunkSink{ctx: context.Background()}
	err := s.ReadVolumeFile(readReq(path), sink)
	return sink.data, err
}
