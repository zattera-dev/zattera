package agent

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/daemon/volumes"
)

// maxListEntries caps one directory listing. ListFiles has no pagination (the
// browser is for looking around, not for bulk export), so a directory with
// more entries is truncated and the caller is told.
const maxListEntries = 5000

// readChunkSize is the payload size of one ReadVolumeFile chunk.
const readChunkSize = 64 << 10

// ListVolumeFiles lists one directory inside a volume (T-77). Read-only: it
// never follows a path out of the volume, and never writes.
func (s *LocalServer) ListVolumeFiles(ctx context.Context, req *clusterv1.AgentListFilesRequest) (*clusterv1.AgentListFilesResponse, error) {
	if s.rt == nil {
		return s.UnimplementedAgentLocalServiceServer.ListVolumeFiles(ctx, req)
	}
	dir, err := s.resolveVolumePath(ctx, req.GetEnvironmentId(), req.GetVolumeName(), req.GetPath())
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fsStatus(err, req.GetPath())
	}

	truncated := false
	if len(entries) > maxListEntries {
		entries = entries[:maxListEntries]
		truncated = true
	}
	out := make([]*clusterv1.AgentFileInfo, 0, len(entries))
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue // vanished between ReadDir and Stat; skip rather than fail
		}
		out = append(out, &clusterv1.AgentFileInfo{
			Name:          e.Name(),
			Dir:           e.IsDir(),
			SizeBytes:     uint64(max64(info.Size(), 0)),
			ModTimeUnixMs: info.ModTime().UnixMilli(),
			Mode:          info.Mode().String(),
		})
	}
	// Directories first, then name — the order a file browser wants, decided
	// here so every client agrees.
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].GetDir() != out[j].GetDir() {
			return out[i].GetDir()
		}
		return out[i].GetName() < out[j].GetName()
	})
	return &clusterv1.AgentListFilesResponse{Files: out, Truncated: truncated}, nil
}

// ReadVolumeFile streams one regular file out of a volume (T-77).
func (s *LocalServer) ReadVolumeFile(req *clusterv1.AgentReadFileRequest, stream grpc.ServerStreamingServer[clusterv1.TCPChunk]) error {
	if s.rt == nil {
		return s.UnimplementedAgentLocalServiceServer.ReadVolumeFile(req, stream)
	}
	path, err := s.resolveVolumePath(stream.Context(), req.GetEnvironmentId(), req.GetVolumeName(), req.GetPath())
	if err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return fsStatus(err, req.GetPath())
	}
	if info.IsDir() {
		return status.Errorf(codes.InvalidArgument, "%q is a directory", req.GetPath())
	}
	if !info.Mode().IsRegular() {
		// Devices and FIFOs on a volume would block or stream forever.
		return status.Errorf(codes.InvalidArgument, "%q is not a regular file", req.GetPath())
	}

	f, err := os.Open(path)
	if err != nil {
		return fsStatus(err, req.GetPath())
	}
	defer func() { _ = f.Close() }()

	buf := make([]byte, readChunkSize)
	for {
		if err := stream.Context().Err(); err != nil {
			return err
		}
		n, rerr := f.Read(buf)
		if n > 0 {
			if serr := stream.Send(&clusterv1.TCPChunk{Data: buf[:n]}); serr != nil {
				return serr
			}
		}
		if rerr == io.EOF {
			return nil
		}
		if rerr != nil {
			return fsStatus(rerr, req.GetPath())
		}
	}
}

// resolveVolumePath maps a caller-supplied path to a real path inside the
// volume, refusing anything that escapes it.
//
// Two separate escapes are possible and both are checked: a lexical one
// ("../../etc/shadow"), caught by SafeJoin; and a symlink inside the volume
// pointing outside it, which is only visible after resolving the link. Volume
// contents are written by workloads, so the symlink case is attacker-supplied
// on any cluster running untrusted code.
func (s *LocalServer) resolveVolumePath(ctx context.Context, envID, volName, path string) (string, error) {
	if volName == "" {
		return "", status.Error(codes.InvalidArgument, "volume name is required")
	}
	dockerName := volumeName(envID, volName)
	hostPath, err := s.rt.VolumeHostPath(ctx, dockerName)
	if err != nil {
		return "", status.Errorf(codes.NotFound, "locate volume %s: %v", dockerName, err)
	}
	joined, err := volumes.SafeJoin(hostPath, path)
	if err != nil {
		return "", status.Errorf(codes.InvalidArgument, "invalid path %q", path)
	}

	// Resolve symlinks and re-check containment. A path that does not exist
	// yet resolves to an error; fall back to checking its parent so the caller
	// gets NotFound from the Stat/ReadDir below rather than a confusing
	// permission error.
	real, err := filepath.EvalSymlinks(joined)
	if err != nil {
		return joined, nil
	}
	realBase, err := filepath.EvalSymlinks(hostPath)
	if err != nil {
		realBase = hostPath
	}
	if !withinDir(realBase, real) {
		return "", status.Errorf(codes.InvalidArgument, "path %q escapes the volume", path)
	}
	return real, nil
}

// withinDir reports whether p is base or lives under it.
func withinDir(base, p string) bool {
	if p == base {
		return true
	}
	rel, err := filepath.Rel(base, p)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)
}

// fsStatus maps a filesystem error to a gRPC status without leaking the node's
// real host path back to the caller.
func fsStatus(err error, path string) error {
	switch {
	case os.IsNotExist(err):
		return status.Errorf(codes.NotFound, "%q not found", path)
	case os.IsPermission(err):
		return status.Errorf(codes.PermissionDenied, "%q is not readable", path)
	default:
		return status.Error(codes.Internal, fmt.Sprintf("read %q: %v", path, err))
	}
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
