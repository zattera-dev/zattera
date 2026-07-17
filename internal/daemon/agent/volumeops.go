package agent

import (
	"context"
	"fmt"
	"io"
	"strings"

	"google.golang.org/grpc"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/daemon/runtime"
	"github.com/zattera-dev/zattera/internal/daemon/volumes"
)

// SnapshotVolume snapshots the named volume's host path to S3 (T-65). It
// optionally runs a pre-hook command inside the mounting container (e.g.
// pg_dump) first, then chunks + dedups + encrypts via the snapshot engine,
// streaming progress. The final message carries the manifest key.
func (s *LocalServer) SnapshotVolume(req *clusterv1.AgentSnapshotVolumeRequest, stream grpc.ServerStreamingServer[clusterv1.VolumeOpProgress]) error {
	if s.rt == nil {
		return s.UnimplementedAgentLocalServiceServer.SnapshotVolume(req, stream)
	}
	ctx := stream.Context()
	vol := req.GetVolume()
	dockerName := volumeName(vol.GetEnvironmentId(), vol.GetName())
	hostPath, err := s.rt.VolumeHostPath(ctx, dockerName)
	if err != nil {
		return sendOpErr(stream, fmt.Errorf("locate volume %s: %w", dockerName, err))
	}

	if cmd := req.GetPreHookCommand(); cmd != "" && req.GetPreHookContainerId() != "" {
		_ = stream.Send(&clusterv1.VolumeOpProgress{Phase: "hooks"})
		if err := s.runPreHook(ctx, req.GetPreHookContainerId(), cmd); err != nil {
			return sendOpErr(stream, err)
		}
	}

	eng, err := engineFromTarget(req.GetS3())
	if err != nil {
		return sendOpErr(stream, err)
	}
	snapID := req.GetSnapshot().GetMeta().GetId()
	createdAt := req.GetSnapshot().GetMeta().GetCreatedAt().AsTime().Unix()

	_ = stream.Send(&clusterv1.VolumeOpProgress{Phase: "upload"})
	m, err := eng.Snapshot(ctx, hostPath, snapID, createdAt, func(done int64) {
		_ = stream.Send(&clusterv1.VolumeOpProgress{Phase: "upload", BytesDone: uint64(done)})
	})
	if err != nil {
		return sendOpErr(stream, err)
	}
	return stream.Send(&clusterv1.VolumeOpProgress{
		Phase: "done", ManifestKey: snapID, BytesDone: uint64(m.TarBytes), BytesTotal: uint64(m.TarBytes),
	})
}

// RestoreVolume restores a snapshot into the volume's host path. The control
// plane guarantees the service is stopped before dispatching (single-writer).
func (s *LocalServer) RestoreVolume(req *clusterv1.AgentRestoreVolumeRequest, stream grpc.ServerStreamingServer[clusterv1.VolumeOpProgress]) error {
	if s.rt == nil {
		return s.UnimplementedAgentLocalServiceServer.RestoreVolume(req, stream)
	}
	ctx := stream.Context()
	vol := req.GetVolume()
	dockerName := volumeName(vol.GetEnvironmentId(), vol.GetName())
	if err := s.rt.EnsureVolume(ctx, dockerName, map[string]string{runtime.ManagedLabel: "true"}); err != nil {
		return sendOpErr(stream, fmt.Errorf("ensure volume %s: %w", dockerName, err))
	}
	hostPath, err := s.rt.VolumeHostPath(ctx, dockerName)
	if err != nil {
		return sendOpErr(stream, fmt.Errorf("locate volume %s: %w", dockerName, err))
	}
	eng, err := engineFromTarget(req.GetS3())
	if err != nil {
		return sendOpErr(stream, err)
	}
	_ = stream.Send(&clusterv1.VolumeOpProgress{Phase: "download"})
	if err := eng.Restore(ctx, req.GetManifestKey(), hostPath); err != nil {
		return sendOpErr(stream, err)
	}
	return stream.Send(&clusterv1.VolumeOpProgress{Phase: "done"})
}

// runPreHook execs a shell command in the mounting container and fails on a
// non-zero exit (so a broken pre-hook aborts the snapshot).
func (s *LocalServer) runPreHook(ctx context.Context, containerID, command string) error {
	code, err := s.rt.Exec(ctx, containerID, runtime.ExecSpec{Command: []string{"/bin/sh", "-c", command}},
		nil, io.Discard, io.Discard, nil)
	if err != nil {
		return fmt.Errorf("pre-hook exec: %w", err)
	}
	if code != 0 {
		return fmt.Errorf("pre-hook exited %d", code)
	}
	return nil
}

// engineFromTarget builds a snapshot Engine against the S3 target the control
// plane supplied (creds + data key over the mTLS hop).
func engineFromTarget(t *clusterv1.S3Target) (*volumes.Engine, error) {
	if t == nil {
		return nil, fmt.Errorf("no s3 target provided")
	}
	endpoint, useSSL := parseEndpoint(t.GetEndpoint())
	store, err := volumes.NewS3Store(volumes.S3Config{
		Endpoint:  endpoint,
		Region:    t.GetRegion(),
		Bucket:    t.GetBucket(),
		Prefix:    t.GetPrefix(),
		AccessKey: t.GetAccessKey(),
		SecretKey: t.GetSecretKey(),
		UseSSL:    useSSL,
	})
	if err != nil {
		return nil, err
	}
	return volumes.NewEngine(store, t.GetDataKey(), volumes.Options{})
}

// parseEndpoint strips an optional scheme and reports whether TLS is used
// (default true when no scheme is given, as for AWS S3).
func parseEndpoint(ep string) (host string, useSSL bool) {
	switch {
	case strings.HasPrefix(ep, "https://"):
		return strings.TrimPrefix(ep, "https://"), true
	case strings.HasPrefix(ep, "http://"):
		return strings.TrimPrefix(ep, "http://"), false
	default:
		return ep, true
	}
}

func sendOpErr(stream grpc.ServerStreamingServer[clusterv1.VolumeOpProgress], err error) error {
	_ = stream.Send(&clusterv1.VolumeOpProgress{Phase: "error", Error: err.Error()})
	return err
}
