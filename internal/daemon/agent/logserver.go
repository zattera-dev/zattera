package agent

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/logstore"
)

// StreamResolver maps a log selector to the node-local stream ids (assignment
// ids) it should read, using the metadata the agent already holds.
type StreamResolver func(sel *zatterav1.LogSelector) []logstore.StreamID

// LogServer implements AgentLocalService.QueryLogs from the node-local logstore
// (T-54). The control plane's LogService fans this out across nodes and merges.
type LogServer struct {
	store   *logstore.Segmented
	resolve StreamResolver
}

// NewLogServer builds the agent log query server.
func NewLogServer(store *logstore.Segmented, resolve StreamResolver) *LogServer {
	return &LogServer{store: store, resolve: resolve}
}

// QueryLogs streams stored (and, when follow is set, live) log lines for the
// selector's local streams.
func (s *LogServer) QueryLogs(q *zatterav1.LogQuery, stream grpc.ServerStreamingServer[zatterav1.LogLine]) error {
	streams := s.resolve(q.GetSelector())
	if len(streams) == 0 {
		return nil
	}
	ctx := stream.Context()

	lq := logstore.Query{Streams: streams, Limit: int(q.GetLimit())}
	if q.GetSince() != nil {
		lq.Since = q.GetSince().AsTime()
	}
	if q.GetUntil() != nil {
		lq.Until = q.GetUntil().AsTime()
	}
	entries, err := s.store.Query(ctx, lq)
	if err != nil {
		return err
	}
	// The stored entries do not carry the instance id, so tag every line for a
	// single-stream query; multi-stream merge is the control plane's job.
	inst := ""
	if len(streams) == 1 {
		inst = string(streams[0])
	}
	for _, e := range entries {
		if err := stream.Send(toLogLine(e, inst)); err != nil {
			return err
		}
	}
	if !q.GetFollow() {
		return nil
	}

	ch, err := s.store.Follow(ctx, logstore.Query{Streams: streams, Since: time.Now()})
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case e, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(toLogLine(e, inst)); err != nil {
				return err
			}
		}
	}
}

func toLogLine(e logstore.Entry, instanceID string) *zatterav1.LogLine {
	src := zatterav1.LogSource_LOG_SOURCE_STDOUT
	if e.Stderr {
		src = zatterav1.LogSource_LOG_SOURCE_STDERR
	}
	return &zatterav1.LogLine{
		Time:       timestamppb.New(e.Time),
		InstanceId: instanceID,
		Source:     src,
		Line:       e.Line,
	}
}
