package api

import (
	"context"
	"io"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// fakeLogStream replays a fixed set of lines then io.EOF.
type fakeLogStream struct {
	lines []*zatterav1.LogLine
	i     int
}

func (f *fakeLogStream) Recv() (*zatterav1.LogLine, error) {
	if f.i >= len(f.lines) {
		return nil, io.EOF
	}
	l := f.lines[f.i]
	f.i++
	return l, nil
}

// fakeLogDialer returns per-node streams; nodes in dead yield a dial error.
type fakeLogDialer struct {
	streams map[string][]*zatterav1.LogLine
	dead    map[string]bool
}

func (d *fakeLogDialer) QueryLogs(_ context.Context, node *zatterav1.Node, _ *zatterav1.LogQuery) (LogStream, error) {
	id := node.GetMeta().GetId()
	if d.dead[id] {
		return nil, context.DeadlineExceeded
	}
	return &fakeLogStream{lines: d.streams[id]}, nil
}

// collectSink is a minimal ServerStreamingServer capturing sent lines.
type collectSink struct {
	grpc.ServerStream
	ctx  context.Context
	sent []*zatterav1.LogLine
}

func (s *collectSink) Send(l *zatterav1.LogLine) error { s.sent = append(s.sent, l); return nil }
func (s *collectSink) Context() context.Context        { return s.ctx }

func logLine(node string, sec int64, text string) *zatterav1.LogLine {
	return &zatterav1.LogLine{Time: timestamppb.New(time.Unix(sec, 0)), InstanceId: node, Line: text}
}

func seedLogNodes(rs *raftstore.Store, ids ...string) {
	st := rs.State()
	for _, id := range ids {
		st.PutNode(&zatterav1.Node{Meta: &zatterav1.Meta{Id: id}, Status: zatterav1.NodeStatus_NODE_STATUS_ALIVE})
		// One RUN assignment per node so the selector resolves to it.
		st.PutAssignment(&zatterav1.Assignment{
			Meta: &zatterav1.Meta{Id: "asg-" + id}, EnvironmentId: "env1", ProjectId: "proj", AppId: "app", NodeId: id,
			Desired: zatterav1.AssignmentDesired_ASSIGNMENT_DESIRED_RUN,
		})
	}
}

func TestLogFanoutMergeOrders(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	seedLogNodes(rs, "n1", "n2", "n3")
	dialer := &fakeLogDialer{streams: map[string][]*zatterav1.LogLine{
		"n1": {logLine("n1", 1, "a"), logLine("n1", 4, "d")},
		"n2": {logLine("n2", 2, "b"), logLine("n2", 5, "e")},
		"n3": {logLine("n3", 3, "c"), logLine("n3", 6, "f")},
	}}
	srv := NewLogServer(rs.State(), dialer, clock.NewFake(), nil)

	sink := &collectSink{ctx: context.Background()}
	if err := srv.Query(&zatterav1.LogQuery{Selector: &zatterav1.LogSelector{EnvironmentId: "env1"}}, sink); err != nil {
		t.Fatal(err)
	}
	want := "abcdef"
	got := ""
	for _, l := range sink.sent {
		got += l.GetLine()
	}
	if got != want {
		t.Fatalf("merged order = %q, want %q", got, want)
	}
}

func TestLogFanoutDeadNodePartial(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	seedLogNodes(rs, "n1", "n2")
	dialer := &fakeLogDialer{
		streams: map[string][]*zatterav1.LogLine{"n1": {logLine("n1", 1, "a"), logLine("n1", 2, "b")}},
		dead:    map[string]bool{"n2": true},
	}
	srv := NewLogServer(rs.State(), dialer, clock.NewFake(), nil)

	sink := &collectSink{ctx: context.Background()}
	if err := srv.Query(&zatterav1.LogQuery{Selector: &zatterav1.LogSelector{EnvironmentId: "env1"}}, sink); err != nil {
		t.Fatal(err)
	}
	if len(sink.sent) != 2 {
		t.Fatalf("dead node should yield partial results, got %d lines", len(sink.sent))
	}
}

func TestLogFanoutSinceLimit(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	seedLogNodes(rs, "n1")
	var lines []*zatterav1.LogLine
	for i := int64(1); i <= 10; i++ {
		lines = append(lines, logLine("n1", i, string(rune('0'+i))))
	}
	dialer := &fakeLogDialer{streams: map[string][]*zatterav1.LogLine{"n1": lines}}
	srv := NewLogServer(rs.State(), dialer, clock.NewFake(), nil)

	// since=4 → drop 1..3; limit=3 → keep the most recent 3 (8,9,10).
	sink := &collectSink{ctx: context.Background()}
	q := &zatterav1.LogQuery{
		Selector: &zatterav1.LogSelector{EnvironmentId: "env1"},
		Since:    timestamppb.New(time.Unix(4, 0)),
		Limit:    3,
	}
	if err := srv.Query(q, sink); err != nil {
		t.Fatal(err)
	}
	if len(sink.sent) != 3 {
		t.Fatalf("limit not applied: got %d", len(sink.sent))
	}
	if sink.sent[0].GetTime().AsTime().Unix() != 8 || sink.sent[2].GetTime().AsTime().Unix() != 10 {
		t.Fatalf("since/limit window wrong: %d..%d", sink.sent[0].GetTime().AsTime().Unix(), sink.sent[2].GetTime().AsTime().Unix())
	}
}

func TestLogFanoutEnrichesNames(t *testing.T) {
	rs := raftstore.NewTestStore(t)
	st := rs.State()
	st.PutApp(&zatterav1.App{Meta: &zatterav1.Meta{Id: "app"}, ProjectId: "proj", Name: "api"})
	st.PutEnvironment(&zatterav1.Environment{Meta: &zatterav1.Meta{Id: "env1"}, ProjectId: "proj", AppId: "app", Name: "production"})
	seedLogNodes(rs, "n1")
	// Line's instance_id points at the assignment so names resolve.
	dialer := &fakeLogDialer{streams: map[string][]*zatterav1.LogLine{
		"n1": {{Time: timestamppb.New(time.Unix(1, 0)), InstanceId: "asg-n1", Line: "hi"}},
	}}
	srv := NewLogServer(st, dialer, clock.NewFake(), nil)

	sink := &collectSink{ctx: context.Background()}
	if err := srv.Query(&zatterav1.LogQuery{Selector: &zatterav1.LogSelector{EnvironmentId: "env1"}}, sink); err != nil {
		t.Fatal(err)
	}
	if len(sink.sent) != 1 || sink.sent[0].GetAppName() != "api" || sink.sent[0].GetEnvironmentName() != "production" {
		t.Fatalf("names not enriched: %+v", sink.sent)
	}
}
