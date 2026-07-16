package scheduler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/leaderrunner"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// builderLostTimeout is how long the dispatcher waits for the next build event
// before giving up on a builder and failing the build. It must comfortably
// exceed a cold buildkitd provision (image pull + boot), during which the
// builder emits periodic progress heartbeats.
const builderLostTimeout = 4 * time.Minute

// buildLogRingSize caps the in-memory per-build log ring kept on the control
// node until durable log storage lands (T-40).
const buildLogRingSize = 500

// errBuilderLost is returned when a builder stops emitting events mid-build.
var errBuilderLost = errors.New("scheduler: builder lost")

// BuildStream is the receive side of an AgentLocalService.RunBuild stream.
type BuildStream interface {
	Recv() (*clusterv1.BuildEvent, error)
}

// BuildDialer opens a RunBuild stream to a builder node's AgentLocalService
// (over the mesh in production; a fake in tests).
type BuildDialer interface {
	RunBuild(ctx context.Context, node *zatterav1.Node, req *clusterv1.RunBuildRequest) (BuildStream, error)
}

// GRPCBuildDialer is the production BuildDialer: it dials a builder node's
// AgentLocalService over the mesh with node mTLS. Connect supplies the
// per-node client connection (node cert + mesh address); it stays injectable so
// the daemon owns the transport details.
type GRPCBuildDialer struct {
	Connect func(ctx context.Context, node *zatterav1.Node) (*grpc.ClientConn, error)
}

// RunBuild opens the stream, keeping the connection alive for its lifetime.
func (g GRPCBuildDialer) RunBuild(ctx context.Context, node *zatterav1.Node, req *clusterv1.RunBuildRequest) (BuildStream, error) {
	conn, err := g.Connect(ctx, node)
	if err != nil {
		return nil, err
	}
	stream, err := clusterv1.NewAgentLocalServiceClient(conn).RunBuild(ctx, req)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &grpcBuildStream{stream: stream, conn: conn}, nil
}

type grpcBuildStream struct {
	stream grpc.ServerStreamingClient[clusterv1.BuildEvent]
	conn   *grpc.ClientConn
}

func (b *grpcBuildStream) Recv() (*clusterv1.BuildEvent, error) {
	ev, err := b.stream.Recv()
	if err != nil { // EOF or failure ends the stream; release the connection
		_ = b.conn.Close()
	}
	return ev, err
}

// BuildDispatcherConfig carries the addresses the dispatcher stamps into build
// requests: where the builder fetches source and where it pushes the image.
type BuildDispatcherConfig struct {
	// SourceURLBase is the control-plane blob endpoint prefix, e.g.
	// "https://10.90.0.1:8443/internal/blobs/"; the tarball digest is appended.
	SourceURLBase string
	// RegistryAddr is the embedded registry "host:port" images push to.
	RegistryAddr string
	// LocalLoad means builders load images into the local Docker store instead
	// of pushing to the registry (single-node/dev, T-54); deploy by tag, not by
	// registry digest, since a loaded image has no registry digest.
	LocalLoad bool
}

// BuildDispatcher assigns QUEUED builds to builder nodes, streams their events,
// and records the outcome durably. It runs on the leader.
type BuildDispatcher struct {
	store *raftstore.Store
	clk   clock.Clock
	log   *slog.Logger
	dial  BuildDialer
	cfg   BuildDispatcherConfig

	lostTimeout time.Duration

	mu       sync.Mutex
	inflight map[string]bool
	rings    map[string][]string
}

// NewBuildDispatcher constructs the dispatcher.
func NewBuildDispatcher(store *raftstore.Store, clk clock.Clock, dial BuildDialer, cfg BuildDispatcherConfig, log *slog.Logger) *BuildDispatcher {
	if log == nil {
		log = slog.Default()
	}
	if clk == nil {
		clk = clock.Real{}
	}
	return &BuildDispatcher{
		store: store, clk: clk, log: log, dial: dial, cfg: cfg,
		lostTimeout: builderLostTimeout,
		inflight:    map[string]bool{}, rings: map[string][]string{},
	}
}

// SetBuilderTimeout overrides the no-event builder-lost timeout (tests use a
// short window; production keeps the default).
func (d *BuildDispatcher) SetBuilderTimeout(t time.Duration) { d.lostTimeout = t }

// Run dispatches builds while this node leads, resuming on re-election.
func (d *BuildDispatcher) Run(ctx context.Context) {
	leaderrunner.Run(ctx, d.store, d.clk, d.leaderLoop)
}

func (d *BuildDispatcher) leaderLoop(ctx context.Context) {
	sub := d.store.State().Watch(state.KindBuild, state.KindNode)
	defer sub.Close()
	tick := d.clk.NewTicker(15 * time.Second)
	defer tick.Stop()
	for {
		d.reconcile(ctx)
		select {
		case <-ctx.Done():
			return
		case <-d.store.LeaderCh():
			if !d.store.IsLeader() {
				return
			}
		case <-sub.Notify():
			sub.Drain()
		case <-tick.C():
		}
	}
}

// reconcile dispatches every QUEUED build not already in flight. Exported for
// tests to drive a single pass.
func (d *BuildDispatcher) reconcile(ctx context.Context) {
	if !d.store.IsLeader() {
		return
	}
	for _, b := range d.store.State().ListBuilds("") {
		if b.GetStatus() != zatterav1.BuildStatus_BUILD_STATUS_QUEUED {
			continue
		}
		if !d.claim(b.GetMeta().GetId()) {
			continue
		}
		go d.dispatch(ctx, b)
	}
}

// claim marks a build as in-flight; returns false if already claimed.
func (d *BuildDispatcher) claim(id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.inflight[id] {
		return false
	}
	d.inflight[id] = true
	return true
}

func (d *BuildDispatcher) release(id string) {
	d.mu.Lock()
	delete(d.inflight, id)
	d.mu.Unlock()
}

// dispatch runs one build to completion against a chosen builder node.
func (d *BuildDispatcher) dispatch(ctx context.Context, b *zatterav1.Build) {
	defer d.release(b.GetMeta().GetId())

	node, ok := d.pickBuilder()
	if !ok {
		// No builder available right now; leave QUEUED for a later pass.
		d.log.Warn("no builder node available", "build", b.GetMeta().GetId())
		return
	}

	// Mark RUNNING (durable) before streaming so a failover can observe it.
	b.Status = zatterav1.BuildStatus_BUILD_STATUS_RUNNING
	b.NodeId = node.GetMeta().GetId()
	b.StartedAt = timestamppb.New(d.clk.Now())
	if err := d.putBuild(ctx, b); err != nil {
		return
	}

	// OCI repository names must be lowercase; project/app ids are ULIDs
	// (uppercase Crockford base32), so lowercase the repo path. The tag (build
	// id) may keep its case.
	repo := strings.ToLower(fmt.Sprintf("%s/%s/%s", d.cfg.RegistryAddr, b.GetProjectId(), b.GetAppId()))
	req := &clusterv1.RunBuildRequest{
		Build:        b,
		SourceUrl:    d.cfg.SourceURLBase + b.GetTarballDigest(),
		PushImageRef: repo + ":" + strings.ToLower(b.GetMeta().GetId()),
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := d.dial.RunBuild(runCtx, node, req)
	if err != nil {
		d.fail(ctx, b, "dispatch: "+err.Error())
		return
	}

	var digest string
	var builtPlatforms []string
	for {
		ev, err := d.recv(runCtx, stream)
		if errors.Is(err, errBuilderLost) {
			cancel()
			d.fail(ctx, b, "builder lost")
			return
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			d.fail(ctx, b, "build stream: "+err.Error())
			return
		}
		if line := ev.GetLog().GetLine(); line != "" {
			d.appendLog(b.GetMeta().GetId(), line)
		}
		if ev.GetImageDigest() != "" {
			digest = ev.GetImageDigest()
		}
		if len(ev.GetPlatforms()) > 0 {
			builtPlatforms = ev.GetPlatforms()
		}
		if ev.GetError() != "" {
			d.fail(ctx, b, ev.GetError())
			return
		}
		if ev.GetStatus() == zatterav1.BuildStatus_BUILD_STATUS_FAILED {
			d.fail(ctx, b, "build failed")
			return
		}
	}

	if digest == "" {
		d.fail(ctx, b, "build ended without producing an image")
		return
	}
	// Deploy the digest-pinned reference so the exact index/manifest is run.
	// With local-load there is no registry digest, so deploy the tag ref (the
	// image was loaded into the node's Docker store under exactly this tag).
	deployRef := repo + "@" + digest
	if d.cfg.LocalLoad {
		deployRef = req.PushImageRef
	}
	// The builder reports what it actually built: an empty requested list
	// resolves to its native platform, so the release records the real arch.
	if len(builtPlatforms) > 0 {
		b.Platforms = builtPlatforms
	}
	d.succeed(ctx, b, deployRef)
}

// recv reads the next event, treating a silence longer than builderLostTimeout
// as a lost builder.
func (d *BuildDispatcher) recv(ctx context.Context, stream BuildStream) (*clusterv1.BuildEvent, error) {
	type result struct {
		ev  *clusterv1.BuildEvent
		err error
	}
	ch := make(chan result, 1)
	go func() {
		ev, err := stream.Recv()
		ch <- result{ev, err}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-d.clk.After(d.lostTimeout):
		return nil, errBuilderLost
	case r := <-ch:
		return r.ev, r.err
	}
}

// pickBuilder returns the lowest-id ALIVE node labelled builder=true.
func (d *BuildDispatcher) pickBuilder() (*zatterav1.Node, bool) {
	var builders []*zatterav1.Node
	for _, n := range d.store.State().ListNodes() {
		if n.GetStatus() != zatterav1.NodeStatus_NODE_STATUS_ALIVE {
			continue
		}
		if n.GetLabels()["builder"] == "true" {
			builders = append(builders, n)
		}
	}
	if len(builders) == 0 {
		return nil, false
	}
	sort.Slice(builders, func(i, j int) bool {
		return builders[i].GetMeta().GetId() < builders[j].GetMeta().GetId()
	})
	return builders[0], true
}

func (d *BuildDispatcher) fail(ctx context.Context, b *zatterav1.Build, msg string) {
	b.Status = zatterav1.BuildStatus_BUILD_STATUS_FAILED
	b.Error = msg
	b.FinishedAt = timestamppb.New(d.clk.Now())
	d.log.Warn("build failed", "build", b.GetMeta().GetId(), "err", msg)
	_ = d.putBuild(ctx, b)
}

func (d *BuildDispatcher) succeed(ctx context.Context, b *zatterav1.Build, imageRef string) {
	b.Status = zatterav1.BuildStatus_BUILD_STATUS_SUCCEEDED
	b.ImageRef = imageRef
	b.FinishedAt = timestamppb.New(d.clk.Now())
	d.log.Info("build succeeded", "build", b.GetMeta().GetId(), "image", imageRef)
	_ = d.putBuild(ctx, b)
}

func (d *BuildDispatcher) putBuild(ctx context.Context, b *zatterav1.Build) error {
	return d.apply(ctx, &clusterv1.Command{Mutation: &clusterv1.Command_PutBuild{PutBuild: &clusterv1.PutBuild{Build: b}}})
}

func (d *BuildDispatcher) apply(ctx context.Context, cmd *clusterv1.Command) error {
	cmd.RequestId = ids.New()
	cmd.Actor = "system:builder"
	cmd.Time = timestamppb.New(d.clk.Now())
	err := d.store.Apply(ctx, cmd)
	if err != nil && !errors.Is(err, raftstore.ErrNotLeader) {
		d.log.Warn("build apply failed", "err", err)
	}
	return err
}

// appendLog appends a build log line to the in-memory ring (T-40 makes this
// durable via the log store).
func (d *BuildDispatcher) appendLog(id, line string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	r := d.rings[id]
	r = append(r, line)
	if len(r) > buildLogRingSize {
		r = r[len(r)-buildLogRingSize:]
	}
	d.rings[id] = r
}

// BuildLog returns the buffered log lines for a build (best-effort, in-memory).
func (d *BuildDispatcher) BuildLog(id string) []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.rings[id]...)
}
