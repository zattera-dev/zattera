package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
)

// session runs one connection lifecycle: dial, open the stream, hello +
// heartbeats from a single sender goroutine, and receive pushed messages here
// until the stream ends. Returns nil on a clean close (ctx canceled / EOF).
func (a *Agent) session(ctx context.Context) error {
	conn, err := a.cfg.Dial(ctx)
	if err != nil {
		return fmt.Errorf("agent: dial control: %w", err)
	}
	defer func() { _ = conn.Close() }()

	sctx, cancel := context.WithCancel(ctx)
	defer cancel()

	client := clusterv1.NewAgentSyncServiceClient(conn)
	stream, err := client.Sync(sctx)
	if err != nil {
		return fmt.Errorf("agent: open sync: %w", err)
	}

	// gRPC allows one goroutine sending while another receives, but not two
	// concurrent senders — so ALL sends (hello, heartbeats, acks) funnel through
	// one sender goroutine via sendCh.
	sendCh := make(chan *clusterv1.AgentMessage, 8)
	go a.runSender(sctx, cancel, stream, sendCh)

	for {
		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || sctx.Err() != nil {
				return nil
			}
			return err
		}
		switch b := msg.Body.(type) {
		case *clusterv1.ControlMessage_Assignments:
			a.applyAssignments(b.Assignments)
			// Best-effort ack; drop if the sender is busy (control re-pushes).
			select {
			case sendCh <- ackMessage(b.Assignments.GetVersion()):
			default:
			}
		case *clusterv1.ControlMessage_Activate:
			a.log.Info("activate requested", "env", b.Activate.GetEnvironmentId())
		}
	}
}

// runSender owns the write side of the stream: it sends the hello first, then a
// heartbeat on every tick and any queued messages. A send failure cancels the
// session so the receive loop unblocks and the outer loop reconnects.
func (a *Agent) runSender(ctx context.Context, cancel context.CancelFunc, stream clusterv1.AgentSyncService_SyncClient, sendCh <-chan *clusterv1.AgentMessage) {
	hello := &clusterv1.AgentMessage{Body: &clusterv1.AgentMessage_Hello{Hello: &clusterv1.AgentHello{
		NodeId:            a.cfg.NodeID,
		BinaryVersion:     a.cfg.Version,
		AssignmentVersion: a.version(),
	}}}
	if err := stream.Send(hello); err != nil {
		a.log.Warn("agent hello send failed", "node", a.cfg.NodeID, "err", err)
		cancel()
		return
	}

	tick := a.clock.NewTicker(a.cfg.HeartbeatInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C():
			if err := stream.Send(a.heartbeat()); err != nil {
				a.log.Warn("agent heartbeat send failed", "node", a.cfg.NodeID, "err", err)
				cancel()
				return
			}
		case batch := <-a.statusCh:
			if err := stream.Send(&clusterv1.AgentMessage{Body: &clusterv1.AgentMessage_Status{Status: batch}}); err != nil {
				cancel()
				return
			}
		case msg := <-sendCh:
			if err := stream.Send(msg); err != nil {
				cancel()
				return
			}
		}
	}
}

// heartbeat builds a heartbeat message from the current host sample plus the
// latest per-instance/per-env samples the metrics sampler published (T-61).
func (a *Agent) heartbeat() *clusterv1.AgentMessage {
	s := a.cfg.Sample()
	instances, proxy := a.liveSamples()
	return &clusterv1.AgentMessage{Body: &clusterv1.AgentMessage_Heartbeat{Heartbeat: &clusterv1.Heartbeat{
		Time:             timestamppb.New(a.clock.Now()),
		CpuPercent:       s.CPUPercent,
		MemoryUsedBytes:  s.MemoryUsedBytes,
		MemoryTotalBytes: s.MemoryTotalBytes,
		DiskUsedBytes:    s.DiskUsedBytes,
		DiskTotalBytes:   s.DiskTotalBytes,
		Instances:        instances,
		Proxy:            proxy,
	}}}
}

func ackMessage(version uint64) *clusterv1.AgentMessage {
	return &clusterv1.AgentMessage{Body: &clusterv1.AgentMessage_Ack{Ack: &clusterv1.AssignmentsAck{Version: version}}}
}

// gopsutilSampler probes CPU, memory and disk. Any failing probe degrades that
// dimension to zero rather than failing the heartbeat.
func gopsutilSampler(diskPath string, log *slog.Logger) SampleFunc {
	return func() HostSample {
		var s HostSample
		// interval 0 → usage since the previous call (non-blocking).
		if pct, err := cpu.Percent(0, false); err == nil && len(pct) > 0 {
			s.CPUPercent = pct[0]
		} else if err != nil {
			log.Debug("cpu sample failed", "err", err)
		}
		if vm, err := mem.VirtualMemory(); err == nil {
			s.MemoryUsedBytes = vm.Used
			s.MemoryTotalBytes = vm.Total
		} else {
			log.Debug("memory sample failed", "err", err)
		}
		if du, err := disk.Usage(diskPath); err == nil {
			s.DiskUsedBytes = du.Used
			s.DiskTotalBytes = du.Total
		} else {
			log.Debug("disk sample failed", "err", err, "path", diskPath)
		}
		return s
	}
}
