package daemon

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"time"

	"google.golang.org/grpc"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	"github.com/zattera-dev/zattera/internal/config"
	"github.com/zattera-dev/zattera/internal/daemon/api"
	"github.com/zattera-dev/zattera/internal/daemon/ca"
	"github.com/zattera-dev/zattera/internal/daemon/mesh"
	"github.com/zattera-dev/zattera/internal/daemon/mesh/relay"
)

// relayListenPort is the control-node DERP-lite relay TCP port.
const relayListenPort = "7443"

// meshsockLabel marks a node as meshsock-capable (mirrors api.MeshsockLabel).
const meshsockLabel = api.MeshsockLabel

// startRelayServer runs the control-node TCP relay (T-58) on :7443 with the CA
// server cert, requiring node client certs. meshsock nodes fall back to it when
// no UDP path works. Best-effort: a listen failure is logged, not fatal.
func startRelayServer(ctx context.Context, authority *ca.CA, cfg config.Config, log *slog.Logger) {
	serverTLS, err := authority.ServerTLSConfig([]string{"localhost"}, serverIPs(controlMeshIP(cfg)))
	if err != nil {
		log.Warn("relay: server tls", "err", err)
		return
	}
	serverTLS.ClientAuth = tls.RequireAndVerifyClientCert
	lis, err := tls.Listen("tcp", ":"+relayListenPort, serverTLS)
	if err != nil {
		log.Warn("relay: listen", "err", err)
		return
	}
	srv := relay.NewServer(log)
	go func() {
		if err := srv.Serve(ctx, lis, relay.NodeIDFromURISANs); err != nil && ctx.Err() == nil {
			log.Warn("relay server stopped", "err", err)
		}
	}()
	log.Info("relay server listening", "addr", lis.Addr().String())
}

// meshsockSetup wires a worker's meshsock datapath: a MeshService client for
// punch coordination, a relay client to the control relay, and the background
// runners feeding the device's bind. It returns the MeshsockConfig to hand to
// mesh Up (nil when meshsock is disabled). dm must be the DeviceManager that
// will be brought up — its InjectRelayed/PunchNow are safe before Up (no-op
// until the bind exists).
func meshsockSetup(ctx context.Context, cfg config.Config, jr *joinResult, dm *mesh.DeviceManager, log *slog.Logger) (*mesh.MeshsockConfig, error) {
	if !cfg.Mesh.MeshsockEnabled() || !jr.MeshEnabled {
		return nil, nil
	}
	caHash, err := clusterCAHash(jr.caPEM)
	if err != nil {
		return nil, err
	}
	controlHost, _, err := net.SplitHostPort(jr.ControlGRPCAddr)
	if err != nil {
		controlHost = jr.ControlGRPCAddr
	}
	creds, err := workerControlCreds(jr, controlHost)
	if err != nil {
		return nil, err
	}

	// MeshService client for RequestPunch + PunchStream.
	conn, err := grpc.NewClient(jr.ControlGRPCAddr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("daemon: mesh client: %w", err)
	}
	meshClient := clusterv1.NewMeshServiceClient(conn)
	go runPunchStream(ctx, meshClient, jr.NodeID, dm.PunchNow)

	// Relay client to the control relay (dials the control's mesh IP:7443 over
	// the hub tunnel, node mTLS), injecting received packets into the bind.
	relayTLS, err := relayClientTLS(jr, controlHost)
	if err != nil {
		return nil, err
	}
	relayAddr := net.JoinHostPort(controlHost, relayListenPort)
	relayCli := relay.NewClient(relay.Config{
		NodeID:    jr.NodeID,
		Dial:      relay.DialTLS(relayAddr, relayTLS),
		OnReceive: dm.InjectRelayed,
		Logger:    log,
	})
	go relayCli.Run(ctx)

	log.Info("meshsock datapath enabled", "control_relay", relayAddr)
	return &mesh.MeshsockConfig{
		NodeID: jr.NodeID,
		CAHash: caHash,
		Punch:  &punchRequester{client: meshClient, nodeID: jr.NodeID},
		Relay:  relayCli.Send,
	}, nil
}

// punchRequester implements meshsock.PunchRequester over MeshService.
type punchRequester struct {
	client clusterv1.MeshServiceClient
	nodeID string
}

func (p *punchRequester) RequestPunch(target string) ([]netip.AddrPort, time.Time, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := p.client.RequestPunch(ctx, &clusterv1.RequestPunchRequest{NodeId: p.nodeID, TargetNodeId: target})
	if err != nil || !resp.GetCoordinated() {
		return nil, time.Time{}, false
	}
	return parseAddrPorts(resp.GetTargetEndpoints()), resp.GetPunchAt().AsTime(), true
}

// runPunchStream keeps the node's PunchStream open, delivering control-pushed
// PunchCommands to the device's bind. Reconnects with backoff.
func runPunchStream(ctx context.Context, client clusterv1.MeshServiceClient, nodeID string, punchNow func(string, []netip.AddrPort, time.Time)) {
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		stream, err := client.PunchStream(ctx, &clusterv1.PunchStreamRequest{NodeId: nodeID})
		if err == nil {
			for {
				cmd, rerr := stream.Recv()
				if rerr != nil {
					break
				}
				punchNow(cmd.GetPeerNodeId(), parseAddrPorts(cmd.GetPeerEndpoints()), cmd.GetPunchAt().AsTime())
			}
		}
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 15*time.Second {
			backoff *= 2
		}
	}
}

// relayClientTLS builds the mTLS config a worker uses to dial the control relay.
func relayClientTLS(jr *joinResult, serverName string) (*tls.Config, error) {
	cert, err := tls.X509KeyPair(jr.certPEM, jr.keyPEM)
	if err != nil {
		return nil, fmt.Errorf("daemon: relay client cert: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(jr.caPEM) {
		return nil, fmt.Errorf("daemon: relay client CA")
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ServerName:   serverName,
	}, nil
}

// clusterCAHash returns sha256 of the cluster CA certificate DER — the meshsock
// probe key salt (matches the gossip key derivation).
func clusterCAHash(caPEM []byte) ([]byte, error) {
	block, _ := pem.Decode(caPEM)
	if block == nil {
		return nil, fmt.Errorf("daemon: cluster CA is not PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("daemon: parse cluster CA: %w", err)
	}
	sum := sha256.Sum256(cert.Raw)
	return sum[:], nil
}

// parseAddrPorts drops unparseable entries.
func parseAddrPorts(ss []string) []netip.AddrPort {
	var out []netip.AddrPort
	for _, s := range ss {
		if ap, err := netip.ParseAddrPort(s); err == nil {
			out = append(out, ap)
		}
	}
	return out
}
