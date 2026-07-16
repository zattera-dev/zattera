package relay

import (
	"context"
	"crypto/tls"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
)

// writeQueueDepth bounds a destination connection's pending frames. A slow
// receiver must not block the relay: when its queue is full we drop the oldest
// frame (UDP semantics — WireGuard retransmits).
const writeQueueDepth = 256

// Server is the control-node TCP relay. Each connected node registers by node
// id (from its mTLS identity); frames addressed to a node are forwarded to its
// connection, dropped if absent.
type Server struct {
	log *slog.Logger

	mu    sync.Mutex
	conns map[string]*relayConn // node id → connection
}

// relayConn wraps a client connection with a drop-oldest write queue served by
// a dedicated writer goroutine (so one slow peer can't stall the reader).
type relayConn struct {
	nodeID string
	conn   net.Conn
	out    chan framed
	closed chan struct{}
	once   sync.Once
}

type framed struct {
	src     string
	payload []byte
}

// NewServer builds the relay server.
func NewServer(log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{log: log, conns: map[string]*relayConn{}}
}

// Serve accepts mTLS connections on lis until ctx is canceled. peerNodeID
// extracts the authenticated node id from a connection's TLS state.
func (s *Server) Serve(ctx context.Context, lis net.Listener, peerNodeID func(*tls.ConnectionState) (string, bool)) error {
	go func() { <-ctx.Done(); _ = lis.Close() }()
	for {
		conn, err := lis.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go s.handle(ctx, conn, peerNodeID)
	}
}

func (s *Server) handle(ctx context.Context, conn net.Conn, peerNodeID func(*tls.ConnectionState) (string, bool)) {
	tc, ok := conn.(*tls.Conn)
	if !ok {
		_ = conn.Close()
		return
	}
	if err := tc.HandshakeContext(ctx); err != nil {
		_ = conn.Close()
		return
	}
	st := tc.ConnectionState()
	nodeID, ok := peerNodeID(&st)
	if !ok || len(nodeID) != nodeIDLen {
		s.log.Warn("relay: rejecting connection without a valid node identity")
		_ = conn.Close()
		return
	}

	rc := &relayConn{nodeID: nodeID, conn: conn, out: make(chan framed, writeQueueDepth), closed: make(chan struct{})}
	s.register(rc)
	defer s.unregister(rc)

	go rc.writeLoop(s.log)
	s.readLoop(rc)
}

// readLoop forwards each frame this client sends to the destination's queue.
func (s *Server) readLoop(rc *relayConn) {
	defer rc.close()
	for {
		dst, payload, err := readFrame(rc.conn)
		if err != nil {
			if err != io.EOF {
				s.log.Debug("relay: read", "node", rc.nodeID, "err", err)
			}
			return
		}
		s.mu.Lock()
		dstConn := s.conns[dst]
		s.mu.Unlock()
		if dstConn == nil {
			continue // destination not connected: drop (UDP semantics)
		}
		dstConn.enqueue(framed{src: rc.nodeID, payload: payload})
	}
}

func (s *Server) register(rc *relayConn) {
	s.mu.Lock()
	if old := s.conns[rc.nodeID]; old != nil {
		old.close()
	}
	s.conns[rc.nodeID] = rc
	s.mu.Unlock()
	s.log.Info("relay client connected", "node", rc.nodeID)
}

func (s *Server) unregister(rc *relayConn) {
	s.mu.Lock()
	if s.conns[rc.nodeID] == rc {
		delete(s.conns, rc.nodeID)
	}
	s.mu.Unlock()
	rc.close()
}

// enqueue appends to the write queue, dropping the OLDEST frame when full so a
// slow peer never blocks the relay.
func (rc *relayConn) enqueue(f framed) {
	for {
		select {
		case rc.out <- f:
			return
		case <-rc.closed:
			return
		default:
			select {
			case <-rc.out: // drop oldest, retry
			default:
			}
		}
	}
}

// writeLoop drains the queue to the socket, rewriting the frame's node id to
// the SOURCE so the receiver knows who sent it.
func (rc *relayConn) writeLoop(log *slog.Logger) {
	for {
		select {
		case <-rc.closed:
			return
		case f := <-rc.out:
			if err := writeFrame(rc.conn, f.src, f.payload); err != nil {
				log.Debug("relay: write", "node", rc.nodeID, "err", err)
				rc.close()
				return
			}
		}
	}
}

func (rc *relayConn) close() {
	rc.once.Do(func() {
		close(rc.closed)
		_ = rc.conn.Close()
	})
}

// NodeIDFromURISANs extracts the zattera node id from a peer certificate's URI
// SANs (zattera://node/<id>). Shared by the relay server and any mTLS caller
// identification.
func NodeIDFromURISANs(state *tls.ConnectionState) (string, bool) {
	if state == nil || len(state.PeerCertificates) == 0 {
		return "", false
	}
	const prefix = "zattera://node/"
	for _, uri := range state.PeerCertificates[0].URIs {
		if s := uri.String(); strings.HasPrefix(s, prefix) {
			return strings.TrimPrefix(s, prefix), true
		}
	}
	return "", false
}
