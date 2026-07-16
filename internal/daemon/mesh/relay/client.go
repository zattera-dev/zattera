package relay

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"sync"
	"time"
)

// RelayPort is the control-node relay TCP port.
const RelayPort = 7443

// reconnectBackoff bounds reconnection attempts.
const (
	reconnectMin = 500 * time.Millisecond
	reconnectMax = 15 * time.Second
)

// Client maintains one connection to the lowest-RTT control relay, reconnecting
// with backoff, and delivers received frames to a sink. Send frames opaque WG
// packets to a destination node.
type Client struct {
	nodeID    string
	dial      func(ctx context.Context) (net.Conn, string, error) // → conn, relay addr
	onReceive func(srcNodeID string, payload []byte)
	log       *slog.Logger

	mu   sync.Mutex
	conn net.Conn
	up   bool
}

// Config parameterizes the relay client.
type Config struct {
	NodeID string
	// Dial connects to the currently-preferred relay and returns the conn plus
	// a label (address) for logging. It should already pick the lowest-RTT
	// control node (the caller supplies RTTs). It must complete the mTLS
	// handshake or return an error.
	Dial func(ctx context.Context) (net.Conn, string, error)
	// OnReceive is called for each relayed WG packet, from srcNodeID.
	OnReceive func(srcNodeID string, payload []byte)
	Logger    *slog.Logger
}

// NewClient builds a relay client (call Run to connect).
func NewClient(cfg Config) *Client {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Client{nodeID: cfg.NodeID, dial: cfg.Dial, onReceive: cfg.OnReceive, log: log}
}

// Run connects and maintains the relay session until ctx is canceled.
func (c *Client) Run(ctx context.Context) {
	backoff := reconnectMin
	for {
		if ctx.Err() != nil {
			return
		}
		conn, addr, err := c.dial(ctx)
		if err != nil {
			c.log.Debug("relay: dial failed", "err", err)
			if !sleep(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff)
			continue
		}
		c.log.Info("relay connected", "relay", addr)
		backoff = reconnectMin
		c.setConn(conn)
		c.readLoop(ctx, conn) // blocks until the connection drops
		c.setConn(nil)
		if ctx.Err() != nil {
			return
		}
		c.log.Info("relay disconnected; reconnecting")
	}
}

// Send frames a WG packet to dstNodeID over the relay. Returns an error when no
// relay connection is currently up (the caller keeps trying UDP paths).
func (c *Client) Send(dstNodeID string, payload []byte) error {
	c.mu.Lock()
	conn, up := c.conn, c.up
	c.mu.Unlock()
	if !up || conn == nil {
		return net.ErrClosed
	}
	// One writer at a time: guard the socket write.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return net.ErrClosed
	}
	_ = c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return writeFrame(c.conn, dstNodeID, payload)
}

func (c *Client) readLoop(ctx context.Context, conn net.Conn) {
	for {
		src, payload, err := readFrame(conn)
		if err != nil {
			return
		}
		if c.onReceive != nil {
			c.onReceive(src, payload)
		}
		if ctx.Err() != nil {
			return
		}
	}
}

func (c *Client) setConn(conn net.Conn) {
	c.mu.Lock()
	if c.conn != nil && c.conn != conn {
		_ = c.conn.Close()
	}
	c.conn = conn
	c.up = conn != nil
	c.mu.Unlock()
}

func nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > reconnectMax {
		return reconnectMax
	}
	return d
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// DialTLS is a convenience Dial for a single relay address with the given TLS
// config. Multi-relay RTT selection wraps this.
func DialTLS(addr string, tlsCfg *tls.Config) func(ctx context.Context) (net.Conn, string, error) {
	return func(ctx context.Context) (net.Conn, string, error) {
		d := &tls.Dialer{Config: tlsCfg}
		conn, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, addr, err
		}
		return conn, addr, nil
	}
}
