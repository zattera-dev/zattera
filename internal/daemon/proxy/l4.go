package proxy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"time"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
)

// l4DialTimeout bounds dialing an L4 backend.
const l4DialTimeout = 5 * time.Second

// L4 is the raw TCP passthrough proxy (spec F5). It reconciles one listener per
// L4Route.public_port to the current RouteSnapshot, and for each accepted
// connection picks a healthy backend (P2C) and splices bytes both ways.
type L4 struct {
	source RouteSource
	lb     *balancer
	log    *slog.Logger
	rnd    func(n int) int

	// listen opens the listener for a public port (injectable for tests).
	listen func(port uint32) (net.Listener, error)
	// dial connects to a backend (injectable for tests).
	dial func(addr string) (net.Conn, error)

	mu        sync.Mutex
	listeners map[uint32]*l4Listener
}

// NewL4 builds the L4 proxy over a route source. node is this node's id (used
// to break P2C ties toward node-local backends).
func NewL4(source RouteSource, node string, log *slog.Logger) *L4 {
	if log == nil {
		log = slog.Default()
	}
	return &L4{
		source: source, lb: newBalancer(node), log: log, rnd: rand.IntN,
		listen: func(port uint32) (net.Listener, error) {
			return net.Listen("tcp", fmt.Sprintf(":%d", port))
		},
		dial:      func(addr string) (net.Conn, error) { return net.DialTimeout("tcp", addr, l4DialTimeout) },
		listeners: map[uint32]*l4Listener{},
	}
}

// Run reconciles listeners to each snapshot until ctx is canceled, then closes
// all listeners (established connections drain on their own).
func (l *L4) Run(ctx context.Context) {
	updates := l.source.Updates(ctx)
	for {
		select {
		case <-ctx.Done():
			l.closeAll()
			return
		case snap, ok := <-updates:
			if !ok {
				l.closeAll()
				return
			}
			l.reconcile(snap)
		}
	}
}

// reconcile opens listeners for new L4 ports, updates endpoints for existing
// ones, and closes listeners for ports no longer present. Closing a listener
// only stops accepting — established connections keep flowing (snapshot churn
// must not drop live traffic).
func (l *L4) reconcile(snap *clusterv1.RouteSnapshot) {
	want := map[uint32]*clusterv1.L4Route{}
	for _, r := range snap.GetL4Routes() {
		if r.GetPublicPort() != 0 {
			want[r.GetPublicPort()] = r
		}
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// Add/update.
	for port, route := range want {
		if existing, ok := l.listeners[port]; ok {
			existing.route.Store(route) // new connections use fresh endpoints
			continue
		}
		ln, err := l.listen(port)
		if err != nil {
			l.log.Warn("l4 listen failed", "port", port, "err", err)
			continue
		}
		ll := &l4Listener{ln: ln, l4: l}
		ll.route.Store(route)
		l.listeners[port] = ll
		go ll.serve()
	}
	// Remove ports no longer wanted.
	for port, ll := range l.listeners {
		if _, ok := want[port]; !ok {
			_ = ll.ln.Close() // stop accepting; live conns drain
			delete(l.listeners, port)
		}
	}
}

func (l *L4) closeAll() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for port, ll := range l.listeners {
		_ = ll.ln.Close()
		delete(l.listeners, port)
	}
}

// l4Listener is one public-port listener with its current route (endpoints)
// swappable atomically as snapshots arrive.
type l4Listener struct {
	ln    net.Listener
	l4    *L4
	route atomic.Pointer[clusterv1.L4Route]
}

func (ll *l4Listener) serve() {
	for {
		conn, err := ll.ln.Accept()
		if err != nil {
			return // listener closed
		}
		go ll.l4.handle(conn, ll.route.Load())
	}
}

// handle picks a backend and splices the connection bidirectionally.
func (l *L4) handle(client net.Conn, route *clusterv1.L4Route) {
	defer func() { _ = client.Close() }()

	ep := l.lb.pick(route.GetEndpoints(), l.rnd)
	if ep == nil {
		return // no healthy backend; drop the connection
	}
	release := l.lb.acquire(ep.GetAddr())
	defer release()

	upstream, err := l.dial(ep.GetAddr())
	if err != nil {
		l.log.Debug("l4 dial failed", "addr", ep.GetAddr(), "err", err)
		return
	}
	defer func() { _ = upstream.Close() }()

	// Splice both directions; half-close each side as its source EOFs so the
	// peer can still drain its own direction.
	done := make(chan struct{}, 2)
	go splice(upstream, client, done) // client → upstream
	go splice(client, upstream, done) // upstream → client
	<-done
	<-done
}

// splice copies src→dst, then half-closes dst's write side (CloseWrite) so the
// far end observes EOF. Signals done when the copy finishes.
func splice(dst, src net.Conn, done chan<- struct{}) {
	_, _ = io.Copy(dst, src)
	if cw, ok := dst.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
	}
	done <- struct{}{}
}
