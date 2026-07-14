package proxy

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
)

// testListeners hands the L4 proxy loopback listeners and records the actual
// bound address per public port so tests can dial them.
type testListeners struct {
	mu    sync.Mutex
	addrs map[uint32]string
}

func newTestListeners() *testListeners { return &testListeners{addrs: map[uint32]string{}} }

func (tl *testListeners) listen(port uint32) (net.Listener, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	tl.mu.Lock()
	tl.addrs[port] = ln.Addr().String()
	tl.mu.Unlock()
	return ln, nil
}

func (tl *testListeners) addr(port uint32) string {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	return tl.addrs[port]
}

func acceptLoop(t *testing.T, ln net.Listener, handle func(net.Conn)) {
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				handle(c)
			}(c)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
}

// tcpEcho is a backend that echoes bytes.
func tcpEcho(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	acceptLoop(t, ln, func(c net.Conn) { _, _ = io.Copy(c, c) })
	return ln.Addr().String()
}

// tcpTag writes a 1-byte tag on connect, then echoes.
func tcpTag(t *testing.T, tag byte) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	acceptLoop(t, ln, func(c net.Conn) {
		_, _ = c.Write([]byte{tag})
		_, _ = io.Copy(c, c)
	})
	return ln.Addr().String()
}

func l4Endpoint(addr string) *clusterv1.Endpoint {
	return &clusterv1.Endpoint{Addr: addr, NodeId: "n1", Healthy: true}
}

func l4Route(port uint32, addrs ...string) *clusterv1.L4Route {
	var eps []*clusterv1.Endpoint
	for _, a := range addrs {
		eps = append(eps, l4Endpoint(a))
	}
	return &clusterv1.L4Route{PublicPort: port, Protocol: "tcp", Endpoints: eps}
}

func newL4(t *testing.T, tl *testListeners) *L4 {
	l := NewL4(&StaticRouteSource{}, "n1", nil)
	l.listen = tl.listen
	return l
}

func TestL4EchoThrough(t *testing.T) {
	tl := newTestListeners()
	back := tcpEcho(t)
	l := newL4(t, tl)
	l.reconcile(&clusterv1.RouteSnapshot{Version: 1, L4Routes: []*clusterv1.L4Route{l4Route(15000, back)}})

	conn, err := net.Dial("tcp", tl.addr(15000))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "hello" {
		t.Fatalf("echo = %q", buf)
	}
}

func TestL4BalancesAcrossBackends(t *testing.T) {
	tl := newTestListeners()
	a, b := tcpTag(t, 'a'), tcpTag(t, 'b')
	l := newL4(t, tl)
	l.reconcile(&clusterv1.RouteSnapshot{Version: 1, L4Routes: []*clusterv1.L4Route{l4Route(15001, a, b)}})
	addr := tl.addr(15001)

	hits := map[byte]int{}
	for i := 0; i < 100; i++ {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 1)
		if _, err := io.ReadFull(conn, buf); err != nil {
			t.Fatal(err)
		}
		hits[buf[0]]++
		_ = conn.Close()
	}
	if hits['a'] == 0 || hits['b'] == 0 {
		t.Fatalf("P2C did not spread across both backends: %v", hits)
	}
}

func TestL4NoHealthyBackendDrops(t *testing.T) {
	tl := newTestListeners()
	l := newL4(t, tl)
	route := &clusterv1.L4Route{PublicPort: 15009, Endpoints: []*clusterv1.Endpoint{{Addr: "127.0.0.1:1", Healthy: false}}}
	l.reconcile(&clusterv1.RouteSnapshot{Version: 1, L4Routes: []*clusterv1.L4Route{route}})

	conn, err := net.Dial("tcp", tl.addr(15009))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	// The proxy accepts then immediately closes (no backend) → read returns EOF.
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected the connection to be closed with no healthy backend")
	}
}

func TestL4PortAddRemoveKeepsLiveConns(t *testing.T) {
	tl := newTestListeners()
	a, b := tcpEcho(t), tcpEcho(t)
	l := newL4(t, tl)
	l.reconcile(&clusterv1.RouteSnapshot{Version: 1, L4Routes: []*clusterv1.L4Route{
		l4Route(15002, a),
		l4Route(15003, b),
	}})
	addr2, addr3 := tl.addr(15002), tl.addr(15003)

	// Establish a live connection on port 15003.
	live, err := net.Dial("tcp", addr3)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = live.Close() }()
	roundtrip(t, live, "x")

	// Snapshot swap: drop port 15002, keep 15003 unchanged.
	l.reconcile(&clusterv1.RouteSnapshot{Version: 2, L4Routes: []*clusterv1.L4Route{l4Route(15003, b)}})

	// The live connection on the untouched port must keep working.
	roundtrip(t, live, "y")

	// New connections to the removed port fail.
	if c, err := net.DialTimeout("tcp", addr2, 500*time.Millisecond); err == nil {
		_ = c.Close()
		t.Fatal("removed port should no longer accept connections")
	}
	// The untouched port still accepts new connections too.
	fresh, err := net.Dial("tcp", addr3)
	if err != nil {
		t.Fatalf("untouched port stopped accepting: %v", err)
	}
	defer func() { _ = fresh.Close() }()
	roundtrip(t, fresh, "z")
}

func roundtrip(t *testing.T, conn net.Conn, msg string) {
	t.Helper()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write([]byte(msg)); err != nil {
		t.Fatalf("write %q: %v", msg, err)
	}
	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read %q: %v", msg, err)
	}
	if string(buf) != msg {
		t.Fatalf("roundtrip got %q, want %q", buf, msg)
	}
}
