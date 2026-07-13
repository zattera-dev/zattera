package mesh

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"

	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

// Disco is a minimal STUN-lite reflexive-address discovery (Phase B). A node
// pings each control node's disco port; the control echoes back the UDP source
// address it observed, letting the node learn its own reflexive endpoint for
// direct worker↔worker peering. Packets are HMAC-tagged for integrity.
//
// Keying note: the HMAC key is derived from the node's WireGuard PUBLIC key
// XOR the cluster CA hash. The control plane only holds peers' public keys, so
// keying on the public key (not the private key) lets BOTH sides derive the
// same key — the disco bar is "good enough for observation integrity"; the
// authenticated ReportObservedEndpoint RPC (mTLS) is the real trust gate.
const (
	discoPingType byte = 1
	discoPongType byte = 2

	discoDefaultInterval = 30 * time.Second
	discoReadTimeout     = 2 * time.Second

	discoHeaderLen = 1 + 8 + 2 // type + txid + strlen
	discoMACLen    = sha256.Size
)

// DiscoKey derives the shared disco HMAC key for a node.
func DiscoKey(pub wgtypes.Key, caHash []byte) []byte {
	var seed [32]byte
	for i := range seed {
		var c byte
		if i < len(caHash) {
			c = caHash[i]
		}
		seed[i] = pub[i] ^ c
	}
	sum := sha256.Sum256(seed[:])
	return sum[:]
}

// encodeDisco renders a signed disco frame: type|txid|len|payload|hmac.
func encodeDisco(typ byte, txID uint64, payload string, key []byte) []byte {
	buf := make([]byte, 0, discoHeaderLen+len(payload)+discoMACLen)
	buf = append(buf, typ)
	buf = binary.BigEndian.AppendUint64(buf, txID)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(payload)))
	buf = append(buf, payload...)
	mac := hmacSum(key, buf)
	return append(buf, mac...)
}

// decodeDisco parses a frame WITHOUT verifying (the caller needs the payload's
// node id to pick the key), returning the signed portion + mac for verifyDisco.
func decodeDisco(b []byte) (typ byte, txID uint64, payload string, signed, mac []byte, err error) {
	if len(b) < discoHeaderLen+discoMACLen {
		return 0, 0, "", nil, nil, errors.New("mesh: disco frame too short")
	}
	signed = b[:len(b)-discoMACLen]
	mac = b[len(b)-discoMACLen:]
	typ = signed[0]
	txID = binary.BigEndian.Uint64(signed[1:9])
	slen := int(binary.BigEndian.Uint16(signed[9:11]))
	if discoHeaderLen+slen != len(signed) {
		return 0, 0, "", nil, nil, errors.New("mesh: disco length mismatch")
	}
	payload = string(signed[discoHeaderLen:])
	return typ, txID, payload, signed, mac, nil
}

func verifyDisco(signed, mac, key []byte) bool {
	return hmac.Equal(mac, hmacSum(key, signed))
}

func hmacSum(key, msg []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(msg)
	return h.Sum(nil)
}

// DiscoResponder is the control-side echo server. For each valid ping it returns
// a pong carrying the observed UDP source address.
type DiscoResponder struct {
	conn       *net.UDPConn
	keyForNode func(nodeID string) ([]byte, bool)
	log        *slog.Logger
}

// NewDiscoResponder binds a UDP listener on addr ("ip:port").
func NewDiscoResponder(addr string, keyForNode func(string) ([]byte, bool), log *slog.Logger) (*DiscoResponder, error) {
	if log == nil {
		log = slog.Default()
	}
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("mesh: disco resolve %s: %w", addr, err)
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, fmt.Errorf("mesh: disco listen %s: %w", addr, err)
	}
	return &DiscoResponder{conn: conn, keyForNode: keyForNode, log: log}, nil
}

// Addr is the bound listen address.
func (r *DiscoResponder) Addr() string { return r.conn.LocalAddr().String() }

// Serve reads and answers pings until ctx is canceled.
func (r *DiscoResponder) Serve(ctx context.Context) {
	go func() { <-ctx.Done(); _ = r.conn.Close() }()
	buf := make([]byte, 1500)
	for {
		n, src, err := r.conn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		r.handle(buf[:n], src)
	}
}

func (r *DiscoResponder) handle(pkt []byte, src *net.UDPAddr) {
	typ, txID, nodeID, signed, mac, err := decodeDisco(pkt)
	if err != nil || typ != discoPingType {
		return
	}
	key, ok := r.keyForNode(nodeID)
	if !ok || !verifyDisco(signed, mac, key) {
		return // unknown node or bad signature
	}
	pong := encodeDisco(discoPongType, txID, src.String(), key)
	if _, err := r.conn.WriteToUDP(pong, src); err != nil {
		r.log.Debug("disco pong write failed", "node", nodeID, "err", err)
	}
}

// Close stops the responder.
func (r *DiscoResponder) Close() error { return r.conn.Close() }

// DiscoPinger is the node-side prober. It pings each control's disco endpoint
// and reports the reflexive address the control observed.
type DiscoPinger struct {
	NodeID   string
	Key      []byte
	Controls []string // control disco endpoints "ip:port"
	Interval time.Duration
	Clock    clock.Clock
	Report   func(observedEndpoint string)
	Logger   *slog.Logger

	txID uint64
}

// Run probes every Interval until ctx is canceled.
func (p *DiscoPinger) Run(ctx context.Context) {
	clk := p.Clock
	if clk == nil {
		clk = clock.Real{}
	}
	interval := p.Interval
	if interval <= 0 {
		interval = discoDefaultInterval
	}
	tick := clk.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C():
			for _, c := range p.Controls {
				if ep, err := p.probe(ctx, c); err == nil && ep != "" && p.Report != nil {
					p.Report(ep)
				}
			}
		}
	}
}

// probe sends one ping to controlAddr and returns the observed endpoint from the
// pong.
func (p *DiscoPinger) probe(_ context.Context, controlAddr string) (string, error) {
	raddr, err := net.ResolveUDPAddr("udp", controlAddr)
	if err != nil {
		return "", err
	}
	conn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close() }()

	p.txID++
	txID := p.txID
	if _, err := conn.Write(encodeDisco(discoPingType, txID, p.NodeID, p.Key)); err != nil {
		return "", err
	}
	_ = conn.SetReadDeadline(time.Now().Add(discoReadTimeout))
	buf := make([]byte, 1500)
	n, err := conn.Read(buf)
	if err != nil {
		return "", err
	}
	typ, rxID, endpoint, signed, mac, err := decodeDisco(buf[:n])
	if err != nil || typ != discoPongType || rxID != txID {
		return "", errors.New("mesh: bad disco pong")
	}
	if !verifyDisco(signed, mac, p.Key) {
		return "", errors.New("mesh: disco pong signature mismatch")
	}
	return endpoint, nil
}
