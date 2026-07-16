// Package relay is the Phase D DERP-lite fallback (ADR-0003): every control
// node runs an mTLS TCP relay; a meshsock node with no working UDP path frames
// its already-encrypted WireGuard packets as (dst node, payload) and the relay
// forwards them to the destination's connection. The relay never sees plaintext
// — it moves opaque WG datagrams between two nodes that cannot reach each other
// directly.
package relay

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	// nodeIDLen is the fixed on-wire node id width. Zattera ids are 26-char
	// ULIDs; framing them fixed-width avoids a length prefix for the id.
	nodeIDLen = 26
	// MaxPayload caps a relayed WG datagram (well over the mesh MTU).
	MaxPayload = 2048
	// frameHeaderLen is nodeID(26) + payloadLen(2).
	frameHeaderLen = nodeIDLen + 2
)

var (
	errNodeIDLen  = errors.New("relay: node id must be 26 bytes")
	errPayloadCap = errors.New("relay: payload exceeds cap")
)

// writeFrame writes [dstNodeID(26)][len(u16)][payload] to w. The frame the
// server reads carries the DESTINATION; the frame the server forwards carries
// the SOURCE (rewritten) so the receiver knows who sent it.
func writeFrame(w io.Writer, nodeID string, payload []byte) error {
	if len(nodeID) != nodeIDLen {
		return errNodeIDLen
	}
	if len(payload) > MaxPayload {
		return errPayloadCap
	}
	var hdr [frameHeaderLen]byte
	copy(hdr[:nodeIDLen], nodeID)
	binary.BigEndian.PutUint16(hdr[nodeIDLen:], uint16(len(payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

// readFrame reads one framed message from r.
func readFrame(r io.Reader) (nodeID string, payload []byte, err error) {
	var hdr [frameHeaderLen]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return "", nil, err
	}
	n := binary.BigEndian.Uint16(hdr[nodeIDLen:])
	if n > MaxPayload {
		return "", nil, errPayloadCap
	}
	payload = make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return "", nil, err
	}
	return string(hdr[:nodeIDLen]), payload, nil
}
