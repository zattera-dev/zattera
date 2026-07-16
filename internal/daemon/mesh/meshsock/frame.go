// Package meshsock is the custom wireguard-go conn.Bind (ADR-0003 Phase C/D):
// one UDP socket multiplexes WireGuard transport packets and disco probe
// frames, a per-peer path state machine upgrades hub-routed peers to direct or
// hole-punched UDP paths, and (Phase D) falls back to a TCP relay when no UDP
// path works. Kernel-WG nodes never load this package — they stay on phases
// A/B (hub + direct peering).
//
// meshsock deliberately does NOT import the parent mesh package: mesh's
// DeviceManager constructs the Bind, so an import back up would cycle. The
// probe frame codec and key derivation are self-contained here.
package meshsock

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

// Frame layout (everything after the magic is HMAC-signed):
//
//	[0]      0xff magic — WireGuard message types are 1..4, so the first byte
//	         cleanly discriminates probe frames from WG transport packets.
//	[1]      type: probePing | probePong
//	[2:10]   txID (big-endian)
//	[10:12]  payload length (big-endian)
//	[12:...] payload
//	[last 32] HMAC-SHA256 over bytes [0 : len-32]
const (
	frameMagic byte = 0xff

	probePing byte = 1
	probePong byte = 2

	frameHeaderLen = 1 + 1 + 8 + 2
	frameMACLen    = sha256.Size

	// maxFrameLen bounds probe frames well under any sane MTU.
	maxFrameLen = 512
)

var errBadFrame = errors.New("meshsock: malformed probe frame")

// IsProbeFrame reports whether pkt is a meshsock probe frame (vs a WireGuard
// transport packet).
func IsProbeFrame(pkt []byte) bool { return len(pkt) > 0 && pkt[0] == frameMagic }

// probeKey derives the per-node probe HMAC key from the node's WireGuard
// public key and the cluster CA hash. Both sides can derive any node's key
// (public inputs); the HMAC provides observation integrity, not secrecy — the
// real trust gate is WireGuard itself (mirrors the T-20 disco keying note).
func probeKey(wgPub [32]byte, caHash []byte) []byte {
	h := sha256.New()
	h.Write([]byte("zattera-meshsock-probe-v1:"))
	h.Write(wgPub[:])
	h.Write(caHash)
	return h.Sum(nil)
}

// encodeFrame renders a signed probe frame.
func encodeFrame(typ byte, txID uint64, payload []byte, key []byte) []byte {
	buf := make([]byte, 0, frameHeaderLen+len(payload)+frameMACLen)
	buf = append(buf, frameMagic, typ)
	buf = binary.BigEndian.AppendUint64(buf, txID)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(payload)))
	buf = append(buf, payload...)
	mac := hmac.New(sha256.New, key)
	mac.Write(buf)
	return mac.Sum(buf)
}

// decodeFrame parses a probe frame WITHOUT verifying the MAC — the caller
// needs the payload's node id to select the key first. It returns the signed
// region and mac for verifyFrame.
func decodeFrame(pkt []byte) (typ byte, txID uint64, payload, signed, mac []byte, err error) {
	if len(pkt) < frameHeaderLen+frameMACLen || len(pkt) > maxFrameLen || pkt[0] != frameMagic {
		return 0, 0, nil, nil, nil, errBadFrame
	}
	signed = pkt[: len(pkt)-frameMACLen : len(pkt)-frameMACLen]
	mac = pkt[len(pkt)-frameMACLen:]
	typ = pkt[1]
	txID = binary.BigEndian.Uint64(pkt[2:10])
	plen := int(binary.BigEndian.Uint16(pkt[10:12]))
	if frameHeaderLen+plen+frameMACLen != len(pkt) {
		return 0, 0, nil, nil, nil, errBadFrame
	}
	payload = pkt[frameHeaderLen : frameHeaderLen+plen]
	return typ, txID, payload, signed, mac, nil
}

// verifyFrame checks the frame's HMAC under key.
func verifyFrame(signed, mac, key []byte) bool {
	h := hmac.New(sha256.New, key)
	h.Write(signed)
	return hmac.Equal(mac, h.Sum(nil))
}

// pingPayload / pongPayload build and split probe payloads.
//
//	ping: <senderNodeID>            (signed with sender's key)
//	pong: <ponderNodeID>|<observed> (signed with the PING sender's key, so the
//	      original prober verifies with its own key; observed is the UDP
//	      source address the ponger saw — the prober's reflexive endpoint)
func pingPayload(senderID string) []byte { return []byte(senderID) }

func pongPayload(ponderID, observed string) []byte {
	return []byte(ponderID + "|" + observed)
}

func splitPong(payload []byte) (ponderID, observed string, ok bool) {
	s := string(payload)
	for i := 0; i < len(s); i++ {
		if s[i] == '|' {
			return s[:i], s[i+1:], true
		}
	}
	return "", "", false
}
