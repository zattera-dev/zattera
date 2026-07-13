package mesh

import (
	"context"
	"crypto/sha256"
	"testing"

	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

func TestDisco(t *testing.T) {
	caHash := sha256.Sum256([]byte("cluster-ca"))
	key := DiscoKey(key(0x07), caHash[:])

	t.Run("codec round trip", func(t *testing.T) {
		frame := encodeDisco(discoPingType, 42, "node-abc", key)
		typ, txID, payload, signed, mac, err := decodeDisco(frame)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if typ != discoPingType || txID != 42 || payload != "node-abc" {
			t.Fatalf("decoded wrong: typ=%d tx=%d payload=%q", typ, txID, payload)
		}
		if !verifyDisco(signed, mac, key) {
			t.Fatal("valid frame should verify")
		}
	})

	t.Run("tampered payload fails verification", func(t *testing.T) {
		frame := encodeDisco(discoPongType, 1, "1.2.3.4:5678", key)
		frame[discoHeaderLen] ^= 0xFF // flip a payload byte
		_, _, _, signed, mac, err := decodeDisco(frame)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if verifyDisco(signed, mac, key) {
			t.Fatal("tampered frame must not verify")
		}
	})

	t.Run("wrong key fails verification", func(t *testing.T) {
		frame := encodeDisco(discoPingType, 9, "n", key)
		_, _, _, signed, mac, _ := decodeDisco(frame)
		other := DiscoKey(key2(0x08), caHash[:])
		if verifyDisco(signed, mac, other) {
			t.Fatal("frame must not verify under a different key")
		}
	})

	t.Run("short frame is rejected", func(t *testing.T) {
		if _, _, _, _, _, err := decodeDisco([]byte{1, 2, 3}); err == nil {
			t.Fatal("short frame should error")
		}
	})

	t.Run("responder echoes the observed endpoint over loopback", func(t *testing.T) {
		resp, err := NewDiscoResponder("127.0.0.1:0", func(nodeID string) ([]byte, bool) {
			if nodeID == "w1" {
				return key, true
			}
			return nil, false
		}, discard())
		if err != nil {
			t.Fatalf("responder: %v", err)
		}
		defer func() { _ = resp.Close() }()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go resp.Serve(ctx)

		pinger := &DiscoPinger{NodeID: "w1", Key: key}
		observed, err := pinger.probe(context.Background(), resp.Addr())
		if err != nil {
			t.Fatalf("probe: %v", err)
		}
		// The observed endpoint is the pinger's own reflexive 127.0.0.1:port.
		if observed == "" || observed[:9] != "127.0.0.1" {
			t.Fatalf("observed endpoint = %q, want 127.0.0.1:*", observed)
		}
	})

	t.Run("responder ignores unknown nodes", func(t *testing.T) {
		resp, err := NewDiscoResponder("127.0.0.1:0", func(string) ([]byte, bool) { return nil, false }, discard())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Close() }()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go resp.Serve(ctx)

		pinger := &DiscoPinger{NodeID: "ghost", Key: key}
		if _, err := pinger.probe(context.Background(), resp.Addr()); err == nil {
			t.Fatal("probe of an unknown node should time out / error")
		}
	})
}

// key2 builds a distinct deterministic key (helper `key` is in device_test.go).
func key2(b byte) wgtypes.Key {
	var k wgtypes.Key
	k[1] = b
	return k
}
