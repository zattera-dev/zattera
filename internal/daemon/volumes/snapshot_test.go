package volumes

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// testKey is a deterministic 32-byte data key.
func testKey() []byte {
	k := make([]byte, 32)
	for i := range k {
		k[i] = byte(i * 7)
	}
	return k
}

// smallChunks makes edits produce many chunks from a small tree.
var smallChunks = Options{AverageSize: 4 << 10, MinSize: 1 << 10, MaxSize: 16 << 10}

// genData returns n bytes of deterministic pseudo-random data (a simple LCG).
func genData(n int, seed uint64) []byte {
	out := make([]byte, n)
	x := seed
	for i := range out {
		x = x*6364136223846793005 + 1442695040888963407
		out[i] = byte(x >> 33)
	}
	return out
}

var fixedMtime = time.Unix(1_700_000_000, 0)

// writeTree writes files (relative path → content) under dir with a fixed mtime.
func writeTree(t *testing.T, dir string, files map[string][]byte) {
	t.Helper()
	for rel, data := range files {
		full := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, data, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(full, fixedMtime, fixedMtime); err != nil {
			t.Fatal(err)
		}
	}
}

// tarBytes returns the deterministic tar of a directory (for byte-equality).
func tarBytes(t *testing.T, dir string) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := writeDeterministicTar(&buf, dir); err != nil {
		t.Fatalf("tar: %v", err)
	}
	return buf.Bytes()
}

func chunkSet(m *Manifest) map[string]bool {
	s := map[string]bool{}
	for _, c := range m.Chunks {
		s[c.Hash] = true
	}
	return s
}

func TestCryptoRoundTrip(t *testing.T) {
	key := testKey()
	msg := []byte("hello volume snapshot")
	sealed, err := seal(key, msg)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(sealed, msg) {
		t.Fatal("sealed output leaks plaintext")
	}
	got, err := open(key, sealed)
	if err != nil || !bytes.Equal(got, msg) {
		t.Fatalf("round trip = %q (err %v), want %q", got, err, msg)
	}
	// Same plaintext seals to different bytes (random nonce).
	sealed2, _ := seal(key, msg)
	if bytes.Equal(sealed, sealed2) {
		t.Fatal("two seals of the same plaintext must differ (nonce reuse)")
	}
	// A wrong key fails authentication.
	bad := testKey()
	bad[0] ^= 0xFF
	if _, err := open(bad, sealed); err == nil {
		t.Fatal("open with the wrong key must fail")
	}
}

func TestDeterministicTarStable(t *testing.T) {
	files := map[string][]byte{
		"a.txt":       []byte("alpha"),
		"sub/b.bin":   genData(1000, 1),
		"sub/c/d.txt": []byte("nested"),
	}
	d1, d2 := t.TempDir(), t.TempDir()
	writeTree(t, d1, files)
	writeTree(t, d2, files)
	if !bytes.Equal(tarBytes(t, d1), tarBytes(t, d2)) {
		t.Fatal("identical trees must tar byte-identically")
	}
}

func TestSnapshotRestoreByteIdentical(t *testing.T) {
	ctx := context.Background()
	src := t.TempDir()
	writeTree(t, src, map[string][]byte{
		"config.json":  []byte(`{"k":"v"}`),
		"data/big.bin": genData(300<<10, 42),
		"data/small":   genData(10, 7),
	})

	eng, err := NewEngine(NewMemStore(), testKey(), smallChunks)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Snapshot(ctx, src, "snap1", fixedMtime.Unix()); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	dst := t.TempDir()
	if err := eng.Restore(ctx, "snap1", dst); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if !bytes.Equal(tarBytes(t, src), tarBytes(t, dst)) {
		t.Fatal("restored tree is not byte-identical to the source")
	}
}

func TestChunkingStabilityAndDedup(t *testing.T) {
	ctx := context.Background()
	src := t.TempDir()
	writeTree(t, src, map[string][]byte{"big.bin": genData(512<<10, 99)})
	store := NewMemStore()
	eng, _ := NewEngine(store, testKey(), smallChunks)

	m1, err := eng.Snapshot(ctx, src, "s1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(m1.Chunks) < 5 {
		t.Fatalf("expected several chunks, got %d", len(m1.Chunks))
	}
	objAfterFirst := store.Len()

	// Snapshot the SAME data again: identical chunk set, and no new chunk
	// objects (only a second manifest).
	m2, err := eng.Snapshot(ctx, src, "s2", 2)
	if err != nil {
		t.Fatal(err)
	}
	if !equalHashes(m1, m2) {
		t.Fatal("identical data produced a different chunk set")
	}
	if got := store.Len(); got != objAfterFirst+1 {
		t.Fatalf("re-snapshot added %d objects, want 1 (the manifest)", got-objAfterFirst)
	}

	// A one-byte change (same length, same mtime) rechunks only locally.
	data := genData(512<<10, 99)
	data[200<<10] ^= 0xFF
	writeTree(t, src, map[string][]byte{"big.bin": data})
	m3, err := eng.Snapshot(ctx, src, "s3", 3)
	if err != nil {
		t.Fatal(err)
	}
	before := chunkSet(m1)
	newCount := 0
	for _, c := range m3.Chunks {
		if !before[c.Hash] {
			newCount++
		}
	}
	if newCount == 0 || newCount > 2 {
		t.Fatalf("a 1-byte change produced %d new chunks, want 1-2", newCount)
	}
}

func TestPruneLeavesSharedChunks(t *testing.T) {
	ctx := context.Background()
	src := t.TempDir()
	writeTree(t, src, map[string][]byte{"big.bin": genData(256<<10, 5)})
	store := NewMemStore()
	eng, _ := NewEngine(store, testKey(), smallChunks)

	m1, _ := eng.Snapshot(ctx, src, "s1", 1)

	// A second snapshot with a localized change: shares most chunks, adds a few.
	data := genData(256<<10, 5)
	data[100<<10] ^= 0xAA
	writeTree(t, src, map[string][]byte{"big.bin": data})
	m2, _ := eng.Snapshot(ctx, src, "s2", 2)

	// Both live → prune removes nothing.
	if n, err := eng.Prune(ctx); err != nil || n != 0 {
		t.Fatalf("prune with both manifests removed %d (err %v), want 0", n, err)
	}

	// Drop s2's manifest; prune must delete s2-exclusive chunks but keep shared.
	if err := store.Delete(ctx, manifestPrefix+"s2"); err != nil {
		t.Fatal(err)
	}
	exclusive := map[string]bool{}
	shared := chunkSet(m1)
	for _, c := range m2.Chunks {
		if !shared[c.Hash] {
			exclusive[c.Hash] = true
		}
	}
	if len(exclusive) == 0 {
		t.Fatal("test setup: s2 should have some exclusive chunks")
	}
	deleted, err := eng.Prune(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != len(exclusive) {
		t.Fatalf("prune deleted %d, want %d (s2-exclusive)", deleted, len(exclusive))
	}
	// Shared chunks survive; s1 still restores.
	for h := range shared {
		if ok, _ := store.Has(ctx, chunkPrefix+h); !ok {
			t.Fatalf("prune removed a shared chunk %s", h)
		}
	}
	dst := t.TempDir()
	writeTree(t, dst, map[string][]byte{"placeholder": {}}) // ensure dst exists
	_ = os.Remove(filepath.Join(dst, "placeholder"))
	if err := eng.Restore(ctx, "s1", dst); err != nil {
		t.Fatalf("s1 must still restore after prune: %v", err)
	}
	_ = m1
}

func equalHashes(a, b *Manifest) bool {
	if len(a.Chunks) != len(b.Chunks) {
		return false
	}
	for i := range a.Chunks {
		if a.Chunks[i].Hash != b.Chunks[i].Hash {
			return false
		}
	}
	return true
}
