// Package volumes implements the content-addressed snapshot engine (spec §3.11):
// a deterministic tar of a volume's host path is content-defined-chunked
// (FastCDC), each chunk is deduplicated by sha256, compressed (zstd), encrypted
// (AES-GCM), and stored as an object; a per-snapshot manifest lists the ordered
// chunk hashes. Restore streams the chunks back through tar extract; Prune
// garbage-collects chunks no manifest references.
//
// The engine operates on an already-quiesced path — quiescing a running
// database (the pre-hook) is the orchestration layer's job (T-65).
package volumes

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	"github.com/jotfs/fastcdc-go"
	"github.com/klauspost/compress/zstd"
)

const (
	chunkPrefix    = "chunks/"
	manifestPrefix = "manifests/"
	manifestVer    = 1
)

// ChunkRef is one chunk's identity + plaintext size, in tar order.
type ChunkRef struct {
	Hash string `json:"hash"`
	Size int    `json:"size"`
}

// Manifest describes one snapshot: the ordered chunk list and totals.
type Manifest struct {
	Version       int        `json:"version"`
	Chunks        []ChunkRef `json:"chunks"`
	TarBytes      int64      `json:"tar_bytes"`
	CreatedAtUnix int64      `json:"created_at_unix"`
}

// Options tunes chunk sizing; zero values fall back to the ~1MB defaults.
type Options struct {
	AverageSize int
	MinSize     int
	MaxSize     int
}

// Engine snapshots and restores volume paths against an ObjectStore.
type Engine struct {
	store ObjectStore
	key   []byte
	opts  Options
	enc   *zstd.Encoder
	dec   *zstd.Decoder
}

// NewEngine builds an engine. key is the 32-byte cluster data key.
func NewEngine(store ObjectStore, key []byte, opts Options) (*Engine, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("volumes: data key must be 32 bytes, got %d", len(key))
	}
	if opts.AverageSize == 0 {
		opts.AverageSize = defaultAvgChunk
	}
	if opts.MinSize == 0 {
		opts.MinSize = min(defaultMinChunk, opts.AverageSize/4+1)
	}
	if opts.MaxSize == 0 {
		opts.MaxSize = max(defaultMaxChunk, opts.AverageSize*4)
	}
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	return &Engine{store: store, key: append([]byte(nil), key...), opts: opts, enc: enc, dec: dec}, nil
}

// Progress reports cumulative uploaded bytes during a snapshot (bytesTotal is 0
// — the tar size is unknown until the walk finishes). nil disables reporting.
type Progress func(bytesUploaded int64)

// Snapshot tars srcPath, chunks + dedups + stores it, writes the manifest under
// snapshotID, and returns the manifest. createdAtUnix stamps the manifest (the
// caller owns the clock). progress may be nil.
func (e *Engine) Snapshot(ctx context.Context, srcPath, snapshotID string, createdAtUnix int64, progress Progress) (*Manifest, error) {
	pr, pw := io.Pipe()
	go func() { pw.CloseWithError(writeDeterministicTar(pw, srcPath)) }()

	chunker, err := fastcdc.NewChunker(pr, fastcdc.Options{
		AverageSize: e.opts.AverageSize, MinSize: e.opts.MinSize, MaxSize: e.opts.MaxSize,
	})
	if err != nil {
		_ = pr.CloseWithError(err)
		return nil, err
	}

	m := &Manifest{Version: manifestVer, CreatedAtUnix: createdAtUnix}
	var uploaded int64
	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("volumes: chunking: %w", err)
		}
		sum := sha256.Sum256(chunk.Data)
		hash := hex.EncodeToString(sum[:])
		m.Chunks = append(m.Chunks, ChunkRef{Hash: hash, Size: chunk.Length})
		m.TarBytes += int64(chunk.Length)

		key := chunkPrefix + hash
		has, err := e.store.Has(ctx, key)
		if err != nil {
			return nil, err
		}
		if has {
			continue // dedup: identical chunk already stored
		}
		sealed, err := seal(e.key, e.enc.EncodeAll(chunk.Data, nil))
		if err != nil {
			return nil, err
		}
		if err := e.store.Put(ctx, key, sealed); err != nil {
			return nil, err
		}
		uploaded += int64(chunk.Length)
		if progress != nil {
			progress(uploaded)
		}
	}

	if err := e.putManifest(ctx, snapshotID, m); err != nil {
		return nil, err
	}
	return m, nil
}

// Restore streams the snapshot's chunks back through a tar extract into dstPath
// (which must already exist).
func (e *Engine) Restore(ctx context.Context, snapshotID, dstPath string) error {
	m, err := e.getManifest(ctx, snapshotID)
	if err != nil {
		return err
	}
	pr, pw := io.Pipe()
	go func() { pw.CloseWithError(e.streamChunks(ctx, m, pw)) }()
	if err := extractTar(pr, dstPath); err != nil {
		_ = pr.CloseWithError(err)
		return err
	}
	return nil
}

// streamChunks writes the manifest's chunks, decrypted and decompressed, in
// order into w.
func (e *Engine) streamChunks(ctx context.Context, m *Manifest, w io.Writer) error {
	for _, ref := range m.Chunks {
		blob, err := e.store.Get(ctx, chunkPrefix+ref.Hash)
		if err != nil {
			return err
		}
		comp, err := open(e.key, blob)
		if err != nil {
			return err
		}
		plain, err := e.dec.DecodeAll(comp, nil)
		if err != nil {
			return fmt.Errorf("volumes: decompress: %w", err)
		}
		if _, err := w.Write(plain); err != nil {
			return err
		}
	}
	return nil
}

// Prune deletes chunk objects that no manifest references and returns the count
// removed. Safe to run while snapshots are read (it only removes orphans).
func (e *Engine) Prune(ctx context.Context) (int, error) {
	manifestKeys, err := e.store.List(ctx, manifestPrefix)
	if err != nil {
		return 0, err
	}
	live := map[string]bool{}
	for _, mk := range manifestKeys {
		m, err := e.readManifestKey(ctx, mk)
		if err != nil {
			return 0, err
		}
		for _, ref := range m.Chunks {
			live[ref.Hash] = true
		}
	}

	chunkKeys, err := e.store.List(ctx, chunkPrefix)
	if err != nil {
		return 0, err
	}
	deleted := 0
	for _, ck := range chunkKeys {
		hash := ck[len(chunkPrefix):]
		if live[hash] {
			continue
		}
		if err := e.store.Delete(ctx, ck); err != nil {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

func (e *Engine) putManifest(ctx context.Context, snapshotID string, m *Manifest) error {
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	sealed, err := seal(e.key, e.enc.EncodeAll(raw, nil))
	if err != nil {
		return err
	}
	return e.store.Put(ctx, manifestPrefix+snapshotID, sealed)
}

func (e *Engine) getManifest(ctx context.Context, snapshotID string) (*Manifest, error) {
	return e.readManifestKey(ctx, manifestPrefix+snapshotID)
}

func (e *Engine) readManifestKey(ctx context.Context, key string) (*Manifest, error) {
	blob, err := e.store.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	comp, err := open(e.key, blob)
	if err != nil {
		return nil, err
	}
	raw, err := e.dec.DecodeAll(comp, nil)
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("volumes: manifest decode: %w", err)
	}
	return &m, nil
}
