//go:build integration

package integration

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/zattera-dev/zattera/internal/daemon/volumes"
)

const (
	minioImage  = "minio/minio:latest"
	minioAccess = "minioadmin"
	minioSecret = "minioadmin"
	minioPort   = "39017"
	minioBucket = "snapshots"
)

// TestSnapshotMinIO runs the snapshot engine against a real MinIO container:
// snapshot → wipe → restore is byte-identical, and prune leaves shared chunks.
func TestSnapshotMinIO(t *testing.T) {
	RequireDocker(t)
	endpoint := startMinIO(t)

	cli, err := minio.New(endpoint, &minio.Options{
		Creds: credentials.NewStaticV4(minioAccess, minioSecret, ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := cli.MakeBucket(ctx, minioBucket, minio.MakeBucketOptions{}); err != nil {
		t.Fatalf("make bucket: %v", err)
	}

	store, err := volumes.NewS3Store(volumes.S3Config{
		Endpoint: endpoint, Bucket: minioBucket, Prefix: "cluster1/",
		AccessKey: minioAccess, SecretKey: minioSecret,
	})
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	eng, err := volumes.NewEngine(store, key, volumes.Options{AverageSize: 8 << 10})
	if err != nil {
		t.Fatal(err)
	}

	// A volume with a couple of files.
	src := t.TempDir()
	writeFile(t, src, "db/data.bin", bigData(256<<10, 1))
	writeFile(t, src, "db/wal.log", []byte("write-ahead log\n"))

	if _, err := eng.Snapshot(ctx, src, "snap1", time.Now().Unix()); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// Wipe and restore.
	dst := t.TempDir()
	if err := eng.Restore(ctx, "snap1", dst); err != nil {
		t.Fatalf("restore: %v", err)
	}
	assertDirsEqual(t, src, dst)

	// A second snapshot sharing most chunks, then drop it and prune.
	writeFile(t, src, "db/wal.log", []byte("write-ahead log\nmore\n"))
	if _, err := eng.Snapshot(ctx, src, "snap2", time.Now().Unix()); err != nil {
		t.Fatalf("snapshot 2: %v", err)
	}
	if n, err := eng.Prune(ctx); err != nil || n != 0 {
		t.Fatalf("prune with both live removed %d (err %v), want 0", n, err)
	}
	if err := store.Delete(ctx, "manifests/snap2"); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Prune(ctx); err != nil {
		t.Fatalf("prune: %v", err)
	}
	// snap1 still restores intact after prune.
	dst2 := t.TempDir()
	if err := eng.Restore(ctx, "snap1", dst2); err != nil {
		t.Fatalf("snap1 restore after prune: %v", err)
	}
	assertDirsEqual(t, src2Files(t, src), dst2) // src changed; compare snap1's tree
}

// startMinIO runs a throwaway MinIO container and returns its endpoint.
func startMinIO(t *testing.T) string {
	t.Helper()
	name := "zattera-minio-test"
	_ = exec.Command("docker", "rm", "-f", name).Run()
	run := exec.Command("docker", "run", "-d", "--rm", "--name", name,
		"-p", minioPort+":9000",
		"-e", "MINIO_ROOT_USER="+minioAccess,
		"-e", "MINIO_ROOT_PASSWORD="+minioSecret,
		minioImage, "server", "/data")
	if out, err := run.CombinedOutput(); err != nil {
		t.Fatalf("docker run minio: %v\n%s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })

	endpoint := "127.0.0.1:" + minioPort
	cli, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(minioAccess, minioSecret, "")})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := cli.ListBuckets(context.Background()); err == nil {
			return endpoint
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatal("minio did not become ready")
	return ""
}

func writeFile(t *testing.T, dir, rel string, data []byte) {
	t.Helper()
	full := filepath.Join(dir, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func bigData(n int, seed uint64) []byte {
	out := make([]byte, n)
	x := seed
	for i := range out {
		x = x*6364136223846793005 + 1442695040888963407
		out[i] = byte(x >> 33)
	}
	return out
}

// src2Files snapshots the current src into a fresh dir holding only snap1's
// content (data.bin unchanged; wal.log at its snap1 value) for post-prune compare.
func src2Files(t *testing.T, src string) string {
	t.Helper()
	dir := t.TempDir()
	writeFile(t, dir, "db/data.bin", mustRead(t, filepath.Join(src, "db/data.bin")))
	writeFile(t, dir, "db/wal.log", []byte("write-ahead log\n"))
	return dir
}

func mustRead(t *testing.T, p string) []byte {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// assertDirsEqual compares two trees by relative path + file content.
func assertDirsEqual(t *testing.T, a, b string) {
	t.Helper()
	fa := readTree(t, a)
	fb := readTree(t, b)
	if len(fa) != len(fb) {
		t.Fatalf("tree size mismatch: %d vs %d files", len(fa), len(fb))
	}
	for rel, data := range fa {
		other, ok := fb[rel]
		if !ok {
			t.Fatalf("missing %s in restored tree", rel)
		}
		if !bytes.Equal(data, other) {
			t.Fatalf("content mismatch for %s", rel)
		}
	}
}

func readTree(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		out[rel] = mustRead(t, p)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}
