package volumes

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Chunk sizing defaults (spec §3.11): ~1MB average content-defined chunks.
const (
	defaultAvgChunk = 1 << 20 // 1 MiB
	defaultMinChunk = 1 << 18 // 256 KiB
	defaultMaxChunk = 1 << 22 // 4 MiB
)

// writeDeterministicTar streams a byte-stable tar of root into w: a sorted walk
// with zeroed access/change times and second-truncated mtime, uid/gid/mode
// preserved. Byte-identical input trees produce byte-identical tars, which is
// what makes content-defined chunk dedup work across snapshots.
func writeDeterministicTar(w io.Writer, root string) error {
	tw := tar.NewWriter(w)
	rels, err := sortedEntries(root)
	if err != nil {
		return err
	}
	for _, rel := range rels {
		full := filepath.Join(root, rel)
		fi, err := os.Lstat(full)
		if err != nil {
			return err
		}
		var link string
		if fi.Mode()&os.ModeSymlink != 0 {
			if link, err = os.Readlink(full); err != nil {
				return err
			}
		}
		hdr, err := tar.FileInfoHeader(fi, link)
		if err != nil {
			return err
		}
		hdr.Name = rel
		if fi.IsDir() {
			hdr.Name += "/"
		}
		// Determinism: drop access/change times and sub-second mtime.
		hdr.AccessTime = time.Time{}
		hdr.ChangeTime = time.Time{}
		hdr.ModTime = hdr.ModTime.Truncate(time.Second)
		hdr.Format = tar.FormatPAX
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if fi.Mode().IsRegular() {
			f, err := os.Open(full)
			if err != nil {
				return err
			}
			_, err = io.Copy(tw, f)
			_ = f.Close()
			if err != nil {
				return err
			}
		}
	}
	return tw.Close()
}

// sortedEntries returns every path under root (relative, excluding root itself),
// sorted lexicographically for a stable walk order.
func sortedEntries(root string) ([]string, error) {
	var rels []string
	err := filepath.WalkDir(root, func(p string, _ os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p == root {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		rels = append(rels, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(rels)
	return rels, nil
}

// extractTar writes a tar stream into dst, recreating dirs, files, symlinks and
// their mode/mtime/uid/gid. dst must already exist.
func extractTar(r io.Reader, dst string) error {
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		target, err := safeJoin(dst, hdr.Name)
		if err != nil {
			return err
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, os.FileMode(hdr.Mode)); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(hdr.Mode))
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tr); err != nil {
				_ = f.Close()
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
		case tar.TypeSymlink:
			_ = os.Remove(target)
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				return err
			}
		default:
			continue // skip device/fifo/etc — not expected in app volumes
		}
		restoreAttrs(target, hdr)
	}
}

// restoreAttrs applies ownership and mtime (best effort — chown needs root).
func restoreAttrs(target string, hdr *tar.Header) {
	if hdr.Typeflag != tar.TypeSymlink {
		_ = os.Chown(target, hdr.Uid, hdr.Gid)
		if !hdr.ModTime.IsZero() {
			_ = os.Chtimes(target, hdr.ModTime, hdr.ModTime)
		}
	}
}

// safeJoin joins base and name, rejecting paths that escape base (tar-slip).
func safeJoin(base, name string) (string, error) {
	clean := filepath.Join(base, filepath.Clean("/"+name))
	if clean != base && !isSubpath(base, clean) {
		return "", fmt.Errorf("volumes: unsafe tar path %q", name)
	}
	return clean, nil
}

func isSubpath(base, p string) bool {
	rel, err := filepath.Rel(base, p)
	if err != nil {
		return false
	}
	return rel != ".." && !filepath.IsAbs(rel) && !hasDotDotPrefix(rel)
}

func hasDotDotPrefix(rel string) bool {
	return rel == ".." || len(rel) >= 3 && rel[:3] == ".."+string(filepath.Separator)
}
