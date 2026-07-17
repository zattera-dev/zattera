package volumes

import (
	"context"
	"sort"
	"strings"
	"sync"
)

// ObjectStore is the content-addressed blob backend a snapshot Engine writes to
// (S3/MinIO in production, an in-memory store in tests). Keys are flat strings
// like "chunks/<hash>" and "manifests/<id>".
type ObjectStore interface {
	// Has reports whether key exists (a cheap HEAD — drives chunk dedup).
	Has(ctx context.Context, key string) (bool, error)
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, data []byte) error
	// List returns every key with the given prefix.
	List(ctx context.Context, prefix string) ([]string, error)
	Delete(ctx context.Context, key string) error
}

// MemStore is an in-memory ObjectStore for tests. Safe for concurrent use.
type MemStore struct {
	mu   sync.Mutex
	objs map[string][]byte
}

// NewMemStore builds an empty in-memory store.
func NewMemStore() *MemStore { return &MemStore{objs: map[string][]byte{}} }

func (m *MemStore) Has(_ context.Context, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.objs[key]
	return ok, nil
}

func (m *MemStore) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.objs[key]
	if !ok {
		return nil, errNotFound{key}
	}
	return append([]byte(nil), v...), nil
}

func (m *MemStore) Put(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objs[key] = append([]byte(nil), data...)
	return nil
}

func (m *MemStore) List(_ context.Context, prefix string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for k := range m.objs {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (m *MemStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objs, key)
	return nil
}

// Len reports the number of stored objects (test introspection).
func (m *MemStore) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.objs)
}

type errNotFound struct{ key string }

func (e errNotFound) Error() string { return "volumes: object not found: " + e.key }
