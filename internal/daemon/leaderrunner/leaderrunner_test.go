package leaderrunner

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeStore is a controllable leadership source.
type fakeStore struct {
	mu     sync.Mutex
	leader bool
	ch     chan bool
}

func newFakeStore() *fakeStore { return &fakeStore{ch: make(chan bool, 1)} }

func (f *fakeStore) IsLeader() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.leader
}

func (f *fakeStore) LeaderCh() <-chan bool { return f.ch }

func (f *fakeStore) set(leader bool) {
	f.mu.Lock()
	f.leader = leader
	f.mu.Unlock()
	select {
	case f.ch <- leader:
	default:
	}
}

// TestRunInvokesLoopOnlyWhileLeader verifies the loop starts on election, its
// context stays live while leading, and it is re-invoked after a demotion +
// re-election.
func TestRunInvokesLoopOnlyWhileLeader(t *testing.T) {
	store := newFakeStore()
	var invocations atomic.Int64
	running := make(chan struct{}, 8)
	release := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		Run(ctx, store, nil, func(loopCtx context.Context) {
			invocations.Add(1)
			running <- struct{}{}
			// Emulate a leaderLoop: return when leadership is lost or ctx ends.
			select {
			case <-loopCtx.Done():
			case <-store.LeaderCh():
			case <-release:
			}
		})
		close(done)
	}()

	// Not leader yet: the loop must not run.
	select {
	case <-running:
		t.Fatal("loop ran before leadership")
	case <-time.After(100 * time.Millisecond):
	}

	// Elect: loop runs.
	store.set(true)
	select {
	case <-running:
	case <-time.After(2 * time.Second):
		t.Fatal("loop did not start after election")
	}

	// Demote: the loop observes LeaderCh and returns; it must not immediately
	// re-run while we are not leader.
	store.set(false)
	select {
	case <-running:
		t.Fatal("loop re-ran while not leader")
	case <-time.After(150 * time.Millisecond):
	}

	// Re-elect: loop runs again.
	store.set(true)
	select {
	case <-running:
	case <-time.After(2 * time.Second):
		t.Fatal("loop did not resume after re-election")
	}

	if got := invocations.Load(); got != 2 {
		t.Fatalf("loop invoked %d times, want 2", got)
	}

	// Cancel unwinds Run even while the loop is active.
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

// TestRunReturnsWhenCanceledIdle verifies Run exits promptly when ctx is
// canceled while waiting for leadership (never having led).
func TestRunReturnsWhenCanceledIdle(t *testing.T) {
	store := newFakeStore()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		Run(ctx, store, nil, func(context.Context) { t.Error("loop should not run") })
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return when canceled idle")
	}
}
