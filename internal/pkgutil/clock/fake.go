package clock

import (
	"sync"
	"time"
)

// Fake is a deterministic Clock driven by Advance. Timers fire synchronously
// inside Advance, in chronological order.
type Fake struct {
	mu      sync.Mutex
	now     time.Time
	waiters []*fakeWaiter
}

type fakeWaiter struct {
	at       time.Time
	ch       chan time.Time
	interval time.Duration // 0 = one-shot (After); >0 = ticker
	stopped  bool
}

// NewFake starts at a fixed, arbitrary epoch for reproducible tests.
func NewFake() *Fake {
	return &Fake{now: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *Fake) After(d time.Duration) <-chan time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	w := &fakeWaiter{at: f.now.Add(d), ch: make(chan time.Time, 1)}
	f.waiters = append(f.waiters, w)
	return w.ch
}

func (f *Fake) Sleep(d time.Duration) { <-f.After(d) }

func (f *Fake) NewTicker(d time.Duration) Ticker {
	f.mu.Lock()
	defer f.mu.Unlock()
	w := &fakeWaiter{at: f.now.Add(d), ch: make(chan time.Time, 1), interval: d}
	f.waiters = append(f.waiters, w)
	return &fakeTicker{f: f, w: w}
}

type fakeTicker struct {
	f *Fake
	w *fakeWaiter
}

func (t *fakeTicker) C() <-chan time.Time { return t.w.ch }

func (t *fakeTicker) Stop() {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	t.w.stopped = true
}

// Advance moves the clock forward, firing due timers/tickers in order.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	target := f.now.Add(d)
	for {
		// Find the earliest due waiter.
		var next *fakeWaiter
		for _, w := range f.waiters {
			if w.stopped {
				continue
			}
			if !w.at.After(target) && (next == nil || w.at.Before(next.at)) {
				next = w
			}
		}
		if next == nil {
			break
		}
		f.now = next.at
		select {
		case next.ch <- f.now:
		default: // ticker semantics: drop if the receiver is behind
		}
		if next.interval > 0 {
			next.at = next.at.Add(next.interval)
		} else {
			next.stopped = true
		}
	}
	f.now = target
	// Compact stopped waiters.
	kept := f.waiters[:0]
	for _, w := range f.waiters {
		if !w.stopped {
			kept = append(kept, w)
		}
	}
	f.waiters = kept
	f.mu.Unlock()
}
