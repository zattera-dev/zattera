// Package clock abstracts time for testability. Every timeout, ticker or
// deadline in daemon control loops MUST go through a Clock so simcluster and
// unit tests can drive time deterministically with Fake.
package clock

import "time"

// Clock is the minimal time surface control loops need.
type Clock interface {
	Now() time.Time
	// After behaves like time.After.
	After(d time.Duration) <-chan time.Time
	// NewTicker behaves like time.NewTicker.
	NewTicker(d time.Duration) Ticker
	// Sleep blocks for d.
	Sleep(d time.Duration)
}

// Ticker mirrors time.Ticker behind an interface.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// Real is the production clock.
type Real struct{}

func (Real) Now() time.Time                         { return time.Now() }
func (Real) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (Real) Sleep(d time.Duration)                  { time.Sleep(d) }
func (Real) NewTicker(d time.Duration) Ticker       { return realTicker{time.NewTicker(d)} }

type realTicker struct{ t *time.Ticker }

func (r realTicker) C() <-chan time.Time { return r.t.C }
func (r realTicker) Stop()               { r.t.Stop() }
