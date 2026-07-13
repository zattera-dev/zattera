package clock

import (
	"testing"
	"time"
)

func TestFakeAfter(t *testing.T) {
	f := NewFake()
	ch := f.After(5 * time.Second)
	select {
	case <-ch:
		t.Fatal("fired early")
	default:
	}
	f.Advance(4 * time.Second)
	select {
	case <-ch:
		t.Fatal("fired at 4s")
	default:
	}
	f.Advance(2 * time.Second)
	select {
	case ts := <-ch:
		if got := ts.Sub(NewFake().Now()); got != 5*time.Second {
			t.Fatalf("fired at +%v, want +5s", got)
		}
	default:
		t.Fatal("did not fire")
	}
}

func TestFakeTicker(t *testing.T) {
	f := NewFake()
	tk := f.NewTicker(time.Second)
	defer tk.Stop()

	fired := 0
	for i := 0; i < 3; i++ {
		f.Advance(time.Second)
		select {
		case <-tk.C():
			fired++
		default:
		}
	}
	if fired != 3 {
		t.Fatalf("ticker fired %d times, want 3", fired)
	}

	tk.Stop()
	f.Advance(5 * time.Second)
	select {
	case <-tk.C():
		t.Fatal("stopped ticker fired")
	default:
	}
}

func TestFakeOrderedFiring(t *testing.T) {
	f := NewFake()
	a := f.After(1 * time.Second)
	b := f.After(2 * time.Second)
	f.Advance(3 * time.Second)

	ta := <-a
	tb := <-b
	if !ta.Before(tb) {
		t.Fatalf("timers fired out of order: %v vs %v", ta, tb)
	}
	if f.Now().Sub(NewFake().Now()) != 3*time.Second {
		t.Fatal("clock did not land on target")
	}
}
