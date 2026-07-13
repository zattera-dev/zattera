package state

import "sync"

// Change identifies one mutated object.
type Change struct {
	Kind Kind
	ID   string
}

// Hub fans out change notifications to subscribers without ever blocking the
// FSM apply path. Changes are coalesced per subscriber (a set keyed by
// Kind+ID); the subscriber gets a level-triggered signal and drains the
// pending set when it is ready. Slow subscribers lose granularity, never
// correctness — control loops re-read state anyway.
type Hub struct {
	mu   sync.Mutex
	subs map[*Subscription]struct{}
}

func newHub() *Hub {
	return &Hub{subs: map[*Subscription]struct{}{}}
}

// Subscription receives coalesced change notifications.
type Subscription struct {
	hub    *Hub
	kinds  map[Kind]struct{} // nil = all kinds
	mu     sync.Mutex
	pending map[Change]struct{}
	signal chan struct{} // cap 1, level-triggered
	closed bool
}

func (h *Hub) subscribe(kinds []Kind) *Subscription {
	sub := &Subscription{
		hub:     h,
		pending: map[Change]struct{}{},
		signal:  make(chan struct{}, 1),
	}
	if len(kinds) > 0 {
		sub.kinds = make(map[Kind]struct{}, len(kinds))
		for _, k := range kinds {
			sub.kinds[k] = struct{}{}
		}
	}
	h.mu.Lock()
	h.subs[sub] = struct{}{}
	h.mu.Unlock()
	return sub
}

func (h *Hub) publish(c Change) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for sub := range h.subs {
		sub.offer(c)
	}
}

func (s *Subscription) offer(c Change) {
	if s.kinds != nil {
		if _, ok := s.kinds[c.Kind]; !ok {
			return
		}
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.pending[c] = struct{}{}
	s.mu.Unlock()
	select {
	case s.signal <- struct{}{}:
	default:
	}
}

// Notify returns a channel that receives a value whenever there are pending
// changes. It is level-triggered with capacity 1: after receiving, call
// Drain to collect what changed, then wait again.
func (s *Subscription) Notify() <-chan struct{} {
	return s.signal
}

// Drain returns and clears the pending change set.
func (s *Subscription) Drain() []Change {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Change, 0, len(s.pending))
	for c := range s.pending {
		out = append(out, c)
	}
	s.pending = map[Change]struct{}{}
	return out
}

// Close unsubscribes. Safe to call multiple times.
func (s *Subscription) Close() {
	s.hub.mu.Lock()
	delete(s.hub.subs, s)
	s.hub.mu.Unlock()
	s.mu.Lock()
	s.closed = true
	s.pending = map[Change]struct{}{}
	s.mu.Unlock()
}
