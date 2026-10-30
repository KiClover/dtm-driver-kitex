package testkit

import (
	"sync"
	"time"
)

// Clock is a manual clock with a timer queue for deterministic resolver
// backoff tests. Timers fire only when the test advances time.
type Clock struct {
	mu   sync.Mutex
	now  time.Time
	seq  int
	tmap map[int]*timer
}

type timer struct {
	fireAt time.Time
	fn     func()
	called bool
}

// NewClock creates a clock starting at an arbitrary but fixed time.
func NewClock() *Clock {
	return &Clock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), tmap: make(map[int]*timer)}
}

// Now returns the current fake time.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// AfterFunc schedules fn to run when the clock advances past d. The returned
// stop function cancels it; stop returns false if the timer already fired.
func (c *Clock) AfterFunc(d time.Duration, fn func()) func() bool {
	c.mu.Lock()
	c.seq++
	id := c.seq
	t := &timer{fireAt: c.now.Add(d), fn: fn}
	c.tmap[id] = t
	c.mu.Unlock()

	return func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		if t.called {
			return false
		}
		t.called = true
		delete(c.tmap, id)
		return true
	}
}

// Advance moves the clock forward by d, firing every timer whose deadline is
// reached, in deadline order. The clock moves to each timer's fire time
// before running its fn, so a timer scheduled inside a callback with delay x
// fires x after that callback's fire time — matching real time semantics —
// and still fires within the same Advance call when due.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	target := c.now.Add(d)
	c.now = target
	c.mu.Unlock()

	for {
		c.mu.Lock()
		var next *timer
		var nextID int
		for id, t := range c.tmap {
			if t.fireAt.After(target) {
				continue
			}
			if next == nil || t.fireAt.Before(next.fireAt) {
				next, nextID = t, id
			}
		}
		if next != nil {
			delete(c.tmap, nextID)
			c.now = next.fireAt
		}
		c.mu.Unlock()

		if next == nil {
			return
		}
		next.fn()
	}
}

// PendingTimers returns how many timers are still scheduled.
func (c *Clock) PendingTimers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.tmap)
}
