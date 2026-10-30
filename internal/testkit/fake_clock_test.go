package testkit

import (
	"testing"
	"time"
)

func TestClockAdvanceFiresInOrder(t *testing.T) {
	c := NewClock()
	var fired []string
	c.AfterFunc(30*time.Millisecond, func() { fired = append(fired, "late") })
	c.AfterFunc(10*time.Millisecond, func() { fired = append(fired, "early") })

	c.Advance(10 * time.Millisecond)
	if len(fired) != 1 || fired[0] != "early" {
		t.Fatalf("after 10ms fired = %v, want [early]", fired)
	}
	c.Advance(20 * time.Millisecond)
	if len(fired) != 2 || fired[0] != "early" || fired[1] != "late" {
		t.Fatalf("after 30ms fired = %v, want [early late]", fired)
	}
	if c.PendingTimers() != 0 {
		t.Fatalf("pending timers = %d, want 0", c.PendingTimers())
	}
}

func TestClockStopCancels(t *testing.T) {
	c := NewClock()
	fired := false
	stop := c.AfterFunc(time.Second, func() { fired = true })
	if !stop() {
		t.Fatal("stop returned false before firing")
	}
	c.Advance(2 * time.Second)
	if fired {
		t.Fatal("timer fired after stop")
	}
	if stop() {
		t.Fatal("second stop returned true")
	}
}

func TestClockAdvanceFiresTimersScheduledDuringAdvance(t *testing.T) {
	c := NewClock()
	var second time.Time
	c.AfterFunc(5*time.Millisecond, func() {
		c.AfterFunc(5*time.Millisecond, func() { second = c.Now() })
	})
	c.Advance(10 * time.Millisecond)
	if second.IsZero() {
		t.Fatal("chained timer did not fire")
	}
	if c.PendingTimers() != 0 {
		t.Fatalf("pending timers = %d, want 0", c.PendingTimers())
	}
}
