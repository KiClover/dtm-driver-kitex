package dtmkitex

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cloudwego/kitex/pkg/discovery"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"

	. "github.com/dtm-labs/dtmdriver-kitex/internal/testkit"
)

// drain runs fn until it returns true or the deadline passes, yielding to
// other goroutines between attempts. Used for cross-goroutine handoffs that
// have no synchronous hook.
func drain(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !fn() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for expected condition")
		}
		time.Sleep(1 * time.Millisecond)
	}
}

// tickStep is small relative to the 100ms refresh used by the tests, so a
// tick lands the fake clock on the refresh grid even when the resolver
// goroutine arms its timer slightly after the previous settle.
const tickStep = 10 * time.Millisecond

// settle gives the resolver goroutine time to finish the bookkeeping that
// follows a Resolve call (clock reads, state publication, timer arming)
// before the test touches the clock again.
func settle() { time.Sleep(2 * time.Millisecond) }

// tick advances the fake clock until the resolver completes exactly one more
// Resolve call, then lets it settle. Timer arming happens after the state
// update on the resolver goroutine, so the first Advance may find no timer;
// the step-sized retry covers that lag.
func tick(t *testing.T, disc *Discovery, clock *Clock) {
	t.Helper()
	before := disc.ResolveCalls()
	deadline := time.Now().Add(2 * time.Second)
	for {
		clock.Advance(tickStep)
		if disc.ResolveCalls() > before {
			settle()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("resolver did not resolve again (calls=%d)", disc.ResolveCalls())
		}
		time.Sleep(1 * time.Millisecond)
	}
}

// newTestResolver builds a resolver against a fake discovery backend and
// fake ClientConn, registering Close as cleanup.
func newTestResolver(t *testing.T, disc *Discovery, refresh, emptyGrace time.Duration) (*kitexResolver, *ClientConn, *Clock) {
	t.Helper()
	cc := NewClientConn()
	clock := NewClock()
	r := newKitexResolver(cc, disc, disc.Desc(), refresh, emptyGrace, clock)
	t.Cleanup(r.Close)
	return r, cc, clock
}

func TestResolverInitialPublishesState(t *testing.T) {
	disc := NewDiscovery("test", "svc",
		NewInstance("10.0.0.2:8000", 30),
		NewInstance("10.0.0.1:8000", 10),
	)
	r, cc, _ := newTestResolver(t, disc, 5*time.Second, 30*time.Second)

	drain(t, func() bool { return cc.StateCount() == 1 })
	st, ok := cc.LastState()
	if !ok {
		t.Fatal("no state published")
	}
	if len(st.Addresses) != 2 {
		t.Fatalf("got %d addresses, want 2", len(st.Addresses))
	}
	// Addresses are sorted by Addr regardless of discovery order.
	if st.Addresses[0].Addr != "10.0.0.1:8000" || st.Addresses[1].Addr != "10.0.0.2:8000" {
		t.Fatalf("unexpected address order: %v", st.Addresses)
	}
	if w, ok := weightOf(st.Addresses[1].Attributes); !ok || w != 30 {
		t.Fatalf("weight attribute = (%d, %v), want (30, true)", w, ok)
	}
	// The resolver never calls Target itself; Build computes the desc once.
	if r.desc != "svc" {
		t.Fatalf("desc = %q, want discovery service name only", r.desc)
	}
}

func TestResolverInitialFailureRetriesWithBackoff(t *testing.T) {
	boom := errors.New("registry down")
	disc := NewDiscovery("test", "svc")
	disc.SetScript(boom)
	r, cc, clock := newTestResolver(t, disc, 100*time.Millisecond, 30*time.Second)
	_ = r

	// The initial resolve fails on the resolver goroutine: ReportError, no
	// state, backoff timer armed.
	drain(t, func() bool { return cc.ErrorCount() == 1 })
	if cc.StateCount() != 0 {
		t.Fatalf("published %d states on failure path, want 0", cc.StateCount())
	}

	// Second attempt still fails; the backoff timer drives it.
	tick(t, disc, clock)
	if cc.ErrorCount() != 2 {
		t.Fatalf("ErrorCount = %d, want 2", cc.ErrorCount())
	}
	if cc.StateCount() != 0 {
		t.Fatalf("published %d states while failing, want 0", cc.StateCount())
	}

	// Recovery publishes the state.
	disc.SetScript(nil, NewInstance("10.0.0.1:8000", 10))
	tick(t, disc, clock)
	if cc.StateCount() != 1 {
		t.Fatalf("StateCount = %d, want 1 after recovery", cc.StateCount())
	}
}

func TestResolverDeduplicatesAndIgnoresBadWeights(t *testing.T) {
	disc := NewDiscovery("test", "svc",
		NewInstance("10.0.0.1:8000", 0),
		NewInstance("10.0.0.1:8000", 50),
		NewInstance("10.0.0.2:8000", -3),
	)
	r, cc, _ := newTestResolver(t, disc, 5*time.Second, 30*time.Second)
	_ = r

	drain(t, func() bool { return cc.StateCount() == 1 })
	st, _ := cc.LastState()
	if len(st.Addresses) != 2 {
		t.Fatalf("got %d addresses, want 2 (deduplicated)", len(st.Addresses))
	}
	if w, _ := weightOf(st.Addresses[0].Attributes); w != 50 {
		t.Fatalf("addr0 weight = %d, want 50 (max of duplicates)", w)
	}
	if w, _ := weightOf(st.Addresses[1].Attributes); w != 10 {
		t.Fatalf("addr1 weight = %d, want 10 (default for non-positive)", w)
	}
}

func TestResolverSameStateNotRepublished(t *testing.T) {
	disc := NewDiscovery("test", "svc", NewInstance("10.0.0.1:8000", 10))
	r, cc, clock := newTestResolver(t, disc, 100*time.Millisecond, 30*time.Second)
	_ = r

	drain(t, func() bool { return cc.StateCount() == 1 })

	// One full refresh cycle with an unchanged result set.
	tick(t, disc, clock)
	if cc.StateCount() != 1 {
		t.Fatalf("state republished without change: %d states", cc.StateCount())
	}

	// A longer window may fire any number of refreshes; none may publish.
	clock.Advance(500 * time.Millisecond)
	settle()
	if disc.ResolveCalls() < 3 {
		t.Fatalf("refreshes did not run (calls=%d)", disc.ResolveCalls())
	}
	if cc.StateCount() != 1 {
		t.Fatalf("state republished without change: %d states", cc.StateCount())
	}
}

func TestResolverRefreshPicksUpChanges(t *testing.T) {
	disc := NewDiscovery("test", "svc", NewInstance("10.0.0.1:8000", 10))
	r, cc, clock := newTestResolver(t, disc, 100*time.Millisecond, 30*time.Second)
	_ = r

	drain(t, func() bool { return cc.StateCount() == 1 })

	disc.SetScript(nil, NewInstance("10.0.0.1:8000", 10), NewInstance("10.0.0.2:8000", 10))
	tick(t, disc, clock)
	if cc.StateCount() != 2 {
		t.Fatalf("StateCount = %d, want 2 after instance added", cc.StateCount())
	}
	st, _ := cc.LastState()
	if got := len(st.Addresses); got != 2 {
		t.Fatalf("refreshed state has %d addresses, want 2", got)
	}

	// Instance removed -> new state published again.
	disc.SetScript(nil, NewInstance("10.0.0.1:8000", 10))
	tick(t, disc, clock)
	if cc.StateCount() != 3 {
		t.Fatalf("StateCount = %d, want 3 after instance removed", cc.StateCount())
	}
	st, _ = cc.LastState()
	if got := len(st.Addresses); got != 1 {
		t.Fatalf("refreshed state has %d addresses, want 1", got)
	}
}

func TestResolverRefreshErrorKeepsLastState(t *testing.T) {
	disc := NewDiscovery("test", "svc", NewInstance("10.0.0.1:8000", 10))
	r, cc, clock := newTestResolver(t, disc, 100*time.Millisecond, 30*time.Second)
	_ = r

	drain(t, func() bool { return cc.StateCount() == 1 })

	disc.SetScript(errors.New("registry down"))
	tick(t, disc, clock)
	if cc.ErrorCount() != 1 {
		t.Fatalf("ErrorCount = %d, want 1", cc.ErrorCount())
	}
	if cc.StateCount() != 1 {
		t.Fatalf("last state dropped on refresh error")
	}

	// Recovery returns the same instance set; identical state is not resent.
	disc.SetScript(nil, NewInstance("10.0.0.1:8000", 10))
	tick(t, disc, clock)
	if cc.StateCount() != 1 {
		t.Fatalf("recovery republished identical state: %d states", cc.StateCount())
	}
	if cc.ErrorCount() != 1 {
		t.Fatalf("recovery reported an error: %d", cc.ErrorCount())
	}

	// A changed set after recovery publishes normally.
	disc.SetScript(nil, NewInstance("10.0.0.9:8000", 10))
	tick(t, disc, clock)
	if cc.StateCount() != 2 {
		t.Fatalf("StateCount = %d, want 2 after changed set", cc.StateCount())
	}
}

func TestResolverEmptyGraceHoldsThenClears(t *testing.T) {
	disc := NewDiscovery("test", "svc", NewInstance("10.0.0.1:8000", 10))
	r, cc, clock := newTestResolver(t, disc, 100*time.Millisecond, 300*time.Millisecond)
	_ = r

	drain(t, func() bool { return cc.StateCount() == 1 })

	// Registry authoritatively reports empty at t=100ms; the grace window
	// (300ms) starts there.
	disc.SetScript(nil)
	tick(t, disc, clock)
	if got := len(cc.Addresses()); got != 1 {
		t.Fatalf("state cleared before empty grace elapsed: %d addresses", got)
	}

	// t=200ms: still inside the grace window.
	tick(t, disc, clock)
	if cc.StateCount() != 1 {
		t.Fatalf("empty state published inside grace window: %d states", cc.StateCount())
	}
	if got := len(cc.Addresses()); got != 1 {
		t.Fatalf("held state changed inside grace window: %d addresses", got)
	}

	// Keep ticking: the resolve at t=300ms is 200ms into the window and
	// still holds; the resolve at t=400ms observes the full 300ms elapsed
	// and publishes the empty state. (A single large Advance cannot be
	// used: the resolver re-arms its timer after Advance's scan, so the
	// chain stops at the first fire.)
	tick(t, disc, clock) // t=300ms, still holding
	if cc.StateCount() != 1 {
		t.Fatalf("empty state published before grace elapsed: %d states", cc.StateCount())
	}
	published := false
	for i := 0; i < 8; i++ {
		tick(t, disc, clock)
		if cc.StateCount() == 2 {
			published = true
			break
		}
	}
	if !published {
		t.Fatalf("empty state never published after grace elapsed: %d states", cc.StateCount())
	}
	if got := len(cc.Addresses()); got != 0 {
		t.Fatalf("state not cleared after empty grace: %d addresses", got)
	}
}

func TestResolverEmptyGraceResetByRecovery(t *testing.T) {
	disc := NewDiscovery("test", "svc", NewInstance("10.0.0.1:8000", 10))
	r, cc, clock := newTestResolver(t, disc, 100*time.Millisecond, 500*time.Millisecond)
	_ = r

	drain(t, func() bool { return cc.StateCount() == 1 })

	// Empty at t=100ms: grace window starts.
	disc.SetScript(nil)
	tick(t, disc, clock)
	if cc.StateCount() != 1 {
		t.Fatalf("empty published inside grace window: %d states", cc.StateCount())
	}

	// Recovery at t=200ms before the window closes: state kept, grace reset.
	disc.SetScript(nil, NewInstance("10.0.0.1:8000", 10))
	tick(t, disc, clock)
	if cc.StateCount() != 1 {
		t.Fatalf("grace recovery republished: %d states", cc.StateCount())
	}

	// Empty again at t=300ms: a fresh grace window starts from here.
	disc.SetScript(nil)
	tick(t, disc, clock)
	tick(t, disc, clock) // t=400ms, still inside the fresh window
	if cc.StateCount() != 1 {
		t.Fatalf("empty published before the fresh grace window elapsed")
	}

	// Far past the fresh window the empty state publishes; each tick is one
	// refresh cycle, so the publish lands on the first resolve that sees
	// the full 500ms elapsed.
	published := false
	for i := 0; i < 8; i++ {
		tick(t, disc, clock)
		if cc.StateCount() == 2 {
			published = true
			break
		}
	}
	if !published {
		t.Fatalf("empty state never published after fresh grace window: %d states", cc.StateCount())
	}
	if got := len(cc.Addresses()); got != 0 {
		t.Fatalf("state not cleared after fresh grace window: %d addresses", got)
	}
}

func TestResolverUpdateStateErrorBacksOff(t *testing.T) {
	disc := NewDiscovery("test", "svc", NewInstance("10.0.0.1:8000", 10))
	cc := NewClientConn()
	cc.SetUpdateStateError(errors.New("clientconn closed"))
	clock := NewClock()
	r := newKitexResolver(cc, disc, disc.Desc(), 100*time.Millisecond, 30*time.Second, clock)
	t.Cleanup(r.Close)

	// The initial publish attempt fails; the fake records it anyway.
	drain(t, func() bool { return cc.StateCount() == 1 })

	// Two more attempts through the backoff timer: each is recorded by the
	// fake but none surfaces through ReportError.
	tick(t, disc, clock)
	tick(t, disc, clock)
	if got := cc.StateCount(); got != 3 {
		t.Fatalf("StateCount = %d, want 3 (retry attempts recorded)", got)
	}
	if cc.ErrorCount() != 0 {
		t.Fatalf("UpdateState failure surfaced as ReportError")
	}
}

func TestResolverResolveNowCoalesces(t *testing.T) {
	disc := NewDiscovery("test", "svc", NewInstance("10.0.0.1:8000", 10))
	r, cc, _ := newTestResolver(t, disc, time.Hour, 30*time.Second)

	drain(t, func() bool { return cc.StateCount() == 1 })

	// Block the resolver inside Resolve so that a burst of ResolveNow
	// signals can pile up while nobody consumes them: the channel (buffer
	// 1) must keep exactly one pending signal and drop the rest.
	entered := make(chan struct{})
	gate := make(chan struct{})
	var once sync.Once
	disc.SetResolveHook(func(ctx context.Context, desc string) error {
		once.Do(func() {
			close(entered)
			<-gate
		})
		return nil
	})
	r.ResolveNow(resolver.ResolveNowOptions{}) // wake and block in Resolve
	<-entered

	// Burst: only one of these can be pending while Resolve is blocked.
	for i := 0; i < 9; i++ {
		r.ResolveNow(resolver.ResolveNowOptions{})
	}
	close(gate)

	// The blocked resolve completes; the single queued signal triggers
	// exactly one follow-up resolve. Nine bursts must not fan out.
	drain(t, func() bool { return disc.ResolveCalls() == 3 })
	settle()
	if got := disc.ResolveCalls(); got != 3 {
		t.Fatalf("ResolveCalls = %d, want 3 (burst must coalesce to one queued signal)", got)
	}
	if cc.StateCount() != 1 {
		t.Fatalf("coalesced re-resolve published: %d states", cc.StateCount())
	}

	// The wake mechanism is live: a changed set resolves and publishes.
	disc.SetResolveHook(nil)
	disc.SetScript(nil, NewInstance("10.0.0.9:8000", 10))
	r.ResolveNow(resolver.ResolveNowOptions{})
	drain(t, func() bool { return cc.StateCount() == 2 })
	if a := cc.Addresses()[0].Addr; a != "10.0.0.9:8000" {
		t.Fatalf("ResolveNow picked up %q, want 10.0.0.9:8000", a)
	}
}

func TestResolverCloseCancelsBlockedResolve(t *testing.T) {
	disc := NewDiscovery("test", "svc")
	blocked := make(chan struct{})
	disc.SetResolveHook(func(ctx context.Context, desc string) error {
		close(blocked)
		<-ctx.Done() // simulate a wedged registry call
		return ctx.Err()
	})
	cc := NewClientConn()
	r := newKitexResolver(cc, disc, "svc", time.Hour, 30*time.Second, NewClock())

	<-blocked
	r.Close() // must return promptly even though Resolve is blocked

	select {
	case <-r.done:
	default:
		t.Fatal("resolver goroutine did not exit after Close")
	}
	r.Close() // second Close is a no-op
}

func TestResolverCloseWhileWaiting(t *testing.T) {
	disc := NewDiscovery("test", "svc", NewInstance("10.0.0.1:8000", 10))
	r, cc, _ := newTestResolver(t, disc, time.Hour, 30*time.Second)
	drain(t, func() bool { return cc.StateCount() == 1 })

	r.Close()
	select {
	case <-r.done:
	default:
		t.Fatal("resolver goroutine did not exit after Close")
	}
}

func TestResolverMultipleTargetsIsolated(t *testing.T) {
	d1 := NewDiscovery("test", "svc1", NewInstance("10.0.0.1:8000", 10))
	d2 := NewDiscovery("test", "svc2", NewInstance("10.0.0.2:8000", 10))
	cc1, cc2 := NewClientConn(), NewClientConn()
	clock := NewClock()
	r1 := newKitexResolver(cc1, d1, "svc1", 100*time.Millisecond, 30*time.Second, clock)
	r2 := newKitexResolver(cc2, d2, "svc2", 100*time.Millisecond, 30*time.Second, clock)
	defer r1.Close()
	defer r2.Close()

	drain(t, func() bool { return cc1.StateCount() == 1 && cc2.StateCount() == 1 })
	if a := cc1.Addresses()[0].Addr; a != "10.0.0.1:8000" {
		t.Fatalf("svc1 got %q", a)
	}
	if a := cc2.Addresses()[0].Addr; a != "10.0.0.2:8000" {
		t.Fatalf("svc2 got %q", a)
	}

	// Shutting one down does not stop the other.
	r1.Close()
	d2.SetScript(nil, NewInstance("10.0.0.2:8000", 10), NewInstance("10.0.0.3:8000", 10))
	tick(t, d2, clock)
	if cc2.StateCount() != 2 {
		t.Fatalf("cc2 StateCount = %d, want 2", cc2.StateCount())
	}
	if cc1.StateCount() != 1 {
		t.Fatalf("closed r1 affected cc1: %d states", cc1.StateCount())
	}
}

func TestResolverClockInjectorUsed(t *testing.T) {
	// The resolver must use the injected clock for both backoff and refresh
	// timers; with a stopped clock and no ResolveNow, nothing progresses.
	disc := NewDiscovery("test", "svc", NewInstance("10.0.0.1:8000", 10))
	r, cc, _ := newTestResolver(t, disc, 100*time.Millisecond, 30*time.Second)
	_ = r
	drain(t, func() bool { return cc.StateCount() == 1 })

	disc.SetScript(nil, NewInstance("10.0.0.1:8000", 10), NewInstance("10.0.0.2:8000", 10))
	time.Sleep(20 * time.Millisecond)
	if cc.StateCount() != 1 {
		t.Fatalf("state advanced without timer tick or ResolveNow")
	}
}

func TestStateFromResultSortingAndWeights(t *testing.T) {
	result := discovery.Result{
		Cacheable: true,
		CacheKey:  "svc",
		Instances: []discovery.Instance{
			NewInstance("10.0.0.2:8000", 5),
			NewInstance("10.0.0.1:8000", 7),
			NewInstance("10.0.0.3:8000", 0),
		},
	}
	st := stateFromResult(result)
	if len(st.Addresses) != 3 {
		t.Fatalf("got %d addresses, want 3", len(st.Addresses))
	}
	for i, want := range []string{"10.0.0.1:8000", "10.0.0.2:8000", "10.0.0.3:8000"} {
		if st.Addresses[i].Addr != want {
			t.Fatalf("addr[%d] = %q, want %q", i, st.Addresses[i].Addr, want)
		}
	}
	if w, _ := weightOf(st.Addresses[2].Attributes); w != 10 {
		t.Fatalf("non-positive weight not defaulted: %d", w)
	}
}

func TestNextBackoffProgression(t *testing.T) {
	var d time.Duration
	want := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1600 * time.Millisecond,
		3200 * time.Millisecond,
		6400 * time.Millisecond,
		12800 * time.Millisecond,
		25600 * time.Millisecond,
		51200 * time.Millisecond,
		maxBackoff,
	}
	for i, w := range want {
		d = nextBackoff(d)
		if d != w {
			t.Fatalf("step %d: nextBackoff = %v, want %v", i, d, w)
		}
	}
	if d = nextBackoff(d); d != maxBackoff {
		t.Fatalf("backoff did not stay capped: %v", d)
	}
}

func TestSameStateComparison(t *testing.T) {
	mk := func(addr string, w int) resolver.State {
		return resolver.State{Addresses: []resolver.Address{
			{Addr: addr, Attributes: attributes.New(weightAttribute, w)},
		}}
	}
	a := mk("10.0.0.1:1", 10)
	if !sameState(a, mk("10.0.0.1:1", 10)) {
		t.Fatal("identical states compared unequal")
	}
	if sameState(a, mk("10.0.0.1:1", 20)) {
		t.Fatal("different weights compared equal")
	}
	if sameState(a, mk("10.0.0.2:1", 10)) {
		t.Fatal("different addresses compared equal")
	}
	two := resolver.State{Addresses: []resolver.Address{
		{Addr: "10.0.0.1:1"}, {Addr: "10.0.0.2:1"},
	}}
	if sameState(a, two) {
		t.Fatal("different address counts compared equal")
	}
	if !sameState(resolver.State{Addresses: []resolver.Address{}}, resolver.State{Addresses: []resolver.Address{}}) {
		t.Fatal("empty states compared unequal")
	}
}

func TestWeightOfNilAttributes(t *testing.T) {
	if _, ok := weightOf(nil); ok {
		t.Fatal("nil attributes reported a weight")
	}
	if _, ok := weightOf(resolver.Address{}.Attributes); ok {
		t.Fatal("empty attributes reported a weight")
	}
	if _, ok := weightOf(attributes.New("other", 1)); ok {
		t.Fatal("unrelated attribute reported a weight")
	}
	if w, ok := weightOf(attributes.New(weightAttribute, 7)); !ok || w != 7 {
		t.Fatalf("weightOf = (%d, %v), want (7, true)", w, ok)
	}
}
