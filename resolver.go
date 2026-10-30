package dtmkitex

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/cloudwego/kitex/pkg/discovery"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
)

const (
	// maxBackoff caps the exponential retry delay after failures.
	maxBackoff = time.Minute
	// weightAttribute stores the Kitex instance weight on each address. The
	// standard round_robin balancer does not read it; the attribute exists so
	// a custom balancer could, without this resolver promising weighted
	// balancing itself.
	weightAttribute = "kitex.weight"
	// defaultInstanceWeight mirrors Kitex discovery.DefaultWeight, used when
	// an instance reports a non-positive weight.
	defaultInstanceWeight = 10
)

// resolverClock abstracts time so tests can drive timers deterministically.
type resolverClock interface {
	Now() time.Time
	AfterFunc(d time.Duration, fn func()) func() bool
}

// realClock uses the process wall clock and timer wheel.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) AfterFunc(d time.Duration, fn func()) func() bool {
	t := time.AfterFunc(d, fn)
	return func() bool { return t.Stop() }
}

// kitexResolver adapts one Kitex discovery.Resolver to one grpc-go
// resolver.ClientConn. Build constructs it; a single goroutine owns the
// state machine below.
type kitexResolver struct {
	// wired at construction, immutable afterwards
	cc         resolver.ClientConn
	disc       discovery.Resolver
	desc       string
	refresh    time.Duration
	emptyGrace time.Duration
	clock      resolverClock
	ctx        context.Context
	cancel     context.CancelFunc
	done       chan struct{} // closed when run() returns

	// resolveNow wakes run() for a re-resolve; capacity 1 coalesces
	// concurrent ResolveNow calls and timer ticks into at most one pending
	// signal.
	resolveNow chan struct{}

	mu     sync.Mutex
	closed bool
}

// newKitexResolver wires the adapter and starts its single goroutine. The
// initial Resolve happens on that goroutine: a transient registry failure
// reports to the ClientConn and retries with backoff instead of failing
// Build, so a registry that is briefly down does not wedge every RPC.
func newKitexResolver(cc resolver.ClientConn, disc discovery.Resolver, desc string, refresh, emptyGrace time.Duration, clock resolverClock) *kitexResolver {
	ctx, cancel := context.WithCancel(context.Background())
	r := &kitexResolver{
		cc:         cc,
		disc:       disc,
		desc:       desc,
		refresh:    refresh,
		emptyGrace: emptyGrace,
		clock:      clock,
		ctx:        ctx,
		cancel:     cancel,
		done:       make(chan struct{}),
		resolveNow: make(chan struct{}, 1),
	}
	go r.run()
	return r
}

// signalResolveNow queues a re-resolve request without blocking. It is safe
// to call from any goroutine, including timer callbacks and after Close.
func (r *kitexResolver) signalResolveNow() {
	select {
	case r.resolveNow <- struct{}{}:
	default:
	}
}

// run is the single state-machine goroutine. It owns every Resolve call so
// the discovery backend never sees concurrent requests for one target.
func (r *kitexResolver) run() {
	defer close(r.done)

	var (
		lastState  *resolver.State
		backoff    time.Duration
		hadEmpty   bool
		emptySince time.Time
	)

	for {
		result, err := r.disc.Resolve(r.ctx, r.desc)
		if r.ctx.Err() != nil {
			return
		}

		wait := r.refresh
		switch {
		case err != nil:
			// Keep the last good state; tell grpc-go so it can log.
			r.cc.ReportError(err)
			hadEmpty = false
			backoff = nextBackoff(backoff)
			wait = backoff
		case len(result.Instances) == 0:
			// Authoritative empty: hold the last good state through the
			// grace window, then publish an empty state.
			if !hadEmpty {
				hadEmpty = true
				emptySince = r.clock.Now()
			}
			backoff = 0
			if r.clock.Now().Sub(emptySince) >= r.emptyGrace {
				state := resolver.State{Addresses: []resolver.Address{}}
				if lastState == nil || !sameState(*lastState, state) {
					if uerr := r.cc.UpdateState(state); uerr != nil {
						backoff = nextBackoff(backoff)
						wait = backoff
						break
					}
					lastState = &state
				}
			}
		default:
			hadEmpty = false
			emptySince = time.Time{}
			backoff = 0
			state := stateFromResult(result)
			if lastState == nil || !sameState(*lastState, state) {
				if uerr := r.cc.UpdateState(state); uerr != nil {
					// grpc-go asks us to try again later.
					backoff = nextBackoff(backoff)
					wait = backoff
					break
				}
				lastState = &state
			}
		}

		// Park until the next tick (periodic refresh or backoff), an
		// external ResolveNow, or shutdown. One timer is armed at a time.
		stop := r.clock.AfterFunc(wait, func() { r.signalResolveNow() })
		select {
		case <-r.resolveNow:
		case <-r.ctx.Done():
			stop()
			return
		}
		stop()
	}
}

// ResolveNow requests an immediate re-resolve. Concurrent calls coalesce
// into one pending signal and it never blocks.
func (r *kitexResolver) ResolveNow(resolver.ResolveNowOptions) {
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if closed {
		return
	}
	r.signalResolveNow()
}

// Close cancels the state machine and waits for it to exit. It is safe to
// call more than once.
func (r *kitexResolver) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	r.mu.Unlock()

	r.cancel()
	<-r.done
}

// nextBackoff doubles the delay up to maxBackoff, starting at 100ms.
func nextBackoff(prev time.Duration) time.Duration {
	if prev == 0 {
		return 100 * time.Millisecond
	}
	if prev*2 > maxBackoff {
		return maxBackoff
	}
	return prev * 2
}

// stateFromResult maps a Kitex discovery result to a grpc-go state.
// Addresses are deduplicated (highest weight wins) and sorted so consecutive
// results compare deterministically.
func stateFromResult(result discovery.Result) resolver.State {
	weights := make(map[string]int, len(result.Instances))
	for _, inst := range result.Instances {
		addr := inst.Address().String()
		w := inst.Weight()
		if w <= 0 {
			w = defaultInstanceWeight
		}
		if cur, ok := weights[addr]; !ok || w > cur {
			weights[addr] = w
		}
	}
	addrs := make([]resolver.Address, 0, len(weights))
	for addr, w := range weights {
		addrs = append(addrs, resolver.Address{
			Addr:       addr,
			Attributes: attributes.New(weightAttribute, w),
		})
	}
	sort.Slice(addrs, func(i, j int) bool { return addrs[i].Addr < addrs[j].Addr })
	return resolver.State{Addresses: addrs}
}

// sameState reports whether two states carry identical address sets.
func sameState(a, b resolver.State) bool {
	if len(a.Addresses) != len(b.Addresses) {
		return false
	}
	for i := range a.Addresses {
		if a.Addresses[i].Addr != b.Addresses[i].Addr {
			return false
		}
		wa, ok1 := weightOf(a.Addresses[i].Attributes)
		wb, ok2 := weightOf(b.Addresses[i].Attributes)
		if !ok1 || !ok2 || wa != wb {
			return false
		}
	}
	return true
}

func weightOf(attrs *attributes.Attributes) (int, bool) {
	if attrs == nil {
		return 0, false
	}
	w, ok := attrs.Value(weightAttribute).(int)
	return w, ok
}
