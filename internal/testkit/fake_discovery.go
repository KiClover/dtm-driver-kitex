// Package testkit provides fakes for resolver tests: a scripted Kitex
// discovery resolver, a recording grpc-go ClientConn, and an injectable
// clock with a manual timer queue.
package testkit

import (
	"context"
	"fmt"
	"sync"

	"github.com/cloudwego/kitex/pkg/discovery"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
)

// Discovery is a scripted Kitex discovery.Resolver.
type Discovery struct {
	mu sync.Mutex

	name    string
	desc    string
	targets int
	resolves int

	resolveErr error
	instances  []discovery.Instance

	// resolveHook runs inside Resolve with the caller's ctx; use it to block
	// until Close cancels the context, or to inject delays.
	resolveHook func(ctx context.Context, desc string) error
}

var _ discovery.Resolver = (*Discovery)(nil)

// NewDiscovery creates a fake resolver whose Target accepts exactly desc and
// whose Resolve returns the scripted outcome.
func NewDiscovery(name, desc string, instances ...discovery.Instance) *Discovery {
	return &Discovery{name: name, desc: desc, instances: append([]discovery.Instance(nil), instances...)}
}

// SetScript atomically replaces the Resolve outcome.
func (d *Discovery) SetScript(err error, instances ...discovery.Instance) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.resolveErr = err
	d.instances = append([]discovery.Instance(nil), instances...)
}

// SetResolveHook installs a hook invoked before every Resolve return.
func (d *Discovery) SetResolveHook(hook func(ctx context.Context, desc string) error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.resolveHook = hook
}

// Target records the call and returns the scripted description. It panics if
// the endpoint differs from the scripted discovery service, so tests fail
// loudly when the adapter feeds anything other than the discovery service.
func (d *Discovery) Target(ctx context.Context, target rpcinfo.EndpointInfo) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.targets++
	if got := target.ServiceName(); got != d.desc {
		panic(fmt.Sprintf("testkit.Discovery.Target: endpoint %q, want %q", got, d.desc))
	}
	return d.desc
}

// Resolve returns the scripted result after running the optional hook.
func (d *Discovery) Resolve(ctx context.Context, desc string) (discovery.Result, error) {
	d.mu.Lock()
	hook := d.resolveHook
	err := d.resolveErr
	instances := append([]discovery.Instance(nil), d.instances...)
	d.resolves++
	d.mu.Unlock()

	if hook != nil {
		if herr := hook(ctx, desc); herr != nil {
			return discovery.Result{}, herr
		}
	}
	if err != nil {
		return discovery.Result{}, err
	}
	return discovery.Result{Cacheable: true, CacheKey: desc, Instances: instances}, nil
}

// Diff delegates to discovery.DefaultDiff.
func (d *Discovery) Diff(cacheKey string, prev, next discovery.Result) (discovery.Change, bool) {
	return discovery.DefaultDiff(cacheKey, prev, next)
}

// Name returns the scripted resolver name.
func (d *Discovery) Name() string { return d.name }

// Desc returns the scripted discovery service description.
func (d *Discovery) Desc() string { return d.desc }

// TargetCalls returns how many times Target was invoked.
func (d *Discovery) TargetCalls() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.targets
}

// ResolveCalls returns how many times Resolve was invoked.
func (d *Discovery) ResolveCalls() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.resolves
}

// NewInstance builds a Kitex instance with a TCP address.
func NewInstance(address string, weight int) discovery.Instance {
	return discovery.NewInstance("tcp", address, weight, nil)
}
