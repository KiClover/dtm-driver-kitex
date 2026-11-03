package dtmkitex

import (
	"context"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/kitex/pkg/discovery"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	. "github.com/dtm-labs/dtmdriver-kitex/internal/testkit"
	"google.golang.org/grpc/resolver"
)

type multiServiceDiscovery struct{}

func (multiServiceDiscovery) Target(_ context.Context, target rpcinfo.EndpointInfo) string {
	return target.ServiceName()
}

func (multiServiceDiscovery) Resolve(_ context.Context, desc string) (discovery.Result, error) {
	return discovery.Result{Cacheable: true, CacheKey: desc}, nil
}

func (multiServiceDiscovery) Diff(cacheKey string, prev, next discovery.Result) (discovery.Change, bool) {
	return discovery.DefaultDiff(cacheKey, prev, next)
}

func (multiServiceDiscovery) Name() string { return "multi-service" }

// TestRegistryRawTarget pins the grpc.Dial wiring: grpc-go hands the builder
// a Target whose URL carries the backend endpoints (host) and the query
// options. Target.Endpoint() keeps only the path, so Build must reconstruct
// the registry URI from the URL fields or the endpoints and every option
// (refresh_interval, empty_grace, credentials) would be lost.
func TestRegistryRawTarget(t *testing.T) {
	cases := []struct {
		name  string
		dial  string
		query string
	}{
		{"full", "kitex-etcd://127.0.0.1:2379/dtm-server?refresh_interval=1s", "refresh_interval=1s"},
		{"no query", "kitex-consul://10.0.0.1:8500/dtm-server", ""},
		{"multi endpoint", "kitex-etcd://10.0.0.1:2379,10.0.0.2:2379/dtm-server?empty_grace=45s", "empty_grace=45s"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u, err := url.Parse(tc.dial)
			if err != nil {
				t.Fatal(err)
			}
			tgt := resolver.Target{URL: *u}
			// Document the grpc-go v1.59 contract: Endpoint() is the bare path.
			if got := tgt.Endpoint(); got != "dtm-server" {
				t.Fatalf("Endpoint() = %q, want bare path %q", got, "dtm-server")
			}
			if got := registryRawTarget(&tgt.URL); got != tc.dial {
				t.Errorf("registryRawTarget() = %q, want %q", got, tc.dial)
			}
		})
	}
}

// TestBuildViaDialTarget runs Build with a parsed dial target and a fake
// provider to prove the full kitex-etcd scheme path works end to end without
// a registry: the fake discovery is consulted exactly once (desc derivation)
// and the returned resolver starts its publish loop.
func TestBuildViaDialTarget(t *testing.T) {
	resetRegistrationState(t)
	reg := NewRegistry()
	installFakeProvider(t, SchemeEtcd, reg, nil)

	dial := "kitex-etcd://127.0.0.1:2379/dtm-build-target?refresh_interval=2s"
	u, err := url.Parse(dial)
	if err != nil {
		t.Fatal(err)
	}
	b := resolver.Get(SchemeEtcd)
	if b == nil {
		t.Fatal("kitex-etcd builder not registered")
	}
	cc := NewClientConn()
	r, err := b.Build(resolver.Target{URL: *u}, cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if r == nil {
		t.Fatal("Build returned nil resolver")
	}
	defer r.Close()

	// Drain: the empty authoritative result is held by empty grace, so no
	// state is expected; give the resolver goroutine a window to prove it
	// does not crash.
	<-time.After(20 * time.Millisecond)
	if n := cc.ErrorCount(); n > 0 {
		t.Errorf("unexpected ReportError calls: %d", n)
	}
}

func TestBuildAcceptsInlineCredentials(t *testing.T) {
	resetRegistrationState(t)
	installFakeProvider(t, SchemeEtcd, NewRegistry(), nil)
	u, err := url.Parse("kitex-etcd://127.0.0.1:2379/dtm-build-target?user=dtm&password=secret")
	if err != nil {
		t.Fatal(err)
	}
	r, err := resolver.Get(SchemeEtcd).Build(resolver.Target{URL: *u}, NewClientConn(), resolver.BuildOptions{})
	if err != nil {
		t.Fatalf("Build with URI credentials: %v", err)
	}
	r.Close()
}

func TestDiscoveryCacheSeparatesDelimiterContainingCredentials(t *testing.T) {
	resetRegistrationState(t)
	installFakeProvider(t, SchemeEtcd, NewRegistry(), nil)
	dc := mustConfig(t, SchemeEtcd+"://127.0.0.1:2379/svc")

	first, err := defaultDiscoveries.get(dc, credentialProfile{User: "a", Password: "b\x00c"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := defaultDiscoveries.get(dc, credentialProfile{User: "a\x00b", Password: "c"})
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("distinct delimiter-containing credentials reused one discovery resolver")
	}
}

func TestDiscoveryCacheKeyClassification(t *testing.T) {
	equal := []struct {
		name  string
		first string
		other string
	}{
		{
			name:  "service and resolver timing",
			first: SchemeEtcd + "://10.0.0.2:2379,10.0.0.1:2379/orders?refresh_interval=1s&empty_grace=2s",
			other: SchemeEtcd + "://10.0.0.1:2379,10.0.0.2:2379/payments?refresh_interval=3s&empty_grace=4s",
		},
		{
			name:  "consul default weight",
			first: SchemeConsul + "://127.0.0.1:8500/orders?default_weight=7",
			other: SchemeConsul + "://127.0.0.1:8500/payments?default_weight=9",
		},
		{
			name:  "nacos default weight",
			first: SchemeNacos + "://127.0.0.1:8848/orders?default_weight=7",
			other: SchemeNacos + "://127.0.0.1:8848/payments?default_weight=9",
		},
	}
	for _, tc := range equal {
		t.Run("equal/"+tc.name, func(t *testing.T) {
			first := mustConfig(t, tc.first).discoveryCacheKey()
			other := mustConfig(t, tc.other).discoveryCacheKey()
			if first != other {
				t.Fatalf("discovery keys differ:\n%s\n%s", first, other)
			}
		})
	}

	different := []struct {
		name  string
		first string
		other string
	}{
		{"etcd dial timeout", SchemeEtcd + "://127.0.0.1:2379/svc?dial_timeout=1s", SchemeEtcd + "://127.0.0.1:2379/svc?dial_timeout=2s"},
		{"etcd prefix", SchemeEtcd + "://127.0.0.1:2379/svc?prefix=a", SchemeEtcd + "://127.0.0.1:2379/svc?prefix=b"},
		{"etcd default weight", SchemeEtcd + "://127.0.0.1:2379/svc?default_weight=7", SchemeEtcd + "://127.0.0.1:2379/svc?default_weight=9"},
		{"consul scheme", SchemeConsul + "://127.0.0.1:8500/svc?scheme=http", SchemeConsul + "://127.0.0.1:8500/svc?scheme=https"},
		{"consul datacenter", SchemeConsul + "://127.0.0.1:8500/svc?datacenter=dc1", SchemeConsul + "://127.0.0.1:8500/svc?datacenter=dc2"},
		{"nacos namespace", SchemeNacos + "://127.0.0.1:8848/svc?namespace=ns1", SchemeNacos + "://127.0.0.1:8848/svc?namespace=ns2"},
		{"nacos group", SchemeNacos + "://127.0.0.1:8848/svc?group=g1", SchemeNacos + "://127.0.0.1:8848/svc?group=g2"},
		{"nacos cluster", SchemeNacos + "://127.0.0.1:8848/svc?cluster=c1", SchemeNacos + "://127.0.0.1:8848/svc?cluster=c2"},
	}
	for _, tc := range different {
		t.Run("different/"+tc.name, func(t *testing.T) {
			first := mustConfig(t, tc.first).discoveryCacheKey()
			other := mustConfig(t, tc.other).discoveryCacheKey()
			if first == other {
				t.Fatalf("discovery keys unexpectedly match: %s", first)
			}
		})
	}
}

func TestRegistrationCacheKeyClassification(t *testing.T) {
	first := mustConfig(t, SchemeEtcd+"://127.0.0.1:2379/svc?default_weight=7&refresh_interval=1s&empty_grace=2s")
	sameIdentity := mustConfig(t, SchemeEtcd+"://127.0.0.1:2379/svc?default_weight=9&refresh_interval=3s&empty_grace=4s")
	if first.registrationCacheKey() != sameIdentity.registrationCacheKey() {
		t.Fatal("per-call weight and resolver timing partitioned registration cache")
	}

	reorderedEndpoints := mustConfig(t, SchemeEtcd+"://10.0.0.2:2379,10.0.0.1:2379/svc")
	orderedEndpoints := mustConfig(t, SchemeEtcd+"://10.0.0.1:2379,10.0.0.2:2379/svc")
	if reorderedEndpoints.registrationCacheKey() != orderedEndpoints.registrationCacheKey() {
		t.Fatal("endpoint order partitioned registration cache")
	}

	// The final query segment has its own delimiter, and url.Values.Encode
	// escapes delimiters inside query data. A delimiter in the service cannot
	// shift the service/query boundary.
	delimiterService := mustConfig(t, SchemeEtcd+"://127.0.0.1:2379/svc%7Cprefix=a")
	prefixQuery := mustConfig(t, SchemeEtcd+"://127.0.0.1:2379/svc?prefix=a")
	if delimiterService.registrationCacheKey() == prefixQuery.registrationCacheKey() {
		t.Fatal("service delimiter collided with registration query identity")
	}

	otherService := mustConfig(t, SchemeEtcd+"://127.0.0.1:2379/other")
	if first.registrationCacheKey() == otherService.registrationCacheKey() {
		t.Fatal("different services reused one registration identity")
	}

	cases := []struct {
		name  string
		first string
		other string
	}{
		{"etcd dial timeout", SchemeEtcd + "://127.0.0.1:2379/svc?dial_timeout=1s", SchemeEtcd + "://127.0.0.1:2379/svc?dial_timeout=2s"},
		{"etcd prefix", SchemeEtcd + "://127.0.0.1:2379/svc?prefix=a", SchemeEtcd + "://127.0.0.1:2379/svc?prefix=b"},
		{"consul scheme", SchemeConsul + "://127.0.0.1:8500/svc?scheme=http", SchemeConsul + "://127.0.0.1:8500/svc?scheme=https"},
		{"consul datacenter", SchemeConsul + "://127.0.0.1:8500/svc?datacenter=dc1", SchemeConsul + "://127.0.0.1:8500/svc?datacenter=dc2"},
		{"nacos namespace", SchemeNacos + "://127.0.0.1:8848/svc?namespace=ns1", SchemeNacos + "://127.0.0.1:8848/svc?namespace=ns2"},
		{"nacos group", SchemeNacos + "://127.0.0.1:8848/svc?group=g1", SchemeNacos + "://127.0.0.1:8848/svc?group=g2"},
		{"nacos cluster", SchemeNacos + "://127.0.0.1:8848/svc?cluster=c1", SchemeNacos + "://127.0.0.1:8848/svc?cluster=c2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			first := mustConfig(t, tc.first).registrationCacheKey()
			other := mustConfig(t, tc.other).registrationCacheKey()
			if first == other {
				t.Fatalf("registration keys unexpectedly match: %s", first)
			}
		})
	}
}

func TestDiscoveryCacheReusesEffectiveConfiguration(t *testing.T) {
	resetRegistrationState(t)
	var discoveryCalls atomic.Int32
	var registrationCalls atomic.Int32
	installFakeFactories(t, SchemeEtcd,
		func(*discoveryConfig, credentialProfile) (discovery.Resolver, error) {
			discoveryCalls.Add(1)
			return multiServiceDiscovery{}, nil
		},
		func(*discoveryConfig, credentialProfile) (*registrationProvider, error) {
			registrationCalls.Add(1)
			return &registrationProvider{registry: NewRegistry()}, nil
		},
	)

	builder := &resolverBuilder{scheme: SchemeEtcd}
	for _, raw := range []string{
		SchemeEtcd + "://127.0.0.1:2379/orders?refresh_interval=1s&empty_grace=2s",
		SchemeEtcd + "://127.0.0.1:2379/payments?refresh_interval=3s&empty_grace=4s",
	} {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		built, err := builder.Build(resolver.Target{URL: *u}, NewClientConn(), resolver.BuildOptions{})
		if err != nil {
			t.Fatalf("Build(%q): %v", raw, err)
		}
		built.Close()
	}
	if got := discoveryCalls.Load(); got != 1 {
		t.Fatalf("discovery constructed %d times, want 1", got)
	}
	if got := registrationCalls.Load(); got != 0 {
		t.Fatalf("discovery lookup constructed registration %d times", got)
	}
}

func TestDiscoveryCacheSeparatesCredentials(t *testing.T) {
	resetRegistrationState(t)
	installFakeFactories(t, SchemeEtcd,
		func(dc *discoveryConfig, _ credentialProfile) (discovery.Resolver, error) {
			return NewDiscovery(dc.scheme, dc.service), nil
		},
		func(*discoveryConfig, credentialProfile) (*registrationProvider, error) {
			return &registrationProvider{registry: NewRegistry()}, nil
		},
	)
	dc := mustConfig(t, SchemeEtcd+"://127.0.0.1:2379/svc")
	first, err := defaultDiscoveries.get(dc, credentialProfile{User: "dtm", Password: "first"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := defaultDiscoveries.get(dc, credentialProfile{User: "dtm", Password: "second"})
	if err != nil {
		t.Fatal(err)
	}
	again, err := defaultDiscoveries.get(dc, credentialProfile{User: "dtm", Password: "first"})
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("different credentials reused one discovery resolver")
	}
	if first != again {
		t.Fatal("identical credentials did not reuse discovery resolver")
	}
}

func TestRegistrationCacheSeparatesCredentials(t *testing.T) {
	resetRegistrationState(t)
	installFakeFactories(t, SchemeEtcd,
		func(dc *discoveryConfig, _ credentialProfile) (discovery.Resolver, error) {
			return NewDiscovery(dc.scheme, dc.service), nil
		},
		func(*discoveryConfig, credentialProfile) (*registrationProvider, error) {
			return &registrationProvider{registry: NewRegistry()}, nil
		},
	)
	dc := mustConfig(t, SchemeEtcd+"://127.0.0.1:2379/svc")
	first, err := defaultRegistrations.get(dc, credentialProfile{User: "dtm", Password: "first"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := defaultRegistrations.get(dc, credentialProfile{User: "dtm", Password: "second"})
	if err != nil {
		t.Fatal(err)
	}
	again, err := defaultRegistrations.get(dc, credentialProfile{User: "dtm", Password: "first"})
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("different credentials reused one registration provider")
	}
	if first != again {
		t.Fatal("identical credentials did not reuse registration provider")
	}
}

func TestRegistrationCacheIsolatesServicesAndSkipsDiscovery(t *testing.T) {
	resetRegistrationState(t)
	var discoveryCalls atomic.Int32
	var registrationCalls atomic.Int32
	installFakeFactories(t, SchemeEtcd,
		func(dc *discoveryConfig, _ credentialProfile) (discovery.Resolver, error) {
			discoveryCalls.Add(1)
			return NewDiscovery(dc.scheme, dc.service), nil
		},
		func(*discoveryConfig, credentialProfile) (*registrationProvider, error) {
			registrationCalls.Add(1)
			return &registrationProvider{registry: NewRegistry()}, nil
		},
	)

	configs := []*discoveryConfig{
		mustConfig(t, SchemeEtcd+"://127.0.0.1:2379/orders?default_weight=7"),
		mustConfig(t, SchemeEtcd+"://127.0.0.1:2379/orders?default_weight=9"),
		mustConfig(t, SchemeEtcd+"://127.0.0.1:2379/payments"),
	}
	for _, dc := range configs {
		if err := registerService(dc, credentialProfile{}, "127.0.0.1:8080"); err != nil {
			t.Fatalf("registerService(%q): %v", dc.service, err)
		}
	}
	if got := registrationCalls.Load(); got != 2 {
		t.Fatalf("registration constructed %d times, want 2", got)
	}
	if got := discoveryCalls.Load(); got != 0 {
		t.Fatalf("registration lookup constructed discovery %d times", got)
	}
}

// TestBuildSchemeMismatch checks that a builder rejects a target whose scheme
// differs from the scheme it was registered for.
func TestBuildSchemeMismatch(t *testing.T) {
	reg := NewRegistry()
	installFakeProvider(t, SchemeConsul, reg, nil)
	u, err := url.Parse("kitex-etcd://127.0.0.1:2379/svc")
	if err != nil {
		t.Fatal(err)
	}
	b := &resolverBuilder{scheme: SchemeConsul}
	cc := NewClientConn()
	if _, err := b.Build(resolver.Target{URL: *u}, cc, resolver.BuildOptions{}); err == nil {
		t.Fatal("Build with mismatched scheme must fail")
	}
}
