package dtmkitex

import (
	"net/url"
	"testing"
	"time"

	. "github.com/dtm-labs/dtmdriver-kitex/internal/testkit"
	"google.golang.org/grpc/resolver"
)

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
