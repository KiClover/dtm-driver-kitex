package dtmkitex

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"

	"github.com/cloudwego/kitex/pkg/discovery"
	"github.com/cloudwego/kitex/pkg/registry"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"google.golang.org/grpc/resolver"
)

// provider owns all Kitex client objects for one (backend, credential)
// combination: a discovery.Resolver for lookups and a registry.Registry for
// registrations. Providers are cached by canonicalKey so DTM and business
// targets sharing a backend share one underlying client.
type provider struct {
	name      string
	discovery discovery.Resolver
	registry  registry.Registry
	closeFn   func(context.Context) error
	// pinAddress fixes the address used at registration time so backend
	// libraries cannot override it from the environment. Backends whose
	// registry needs a pre-set fixed address implement it; nil means the
	// registry honors registry.Info.Addr as-is.
	pinAddress func(net.Addr) error
}

// providerCache memoizes providers per backend scheme. Shutdown clears it and
// closes every owned client.
type providerCache struct {
	mu        sync.Mutex
	providers map[string]*provider
}

var defaultProviders = &providerCache{providers: map[string]*provider{}}

// get returns the cached provider for cfg or creates one through the scheme's
// registered provider factory. Creation errors are not cached.
func (c *providerCache) get(dc *discoveryConfig, creds credentialProfile) (*provider, error) {
	key := dc.canonicalKey()
	c.mu.Lock()
	defer c.mu.Unlock()
	if p, ok := c.providers[key]; ok {
		return p, nil
	}
	factory, ok := providerFactories[dc.scheme]
	if !ok {
		return nil, fmt.Errorf("dtm-driver-kitex: no provider registered for scheme %q", dc.scheme)
	}
	p, err := factory(dc, creds)
	if err != nil {
		return nil, err
	}
	c.providers[key] = p
	return p, nil
}

// closeAll closes every cached provider and empties the cache.
func (c *providerCache) closeAll(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var firstErr error
	for key, p := range c.providers {
		if p.closeFn != nil {
			if err := p.closeFn(ctx); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		delete(c.providers, key)
	}
	return firstErr
}

// providerFactory builds a provider from normalized config plus resolved
// credentials. Registered per scheme in the backend-specific files.
type providerFactory func(dc *discoveryConfig, creds credentialProfile) (*provider, error)

// providerFactories maps each kitex-<backend> scheme to its factory. Task 7
// fills etcd, Task 8 consul, Task 9 nacos.
var providerFactories = map[string]providerFactory{}

// registerProviderFactory installs a scheme factory; tests may also use it.
func registerProviderFactory(scheme string, f providerFactory) {
	providerFactories[scheme] = f
}

// registryRawTarget reconstructs the registry URI from the parsed dial
// target. grpc-go hands the builder Target.URL (scheme, host, path, query);
// Endpoint() alone would drop the backend endpoints (URL.Host) and every
// query option (URL.RawQuery), so the URI is rebuilt here.
func registryRawTarget(u *url.URL) string {
	raw := u.Scheme + "://" + u.Host + u.Path
	if u.RawQuery != "" {
		raw += "?" + u.RawQuery
	}
	return raw
}

// Build adapts the Kitex discovery backend behind target into grpc-go state
// updates. Parsing failures here are deterministic, so Build fails fast; a
// registry that is merely down is handled by the resolver's retry loop.
func (b *resolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	raw := registryRawTarget(&target.URL)
	if raw == "" {
		return nil, fmt.Errorf("dtm-driver-kitex: resolver target %q has no endpoint", &target.URL)
	}
	dc, _, err := parseRegistryTarget(raw, false)
	if err != nil {
		return nil, err
	}
	if b.scheme != "" && dc.scheme != b.scheme {
		return nil, fmt.Errorf("dtm-driver-kitex: target scheme %q does not match builder %q", dc.scheme, b.scheme)
	}
	creds, err := defaultConfig.resolveCredentials(dc)
	if err != nil {
		return nil, err
	}
	p, err := defaultProviders.get(dc, creds)
	if err != nil {
		return nil, err
	}

	// Kitex resolvers derive their description from an EndpointInfo; we feed
	// a synthetic one carrying only the discovery service name.
	epi := rpcinfo.NewEndpointInfo(dc.service, "", nil, nil)
	desc := p.discovery.Target(context.Background(), epi)

	return newKitexResolver(cc, p.discovery, desc, dc.refreshInterval(), dc.emptyGrace(), realClock{}), nil
}

// registerMu serializes the pin-then-register sequence on shared providers:
// backend registries keep one mutable fixed address, so concurrent
// registrations of different endpoints could interleave pin and Register.
var registerMu sync.Mutex

// register publishes a Kitex service instance for one DTM server. Repeated
// calls with the same target and endpoint are idempotent; the same
// service+endpoint under a different canonical config is a conflict.
func registerService(dc *discoveryConfig, creds credentialProfile, endpoint string) error {
	info, err := newRegistrationInfo(dc.service, endpoint, dc.weight(), nil)
	if err != nil {
		return err
	}
	key := registrationKey{
		Scheme:   dc.scheme,
		Service:  dc.service,
		Endpoint: endpoint,
		ConfigID: dc.canonicalKey(),
	}
	if _, ok := defaultRegistrations.lookup(key); ok {
		return nil
	}
	p, err := defaultProviders.get(dc, creds)
	if err != nil {
		return err
	}
	registerMu.Lock()
	defer registerMu.Unlock()
	if p.pinAddress != nil {
		if err := p.pinAddress(info.Addr); err != nil {
			return fmt.Errorf("dtm-driver-kitex: pin registration address: %w", err)
		}
	}
	if err := p.registry.Register(&info); err != nil {
		return fmt.Errorf("dtm-driver-kitex: register %s at %s: %w", dc.service, endpoint, err)
	}
	defaultRegistrations.add(key, entry{reg: p.registry, info: info})
	return nil
}

// joinEndpoints joins the normalized backend endpoints for provider files
// building backend client endpoint strings.
func joinEndpoints(dc *discoveryConfig) string {
	return strings.Join(dc.endpoints, ",")
}
