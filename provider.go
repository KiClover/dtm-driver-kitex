package dtmkitex

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/cloudwego/kitex/pkg/discovery"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/registry"
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

// Build adapts the Kitex discovery backend behind target into grpc-go state
// updates. Parsing failures here are deterministic, so Build fails fast; a
// registry that is merely down is handled by the resolver's retry loop.
func (b *resolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	raw := target.Endpoint()
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

// register publishes a Kitex service instance for one DTM server. Task 7.
func registerService(dc *discoveryConfig, creds credentialProfile, endpoint string) error {
	p, err := defaultProviders.get(dc, creds)
	if err != nil {
		return err
	}
	_ = p
	return fmt.Errorf("dtm-driver-kitex: registration not implemented")
}

// joinEndpoints joins the normalized backend endpoints for provider files
// building backend client endpoint strings.
func joinEndpoints(dc *discoveryConfig) string {
	return strings.Join(dc.endpoints, ",")
}
