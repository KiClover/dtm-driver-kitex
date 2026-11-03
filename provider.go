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

type registrationProvider struct {
	registry   registry.Registry
	pinAddress func(net.Addr) error
	registerMu sync.Mutex
}

type discoveryCache struct {
	mu        sync.Mutex
	resolvers map[string]discovery.Resolver
}

type registrationCache struct {
	mu        sync.Mutex
	providers map[string]*registrationProvider
}

var defaultDiscoveries = &discoveryCache{resolvers: map[string]discovery.Resolver{}}
var defaultRegistrations = &registrationCache{providers: map[string]*registrationProvider{}}

type discoveryFactory func(*discoveryConfig, credentialProfile) (discovery.Resolver, error)
type registrationFactory func(*discoveryConfig, credentialProfile) (*registrationProvider, error)

var discoveryFactories = map[string]discoveryFactory{}
var registrationFactories = map[string]registrationFactory{}

func registerProviderFactories(scheme string, discovery discoveryFactory, registration registrationFactory) {
	discoveryFactories[scheme] = discovery
	registrationFactories[scheme] = registration
}

func (c *discoveryCache) get(dc *discoveryConfig, creds credentialProfile) (discovery.Resolver, error) {
	key := dc.discoveryCacheKey() + "|credentials:" + creds.cacheKey()
	c.mu.Lock()
	defer c.mu.Unlock()
	if resolver := c.resolvers[key]; resolver != nil {
		return resolver, nil
	}
	factory := discoveryFactories[dc.scheme]
	if factory == nil {
		return nil, fmt.Errorf("dtm-driver-kitex: no provider registered for scheme %q", dc.scheme)
	}
	resolver, err := factory(dc, creds)
	if err != nil {
		return nil, err
	}
	if resolver == nil {
		return nil, fmt.Errorf("dtm-driver-kitex: provider %q returned nil discovery", dc.scheme)
	}
	c.resolvers[key] = resolver
	return resolver, nil
}

func (c *registrationCache) get(dc *discoveryConfig, creds credentialProfile) (*registrationProvider, error) {
	key := dc.registrationCacheKey() + "|credentials:" + creds.cacheKey()
	c.mu.Lock()
	defer c.mu.Unlock()
	if provider := c.providers[key]; provider != nil {
		return provider, nil
	}
	factory := registrationFactories[dc.scheme]
	if factory == nil {
		return nil, fmt.Errorf("dtm-driver-kitex: no provider registered for scheme %q", dc.scheme)
	}
	provider, err := factory(dc, creds)
	if err != nil {
		return nil, err
	}
	if provider == nil || provider.registry == nil {
		return nil, fmt.Errorf("dtm-driver-kitex: provider %q does not support registration", dc.scheme)
	}
	c.providers[key] = provider
	return provider, nil
}

func (b *resolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	if target.URL.Scheme != b.scheme {
		return nil, fmt.Errorf("dtm-driver-kitex: builder scheme %q cannot resolve %q", b.scheme, target.URL.Scheme)
	}
	raw := registryRawTarget(&target.URL)
	rt, err := parseRegistrationTarget(raw)
	if err != nil {
		return nil, err
	}
	dc := rt.discovery
	discoveryResolver, err := defaultDiscoveries.get(dc, inlineCredentials(dc))
	if err != nil {
		return nil, err
	}
	desc := discoveryResolver.Target(context.Background(), rpcinfo.NewEndpointInfo(dc.service, "", nil, nil))
	if strings.TrimSpace(desc) == "" {
		return nil, fmt.Errorf("dtm-driver-kitex: provider %q returned empty discovery description", b.scheme)
	}
	return newKitexResolver(cc, discoveryResolver, desc, dc.refreshInterval(), dc.emptyGrace(), realClock{}), nil
}

func registryRawTarget(u *url.URL) string {
	var b strings.Builder
	b.WriteString(u.Scheme)
	b.WriteString("://")
	b.WriteString(u.Host)
	b.WriteString(u.EscapedPath())
	if u.RawQuery != "" {
		b.WriteString("?")
		b.WriteString(u.RawQuery)
	}
	return b.String()
}

func registerService(dc *discoveryConfig, creds credentialProfile, endpoint string) error {
	provider, err := defaultRegistrations.get(dc, creds)
	if err != nil {
		return err
	}
	info, err := newRegistrationInfo(dc.service, endpoint, dc.weight(), nil)
	if err != nil {
		return err
	}
	provider.registerMu.Lock()
	defer provider.registerMu.Unlock()
	if provider.pinAddress != nil {
		if err := provider.pinAddress(info.Addr); err != nil {
			return fmt.Errorf("dtm-driver-kitex: pin registration address: %w", err)
		}
	}
	if err := provider.registry.Register(&info); err != nil {
		return fmt.Errorf("dtm-driver-kitex: register service: %w", err)
	}
	return nil
}
