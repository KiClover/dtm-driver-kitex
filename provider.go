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

type provider struct {
	discovery  discovery.Resolver
	registry   registry.Registry
	pinAddress func(net.Addr) error
	registerMu sync.Mutex
}

type providerCache struct {
	mu        sync.Mutex
	providers map[string]*provider
}

var defaultProviders = &providerCache{providers: map[string]*provider{}}

func (c *providerCache) get(dc *discoveryConfig, creds credentialProfile) (*provider, error) {
	key := dc.canonicalKey() + "|credentials:" + creds.cacheKey()
	c.mu.Lock()
	defer c.mu.Unlock()
	if p := c.providers[key]; p != nil {
		return p, nil
	}
	factory := providerFactories[dc.scheme]
	if factory == nil {
		return nil, fmt.Errorf("dtm-driver-kitex: no provider registered for scheme %q", dc.scheme)
	}
	p, err := factory(dc, creds)
	if err != nil {
		return nil, err
	}
	if p == nil || p.discovery == nil {
		return nil, fmt.Errorf("dtm-driver-kitex: provider %q returned nil discovery", dc.scheme)
	}
	c.providers[key] = p
	return p, nil
}

type providerFactory func(*discoveryConfig, credentialProfile) (*provider, error)

var providerFactories = map[string]providerFactory{}

func registerProviderFactory(scheme string, factory providerFactory) {
	providerFactories[scheme] = factory
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
	p, err := defaultProviders.get(dc, inlineCredentials(dc))
	if err != nil {
		return nil, err
	}
	desc := p.discovery.Target(context.Background(), rpcinfo.NewEndpointInfo(dc.service, "", nil, nil))
	if strings.TrimSpace(desc) == "" {
		return nil, fmt.Errorf("dtm-driver-kitex: provider %q returned empty discovery description", b.scheme)
	}
	return newKitexResolver(cc, p.discovery, desc, dc.refreshInterval(), dc.emptyGrace(), realClock{}), nil
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
	p, err := defaultProviders.get(dc, creds)
	if err != nil {
		return err
	}
	if p.registry == nil {
		return fmt.Errorf("dtm-driver-kitex: provider %q does not support registration", dc.scheme)
	}
	info, err := newRegistrationInfo(dc.service, endpoint, dc.weight(), nil)
	if err != nil {
		return err
	}
	p.registerMu.Lock()
	defer p.registerMu.Unlock()
	if p.pinAddress != nil {
		if err := p.pinAddress(info.Addr); err != nil {
			return fmt.Errorf("dtm-driver-kitex: pin registration address: %w", err)
		}
	}
	if err := p.registry.Register(&info); err != nil {
		return fmt.Errorf("dtm-driver-kitex: register service: %w", err)
	}
	return nil
}
