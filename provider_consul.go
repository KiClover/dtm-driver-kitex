package dtmkitex

import (
	"fmt"

	"github.com/cloudwego/kitex/pkg/discovery"
	consulapi "github.com/hashicorp/consul/api"
	consul "github.com/kitex-contrib/registry-consul"
)

// This file maps the normalized discoveryConfig plus resolved credentials
// onto registry-consul v0.2.0 objects. Discovery and registration construct
// independent clients so resolving services never creates registration state.

func newConsulConfig(dc *discoveryConfig, creds credentialProfile) (*consulapi.Config, error) {
	cfg := consulapi.DefaultConfig()
	cfg.Address = dc.endpoints[0]

	if scheme := dc.query.Get("scheme"); scheme != "" {
		if scheme != "http" && scheme != "https" {
			return nil, fmt.Errorf("dtm-driver-kitex: query parameter \"scheme\" must be http or https")
		}
		cfg.Scheme = scheme
	}
	if datacenter := dc.query.Get("datacenter"); datacenter != "" {
		cfg.Datacenter = datacenter
	}
	if creds.Token != "" {
		cfg.Token = creds.Token
	}
	return cfg, nil
}

func newConsulDiscovery(dc *discoveryConfig, creds credentialProfile) (discovery.Resolver, error) {
	cfg, err := newConsulConfig(dc, creds)
	if err != nil {
		return nil, err
	}
	resolver, err := consul.NewConsulResolverWithConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("dtm-driver-kitex: create consul resolver: %w", err)
	}
	return resolver, nil
}

func newConsulRegistration(dc *discoveryConfig, creds credentialProfile) (*registrationProvider, error) {
	cfg, err := newConsulConfig(dc, creds)
	if err != nil {
		return nil, err
	}
	// Default options retain the agent-side TCP check. The TTL heartbeat in
	// registry-consul v0.2.0 uses an unpopulated CheckID and is not enabled.
	registry, err := consul.NewConsulRegisterWithConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("dtm-driver-kitex: create consul registry: %w", err)
	}
	return &registrationProvider{registry: registry}, nil
}

func init() {
	registerProviderFactories(SchemeConsul, newConsulDiscovery, newConsulRegistration)
}
