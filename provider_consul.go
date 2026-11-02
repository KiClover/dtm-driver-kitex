package dtmkitex

import (
	"fmt"

	consulapi "github.com/hashicorp/consul/api"
	consul "github.com/kitex-contrib/registry-consul"
)

// This file maps the normalized discoveryConfig plus resolved credentials
// onto registry-consul v0.2.0 objects.
//
// Design notes:
//   - registry-consul's NewConsulRegister/NewConsulResolver only take an
//     address, so both sides are built through the *WithConfig variants which
//     share one api.Config (same scheme/datacenter/token on register+resolve).
//   - info.Addr is honored as-is (no pinAddress): consul's Register parses
//     host and port from info.Addr directly.
//   - The upstream TTL heartbeat is broken in registry-consul v0.2.0:
//     startTTLHeartbeat passes c.opts.check.CheckID to Agent.UpdateTTL, and
//     that field is never populated, so every heartbeat 404s and the check
//     goes critical. The default agent-side TCP check is used instead; it
//     needs no client participation and DTM servers listen before they
//     register.

// newConsulConfig builds the shared api.Config for both registry and resolver.
func newConsulConfig(dc *discoveryConfig, creds credentialProfile) (*consulapi.Config, error) {
	cfg := consulapi.DefaultConfig()
	cfg.Address = dc.endpoints[0]

	if s := dc.query.Get("scheme"); s != "" {
		if s != "http" && s != "https" {
			return nil, fmt.Errorf("dtm-driver-kitex: query parameter \"scheme\" must be http or https")
		}
		cfg.Scheme = s
	}
	if d := dc.query.Get("datacenter"); d != "" {
		cfg.Datacenter = d
	}
	if creds.Token != "" {
		cfg.Token = creds.Token
	}
	return cfg, nil
}

// newConsulProvider builds the Consul discovery resolver and registration
// registry sharing one api.Config.
func newConsulProvider(dc *discoveryConfig, creds credentialProfile) (*provider, error) {
	cfg, err := newConsulConfig(dc, creds)
	if err != nil {
		return nil, err
	}
	disc, err := consul.NewConsulResolverWithConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("dtm-driver-kitex: create consul resolver: %w", err)
	}
	// Default options keep the agent-side TCP check (5s interval). Passing a
	// TTL check would opt into the broken upstream heartbeat (see above).
	reg, err := consul.NewConsulRegisterWithConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("dtm-driver-kitex: create consul registry: %w", err)
	}
	return &provider{
		discovery: disc,
		registry:  reg,
	}, nil
}

func init() {
	registerProviderFactory(SchemeConsul, newConsulProvider)
}
